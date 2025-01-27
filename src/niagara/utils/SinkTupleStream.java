package niagara.utils;

import java.util.ArrayList;

import niagara.connection_server.NiagraServer;

import org.w3c.dom.Node;

/**
 * 
 * TupleStream provides a tuple-oriented stream interface for operators to use.
 * Underneath, the tuples are batched into pages and the pages are passed to the
 * next operator using a page stream. This gives operators a tuple-oriented
 * view, like they want yet avoids element-by-element synchronization. This
 * provides a sink stream to be written to - a similar class provides a source
 * stream.
 * 
 * @version 1.0
 * 
 */
@SuppressWarnings("unchecked")
public final class SinkTupleStream {
	private PageStream pageStream;
	private TuplePage buffer;

	private int status;

	// if sendImmediate is true, tuples are not buffered,
	// page is sent immediately
	private boolean sendImmediate = false;

	// possible statuses for sink streams
	public static final int Open = 0;
	public static final int Closed = 1; // closed indicates EOS

	// should we propagate get_partial requests to the operator or
	// just reflect them upwards - does not work to handle this in
	// Physical Operator - ask me and I'll explain KT
	boolean reflectPartial;

	/**
	 * Constructor
	 */
	public SinkTupleStream(PageStream stream) {
		this(stream, false); // default is not to reflect partial
	}

	/**
	 * Constructor
	 */
	public SinkTupleStream(PageStream stream, boolean reflectPartial) {
		this.pageStream = stream;
		this.buffer = new TuplePage();
		this.reflectPartial = reflectPartial;
		status = Open;
	}

	public void setSendImmediate() {
		sendImmediate = true;
	}

	public boolean isSendImmediate() {
		return sendImmediate;
	}

	/**
	 * This function closes a stream so that no further upward or downward
	 * communication (other than get) is possible. This function is non-
	 * blocking.
	 * 
	 */
	public void endOfStream() throws InterruptedException, ShutdownException {
		// pageStream sends an EOS up stream and sets an isClosed flag
		status = Closed;
		ArrayList ctrl = putCtrlMsg(ControlFlag.EOS, "End of Stream");
		
		// Ignore any remaining messages, i.e., keep putting an EOS.
		while (ctrl != null) {
			ctrl = putCtrlMsg(ControlFlag.EOS, "End of Stream");
		}
		pageStream.endOfStream();

	}

	/**
	 * This function returns a control element put down stream, if any exists.
	 * Otherwise, it returns null. This function is non-blocking. This function
	 * allows physOperator to check for control messages from its sinks after
	 * reading tuples from its sources
	 * 
	 * @return Array list consisting of control flag of first control element
	 *         downstream and ctrl msg; NULLFLAG if there is no such element
	 */
	// 
	public ArrayList getCtrlMsg(int timeout) throws ShutdownException,
			InterruptedException {
		// dont check eos or shutdown, pageStream will do it

		ArrayList ctrl;
		ctrl = pageStream.getCtrlMsgFromSink(timeout);

		if (ctrl == null)
			return null;
		else
			return processCtrlFlag(ctrl);

	}

	/**
	 * Puts a stream tuple element to the sink (in the up stream buffer.) This
	 * is successful only is no control information was previously received from
	 * the sink (in the down stream buffer). If there is control information, an
	 * appropriate control flag is returned This function blocks until either
	 * the output element can be put in the up stream buffer or a control
	 * element is read from the down stream buffer.
	 * 
	 * @param tuple
	 *            The tuple to be passed to the sink (upstream buffer)
	 * 
	 * @return CtrlFlags.NULLFLAG if successful, control flag otherwise
	 * 
	 * @exception java.lang.InterruptedException
	 *                The thread is interrupted in the middle of execution.
	 */

	public ArrayList putTuple(Tuple tuple) throws InterruptedException,
			ShutdownException {
		// try to put the tuple in the buffer, if the buffer is full
		// flush it - leave an empty buffer for next call

		// check eos and shutdown here - catch these more quickly this way
		assert status != Closed : "KT writing after end of stream";
		if (pageStream.shutdownReceived()) {
			throw new ShutdownException(pageStream.getShutdownMsg());
		}

		if (buffer.isFull() || sendImmediate || tuple.isPunctuation()) {
			ArrayList fb;
			fb = flushBuffer();
			if (fb != null) {
				return fb;
			} else {
				buffer.put(tuple);
			}
			return fb;
		} else {
			buffer.put(tuple);
			return null;
		}
	}

	public void putTupleNoCtrlMsg(Tuple tuple) throws InterruptedException,
			ShutdownException {
		// try to put the tuple in the buffer, if the buffer is full
		// flush it - leave an empty buffer for next call

		ArrayList ret = putTuple(tuple);
		assert ret == null : "KT unexpected control message "
				+ ((ControlFlag) ret.get(0)).flagName();
		return;
	}

	/**
	 * put a dom node (typically a document) into the stream - this is for the
	 * use of StreamScan, DTDScan, etc.
	 */
	public void put(Node node) throws InterruptedException, ShutdownException {

		Tuple tuple = null;

		// Let's see if this is a punctuation or not
		Node child = node.getFirstChild();
		if (child != null) {
			// String name = child.getNodeName();
			String namespace = child.getNamespaceURI();
			if (namespace != null
					&& namespace
							.equals("http://www.cse.ogi.edu/dot/niagara/punct")) {
				tuple = new Punctuation(false, 1);
			}
		}

		if (tuple == null)
			// create a tuple that is not a partial result
			tuple = new Tuple(false, 1);

		// Add the object as an attribute of the tuple
		tuple.appendAttribute(node);
		ArrayList ret = putTuple(tuple);

		// stream above an operator using this put should
		// always reflect partials
		assert (ret == null)
				|| ((ControlFlag) ret.get(0) != ControlFlag.GET_PARTIAL) : "KT unexpected get partial request";

		// handle a flush buffer request
		ret = processCtrlFlag(ret);

		// stream above an operator using this put should
		// always reflect partials
		assert ret == null : "KT Unexpected ctrl flag "
				+ ((ControlFlag) ret.get(0)).flagName();
	}

	/**
	 * This functions puts a control message in the sink buffer (upstream
	 * buffer). This function checks for control messages coming from the sink.
	 * If such a control message is found, or if eos is encountered, an
	 * appropriate control flag is returned This function blocks until either
	 * the output element can be put in the up stream buffer or a control
	 * element is read from the down stream buffer.
	 * 
	 * @param controlMsgId
	 *            they type of control message to be put in stream
	 * 
	 * @return CtrlFlags.NULLFLAG on success, control flag otherwise
	 */
	public ArrayList putCtrlMsg(ControlFlag controlMsgId, String ctrlMsg)
			throws java.lang.InterruptedException, ShutdownException {
		// KT control element put should cause partially full page to be
		// sent
		assert buffer.getFlag() == ControlFlag.NULLFLAG : "KT buffer already has a flag!";
		buffer.setFlag(controlMsgId);
		buffer.setCtrlMsg(ctrlMsg);
		ArrayList ctrl = flushBuffer();
		if (ctrl != null) {
			// put failed! better reset the buffer
			buffer.setFlag(ControlFlag.NULLFLAG);
		}
		return ctrl;
	}

	/**
	 * flushes the buffer to the page stream and gets a new empty buffer to fill
	 * up.
	 * 
	 * @return NULLFLAG on success, control flag if a control element was
	 *         encountered during buffer flushing only ctrl flag returned by
	 *         pageStream.putPageToSink is GET_PARTIAL or NULLFLAG
	 */
	public ArrayList flushBuffer() throws ShutdownException,
			InterruptedException {
		if (buffer.isEmpty() && buffer.getFlag() == ControlFlag.NULLFLAG ) {
			// don't flush an empty buffer...
			return null;
		} else {
			ArrayList ctrl = pageStream.putPageToSink(buffer);
			ArrayList ret = null;
			if (ctrl == null) {
				// success
				// get new empty buffer to write in
				buffer = pageStream.getTuplePage();
				buffer.startPutMode();
			} else {
				ret = processCtrlFlag(ctrl);
				
			}
			assert ret == null
					|| (ControlFlag) ret.get(0) == ControlFlag.GET_PARTIAL
					|| (ControlFlag) ret.get(0) == ControlFlag.CHANGE_QUERY
					|| (ControlFlag) ret.get(0) == ControlFlag.MESSAGE
					|| (ControlFlag) ret.get(0) == ControlFlag.READY_TO_FINISH : "KT Unexpected ctrl flag "
					+ ((ControlFlag) ret.get(0)).flagName();
			return ret;
		}
	}

	/**
	 * SinkTupleStream handles some control messages - this function makes sure
	 * the handling is uniform. SinkTupleStream handles some GET_PARTIALS and
	 * some REQ_BUF_FLUSHES
	 * 
	 * @param ctrlFlag
	 *            The control flag received
	 * 
	 * @returns The ctrl flag to be returned to PhysicalOperator. Should be
	 *          either NULLFLAG OR GET_PARTIAL
	 */
	private ArrayList processCtrlFlag(ArrayList ctrl) throws ShutdownException,
			InterruptedException {

		if (ctrl == null)
			return null;

		ControlFlag ctrlFlag = (ControlFlag) ctrl.get(0);
		switch (ctrlFlag) {
		case REQUEST_BUF_FLUSH:
			if (NiagraServer.DEBUG2)
				System.out.println(pageStream.getName()
						+ " received request for buffer flush ");
			return flushBuffer(); // returns NULLFLAG or GET_PARTIAL
		case GET_PARTIAL:
			if (reflectPartial) {
				reflectPartial(); // void
				return null;
			} else {
				ctrl.set(0, ControlFlag.GET_PARTIAL);
				ctrl.set(1, null);
				return ctrl;
			}
		case MESSAGE:
			return ctrl;
		case CHANGE_QUERY:
		case READY_TO_FINISH:
			return ctrl;

		case SHUTDOWN:
			assert pageStream.shutdownReceived() : "KT shutdown flag in queue but pagestream says shutdown not received";
			throw new ShutdownException(pageStream.getShutdownMsg());

		default:
			assert false : "KT Unexpected control flag " + ctrlFlag.flagName();
			return null;
		}
	}


	/**
	 * put a SYNC_PARTIAL into the sink stream (upstream). This function is
	 * called when a GET_PARTIAL is received downstream and the operator using
	 * this SinkTupleStream can not/should not handle GET_PARTIAL messages
	 * 
	 * calls putCtrlMsg (which calls flushBuffer) - this may recurse, but will
	 * always result in the buffer being sent
	 */
	private void reflectPartial() throws InterruptedException,
			ShutdownException {
		// set buffer flag to SYNCH_PARTIAL and try to flushBuffer
		// if flushBuffer fails, it will return GET_PARTIAL - unlikely
		// since I just got a GET_PARTIAL - can not just drop that GET_PARTIAL
		// because others might bounce it back

		// if reflectPartial is true - flushBuffer and therefore
		// putCtrlMsg will always return NULLFLAG
		ArrayList ctrl = putCtrlMsg(ControlFlag.SYNCH_PARTIAL, null);
		assert ctrl == null : "KT Unexpected ctrl flag "
				+ ((ControlFlag) ctrl.get(0)).flagName();
	}

	/**
	 * Return a string representation of this stream
	 * 
	 * @return the string representation of this stream
	 */
	public String toString() {
		String retStr = new String("\nTuple Buffer \n");
		retStr += buffer.toString();
		retStr += "\nPage Stream\n";
		retStr += pageStream.toString();
		retStr += "\nStatus \n" + status;
		return retStr;
	}

	public int getStatus() {
		return status;
	}

	public boolean isClosed() {
		return status == Closed;
	}

	public void setNotifyOnSink(MailboxFlag notifyMe) {
		pageStream.setNotifyOnSink(notifyMe);
	}
}
