package niagara.physical;

import java.util.ArrayList;
import java.util.Random;

import niagara.connection_server.NiagraServer;
import niagara.logical.Bucket;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalProperty;
import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.ControlFlag;
import niagara.utils.FeedbackPunctuation;
import niagara.utils.Guard;
import niagara.utils.Log;
import niagara.utils.LongAttr;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.StringAttr;
import niagara.utils.Tuple;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * Implementation of the Select operator.
 */
@SuppressWarnings("unchecked")
public class PhysicalBucket extends PhysicalOperator {
	// No blocking source streams
	private static final boolean[] blockingSourceStreams = { false };

	// window Attributes
	private Attribute windowAttribute;
	private int windowType;
	private long range;
	private long slide;
	private int count = 0;
	private String widName;
	private long windowId_from = 0;
	private long windowId_to = 0;
	private long punctWid = 0;
	private long prevPunctWid = -1;
	// private int windowCount = 0;
	private boolean propagate = false;
	//	 Exploit
	Boolean exploit = false;
	int[] positions;
	String[] names;
	
	//	 logging test
	int tupleOut = 0;
	int tupleDrop = 0;
	
	
	String guardOutput = "*";
	String fAttr = "";

	private Document doc;
	
	private boolean threadSleptOnce = false;
	private Random rn = new Random();

	// Data template for creating punctuation
	// private String rgstDataChild[] = null;
	// private short rgiDataType[];
	// private String stDataRoot;
	// Data template for creating punctuation
	Tuple tupleDataSample = null;
	private long startSecond;
	private long start;

	AtomicEvaluator ae;
	
	// Feedback
	private Guard outputGuard = new Guard();

	public PhysicalBucket() {
		setBlockingSourceStreams(blockingSourceStreams);
	}

	void processCtrlMsgFromSink(ArrayList ctrl, int streamId)
			throws java.lang.InterruptedException, ShutdownException {
		// downstream control message is GET_PARTIAL
		// We should not get SYNCH_PARTIAL, END_PARTIAL, EOS or NULLFLAG
		// REQ_BUF_FLUSH is handled inside SinkTupleStream
		// here (SHUTDOWN is handled with exceptions)

		if (ctrl == null)
			return;

		ControlFlag ctrlFlag = (ControlFlag) ctrl.get(0);

		switch (ctrlFlag) {
		case GET_PARTIAL:
			processGetPartialFromSink(streamId);
			break;
		case MESSAGE:
					
			FeedbackPunctuation fp = (FeedbackPunctuation) ctrl.get(2);
			
			if(logging){
		//		log.Update(fp.toString(), String.valueOf(tupleOut));
			}
			
			FeedbackPunctuation fpTrans = new FeedbackPunctuation(fp.Type(),fp.Variables(),fp.Comparators(),fp.Values());
			
			// get attribute positions from tuple to check against guards
			names = new String[fpTrans.Variables().size()];
			names = fpTrans.Variables().toArray(names);

			// get positions
			positions = new int[fpTrans.Variables().size()];
			for (int i = 0; i < names.length; i++) {
				positions[i] = outputTupleSchema.getPosition(names[i]);
			}
			
			
			if(exploit)
				outputGuard.add(fp);

			if (propagate) {
				translateForBucket(fpTrans);
				sendFeedbackPunctuation(fpTrans, streamId);
			}
						
			break;
		default:
			assert false : "KT unexpected control message from sink "
					+ ctrlFlag.flagName();
		}
	}

	protected void translateForBucket(FeedbackPunctuation fpTrans)
	{
		String corrTS="";
		long start_ts = startSecond; //Long.parseLong("634018212000000000");

		// need to generalize
		long ts = 0;
		int widPos = 0; 
		
		String bucketId = fpTrans.getValue(widPos);
		long bucketIdNum = Integer.parseInt(bucketId);
				
		ts = start_ts + bucketIdNum * range; 
		
		corrTS = String.valueOf(ts);
		
		fpTrans.setValue(0,corrTS);
		
		fpTrans.setName(0,windowAttribute.getName());
		
	}
	
	public void opInitFrom(LogicalOp logicalOperator) { // I think I should
		// change the logical
		// operator to groupOp
		// and related stuff
		// Get the averaging attribute of the average logical operator
		propagate = ((Bucket) logicalOperator).getPropagate();
		windowAttribute = ((Bucket) logicalOperator).getWindowAttr();
		windowType = ((Bucket) logicalOperator).getWindowType();
		range = ((Bucket) logicalOperator).getWindowRange();
		slide = ((Bucket) logicalOperator).getWindowSlide();
		widName = ((Bucket) logicalOperator).getWid();
		start = ((Bucket) logicalOperator).getStarttime();
		ae = new AtomicEvaluator(windowAttribute.getName());
		exploit = ((Bucket) logicalOperator).getExploit();
		fAttr = ((Bucket) logicalOperator).getFAttr();		
		
		logging = ((Bucket) logicalOperator).getLogging();
		if (logging) {
			log = new Log(this.getName());
		}		
	}

	public Op opCopy() {
		PhysicalBucket p = new PhysicalBucket();
		p.windowAttribute = windowAttribute;
		p.windowType = windowType;
		p.range = range;
		p.slide = slide;
		p.widName = widName;
		p.start = start;
		p.ae = ae;
		p.propagate = propagate;
		p.exploit = exploit;
		p.logging = logging;
		p.log = log;
		p.fAttr = fAttr;
		p.outputGuard = outputGuard.Copy();
		
		return p;
	}

	public void opInitialize() {
		setBlockingSourceStreams(blockingSourceStreams);
		ae.resolveVariables(inputTupleSchemas[0], 0);
	}

	/**
	 * This function processes a tuple element read from a source stream when
	 * the operator is non-blocking. This over-rides the corresponding function
	 * in the base class.
	 * 
	 * @param tupleElement
	 *            The tuple element read from a source stream
	 * @param streamId
	 *            The source stream from which the tuple was read
	 * 
	 * @exception ShutdownException
	 *                query shutdown by user or execution error
	 */

	protected void processTuple(Tuple inputTuple, int streamId)
			throws ShutdownException, InterruptedException {
		Tuple result;
		// If we haven't already picked up a template tuple,
		// copy this one.
		if (tupleDataSample == null)
			tupleDataSample = inputTuple;

		count++;
		result = appendWindowId(inputTuple, streamId);

			if (exploit) 
			{
				// get attribute positions from tuple to check against guards
//				int[] positions = new int[2];
//				String[] names = { "wid_from_bucket", "milepost" };
//	
//				for (int i = 0; i < names.length; i++) {
//					//positions[i] = inputTupleSchemas[0].getPosition(names[i]);
//					positions[i] = outputTupleSchema.getPosition(names[i]);
//				}
	
				// check against guards
				Boolean guardMatch = false;
				for (FeedbackPunctuation fp : outputGuard.elements()) {
					guardMatch = guardMatch
							|| fp
									.match(positions, result
											.getTuple());
				}
	
				if (!guardMatch) {
					putTuple(result, 0);
					tupleOut++;
				}
			} 
			else
			{
				putTuple(result, 0);
				tupleOut++;
			}
				
				//System.out.println(this.getName() + count);
				
				//Thread.yield();

				// System.err.println("Allowed production of tuple with value: "
				// + tupleGuard);
				// System.out.println(this.getName() + "produced a tuple.");
			//} else {
				// putTuple(tupleElement,0);
				// System.err.println("Avoided production of tuple with value: "
				// + tupleGuard);
				// System.err.println(this.getName() +
				// "avoided sending a tuple.");
			//}
		//}
			
		if(rn.nextInt(10000) > 9998 && !threadSleptOnce)
		{
			//Thread.sleep(50000);
			//threadSleptOnce = true;
			//System.out.println("BUCKET FORCE-SLEEP - Tuple-count" + tupleOut);
		}
				
				
		if ((count % slide == 0) && (windowType == 0)) {
			// output a punctuation to say windowId_from is completed
			putTuple(createPunctuation(inputTuple, windowId_from), 0);

		}
		
		if (logging) {
			
			log.Update("TupleOut", String.valueOf(tupleOut));
		}
		//displayTupleAlways(result, "PhysicalBucket::processTuple, end");
	}

	protected Tuple appendWindowId(Tuple inputTuple, int steamId)
			throws ShutdownException, InterruptedException {

		// tuple-based window
		if (windowType == 0) {
			windowId_from = count / slide;
			windowId_to = (count + range) / slide - 1;
		} else if (windowType == 1) { // value-based window
			long timestamp;
			try {
				ArrayList values = new ArrayList();
				ae.getAtomicValues(inputTuple, values);
				//displayTupleAlways(inputTuple, "");
				timestamp = Long.parseLong(((BaseAttr) values.get(0)).toASCII());
			} catch (NumberFormatException nfe) {
				timestamp = 0;
			}

			if (count == 1) {
				startSecond = timestamp;
				if (start == Long.MIN_VALUE)
					start = startSecond;
			}

			windowId_from = (timestamp - start) / slide;
			windowId_to = ((timestamp - start) + range) / slide - 1;

		}
		// int outSize = outputTupleSchema.getLength();

		LongAttr from, to;

		from = new LongAttr(windowId_from);
		to = new LongAttr(windowId_to);
		inputTuple.appendAttribute(from);
		inputTuple.appendAttribute(to);
		return inputTuple;
	}

	//amit: writing wid->ts method
	/*
	 * The formula to use is : ts_start = wid_from * range; ts_end = ts_start + range;
	 * e.g ts_start = 0 * 10 = 0; ts_end = (0 + 1) * 30 = 30
	 */
	protected void getTimestamp(long wid_frm, long ts_start, long ts_end)
	{
		ts_start = wid_frm * slide;   
		// ts_end = (wid_frm + 1) * range; // wrong
		ts_end = ts_start + range;		
	}
		
	/*
	 * protected Tuple appendWindowId (Tuple inputTuple, int steamId) throws
	 * ShutdownException, InterruptedException {
	 * 
	 * long mod; long numOfWindows; long prev = 0;
	 * 
	 * //wid_from = 1, 2, 3 ... //count = 1, 2, 3, ... if (windowType == 0) { //
	 * tuple-based window // Compute the window Id // if ((range % slide) == 0)
	 * { numOfWindows = range / slide; windowId_from = count / slide;
	 * 
	 * mod = count % slide; if (mod != 0) windowId_from += 1; windowId_to =
	 * windowId_from + numOfWindows - 1; } else { numOfWindows = range / slide +
	 * 1; windowId_from = count / slide; mod = count % slide; if (mod != 0)
	 * windowId_from += 1; prev = windowId_from; windowId_to = (range + count) /
	 * slide; if ((range + count) % slide == 0) windowId_to -= 1; }
	 * 
	 * } else if (windowType == 1) { // value-based window
	 * 
	 * long timestamp; try { ArrayList values = new ArrayList ();
	 * ae.getAtomicValues(inputTuple, values); timestamp =
	 * Long.parseLong(((BaseAttr)values.get(0)).toASCII()); } catch
	 * (NumberFormatException nfe) { timestamp = 0; } if ( count==1 )
	 * startSecond = timestamp;
	 * 
	 * if ((range % slide) == 0) { numOfWindows = range / slide; windowId_from =
	 * (timestamp - startSecond + 1) / slide; mod = (timestamp - startSecond +
	 * 1) % slide; if (mod != 0) windowId_from += 1; prev = windowId_from;
	 * windowId_to = windowId_from + numOfWindows - 1; } else { numOfWindows =
	 * range / slide + 1; windowId_from = (timestamp - startSecond + 1) / slide;
	 * mod = (timestamp - startSecond + 1) % slide; if (mod != 0) windowId_from
	 * += 1; prev = windowId_from; windowId_to = (range + (timestamp -
	 * startSecond + 1)) / slide; if (( (range + (timestamp - startSecond + 1))
	 * / slide ) == 0) { windowId_to -= 1; }
	 * 
	 * } }
	 * 
	 * int outSize = outputTupleSchema.getLength();
	 * 
	 * LongAttr from, to;
	 * 
	 * from = new LongAttr(windowId_from); to = new LongAttr(windowId_to);
	 * inputTuple.appendAttribute(from); inputTuple.appendAttribute(to); return
	 * inputTuple;
	 * 
	 * }
	 */

	/**
	 * This function processes a punctuation element read from a source stream
	 * when the operator is non-blocking. This over-rides the corresponding
	 * function in the base class.
	 * 
	 * Punctuations can simply be sent to the next operator from Select
	 * 
	 * @param inputTuple
	 *            The tuple element read from a source stream
	 * @param streamId
	 *            The source stream from which the tuple was read
	 * 
	 * @exception ShutdownException
	 *                query shutdown by user or execution error
	 */
	/*
	 * protected void processPunctuation(StreamPunctuationElement inputTuple,
	 * int streamId) throws ShutdownException, InterruptedException { if
	 * (inputTuple.isPunctuation()) { // for time-based bucket
	 * 
	 * windowCount++; Element from = doc.createElement("wid_from");
	 * from.appendChild(doc.createTextNode("(," + String.valueOf(windowCount) +
	 * ")")); //windowId_from? Element to = doc.createElement("wid_to");
	 * to.appendChild(doc.createTextNode(String.valueOf("*")));
	 * 
	 * inputTuple.appendAttribute(from); inputTuple.appendAttribute(to);
	 * 
	 * putTuple(inputTuple, streamId);
	 * 
	 * } //putTuple(inputTuple, streamId); }
	 */

	protected void processPunctuation(Punctuation inputTuple, int streamId)
			throws ShutdownException, InterruptedException {

		//System.out.println(this.getName() + "received punctuation");
		//long wid = 0;
		long timestamp;

		String punctVal;

		ArrayList values = new ArrayList();
		ae.getAtomicValues(inputTuple, values);

		// assume that the punctuation on winattr should have the format of xxx,
		// which means the end of xxx
		punctVal = ((BaseAttr) values.get(0)).toASCII().trim();
		// length = punctVal.length();
		// String tmp = punctVal.substring(1, length - 1).trim();
		// length = tmp.length();
		// punctVal = tmp.substring(1, length);
		try {
			timestamp = Long.parseLong(punctVal);
		} catch (NumberFormatException e) {
			/*System.err
					.println(punctVal
							+ " is not a long integer - Bucket cannot handle such a punctuating attribute");*/
			return;
		}

		// punctuation may come in a finer granularity; e.g., 20 seconds rather
		// than 1 minute;
//		if (slide != 1)
//			if ((timestamp - startSecond) == 0
//					|| (timestamp - startSecond) % slide != 0)
//				return;
//
//		wid = (long) ((timestamp + 1) - start) / slide - 1;
		
		punctWid = (timestamp - start) / slide - 1;
		if (punctWid == prevPunctWid)
			return;
		else
			prevPunctWid = punctWid;

		if (NiagraServer.DEBUG)
			System.err.println("window_id: " + punctWid + "   timestamp: "
					+ timestamp);
		if (timestamp < -1000)
			System.err.println("sth is wrong ...");

		/*
		 * if ((range % slide) == 0) { wid = (long)(timestamp - startSecond + 1)
		 * / slide; mod = (long)(timestamp - startSecond + 1) % slide; if (mod
		 * != 0) wid += 1; } else { wid =(long) (timestamp - startSecond + 1) /
		 * slide; mod = (long) (timestamp - startSecond + 1) % slide; }
		 */

		// Punctuation punct = null;

		// Element ts = doc.createElement(windowAttrName);
		// ts.appendChild(doc.createTextNode("*"));

		StringAttr ts = new StringAttr("*");
		inputTuple.setAttribute(ae.attributeId, ts);

		StringAttr from, to;

		from = new StringAttr(String.valueOf(punctWid));
		to = new StringAttr("*");

		inputTuple.appendAttribute(from);
		inputTuple.appendAttribute(to);

		putTuple(inputTuple, streamId);
		
		if(logging){
			punctsOut++; // Count the input punctuations for this operator
			log.Update("PunctsOut", String.valueOf(punctsOut));
		}
		//System.out.println(this.getName() + "punctuation");
		//displayTupleAlways(inputTuple, "PhysicalBucket::processPunctuation end");
	}

	/**
	 * This function generates a punctuation based on the timer value using the
	 * template generated by setupDataTemplate
	 */
	private Punctuation createPunctuation(Tuple inputTuple, long value) {
		// This input came from the timer. Generate punctuation
		// based on the timer value, where the time value is
		// (,last), indicating that all values from the beginning
		// to `last' have been seen. Note this assumes the
		// attribute is strictly non-decreasing.
		// Element eChild;
		// Text tPattern;
		short nodeType;

		// Create a new punctuation element
		Punctuation spe = new Punctuation(false);
		for (int iAttr = 0; iAttr < tupleDataSample.size(); iAttr++) {
			// Node ndSample = tupleDataSample.getAttribute(iAttr);
			Object ndSample = tupleDataSample.getAttribute(iAttr);

			if (ndSample instanceof BaseAttr) {
				spe.appendAttribute(((BaseAttr) ndSample).copy());
			} else if (ndSample instanceof Node) {
				nodeType = ((Node) ndSample).getNodeType();
				if (nodeType == Node.ATTRIBUTE_NODE) {
					Attr attr = doc.createAttribute(((Node) ndSample)
							.getNodeName());
					attr.appendChild(doc.createTextNode("*"));
					spe.appendAttribute(attr);
				} else {
					String stName = ((Node) ndSample).getNodeName();
					if (stName.compareTo("#document") == 0)
						stName = new String("document");
					Element ePunct = doc.createElement(stName);
					ePunct.appendChild(doc.createTextNode("*"));
					spe.appendAttribute(ePunct);
				}
			}
		}
		// tPattern = doc.createTextNode("(," +Long.toString(value) + ")");
		// hack
		/*
		 * tPattern = doc.createTextNode(Long.toString(value));
		 * 
		 * eChild = doc.createElement("wid_from_"+widName);
		 * eChild.appendChild(tPattern); spe.appendAttribute(eChild);
		 * 
		 * tPattern = doc.createTextNode("*");
		 * 
		 * eChild = doc.createElement("wid_to_"+widName);
		 * eChild.appendChild(tPattern); spe.appendAttribute(eChild);
		 */
		spe.appendAttribute(new StringAttr(value));
		spe.appendAttribute(new StringAttr("*"));

		//displayTupleAlways(spe, "PhysicalBucket::createPunctuation end");
		
		return spe;
	}

	public boolean isStateful() {
		return false;
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog,
	 *      LogicalProperty, LogicalProperty[])
	 */
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] inputLogProp) {
		double trc = catalog.getDouble("tuple_reading_cost");
		double sumCards = 0;
		for (int i = 0; i < inputLogProp.length; i++)
			sumCards += inputLogProp[i].getCardinality();
		return new Cost(trc * sumCards);
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof PhysicalBucket))
			return false;
		if (o.getClass() != PhysicalBucket.class)
			return false;
		if(((PhysicalBucket)o).propagate != propagate)
			return false;
		if(((PhysicalBucket)o).exploit != exploit)
			return false;
		if(((PhysicalBucket)o).fAttr != fAttr)
			return false;
		
		PhysicalBucket another = (PhysicalBucket) o;

		boolean match = another.windowAttribute.equals(this.windowAttribute)
				&& another.range == this.range && another.slide == this.slide
				&& another.windowType == this.windowType
				&& another.start == this.start;
		if (match)
			System.err.println("PhysicalBucket equal true");
		return match;
		// return getArity() == ((PhysicalBucket) o).getArity();
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return getArity();
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindPhysProp(PhysicalProperty[])
	 */
	public PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props) {
		return input_phys_props[0];
	}

	public void setResultDocument(Document doc) {
		this.doc = doc;
	}

	/**
	 * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
	 */
	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		super.constructTupleSchema(inputSchemas);
		/*
		 * inputTupleSchemas = inputSchemas; outputTupleSchema =
		 * inputSchemas[0].copy();
		 */
		/*
		 * Attribute attrFrom, attrTo;
		 * 
		 * attrFrom = logProp.getAttr("wid_from_"+widName); attrTo =
		 * logProp.getAttr("wid_to_"+widName);
		 * 
		 * outputTupleSchema.addMapping(attrFrom);
		 * outputTupleSchema.addMapping(attrTo);
		 */
	}
}
