package niagara.physical;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;

import niagara.logical.Pace;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.ControlFlag;
import niagara.utils.FeedbackPunctuation;
import niagara.utils.FeedbackType;
import niagara.utils.Guard;
import niagara.utils.Log;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;
import niagara.utils.FeedbackPunctuation.Comparator;

/**
 * <code>PhysicalPace</code> implements a union of a set of incoming streams;
 * In addition, monitors divergence and issues assumed punctuation if divergence is
 * detected.
 * 
 * @see PhysicalOperator
 */
@SuppressWarnings("unchecked")
public class PhysicalPace extends PhysicalOperator {
	private ArrayList[] punctuationRegistry; // array of seen punctuations per input stream
	private int[] rgnRemove;
	private Attrs[] inputAttrs;
	private int[][] attributeMaps;
	private boolean hasMappings;
	private int outSize;
	private boolean propagate;
	private boolean exploit;
	//private boolean logging;
	
	//private int tupleCount = 0;
	
	// amit: adding the following line
	private Guard outputGuard = new Guard();
	int[] positions;
	String[] names;
	int tupleOut = 0;
	
	// FP attributes
	private String fattrsL = "";
	private String fattrsR = "";	
	
	//	 maximum acceptable divergence 
	String divergence;
	
	// attribute-pair to be monitored for divergence - String with two space-seperated values
	String monitorAttr;
	
	// left and right high water marks
	long hwmL = 0;
	long hwmR = 0;
	
	// check variables to avoid repetitive sending of the same feedback
	long previousFeedbackToLeft = 0;
	long previousFeedbackToRight = 0;

	public PhysicalPace() {
		// XXX vpapad: here we have to initialize blockingSourceStreams
		// but we don't know how many input streams we have yet.
		// We postpone it until initFrom - is that too late?
		// KT - I think that should be ok, blockingSourceStreams
		// isn't used until execution - I think...
	}

	public PhysicalPace(int arity) {
		setBlockingSourceStreams(new boolean[arity]);
	}

	public void opInitFrom(LogicalOp logicalOperator) {
		Pace logicalOp = (Pace) logicalOperator;

		exploit = logicalOp.getExploit();
		propagate = logicalOp.getPropagate();
		
		logging = logicalOp.getLogging();
		
		if (logging) {
			log = new Log(this.getName());
		}
		
		fattrsL = logicalOp.getFAttrsL();
		fattrsR = logicalOp.getFAttrsR();

		divergence = ((Pace) logicalOp).getDivergence();
		monitorAttr = ((Pace) logicalOp).getMonitorAttr();
		
		setBlockingSourceStreams(new boolean[logicalOp.getArity()]);
		hasMappings = false;
		if (logicalOp.numMappings() > 0)
			hasMappings = true;
		inputAttrs = logicalOp.getInputAttrs();

		assert logicalOp.getArity() == Array.getLength(inputAttrs) : "Arity doesn't match num input attrs ";
	}

	public void opInitialize() {
		// XXX vpapad: really ugly...
		setBlockingSourceStreams(new boolean[numSourceStreams]);
		punctuationRegistry = new ArrayList[getArity()];
		for (int i = 0; i < punctuationRegistry.length; i++)
			punctuationRegistry[i] = new ArrayList();

		rgnRemove = new int[getArity()];
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
				log.Update(fp.toString(), String.valueOf(tupleOut));
			}
			
			FeedbackPunctuation fpSendL = new FeedbackPunctuation(fp.Type(),fp.Variables(),fp.Comparators(),fp.Values());
			FeedbackPunctuation fpSendR = new FeedbackPunctuation(fp.Type(),fp.Variables(),fp.Comparators(),fp.Values());
			
			// get attribute positions from tuple to check against guards
			names = new String[fp.Variables().size()];
			names = fp.Variables().toArray(names);

			// get positions
			positions = new int[fp.Variables().size()];
			for (int i = 0; i < names.length; i++) {
				positions[i] = outputTupleSchema.getPosition(names[i]);
			}

			
			
			// System.out.println("Received FP");
			///System.out.println(this.getName() + " " + fp.toString());
			
			if(exploit)		
				outputGuard.add(fp);

			if (propagate) {
				
				// amit: Logic similar to join; need to be tested.
				//FeedbackPunctuation fpSend0 = null; 
				//FeedbackPunctuation fpSend1 = null; 
		
								
				fpSendL = split(fpSendL,fattrsL);
				
				fpSendR = splitR(fpSendR,fattrsL,fattrsR);
			
				
			//	fpSend1.Variables().add(arg0)
				
				sendFeedbackPunctuation(fpSendL, 0);
				System.out.println(this.getName() + "Left  -> " + fpSendL.toString());
				
				sendFeedbackPunctuation(fpSendR, 1);
				System.out.println(this.getName() + "Right -> " + fpSendR.toString());
				
			}
			break;
		default:
			assert false : "KT unexpected control message from sink "
				+ ctrlFlag.flagName();
		}
	}
	
	FeedbackPunctuation split(FeedbackPunctuation fp,String fattrsList)
	{
		 FeedbackType _type;
		 ArrayList<String> _variables = new ArrayList<String>();
		 ArrayList<Comparator> _comparators = new ArrayList<Comparator>();
		 ArrayList<String> _values = new ArrayList<String>();
					
		Iterator<String> iter = fp.Variables().iterator();
		
		int posPointer = 0;
		
		String fattrNames[] = fattrsList.split(" ");
		
		while(iter.hasNext())
		{
			String var = iter.next();			
			
			for(String s:fattrNames)
			{
				if(var.equals(s))
				{
					_variables.add(var);
					_comparators.add(fp.Comparators().get(posPointer));
					_values.add(fp.getValue(posPointer));
				}
			}	
			
			posPointer++;			
		}
		
		_variables.trimToSize();
		_comparators.trimToSize();
		_values.trimToSize();

		return new FeedbackPunctuation(fp.Type(),_variables,_comparators,_values);	
						
	}
	
	FeedbackPunctuation splitR(FeedbackPunctuation fp,String fattrsListL, String fattrsListR)
	{
		 FeedbackType _type;
		 ArrayList<String> _variables = new ArrayList<String>();
		 ArrayList<Comparator> _comparators = new ArrayList<Comparator>();
		 ArrayList<String> _values = new ArrayList<String>();
					
		Iterator<String> iter = fp.Variables().iterator();
		
		int posPointer = 0;
		
		String fattrNames[] = fattrsListL.split(" ");
		
		String fattrNamesR[] = fattrsListR.split(" ");
		
		int r_ptr = 0;
		
		while(iter.hasNext())
		{
			String var = iter.next();			
			
			for(String s:fattrNames)
			{
				if(var.equals(s))
				{
					// if var from L matches, grab next var from R and add it to the FP 
					//_variables.add(var);
					String var1 = fattrNamesR[r_ptr++];
					_variables.add(var1);
					_comparators.add(fp.Comparators().get(posPointer));
					_values.add(fp.getValue(posPointer));
				}
			}	
			
			posPointer++;			
		}
		
		_variables.trimToSize();
		_comparators.trimToSize();
		_values.trimToSize();

		return new FeedbackPunctuation(fp.Type(),_variables,_comparators,_values);	
						
	}	
	
//	}
//	
//	if(fattrsList.contains(var + " ") || fattrsList.contains(" " + var) || (!fattrsList.contains(" ") && fattrsList.equals(var)))
//	{
	
	
	/**
	 * This function processes a tuple element read from a source stream when
	 * the operator is non-blocking. This over-rides the corresponding function
	 * in the base class.
	 * 
	 * @param inputTuple
	 *            The tuple element read from a source stream
	 * @param streamId
	 *            The source stream from which the tuple was read
	 * 
	 * @exception ShutdownException
	 *                query shutdown by user or execution error
	 */

	protected void processTuple(Tuple inputTuple, int streamId)
			throws ShutdownException, InterruptedException {
		
		//System.out.println(streamId + " " + inputTuple.toString());
		
		String mValL = "";
		String mValR = "";
		
		String[] mattArray = monitorAttr.split(" ");
		
		String monitorLeftName = mattArray[0];
		String monitorRightName = mattArray[1];
		
		if(streamId == 0)
		{		
			mValL = ((BaseAttr)(inputTuple.getAttribute(inputTupleSchemas[0].getPosition(monitorLeftName)))).attrVal().toString();
			// need an if check here for oop
			hwmL = Long.parseLong(mValL); // update the left hight water mark
		}
		else // if streamId is 1 - i.e. right input
		{
			mValR = ((BaseAttr)(inputTuple.getAttribute(inputTupleSchemas[1].getPosition(monitorRightName)))).attrVal().toString();
			// need an if check here for oop
			hwmR = Long.parseLong(mValR); // update the right high water mark
		}
		
		// send feedback if needed to right
		if(hwmL - hwmR > Long.parseLong(divergence) && hwmL > previousFeedbackToRight)
		{
			ArrayList<String> vars = new ArrayList<String>();
			ArrayList<FeedbackPunctuation.Comparator> comps = new ArrayList<FeedbackPunctuation.Comparator>();
			ArrayList<String> vals = new ArrayList<String>();
			
			vars.add(monitorRightName);
			comps.add(FeedbackPunctuation.Comparator.LT);
			vals.add(String.valueOf(hwmL));
			
			previousFeedbackToRight = hwmL;
			
			vars.trimToSize();
			vals.trimToSize();
			comps.trimToSize();
			
			// Send elements
			FeedbackPunctuation fp = new FeedbackPunctuation(FeedbackType.ASSUMED, vars, comps, vals);
			sendFeedbackPunctuation(fp, 1);  // send FP to the right input
			System.out.println("Left -> " + fp.toString());
		}	
		
		// send feedback if needed to left
		if(hwmR - hwmL > Long.parseLong(divergence) && hwmR > previousFeedbackToLeft)
		{
			ArrayList<String> vars = new ArrayList<String>();
			ArrayList<FeedbackPunctuation.Comparator> comps = new ArrayList<FeedbackPunctuation.Comparator>();
			ArrayList<String> vals = new ArrayList<String>();
			
			vars.add(monitorLeftName);
			comps.add(FeedbackPunctuation.Comparator.LT);
			vals.add(String.valueOf(hwmR));
			
			previousFeedbackToLeft = hwmR;
			
			vars.trimToSize();
			vals.trimToSize();
			comps.trimToSize();
			
			// Send elements
			FeedbackPunctuation fp = new FeedbackPunctuation(FeedbackType.ASSUMED, vars, comps, vals);
			sendFeedbackPunctuation(fp, 0);  // send FP to the left input
			System.out.println("Right -> " + fp.toString());
		}	
		
		if (hasMappings) { // We need to move some attributes
			putTuple(inputTuple.copy(outSize, attributeMaps[streamId]), 0);
		} else {
			// just send the original tuple along
			if (exploit) 
			{

				// check against guards
				Boolean guardMatch = false;
				for (FeedbackPunctuation fp : outputGuard.elements()) {
					guardMatch = guardMatch
							|| fp
									.match(positions, inputTuple
											.getTuple());
				}

				if (!guardMatch) {
					putTuple(inputTuple, 0);
					if (logging) {
						tupleOut++;
						log.Update("TupleOut", String.valueOf(tupleOut));
					}
				}
			} 
			else
			{
				putTuple(inputTuple, 0);
				if (logging) {
					tupleOut++;
					log.Update("TupleOut", String.valueOf(tupleOut));
				}
			}
			//putTuple(inputTuple, 0);
			//tupleCount++;
			//System.out.println(this.getName()+ tupleCount);
			

		}
	}

	/**
	 * This function handles punctuations for the given operator. For Union, we
	 * have to make sure all inputs have reported equal punctuation before
	 * outputting a punctuation.
	 * 
	 * @param tuple
	 *            The current input tuple to examine.
	 * @param streamId
	 *            The id of the source streams the partial result of which are
	 *            to be removed.
	 * 
	 */

	protected void processPunctuation(Punctuation tuple, int streamId)
			throws ShutdownException, InterruptedException {

		//System.out.println(streamId + " " + tuple.toString());

		
		boolean fAllMatch = true, fFound;

		// First, check to see if this punctuation matches a punctuation
		// from all other inputs
		for (int i = 0; i < punctuationRegistry.length && fAllMatch == true; i++) {
			if (i != streamId) {
				fFound = false;
				for (int j = 0; j < punctuationRegistry[i].size() && fFound == false; j++) {
					fFound = tuple.equals((Punctuation) punctuationRegistry[i].get(j));
					if (fFound)
						rgnRemove[i] = j;
				}
				fAllMatch = fFound;
			}
		}

		if (fAllMatch) {
			// Output the punctuation
			putTuple(tuple, 0);
			
			
			if(logging){
				punctsOut++; // Count the input punctuations for this operator
				log.Update("PunctsOut", String.valueOf(punctsOut));
			}
			// Remove the other punctuations, since they are no longer needed
			for (int i = 0; i < punctuationRegistry.length; i++) {
				if (i != streamId)
					punctuationRegistry[i].remove(rgnRemove[i]);
			}
		} else {
			punctuationRegistry[streamId].add(tuple);
		}
	}

	public boolean isStateful() {
		return false;
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(LogicalProperty,
	 *      LogicalProperty[])
	 */
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] inputLogProp) {
		double trc = catalog.getDouble("tuple_reading_cost");
		double sumCards = 0;
		for (int i = 0; i < inputLogProp.length; i++)
			sumCards += inputLogProp[i].getCardinality();
		return new Cost(trc * sumCards);
	}

	/**
	 * @see niagara.optimizer.colombia.Op#copy()
	 */
	public Op opCopy() {
		PhysicalPace newOp = new PhysicalPace(getArity());
		newOp.inputAttrs = inputAttrs;
		newOp.attributeMaps = attributeMaps;
		newOp.hasMappings = hasMappings;
		newOp.outSize = outSize;
		newOp.exploit = exploit;
		newOp.propagate = propagate;
		newOp.logging = logging;
		newOp.log = new Log(this.getName());
		newOp.fattrsL = fattrsL;
		newOp.fattrsR = fattrsR;
		newOp.divergence = divergence;
		newOp.monitorAttr = monitorAttr;
		
		return newOp;
	}

	/**
	 * @see java.lang.Object#equals(Object)
	 */
	public boolean equals(Object o) {
		if (o == null || !(o instanceof PhysicalPace))
			return false;
		if (o.getClass() != PhysicalPace.class)
			return o.equals(this);
		if(((PhysicalPace)o).exploit != exploit)
			return false;
		if(((PhysicalPace)o).propagate != propagate)
			return false;
		if(((PhysicalPace)o).logging != logging)
			return false;
		if(!((PhysicalPace)o).fattrsL.equals(fattrsL))
			return false;
		if(!((PhysicalPace)o).fattrsL.equals(fattrsL))
			return false;
		if(!((PhysicalPace)o).divergence.equals(divergence))
			return false;
		if(!((PhysicalPace)o).monitorAttr.equals(monitorAttr))
			return false;
		
		return getArity() == ((PhysicalPace) o).getArity()
				&& inputAttrs.equals(((PhysicalPace) o).inputAttrs);
	}

	public int hashCode() {
		if (hasMappings)
			return getArity() ^ inputAttrs.hashCode();
		else
			return getArity();
	}

	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		super.constructTupleSchema(inputSchemas);
		outSize = outputTupleSchema.getLength();

		// if no mapping is specified input schemas must have
		// same length and that length is same as length of output schema
		if (hasMappings) {
			int inputArity = Array.getLength(inputAttrs);

			assert inputArity == Array.getLength(inputSchemas) : " input arity not equal to number of input schemas";

			attributeMaps = new int[inputArity][];
			for (int i = 0; i < inputArity; i++) {
				attributeMaps[i] = new int[outSize];
				for (int j = 0; j < outSize; j++) {
					if (inputAttrs[i] == null) {
						attributeMaps[i][j] = -1;
					} else {
						Attribute a = inputAttrs[i].GetAt(j);
						if (a == null) {
							attributeMaps[i][j] = -1;
						} else {
							attributeMaps[i][j] = inputSchemas[i].getPosition(a
									.getName());
						}
					}
				}
			}
		}
	}
}
