package niagara.physical;


import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Vector;

import niagara.logical.Synchronize;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalProperty;
import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;


/**
 * This is the <code>PhysicalSynchronize</code> that extends the
 * <code>PhysicalOperator</code> with the implementation of synchronize operator
 * Currently as operators only support output streams of the same schema, 
 * synchronize operator requires both input streams to have same schema
 * 
 * @version 1.0
 * @author moorthy
 * 
 */
public class PhysicalSynchronize1 extends PhysicalOperator
{
	
	// No blocking source streams
	private static final boolean[]          blockingSourceStreams          = { false, false };
	private final boolean                   OVERRIDE_DEBUG                 = true;
	private int outSize; // save a fcn call each time through
	private int numOutputStreams;
	
	private String left_tsAttributeName;        
	private String right_tsAttributeName;     
	
	private boolean inputTupleSchemaLocationsKnown = false;
	private boolean streamClosed = false;
	
	//attributes of left and right stream
	private int left_tsAttributeInputSchemaPosition;
	private int right_tsAttributeInputSchemaPosition;
	
	private Vector<Tuple> leftStreamBuffer = new Vector<Tuple>();
	private Vector<Tuple> rightStreamBuffer = new Vector<Tuple>();
	
	public PhysicalSynchronize1() {
		this.setBlockingSourceStreams(PhysicalSynchronize1.blockingSourceStreams);
	}
	
	@Override
	public boolean isStateful() {
		 return true;
	}
	
	public void opInitialize() {
		outSize = outputTupleSchema.getLength();
		
	}

	@Override
	protected void opInitFrom(LogicalOp op) {
		left_tsAttributeName = ((Synchronize) op).getLeft_tsAttributeName();
		right_tsAttributeName = ((Synchronize) op).getRight_tsAttributeName();
		numOutputStreams = ((Synchronize) op).getNumberOfOutputs();
	}
	
	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		super.constructTupleSchema(inputSchemas);
		//outputTupleSchema = inputTupleSchemas[0];		
		if (inputTupleSchemaLocationsKnown == false)
	    {
	      lookupTupleSchemaLocations();
	    }
	}
	
	private int getAttributePosition(TupleSchema schema, String attributeName) { 
	    return schema.getPosition(attributeName);
	  }
	
	private void lookupTupleSchemaLocations() {
		left_tsAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], left_tsAttributeName);    
		right_tsAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[1], right_tsAttributeName);
	    inputTupleSchemaLocationsKnown = true;
	  }
	
	private String getValue(Tuple tuple, int attributeLoc) {
	    String value;
	    value = ((BaseAttr) tuple.getAttribute(attributeLoc)).attrVal().toString();
	    return value;
	  }
	
	@Override
    protected void processTuple(final Tuple tupleElement, final int streamId)
      throws ShutdownException, InterruptedException	  {
		
		//column indexes are hard-coded
		//logging information to note arrival order of punctuation tuples
		long time = Long.parseLong(getValue(tupleElement, 0));
		//int streamid = Integer.parseInt(getValue(tupleElement, 1));
		if(-1!=time){
		System.out.println("IP://STREAM ID:"+streamId+", TIME:"+time);
		}
		
		long te_ts = Long.parseLong(getValue(tupleElement,left_tsAttributeInputSchemaPosition));
		if(-1!=te_ts){
			
		
			if(0 == streamId){ //leftstream
				
				boolean tupleEmitted = false;
				boolean bufferEmpty = false;
				//input stream is in order
				
				if(!rightStreamBuffer.isEmpty()){
					Tuple rb_tuple = rightStreamBuffer.firstElement();
					long rbfe_ts = Long.parseLong(getValue(rb_tuple,right_tsAttributeInputSchemaPosition));
					
						//while right buffer has elements with timestamp less than current tuple
						while(rbfe_ts < te_ts){
							Tuple outputTuple = rightStreamBuffer.remove(0);
							putTuple(outputTuple,1);
							System.out.println("STREAM ID:"+1+", TIME:"+rbfe_ts);
							if(rightStreamBuffer.isEmpty()){
								bufferEmpty = true;
								break;
							}
							rb_tuple = rightStreamBuffer.firstElement();
							rbfe_ts = Long.parseLong(getValue(rb_tuple,right_tsAttributeInputSchemaPosition));
						}
						//if right buffer has element with timestamp equal to current tuple
						if(!bufferEmpty && rbfe_ts==te_ts){
							Tuple outputTuple = rightStreamBuffer.remove(0);
							putTuple(outputTuple,1);
							System.out.println("STREAM ID:"+1+", TIME:"+rbfe_ts);
							putTuple(tupleElement,0);
							System.out.println("STREAM ID:"+0+", TIME:"+te_ts);
							tupleEmitted=true;
						}
					
				}
				if(!tupleEmitted){
					leftStreamBuffer.add(tupleElement);
				}
	
				
			} else { //rightstream
				
				boolean tupleEmitted = false;
				boolean bufferEmpty = false;
				//input stream is in order
				if(!leftStreamBuffer.isEmpty()){
					Tuple lb_tuple = leftStreamBuffer.firstElement();
					long lbfe_ts = Long.parseLong(getValue(lb_tuple,left_tsAttributeInputSchemaPosition));
					
					    //while left buffer has elements with timestamp less than current tuple
						while(lbfe_ts < te_ts){
							Tuple outputTuple = leftStreamBuffer.remove(0);
							putTuple(outputTuple,0);
							System.out.println("STREAM ID:"+0+", TIME:"+lbfe_ts);
							if(leftStreamBuffer.isEmpty()){
								bufferEmpty = true;
								break;
							}
							lb_tuple = leftStreamBuffer.firstElement();
							lbfe_ts = Long.parseLong(getValue(lb_tuple,left_tsAttributeInputSchemaPosition));
						}
						//if left buffer has element with timestamp equal to current tuple
						if(!bufferEmpty && lbfe_ts==te_ts){
							Tuple outputTuple = leftStreamBuffer.remove(0);
							putTuple(outputTuple,0);
							System.out.println("STREAM ID:"+0+", TIME:"+lbfe_ts);
							putTuple(tupleElement,1);
							System.out.println("STREAM ID:"+1+", TIME:"+te_ts);
							tupleEmitted=true;
						}
				}
				if(!tupleEmitted){
					rightStreamBuffer.add(tupleElement);
				}
	
				
			}	  
		}
	}
	
	protected void processPunctuation(Punctuation inputTuple, int streamId)
			  throws ShutdownException, InterruptedException  {
		long punctuatedUpToTime = -1;
		
		//no punctuation records expected
		if(0==streamId){
			
		} else {	
			
		}
	}
	
	@Override
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] InputLogProp) {
		// XXX vpapad: really naive. Only considers the hashing cost
	    final float inpCard = InputLogProp[0].getCardinality();
	    final float outputCard = this.logProp.getCardinality();

	    double cost = inpCard * catalog.getDouble("tuple_reading_cost");
	    cost += inpCard * catalog.getDouble("tuple_hashing_cost");
	    cost += outputCard * catalog.getDouble("tuple_construction_cost");
	    return new Cost(cost);
	}

	@Override
	public Op opCopy() {
		final PhysicalSynchronize1 sync = new PhysicalSynchronize1();
		sync.left_tsAttributeName = this.left_tsAttributeName;
		sync.right_tsAttributeName = this.right_tsAttributeName;
		sync.numOutputStreams = this.numOutputStreams;
	    return sync;
	}

	@Override
	public boolean equals(Object other) {
		if ((other == null) || !(other instanceof PhysicalSynchronize1))
	    {
	      return false;
	    }
	    if (other.getClass() != PhysicalSynchronize1.class)
	    {
	      return other.equals(this);
	    }
	    final PhysicalSynchronize1 s = (PhysicalSynchronize1) other;
	    return this.left_tsAttributeName.equals(s.left_tsAttributeName)
	    		&& this.right_tsAttributeName.equals(s.right_tsAttributeName)
	    		&& this.numOutputStreams==s.numOutputStreams;
	}

	@Override
	public int hashCode() {
		return getArity();
	}
  
	public void streamClosed(int streamId) throws ShutdownException {

		if(!streamClosed){
			streamClosed = true;
		} else {
		
			//empty left stream buffer
			if(!leftStreamBuffer.isEmpty()){
				while(!leftStreamBuffer.isEmpty()){
					Tuple outputTuple = leftStreamBuffer.remove(0);
					try {
						putTuple(outputTuple,0);
						long time = Long.parseLong(getValue(outputTuple, 0));
						System.out.println("STREAM ID:"+0+", TIME:"+time);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			} 
			//empty right stream buffer
			else if(!rightStreamBuffer.isEmpty()){
				while(!rightStreamBuffer.isEmpty()){
					Tuple outputTuple = rightStreamBuffer.remove(0);
					try {
						putTuple(outputTuple,1);
						long time = Long.parseLong(getValue(outputTuple, 0));
						System.out.println("STREAM ID:"+1+", TIME:"+time);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
    }
	  
	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#findPhysProp(PhysicalProperty[])
	 */
	public PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props) {
		return input_phys_props[0];
	}
  
	/**
	 * @see niagara.optimizer.colombia.Op#getNumberOfOutputs()
	 */
	public int getNumberOfOutputs() {
		return numOutputStreams;
	}
		
}
