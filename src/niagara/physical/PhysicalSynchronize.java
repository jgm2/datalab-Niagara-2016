package niagara.physical;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
public class PhysicalSynchronize extends PhysicalOperator
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
	private Vector<Long> punctuationBuffer = new Vector<Long>();
	
	public PhysicalSynchronize() {
		this.setBlockingSourceStreams(PhysicalSynchronize.blockingSourceStreams);
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
		//logging information to note arrival order of tuples
		long time = Long.parseLong(getValue(tupleElement, 0));
		int streamid = Integer.parseInt(getValue(tupleElement, 1));
		if(-1!=time){
		System.out.println("IP://STREAM ID:"+streamid+", TIME:"+time);
		}
				
		if(0 == streamId){ //leftstream
			
			if(-1!=Long.parseLong(getValue(tupleElement,left_tsAttributeInputSchemaPosition))){
			
				leftStreamBuffer.add(tupleElement);
			}

						
		} else { //rightstream
			
			if(-1!=Long.parseLong(getValue(tupleElement,left_tsAttributeInputSchemaPosition))){
				
			
				rightStreamBuffer.add(tupleElement);
			}

				
		}	  
	}
	
	protected void processPunctuation(Punctuation inputTuple, int streamId)
			  throws ShutdownException, InterruptedException  {
		
		//column indexes are hard-coded
		//logging information to note arrival order of punctuation tuples
		long time = Long.parseLong(getValue(inputTuple, 0));
		System.out.println("IP://STREAM ID:"+streamId+", PTIME:"+time);
		
		long pt_ts = Long.parseLong(getValue(inputTuple, left_tsAttributeInputSchemaPosition));
		boolean leftbufferEmpty = false; 
		boolean rightbufferEmpty = false;
		
			//check if seen punctuation on other stream yet - tracked through punctuation buffer
			
			if(!punctuationBuffer.isEmpty() && punctuationBuffer.firstElement().longValue()==pt_ts){
				punctuationBuffer.remove(0);
				//sort tuples in each buffer so far
				Collections.sort(leftStreamBuffer,new TsTupleComparator());
				Collections.sort(rightStreamBuffer,new TsTupleComparator());
			
				//if both the buffers have tuples
				if(!leftStreamBuffer.isEmpty() && !rightStreamBuffer.isEmpty()){
					Tuple lb_tuple = leftStreamBuffer.firstElement();
					long lbfe_ts = Long.parseLong(getValue(lb_tuple,left_tsAttributeInputSchemaPosition));
					Tuple rb_tuple = rightStreamBuffer.firstElement();
					long rbfe_ts = Long.parseLong(getValue(rb_tuple,right_tsAttributeInputSchemaPosition));
					
					//when timestamp of first element in both buffers are less than or equal to punctuation timestamp
					while(lbfe_ts<=pt_ts && rbfe_ts<=pt_ts){
						//when left buffer has elements with timestamp less than right buffer
						if(lbfe_ts<rbfe_ts){
							//logic similar to processTuples in order
							while(lbfe_ts < pt_ts){
								Tuple outputTuple = leftStreamBuffer.remove(0);
								putTuple(outputTuple,0);
								System.out.println("STREAM ID:"+0+", TIME:"+lbfe_ts);
								if(leftStreamBuffer.isEmpty()){
									leftbufferEmpty = true;
									break;
								}
								lb_tuple = leftStreamBuffer.firstElement();
								lbfe_ts = Long.parseLong(getValue(lb_tuple,left_tsAttributeInputSchemaPosition));
							}
							if(!leftbufferEmpty && lbfe_ts==rbfe_ts){
								Tuple left_outputTuple = leftStreamBuffer.remove(0);
								putTuple(left_outputTuple,0);
								System.out.println("STREAM ID:"+0+", TIME:"+lbfe_ts);
								Tuple right_outputTuple = rightStreamBuffer.remove(0);
								putTuple(right_outputTuple,1);
								System.out.println("STREAM ID:"+1+", TIME:"+rbfe_ts);
							}
						//when right buffer has elements with timestamp less than left buffer
						} else if(rbfe_ts<lbfe_ts){
							//logic similar to processTuples in order
							while(rbfe_ts < pt_ts){
								Tuple outputTuple = rightStreamBuffer.remove(0);
								putTuple(outputTuple,1);
								System.out.println("STREAM ID:"+1+", TIME:"+rbfe_ts);
								if(rightStreamBuffer.isEmpty()){
									rightbufferEmpty = true;
									break;
								}
								rb_tuple = rightStreamBuffer.firstElement();
								rbfe_ts = Long.parseLong(getValue(rb_tuple,right_tsAttributeInputSchemaPosition));
							}
							if(!rightbufferEmpty && lbfe_ts==rbfe_ts){
								Tuple left_outputTuple = leftStreamBuffer.remove(0);
								putTuple(left_outputTuple,0);
								System.out.println("STREAM ID:"+0+", TIME:"+lbfe_ts);
								Tuple right_outputTuple = rightStreamBuffer.remove(0);
								putTuple(right_outputTuple,1);
								System.out.println("STREAM ID:"+1+", TIME:"+rbfe_ts);
							}
						//when the timestamp of first element in both the buffers are equal
						} else {
							Tuple left_outputTuple = leftStreamBuffer.remove(0);
							putTuple(left_outputTuple,0);
							System.out.println("STREAM ID:"+0+", TIME:"+lbfe_ts);
							Tuple right_outputTuple = rightStreamBuffer.remove(0);
							putTuple(right_outputTuple,1);
							System.out.println("STREAM ID:"+1+", TIME:"+rbfe_ts);
						}
						
						//if either one buffer is empty, break out of while loop
						if(leftStreamBuffer.isEmpty() || rightStreamBuffer.isEmpty()){
							break;
						}
						lb_tuple = leftStreamBuffer.firstElement();
						lbfe_ts = Long.parseLong(getValue(lb_tuple,left_tsAttributeInputSchemaPosition));
						rb_tuple = rightStreamBuffer.firstElement();
						rbfe_ts = Long.parseLong(getValue(rb_tuple,right_tsAttributeInputSchemaPosition));
						
						}
					//when only left buffer is not empty
					} else if(!leftStreamBuffer.isEmpty()){
						Tuple lb_tuple = leftStreamBuffer.firstElement();
						long lbfe_ts = Long.parseLong(getValue(lb_tuple,left_tsAttributeInputSchemaPosition));
						//logic similar to processTuples in order
						while(lbfe_ts<pt_ts){
							Tuple outputTuple = leftStreamBuffer.remove(0);
							putTuple(outputTuple,0);
							System.out.println("STREAM ID:"+0+", TIME:"+lbfe_ts);
							if(leftStreamBuffer.isEmpty()){
								leftbufferEmpty = true;
								break;
							}
							lb_tuple = leftStreamBuffer.firstElement();
							lbfe_ts = Long.parseLong(getValue(lb_tuple,left_tsAttributeInputSchemaPosition));
						}
						if(!leftbufferEmpty && lbfe_ts==pt_ts){
							Tuple left_outputTuple = leftStreamBuffer.remove(0);
							putTuple(left_outputTuple,0);
							System.out.println("STREAM ID:"+0+", TIME:"+lbfe_ts);
							/*Tuple right_outputTuple = rightStreamBuffer.remove(0);
							putTuple(right_outputTuple,1);
							System.out.println("STREAM ID:"+1+", TIME:"+pt_ts);*/
						}
					//when only right buffer is not empty
					} else if(!rightStreamBuffer.isEmpty()){
						Tuple rb_tuple = rightStreamBuffer.firstElement();
						long rbfe_ts = Long.parseLong(getValue(rb_tuple,right_tsAttributeInputSchemaPosition));
						if(rbfe_ts<=pt_ts){
							//logic similar to processTuples in order
							while(rbfe_ts < pt_ts){
								Tuple outputTuple = rightStreamBuffer.remove(0);
								putTuple(outputTuple,1);
								System.out.println("STREAM ID:"+1+", TIME:"+rbfe_ts);
								if(rightStreamBuffer.isEmpty()){
									rightbufferEmpty = true;
									break;
								}
								rb_tuple = rightStreamBuffer.firstElement();
								rbfe_ts = Long.parseLong(getValue(rb_tuple,right_tsAttributeInputSchemaPosition));
							}
							if(!rightbufferEmpty && rbfe_ts==pt_ts){
								/*Tuple left_outputTuple = leftStreamBuffer.remove(0);
								putTuple(left_outputTuple,0);*/
								
								Tuple right_outputTuple = rightStreamBuffer.remove(0);
								putTuple(right_outputTuple,1);
								System.out.println("STREAM ID:"+1+", TIME:"+rbfe_ts);
							}
						}
					}
			} else {
				punctuationBuffer.add(new Long(pt_ts));
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
		final PhysicalSynchronize sync = new PhysicalSynchronize();
		sync.left_tsAttributeName = this.left_tsAttributeName;
		sync.right_tsAttributeName = this.right_tsAttributeName;
		sync.numOutputStreams = this.numOutputStreams;
	    return sync;
	}

	@Override
	public boolean equals(Object other) {
		if ((other == null) || !(other instanceof PhysicalSynchronize))
	    {
	      return false;
	    }
	    if (other.getClass() != PhysicalSynchronize.class)
	    {
	      return other.equals(this);
	    }
	    final PhysicalSynchronize s = (PhysicalSynchronize) other;
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

class TsTupleComparator implements Comparator {
	
	public int compare(Object tuple1, Object tuple2){
		
		String value1 = ((BaseAttr) ((Tuple)tuple1).getAttribute(0)).attrVal().toString();
		String value2 = ((BaseAttr) ((Tuple)tuple2).getAttribute(0)).attrVal().toString();
		long lbfe_ts = Long.parseLong(value1);
		long rbfe_ts = Long.parseLong(value2);
		
		if(lbfe_ts > rbfe_ts){
			return 1;
		} else if(lbfe_ts < rbfe_ts){
			return -1;
		} else 
			return 0;
	}
}
