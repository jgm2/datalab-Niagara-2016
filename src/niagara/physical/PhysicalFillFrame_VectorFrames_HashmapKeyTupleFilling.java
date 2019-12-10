package niagara.physical;
/*package niagara.physical;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.MaximizeAction;

import niagara.logical.FillFrame;
import niagara.logical.FrameX;
import niagara.logical.Variable;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.FrameDetectorForFillFrame;
import niagara.utils.FrameDetectorTupleForFillFrame;
import niagara.utils.FrameDetectorX;
import niagara.utils.FrameOutputX;
import niagara.utils.FrameTupleX;
import niagara.utils.IntegerAttr;
import niagara.utils.LongAttr;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.TSAttr;
import niagara.utils.Tuple;


*//**
 * This is the <code>PhysicalFillFrameOperator</code> that extends the
 * <code>PhysicalOperator</code> with the implementation of fill frame operator
 * 
 * @version 1.0
 * @author moorthy
 * 
 *//*
public class PhysicalFillFrame extends PhysicalOperator
{
	
	// No blocking source streams
	private static final boolean[]          blockingSourceStreams          = { false, false };
	private final boolean                   OVERRIDE_DEBUG                 = true;
	  
	private String fs_groupByAttributeName;        
	private String fs_frameStartAttributeName;     
	private String fs_frameEndAttributeName;       
	private String fs_frameIdAttributeName;        
	private String ff_groupByAttributeName;        
	private String ff_timeAttributeName;
	
	private boolean inputTupleSchemaLocationsKnown = false;
	
	//attributes of frame stream
	private int fs_groupByAttributeInputSchemaPosition;
	private int fs_frameIdAttributeInputSchemaPosition;
	private int fs_frameStartAttributeInputSchemaPosition;
	private int fs_frameEndAttributeInputSchemaPosition;
	
	//attributes of fill frame stream
	private int ff_groupByAttributeInputSchemaPosition;	
	private int ff_timeAttributeInputSchemaPosition;
	
	//to process frameId for frame stream - timestamp/scan
	//AtomicEvaluator ae;
	
	private HashMap<Integer, FrameDetectorForFillFrame> frameStreamHashMap = new HashMap<Integer, FrameDetectorForFillFrame>();
	//buffer of fillframe stream tuples
	//private ArrayList<Tuple> fillFrameTupleBuffer = new ArrayList<Tuple>();
	
	private HashMap<Long,Tuple> fillFrameTupleBuffer = new HashMap<Long, Tuple>();
	
	//for debugging purposes
	private int maxFramesSizeBetweenPunct = 0;
	private int maxTuplesSizeBetweenPunct = 0;
	private double avgFramesSizeBetweenPunct = 0;
	private double avgTuplesSizeBetweenPunct = 0;
	private int countPunct = 0;
	

	public PhysicalFillFrame() {
		this.setBlockingSourceStreams(PhysicalFillFrame.blockingSourceStreams);
	}
	
	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		super.constructTupleSchema(inputSchemas);
		
		if (inputTupleSchemaLocationsKnown == false)
	    {
	      lookupTupleSchemaLocations();
	    }
	}
	
	private int getAttributePosition(TupleSchema schema, String attributeName) { 
	    return schema.getPosition(attributeName);
	  }
	
	private void lookupTupleSchemaLocations() {
		fs_groupByAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[1], fs_groupByAttributeName);    
		fs_frameIdAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[1], fs_frameIdAttributeName);
		fs_frameStartAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[1], fs_frameStartAttributeName);
		fs_frameEndAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[1], fs_frameEndAttributeName);
		ff_groupByAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], ff_groupByAttributeName);
		ff_timeAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], ff_timeAttributeName);
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
		
		if(1 == streamId){ //framestream
			// Extract information from tuple    
		    int fs_groupByAttributeValue = Integer.parseInt(getValue(tupleElement, fs_groupByAttributeInputSchemaPosition));
		    int fs_frameIdAttributeValue = Integer.parseInt(getValue(tupleElement, fs_frameIdAttributeInputSchemaPosition));
		    long fs_frameStartAttributeValue = Long.parseLong(getValue(tupleElement, fs_frameStartAttributeInputSchemaPosition));
		    long fs_frameEndAttributeValue = Long.parseLong(getValue(tupleElement, fs_frameEndAttributeInputSchemaPosition));
		    
		    FrameDetectorTupleForFillFrame fdTuple = new FrameDetectorTupleForFillFrame(fs_frameIdAttributeValue, fs_frameStartAttributeValue, fs_frameEndAttributeValue, fs_groupByAttributeValue);
		    Long[] removeTuplesPstnList = new Long[fillFrameTupleBuffer.size()]; 
		    //initialize to -1
		    for(int i =0; i<removeTuplesPstnList.length;i++){
		    	removeTuplesPstnList[i]=new Long(-1);
		    }
		    int iterList = 0;
			for( Map.Entry<Long, Tuple> fillFrameTuple: fillFrameTupleBuffer.entrySet()){
				int ff_groupByAttributeValue = Integer.parseInt(getValue(fillFrameTuple.getValue(), ff_groupByAttributeInputSchemaPosition));
				long ff_frameTimeAttributeValue = Long.parseLong(getValue(fillFrameTuple.getValue(), ff_timeAttributeInputSchemaPosition));
				if(fdTuple.isTupleInFrame(ff_groupByAttributeValue, ff_frameTimeAttributeValue)){
					fillFrameTuple.getValue().appendAttribute(new IntegerAttr(fs_frameIdAttributeValue));
					putTuple(fillFrameTuple.getValue(), 0);
					//fillFrameTupleBuffer.remove(fillFrameTuple);
					removeTuplesPstnList[iterList++] = fillFrameTuple.getKey();
				}
			}
			
			for(int i=0;i<removeTuplesPstnList.length;i++){
				if(-1==removeTuplesPstnList[i].longValue()){
					break;
				} else {
					fillFrameTupleBuffer.remove(removeTuplesPstnList[i]);
				}
			}
			
			// See if we already have a frame for this groupby attribute
		    if (this.frameStreamHashMap.containsKey(fs_groupByAttributeValue))
		    {
		       FrameDetectorForFillFrame fDetectorX = this.frameStreamHashMap.get(fs_groupByAttributeValue);
		       fDetectorX.insertTupleIntoFrame(fdTuple);
		    }
		    else
		    {
		    	FrameDetectorForFillFrame new_fBuilderX = new FrameDetectorForFillFrame(fs_groupByAttributeValue);
		        this.frameStreamHashMap.put(fs_groupByAttributeValue, new_fBuilderX);
		        new_fBuilderX.insertTupleIntoFrame(fdTuple);
		    }  
		    //System.out.println("end of processTuple for frameStream"+" "+fs_frameStartAttributeValue);
		    
		} else { //fillframestream
			
			//purge fill frame tuples in buffer if tupleSize is 14
			for(Tuple fillFrameTuple: fillFrameTupleBuffer){
				if(14==fillFrameTuple.size()){
					//putTuple(fillFrameTuple, streamId);
					putTuple(fillFrameTuple, 0);
					fillFrameTupleBuffer.remove(fillFrameTuple);
				}
			}
			
			// Extract information from tuple    
		    int ff_groupByAttributeValue = Integer.parseInt(getValue(tupleElement, ff_groupByAttributeInputSchemaPosition));
		    long ff_timeAttributeValue = Long.parseLong(getValue(tupleElement, ff_timeAttributeInputSchemaPosition));
		    boolean ff_frameIdentifiedFlag = false;
		    //check if tuple belongs to frame already in frameStreamHashMap, then purge else put in buffer
		    if (this.frameStreamHashMap.containsKey(ff_groupByAttributeValue))
		    {
		    	FrameDetectorForFillFrame frameDetectorByGroupby = frameStreamHashMap.get(ff_groupByAttributeValue);
		    	for(FrameDetectorTupleForFillFrame fdTuple: frameDetectorByGroupby.getFrameTuples()){
		    		if(fdTuple.isTupleInFrame(ff_groupByAttributeValue, ff_timeAttributeValue)){
		    			tupleElement.appendAttribute(new IntegerAttr(fdTuple.getFrameId()));
						//putTuple(tupleElement, streamId);
		    			putTuple(tupleElement, 0);
						ff_frameIdentifiedFlag = true;
		    		}
		    	}
		    	
		    } 
		  //if fill frame tuple belongs to groupby attr not in hashmap or tuple not in start and end time boundary
		    if(!ff_frameIdentifiedFlag) { 
		    	fillFrameTupleBuffer.put(new Long(ff_timeAttributeValue),tupleElement);
		    }
		   // System.out.println("end of processTuple for fillframeStream"+" "+ff_timeAttributeValue);
		}
    }
	
	protected void processPunctuation(Punctuation inputTuple, int streamId)
			  throws ShutdownException, InterruptedException  {
		long punctuatedUpToTime = -1;
		//Vector<FrameDetectorTupleForFillFrame> framesToOutputX = new Vector<FrameDetectorTupleForFillFrame>();
	    
		countPunct++;
		//System.out.print("Size of frames:");
		for (Map.Entry<Integer, FrameDetectorForFillFrame> entry: frameStreamHashMap.entrySet()) 
        {
    		int framesSize =entry.getValue().frameTuples.size();
    		avgFramesSizeBetweenPunct += framesSize;
    		if(framesSize>maxFramesSizeBetweenPunct){
    			maxFramesSizeBetweenPunct = framesSize;
    		}
        }
		int tuplesSize =fillFrameTupleBuffer.size();
		avgTuplesSizeBetweenPunct += tuplesSize;
		if(tuplesSize>maxTuplesSizeBetweenPunct){
			maxTuplesSizeBetweenPunct = tuplesSize;
		}
		
		//process punctuation only on fill frame stream
		if(0==streamId){
			String pGroupBy = getValue(inputTuple, ff_groupByAttributeInputSchemaPosition);
			long ff_frameStartAttributeValue = Long.parseLong(getValue(inputTuple, ff_timeAttributeInputSchemaPosition));
			//long fs_frameEndAttributeValue = Long.parseLong(getValue(inputTuple, fs_frameEndAttributeInputSchemaPosition));
		    // Case 1: Time and GroupBy Attribute (routerid)
		    if(!pGroupBy.equals("*"))
		    {
		    	int groupByAsInt = Integer.parseInt(pGroupBy);
		    	frameStreamHashMap.get(groupByAsInt).processPunctuationOnTimeAttribute(ff_frameStartAttributeValue);
		    }
		    else
		    {
		    	for (Map.Entry<Integer, FrameDetectorForFillFrame> entry: frameStreamHashMap.entrySet()) 
		        {
		    		entry.getValue().processPunctuationOnTimeAttribute(ff_frameStartAttributeValue);
		        }
		    	
		    }
		    
		    // Pass on punctuation.
		    //outputPunctuation(inputTuple, streamId, punctuatedUpToTime);
			//System.out.println("end of processPunctuation for fillframeStream"+" "+ff_frameStartAttributeValue);
		} else {
			//System.out.println("end of processPunctuation for frameStream");
		}
	}
	
	protected Tuple appendFrameId(Tuple inputTuple, int frameId, int steamId)
			throws ShutdownException, InterruptedException {

		inputTuple.appendAttribute(frameId);
		return inputTuple;
	}
	
	@Override
	public boolean isStateful() {
		 return true;
	}

	@Override
	protected void opInitFrom(LogicalOp op) {
		fs_groupByAttributeName = ((FillFrame) op).getFs_groupByAttributeName();
		fs_frameStartAttributeName = ((FillFrame) op).getFs_frameStartAttributeName();
		fs_frameEndAttributeName = ((FillFrame) op).getFs_frameEndAttributeName();
		fs_frameIdAttributeName = ((FillFrame) op).getFs_frameIdAttributeName();
		ff_groupByAttributeName = ((FillFrame) op).getFf_groupByAttributeName();
		ff_timeAttributeName = ((FillFrame) op).getFf_timeAttributeName();
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
		final PhysicalFillFrame frame = new PhysicalFillFrame();
		frame.fs_groupByAttributeName = this.fs_groupByAttributeName;
		frame.fs_frameStartAttributeName = this.fs_frameStartAttributeName;
		frame.fs_frameEndAttributeName = this.fs_frameEndAttributeName;
		frame.fs_frameIdAttributeName = this.fs_frameIdAttributeName;
		frame.ff_groupByAttributeName = this.ff_groupByAttributeName;
		frame.ff_timeAttributeName = this.ff_timeAttributeName;
	    return frame;
	}

	@Override
	public boolean equals(Object other) {
		if ((other == null) || !(other instanceof PhysicalFillFrame))
	    {
	      return false;
	    }
	    if (other.getClass() != PhysicalFillFrame.class)
	    {
	      return other.equals(this);
	    }
	    final PhysicalFillFrame f = (PhysicalFillFrame) other;
	    return this.fs_groupByAttributeName.equals(f.fs_groupByAttributeName)
	    		&& this.fs_frameStartAttributeName.equals(f.fs_frameStartAttributeName)
	    		&& this.fs_frameEndAttributeName.equals(f.fs_frameEndAttributeName)
	    		&& this.fs_frameIdAttributeName.equals(f.fs_frameIdAttributeName)
	    		&& this.ff_groupByAttributeName.equals(f.ff_groupByAttributeName)
	    		&& this.ff_timeAttributeName.equals(f.ff_timeAttributeName);
	}

	@Override
	public int hashCode() {
		return getArity();
	}
  
	//when stream is closed, then flush any frames still open in memory
	  public void streamClosed(int streamId) throws ShutdownException {
		  System.out.println("maximum frames size between punct:"+maxFramesSizeBetweenPunct+" and maximum tuples size between punct:"+maxTuplesSizeBetweenPunct);
		  System.out.println("avg frames size between punct:"+avgFramesSizeBetweenPunct/countPunct+" and avg tuples size between punct:"+avgTuplesSizeBetweenPunct/countPunct);
	  }
  
}
*/