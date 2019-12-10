
package niagara.utils;

import java.util.Collections;
import java.util.Vector;



/**
 * The frame detector object from frame stream for the fillframe operator 
 * 
 * @version 1.0
 * @author moorthy
 *
 */
public class FrameDetectorForFillFrame_BSTFrames
{ 
	private int groupAttr;
	//public Vector<FrameDetectorTupleForFillFrame> frameTuples ;
	private BinarySearchTree frameTuples;
	public long frameTuplesCount=0;
	
	public BinarySearchTree getFrameTuples() {
		return frameTuples;
	}

	public FrameDetectorForFillFrame_BSTFrames(int groupAttr){
		this.groupAttr = groupAttr;
		frameTuples = new BinarySearchTree();
	}
	
	public void insertTupleIntoFrame(FrameDetectorTupleForFillFrame fTuple) {
		frameTuples.insert(fTuple);
		frameTuplesCount++;
	}
	
	/*public Vector<FrameDetectorTupleForFillFrame> getFrameTuples() {
		return frameTuples;
	}

	public FrameDetectorForFillFrame(int groupAttr){
		this.groupAttr = groupAttr;
		frameTuples = new Vector<FrameDetectorTupleForFillFrame>();
	}
	
	public void insertTupleIntoFrame(FrameDetectorTupleForFillFrame fTuple) {
		frameTuples.add(fTuple);
	}*/

	public int getGroupAttr() {
		return groupAttr;
	}
	
	public long processPunctuationOnTimeAttribute(long pPuncuationTime)
	  {
		long lastFrameId = -1;
		
	    if(!frameTuples.isEmpty()) //return new Vector<FrameDetectorTupleForFillFrame>() ;
		    	
	    {   
		    //Vector<FrameDetectorTupleForFillFrame> framesToOutput = new Vector<FrameDetectorTupleForFillFrame>();
		    FrameDetectorTupleForFillFrame tuple;
		    FrameDetectorTupleForFillFrame lastFrameRemoved = null;
		    
		    tuple = (FrameDetectorTupleForFillFrame) frameTuples.findMin();
		    
		    // Process frameTuples up to punctuation
		    while(tuple.getFrameEndTime() <= pPuncuationTime)
		    //while(frameTuples.firstElement().getFrameEndTime() <= pPuncuationTime)
		    {
		      // Remove first element
		      frameTuples.removeMin();
		      lastFrameRemoved = tuple;
		      
		      frameTuplesCount--;
		      tuple = (FrameDetectorTupleForFillFrame) frameTuples.findMin();
		      
		      if(!frameTuples.isEmpty() && lastFrameRemoved.getFrameId() != tuple.getFrameId()){
		    		lastFrameId = lastFrameRemoved.getFrameId();
		      }
		      
		      if(frameTuples.isEmpty()){
		    	  lastFrameId = lastFrameRemoved.getFrameId();
		    	  break;
		      }
		    }
	    }
	    return lastFrameId;
	  }

}
