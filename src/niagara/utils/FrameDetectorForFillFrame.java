
package niagara.utils;

import java.util.List;
import java.util.Vector;

/**
 * The frame detector object from frame stream for the fillframe operator 
 * 
 * @version 1.0
 * @author moorthy
 *
 */
public class FrameDetectorForFillFrame
{ 
	private int groupAttr;
	private Vector<FrameDetectorTupleForFillFrame> frameTuples;
	public long frameTuplesCount=0;
	
	public Vector<FrameDetectorTupleForFillFrame> getFrameTuples() {
		return frameTuples;
	}

	public FrameDetectorForFillFrame(int groupAttr){
		this.groupAttr = groupAttr;
		frameTuples = new Vector<FrameDetectorTupleForFillFrame>();
	}
	
	public void insertTupleIntoFrame(FrameDetectorTupleForFillFrame fTuple) {
		frameTuples.add(fTuple);
		frameTuplesCount++;
	}
	
	public int getGroupAttr() {
		return groupAttr;
	}
	
	public long processPunctuationOnTimeAttribute(long pPuncuationTime)
	  {
		long lastFrameId = -1;
		
	    if(!frameTuples.isEmpty()) //return new Vector<FrameDetectorTupleForFillFrame>() ;
		    	
	    {   
	    	FrameDetectorTupleForFillFrame lastFrameRemoved = null;
		    // Process frameTuples up to punctuation
		    while(frameTuples.firstElement().getFrameEndTime() <= pPuncuationTime)
		    {
		      // Remove first element
		      lastFrameRemoved = frameTuples.remove(0);
		      if(!frameTuples.isEmpty() && lastFrameRemoved.getFrameId() != frameTuples.firstElement().getFrameId()){
		    		lastFrameId = lastFrameRemoved.getFrameId();
		      }
		      frameTuplesCount--;
		      
		      if(frameTuples.isEmpty()){
		    	  lastFrameId = lastFrameRemoved.getFrameId();
		    	  break;
		      }
		    }
		    
	    }
	    
	    return lastFrameId;
	  }
	
	public int searchFrame(List<FrameDetectorTupleForFillFrame> list, long x){
		int ceilIndex = ceilSearch(list, 0, list.size()-1, x);
		if(-1 != ceilIndex){
			FrameDetectorTupleForFillFrame frame = list.get(ceilIndex);
			if(x>=frame.getFrameStartTime() && x<=frame.getFrameEndTime()){
				return frame.getFrameId();
			} else {
				int index =ceilIndex;
				while(--index >= 0){
					FrameDetectorTupleForFillFrame testframe = list.get(ceilIndex);
					if(x>=testframe.getFrameStartTime() && x<=testframe.getFrameEndTime()){
						return testframe.getFrameId();
					}
				}
			}
		} 
		
		return -1;
	}
	
	/**
	 * source: http://www.geeksforgeeks.org/archives/7342
	 * @param list
	 * @param low
	 * @param high
	 * @param x
	 * @return
	 */
	public int ceilSearch(List<FrameDetectorTupleForFillFrame> list, int low,
			int high, long x) {
		int mid;
		/*
		 * If x is smaller than or equal to the first element, then return the
		 * first element
		 */
		if (x <= list.get(low).getFrameEndTime())
			return low;
		/* If x is greater than the last element, then return -1 */
		if (x > list.get(high).getFrameEndTime())
			return -1;
		/* get the index of middle element of arr[low..high] */
		mid = (low + high) / 2;
		long value = list.get(mid).getFrameEndTime();
		/* low + (high - low)/2 */
		/* If x is same as middle element, then return mid */
		if (value == x)
			return mid;
		/*
		 * If x is greater than arr[mid], then either arr[mid + 1] is ceiling of
		 * x or ceiling lies in arr[mid+1...high]
		 */
		else if (value < x) {
			if (mid + 1 <= high && x <= list.get(mid + 1).getFrameEndTime())
				return mid + 1;
			else
				return ceilSearch(list, mid + 1, high, x);
		}
		/*
		 * If x is smaller than arr[mid], then either arr[mid] is ceiling of x
		 * or ceiling lies in arr[mid-1...high]
		 */
		else {
			if (mid - 1 >= low && x > list.get(mid - 1).getFrameEndTime())
				return mid;
			else
				return ceilSearch(list, low, mid - 1, x);
		}
	}

}
