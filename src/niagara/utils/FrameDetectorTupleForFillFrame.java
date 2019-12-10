package niagara.utils;

/**
 * The FillFrameTuple class stores individual tuple information of the fillframe stream
 * 
 * @version 1.0
 * @author moorthy
 *
 */
public class FrameDetectorTupleForFillFrame implements Comparable<FrameDetectorTupleForFillFrame>
{  
  private int frameId;
  private long frameStartTime;
  private long frameEndTime;
  private int groupbyId;
  
  public FrameDetectorTupleForFillFrame(int frameId, long frameStartTime, long frameEndTime, int groupbyId){
	  this.frameId = frameId;
	  this.frameStartTime = frameStartTime;
	  this.frameEndTime = frameEndTime;
	  this.groupbyId = groupbyId;
  }
  
  public FrameDetectorTupleForFillFrame(long frameEndTime){
	  this.frameEndTime = frameEndTime;
  }
  
  public int getFrameId() {
	return frameId;
  }

  public long getFrameStartTime() {
		return frameStartTime;
	}

  public long getFrameEndTime() {
		return frameEndTime;
	}
	
  public int getGroupbyId() {
	return groupbyId;
}

public boolean isTupleInFrame(int groupbyattr, long timeattr){
	  if((groupbyId == groupbyattr) &&  (timeattr>=frameStartTime) && (timeattr<=frameEndTime)){
		  return true;
	  } else {
		  return false;
	  }
  }

public int compareTo(FrameDetectorTupleForFillFrame fdTuple)
{
	//we search for frametuple (in BinarySearchTree of frameTuples of same groupby attr) corresponding to a fillframetuple by the endTime, 
	//i.e. if fillframeTuple starttime < current node endTime, then the node corresponds to fillframe frameid. Here we assume frames do not overlap.
  if (this.frameEndTime < fdTuple.frameEndTime)
    return -1;
  else if (this.frameEndTime == fdTuple.frameEndTime)
    return 0;
  else
    return 1;
}
}