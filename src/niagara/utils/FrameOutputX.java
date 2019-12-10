package niagara.utils;

/*
 * The FrameOutput class stores individual frame information.
 */
public class FrameOutputX
{  
  public int frameId;
  public long startTime;
  public long endTime;
  public int groupAttr;
    
  public FrameOutputX(){
	  
  }
  
  public FrameOutputX(long start, long end, int attr, int frameid)
  {
    frameId = frameid;
    startTime = start;
    endTime = end;
    groupAttr = attr;
  }

  long getStartTime()
  {
    return this.startTime;
  }
  
  long getEndTime()
  {
    return this.endTime;
  }

public void setEndTime(long endTime) {
	this.endTime = endTime;
}
}