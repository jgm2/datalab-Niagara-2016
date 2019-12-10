package niagara.utils;

import niagara.ndom.saxdom.DocumentImpl;
import niagara.ndom.saxdom.ElementImpl;
import niagara.utils.XMLAttr;


/*
 * The FrameOutput class stores individual frame information.
 */
public class FrameOutput
{  
  public int frameId;
  public String startTime;
  public String endTime;
  public String fragmentStart;
  public String fragmentEnd;
  public String frameStatus;
  public String groupAttr;
  public int length;
    
  FrameOutput(String start, String end, String fragStart, String fragEnd, String status, String attr, int frameid)
  {
    frameId = frameid;
    startTime = start;
    endTime = end;
    fragmentStart = fragStart;
    fragmentEnd = fragEnd;
    frameStatus = status;
    groupAttr = attr;
  }
  
  FrameOutput(String start, String end, String fragStart, String fragEnd, String status, String attr, int frameid, int pLength)
  {
    this(start, end, fragStart, fragEnd, status, attr, frameid);
    length = pLength;
  }
  
  int startTimeAsInt()
  {
    return Integer.parseInt(this.startTime);
  }
  
  int endTimeAsInt()
  {
    return Integer.parseInt(this.endTime);
  }
  
  int fragmentStartAsInt()
  {
    return Integer.parseInt(fragmentStart);
  }
  
  int fragmentEndAsInt()
  {
    return Integer.parseInt(fragmentEnd);
  }
  
  long startTimeAsLong()
  {
    return Long.parseLong(this.startTime);
  }
  
  long endTimeAsLong()
  {
    return Long.parseLong(this.endTime);
  }
  
  long fragmentStartAsLong()
  {
    return Long.parseLong(fragmentStart);
  }
  
  long fragmentEndAsLong()
  {
    return Long.parseLong(fragmentEnd);
  }
}