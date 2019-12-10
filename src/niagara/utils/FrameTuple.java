package niagara.utils;

import niagara.ndom.saxdom.DocumentImpl;
import niagara.ndom.saxdom.ElementImpl;
import niagara.utils.XMLAttr;


/*
 * The FrameTuple class stores tuple information.
 * 
 * Implements the compareTo function to allow comparison sorting by time.
 */
public class FrameTuple implements Comparable<FrameTuple>
{
  
  private String mGroupByAttr;
  private String mTimeAttr;
  private String mFrameAttr;
  private String mFramePredAttr;
  private long mTimeAttrAsLong;
  private int mTimeAttrAsInt;
  
  public FrameTuple()
  {
    this.mGroupByAttr = null;
    this.mTimeAttr = null;
    this.mFrameAttr = null;
    this.mFramePredAttr = null;
  }
  
  public FrameTuple(String pGroupByAttr, String pTimeAttr, String pFrameAttr)
  {
    this.mGroupByAttr = pGroupByAttr;
    this.mTimeAttr = pTimeAttr;
    this.mFrameAttr = pFrameAttr; 
    try
    {
      this.mTimeAttrAsLong = Long.parseLong(this.mTimeAttr);
      this.mTimeAttrAsInt = Integer.parseInt(this.mTimeAttr);
    }
    catch (NumberFormatException e)
    {
      // If time attribute is in long format it might not fit in an int
    }
  }
  
  // Router example
  // groupbyattr = routerid
  // frameattr   = packetloss
  // timeattr    = time
  public FrameTuple(String pGroupByAttr, String pTimeAttr, String pFrameAttr, String pFramePredAttr)
  {
    this.mGroupByAttr = pGroupByAttr;
    this.mTimeAttr = pTimeAttr;
    this.mFrameAttr = pFrameAttr;
    this.mFramePredAttr = pFramePredAttr;
  }
  
  public String getGroupbyAttr()
  {
    return this.mGroupByAttr;
  }

  public void setGroupbyAttr(String pGroupByAttr)
  {
    this.mGroupByAttr = pGroupByAttr;
  }

  public String getTimeAttr()
  {
    return this.mTimeAttr;
  }

  public void setTimeAttr(String pTimeAttr)
  {
    this.mTimeAttr = pTimeAttr;
  }

  public String getFrameAttr()
  {
    return this.mFrameAttr;
  }

  public void setFrameAttr(String pFrameAttr)
  {
    this.mFrameAttr = pFrameAttr;
  }

  public String getFramePredAttr()
  {
    return this.mFramePredAttr;
  }

  public void setFramePredAttr(String pFramePredAttr)
  {
    this.mFramePredAttr = pFramePredAttr;
  }

  
  public int compareTo(FrameTuple pTuple)
  {
    if (this.mTimeAttrAsLong < pTuple.mTimeAttrAsLong)
      return -1;
    else if (this.mTimeAttrAsLong == pTuple.mTimeAttrAsLong)
      return 0;
    else
      return 1;
  }
  
  public String toString()
  {
    String output;
    // Debug printout information
    output = "Time: " + "[" + mTimeAttr + "], " +
        "Group By:" + "[" + mGroupByAttr + "], " +
        "Frame Attr:" + "[" + mFrameAttr + "], " +
        "Predicate:" + "[" + mFramePredAttr + "].";
    
    return output;
  }
  
  int timeAttrAsInt()
  {
    return this.mTimeAttrAsInt;
  }
  
  long timeAttrAsLong()
  {
    return this.mTimeAttrAsLong;
  }
}
