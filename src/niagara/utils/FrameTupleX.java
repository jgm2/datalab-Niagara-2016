package niagara.utils;

import niagara.ndom.saxdom.DocumentImpl;
import niagara.ndom.saxdom.ElementImpl;
import niagara.utils.XMLAttr;


/*
 * The FrameTuple class stores tuple information.
 * 
 * Implements the compareTo function to allow comparison sorting by time.
 */
public class FrameTupleX implements Comparable<FrameTupleX>
{
  
  private int mGroupByAttr;
  private long mTimeAttr;
  private boolean mFramePred;
  
  private FrameTupleX()
  {
    this.mGroupByAttr = -1;
    this.mTimeAttr = -1;
    this.mFramePred = false;
  }
  
  public FrameTupleX(int pGroupByAttr, long pTimeAttr)
  {
    this.mGroupByAttr = pGroupByAttr;
    this.mTimeAttr = pTimeAttr;
    this.mFramePred = false;
  }
  
  // Router example
  // groupbyattr = routerid
  // frameattr   = packetloss
  // timeattr    = time
  public FrameTupleX(int pGroupByAttr, long pTimeAttr, boolean pFramePredAttr)
  {
    this.mGroupByAttr = pGroupByAttr;
    this.mTimeAttr = pTimeAttr;
    this.mFramePred = pFramePredAttr;
  }
  
  public int getGroupbyAttr()
  {
    return this.mGroupByAttr;
  }

  public void setGroupbyAttr(int pGroupByAttr)
  {
    this.mGroupByAttr = pGroupByAttr;
  }

  public long getTimeAttr()
  {
    return this.mTimeAttr;
  }

  public void setTimeAttr(long pTimeAttr)
  {
    this.mTimeAttr = pTimeAttr;
  }

  public boolean getFramePredAttr()
  {
    return this.mFramePred;
  }

  public void setFramePredAttr(boolean pFramePredAttr)
  {
    this.mFramePred = pFramePredAttr;
  }

  
  public int compareTo(FrameTupleX pTuple)
  {
    if (this.mTimeAttr < pTuple.mTimeAttr)
      return -1;
    else if (this.mTimeAttr == pTuple.mTimeAttr)
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
        "Predicate:" + "[" + mFramePred + "].";
    
    return output;
  }
}
