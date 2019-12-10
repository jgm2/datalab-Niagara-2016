package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;

/**
 * Jim Miller (JGM), 05/17/2013
 * An operator to split existing frames along regular window boundaries, for the DEBS 2013 heatmap challenge.
 * 
 * Based off of GridFrame.
 */

public class SplitFrameOnWindowBoundary extends UnaryOperator
{
  private String time1AttributeName;
  private String time2AttributeName;
  private long startTime;
  private long boundaryGap;
  
  public SplitFrameOnWindowBoundary()
  {
    super();
  }
  
  public SplitFrameOnWindowBoundary(String time1Attribute, String time2Attribute, long startTime, long boundaryGap)
  {
    this.time1AttributeName = time1Attribute;
    this.time2AttributeName = time2Attribute;
    this.startTime = startTime;
    this.boundaryGap = boundaryGap;
  }
    
  public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog) throws InvalidPlanException 
  {    
    this.time1AttributeName = e.getAttribute("time1attr");
    this.time2AttributeName = e.getAttribute("time2attr");
    this.startTime = Long.parseLong(e.getAttribute("starttime"));
    this.boundaryGap = Long.parseLong(e.getAttribute("boundarygap"));
  }
  
  public String getTime1AttributeName()
  {
    return this.time1AttributeName;
  }
  
  
  public String getTime2AttributeName()
  {
    return this.time2AttributeName;
  }

  public long getStartTime()
  {
    return this.startTime;
  }
  
  public long getBoundaryGap()
  {
    return this.boundaryGap;
  }
  
  public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
	  LogicalProperty result = input[0].copy();

	  return result;
  }

  @Override
  public Op opCopy()
  {
    final SplitFrameOnWindowBoundary op = new SplitFrameOnWindowBoundary(this.time1AttributeName, this.time2AttributeName,
   		 this.startTime, this.boundaryGap);
    return op;
  }

  @Override
  public boolean equals(final Object o)
  {
    if ((o == null) || !(o instanceof SplitFrameOnWindowBoundary))
      return false;
    if (o.getClass() != SplitFrameOnWindowBoundary.class)
      return o.equals(this);
    final SplitFrameOnWindowBoundary f = (SplitFrameOnWindowBoundary) o;
    return (this.time1AttributeName.equals(f.time1AttributeName)) && (this.time2AttributeName.equals(f.time2AttributeName))
        && (this.startTime == f.startTime) && (this.boundaryGap == f.boundaryGap);
  }

  @Override
  public int hashCode()
  {
	 // (JGM) Probably not a good hashcode.
    final String hashString = this.time2AttributeName;
    return hashString.hashCode();
  }
}
