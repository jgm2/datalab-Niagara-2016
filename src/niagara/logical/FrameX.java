package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.varType;

import org.w3c.dom.Element;

/**
 * This is a test performance based implementation of the frame operator
 * Want to see if a minimal detector scheme performs better than windows.
 * 
 * @version 1.0
 * 
 */
public class FrameX extends UnaryOperator
{
  private String groupByAttributeName;        // Grouping attribute (detectorid)
  private String timeAttributeName;           // Time attribute (timestamp)
  private String framePredicateAttributeName; // Predicate attribute from the Apply
  private int frameSize;                      // Minimum Duration 
  private int segmentationFlag;				  // Flag to decide between threshold only or segmentation frames
  
  public FrameX()
  {
    super();
  }
  
  
  public FrameX(String groupByAttribute, String timeAttribute, String framePredicateAttribute, int frameSize, int segmentationFlag)
  {

    this.groupByAttributeName = groupByAttribute;
    this.timeAttributeName = timeAttribute;
    this.framePredicateAttributeName = framePredicateAttribute;
    this.frameSize = frameSize;
    this.segmentationFlag = segmentationFlag;
  }
    
  public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog) throws InvalidPlanException 
  {    
    this.groupByAttributeName = e.getAttribute("groupby");
    this.timeAttributeName = e.getAttribute("timeattr");
    this.framePredicateAttributeName = e.getAttribute("framepredattr");
    this.frameSize = Integer.parseInt(e.getAttribute("framesize"));
    this.segmentationFlag = Integer.parseInt(e.getAttribute("segmentationflag"));
  }
  
  public String getGroupByAttributeName()
  {
    return this.groupByAttributeName;
  }

  public String getTimeAttributeName()
  {
    return this.timeAttributeName;
  }
  
  public String getFramePredicateAttributeName()
  {
    return this.framePredicateAttributeName;
  }
  
  public int getFrameSize()
  {
    return this.frameSize;
  }
  
  public int getSegmentationFlag()
  {
    return this.segmentationFlag;
  }
  
  public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {

    LogicalProperty inpLogProp = input[0];

    LogicalProperty result = inpLogProp.copy();
    
    result.addAttr(new Variable("frameId", varType.ELEMENT_VAR));
    result.addAttr(new Variable("frameStartTime", varType.ELEMENT_VAR));
    result.addAttr(new Variable("frameEndTime", varType.ELEMENT_VAR));
    result.addAttr(new Variable("frameGroupBy", varType.ELEMENT_VAR));
    
    return result;
  }


  @Override
  public Op opCopy()
  {
    final FrameX op = new FrameX(this.groupByAttributeName,
        this.timeAttributeName, this.framePredicateAttributeName, this.frameSize, this.segmentationFlag);
    return op;
  }


  @Override
  public boolean equals(final Object o)
  {
    if ((o == null) 
    		|| !(o instanceof FrameX)
    		)
    {
      return false;
    }
    if (o.getClass() != FrameX.class)
    {
      return o.equals(this);
    }
    final FrameX f = (FrameX) o;
    return (this.framePredicateAttributeName.equals(f.framePredicateAttributeName))
        && (this.frameSize == f.frameSize) && (this.groupByAttributeName == f.groupByAttributeName)
        && (this.segmentationFlag == f.segmentationFlag);
  }


  /*
   * (non-Javadoc)
   * 
   * @see niagara.optimizer.colombia.Op#hashCode()
   */
  @Override
  public int hashCode()
  {
    // Combining groupby and frameattribute strings and hashing over that good
    // enough?
    final String hashString = this.groupByAttributeName;
    return hashString.hashCode();
  }
}
