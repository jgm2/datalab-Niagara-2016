package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.varType;

import org.w3c.dom.Element;

/**
 * This is the logical class for the delta frame operator.
 * 
 * @version 1.0
 * 
 */
public class DeltaFrame extends UnaryOperator
{
  // ===========================================================
  // Fields
  // ===========================================================
  private String mGroupByAttribute;
  private String mAggregateAttribute;
  private String mTime;
  private String mDelta;
  
  // ===========================================================
  // Constructors
  // ===========================================================  
  public DeltaFrame()
  {
    super();
  }
  
  public DeltaFrame(String pGroupByAttribute, String pAggregateAttribute, String pTime, String pDelta )
  {
    this.mGroupByAttribute = pGroupByAttribute;
    this.mAggregateAttribute = pAggregateAttribute;
    this.mTime = pTime;
    this.mDelta = pDelta;
  }
      
  // ===========================================================
  // Methods for/from SuperClass/Interfaces
  // ===========================================================  
  @Override
  public Op opCopy()
  {
    return new DeltaFrame(mGroupByAttribute, mAggregateAttribute, mTime, mDelta);
  }

  @Override
  public boolean equals(Object pOther)
  {
    if ((pOther == null) || !(pOther instanceof DeltaFrame))
    {
      return false;
    }
    if (pOther.getClass() != DeltaFrame.class)
    {
      return pOther.equals(this);
    }
    final DeltaFrame aggFrame = (DeltaFrame) pOther;
    return ((this.mAggregateAttribute.equals(aggFrame.mAggregateAttribute))
        && (this.mGroupByAttribute.equals(aggFrame.mGroupByAttribute))
        && (this.mDelta.equals(aggFrame.mDelta)) 
        && (this.mTime.equals(aggFrame.mTime)));
  }

  @Override
  public int hashCode()
  {
    final String hashString = this.mGroupByAttribute + this.mAggregateAttribute;
    return hashString.hashCode();
  }
  
  // ===========================================================
  // Methods
  // ===========================================================
  protected  DeltaFrame getInstance()
  {
    return new DeltaFrame(mGroupByAttribute, mAggregateAttribute, mTime, mDelta);
  }
 
  public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog) throws InvalidPlanException 
  {
    this.mGroupByAttribute = e.getAttribute("groupby");
    this.mAggregateAttribute = e.getAttribute("aggregateattr");
    this.mTime = e.getAttribute("time");
    this.mDelta = e.getAttribute("delta");    
  }
  
  public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) 
  {
    LogicalProperty inpLogProp = input[0];
    LogicalProperty result = inpLogProp.copy();
    
    result.addAttr(new Variable("frameId", varType.ELEMENT_VAR));
    result.addAttr(new Variable("frameStartTime", varType.ELEMENT_VAR));
    result.addAttr(new Variable("frameEndTime", varType.ELEMENT_VAR));
    result.addAttr(new Variable("frameFragmentStartTime", varType.ELEMENT_VAR));
    result.addAttr(new Variable("frameFragmentEndTime", varType.ELEMENT_VAR));
    result.addAttr(new Variable("frameStatus", varType.ELEMENT_VAR));
    result.addAttr(new Variable("frameGroupBy", varType.ELEMENT_VAR));
    result.addAttr(new Variable("frameLength", varType.ELEMENT_VAR));
    
    return result;
  }  
  
  //===========================================================
  // Getter & Setter
  // ===========================================================
  public String getGroupByAttribute()
  {
    return this.mGroupByAttribute;
  }

  public String getAggregateAttribute()
  {
    return this.mAggregateAttribute;
  }

  public String getTimeAttribute()
  {
    return this.mTime;
  }
  
  public String getTarget()
  {
    return this.mDelta;
  }
}
