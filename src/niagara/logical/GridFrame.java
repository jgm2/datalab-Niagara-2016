package niagara.logical;

import java.util.Arrays;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.varType;

import org.w3c.dom.Element;

/**
 * Jim Miller (JGM), 05/09/2013
 * First attempt at a boundary frame operator for a 2-D grid, for the DEBS 2013 heatmap challenge.
 * 
 * Based off of FrameX.
 */

public class GridFrame extends UnaryOperator
{
  private String groupByAttributeName;       // Grouping attribute (pid)
  private String timeAttributeName;				// Time attribute (ts)
  private String xGridAttributeName;         // x-direction attribute (x)
  private String yGridAttributeName; 			// y-direction attribute (y)
  private int xCells;                      	// Number of grid subdivisions in the x direction (xcells)
  private int yCells;				  				// Number of grid subdivisions in the y direction (ycells)
  private int[] corners;							// Absolute values of the lower-left and upper-right corners (x1, y1, x2, y2) (corners)
  
  public GridFrame()
  {
    super();
  }
  
  public GridFrame(String groupByAttribute, String timeAttribute, String xGridAttribute, String yGridAttribute, int xCells, int yCells, int[] corners)
  {
    this.groupByAttributeName = groupByAttribute;
    this.timeAttributeName = timeAttribute;
    this.xGridAttributeName = xGridAttribute;
    this.yGridAttributeName = yGridAttribute;
    this.xCells = xCells;
    this.yCells = yCells;
    this.corners = new int[4];
    System.arraycopy(corners, 0, this.corners, 0, 4);
  }
    
  public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog) throws InvalidPlanException 
  {    
    this.groupByAttributeName = e.getAttribute("groupby");
    this.timeAttributeName = e.getAttribute("timeattr");
    this.xGridAttributeName = e.getAttribute("xgridattr");
    this.yGridAttributeName = e.getAttribute("ygridattr");
    this.xCells = Integer.parseInt(e.getAttribute("xcells"));
    this.yCells = Integer.parseInt(e.getAttribute("ycells"));
    String[] cornerStrings = parseInputAttrs(e.getAttribute("corners"));
    
    corners = new int[4];
    for (int i = 0; i < 4; i++) {
      corners[i] = Integer.parseInt(cornerStrings[i]);
    }
  }
  
  public String getGroupByAttributeName()
  {
    return this.groupByAttributeName;
  }
  
  
  public String getTimeAttributeName()
  {
    return this.timeAttributeName;
  }

  public String getXGridAttributeName()
  {
    return this.xGridAttributeName;
  }
  
  public String getYGridAttributeName()
  {
    return this.yGridAttributeName;
  }
  
  public int getXCells()
  {
    return this.xCells;
  }
  
  public int getYCells()
  {
    return this.yCells;
  }
  
  public int[] getCorners()
  {
    return this.corners;
  }
  
  public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
	  LogicalProperty result = input[0].copy();

	  result.addAttr(new Variable("st_ts", varType.ELEMENT_VAR));
	  result.addAttr(new Variable("end_ts", varType.ELEMENT_VAR));
	  result.addAttr(new Variable("cid", varType.ELEMENT_VAR));
	  result.addAttr(new Variable("c_midx", varType.ELEMENT_VAR));
	  result.addAttr(new Variable("c_midy", varType.ELEMENT_VAR));

	  return result;
  }

  @Override
  public Op opCopy()
  {
    final GridFrame op = new GridFrame(this.groupByAttributeName, this.timeAttributeName,
        this.xGridAttributeName, this.yGridAttributeName, this.xCells, this.yCells, this.corners);
    return op;
  }

  @Override
  public boolean equals(final Object o)
  {
    if ((o == null) || !(o instanceof GridFrame))
      return false;
    if (o.getClass() != GridFrame.class)
      return o.equals(this);
    final GridFrame f = (GridFrame) o;
    return (this.groupByAttributeName.equals(f.groupByAttributeName)) && (this.timeAttributeName.equals(f.timeAttributeName))
        && (this.xGridAttributeName.equals(f.xGridAttributeName)) && (this.yGridAttributeName.equals(f.yGridAttributeName))
        && (this.xCells == f.xCells) && (this.yCells == f.yCells) && Arrays.equals(this.corners, f.corners);
  }

  @Override
  public int hashCode()
  {
	 // (JGM) Probably not a good hashcode.
    final String hashString = this.groupByAttributeName;
    return hashString.hashCode();
  }
}
