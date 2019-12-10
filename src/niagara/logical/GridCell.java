package niagara.logical;

import java.util.Arrays;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.varType;

import org.w3c.dom.Element;

/**
 * Jim Miller (JGM), 06/2013, 11/2013
 * The operator that performs cid-determination in the windows version of
 * the heatmap plan, for the DEBS 2013 heatmap challenge.
 * 
 * This is a cell-determiner for a 2-D grid, as a component of a series of operators which calculate "2-D windows".
 * 
 * Based off of HeatmapMapSidToPid, used in the frames version of the DEBS heatmap query.
 * 
 */

public class GridCell extends UnaryOperator {

	//	private int numDestinationStreams;
	//
	//	public void addDestinationStreams() {
	//		numDestinationStreams++;
	//	};
	
	private String xCoordAttributeName;       // x-direction attribute (x)
	private String yCoordAttributeName; 		// y-direction attribute (y)
	private int xCells;                      	// Number of grid subdivisions in the x direction (xcells)
	private int yCells;				  				// Number of grid subdivisions in the y direction (ycells)
	private int[] corners;							// Absolute values of the lower-left and upper-right corners (x1, y1, x2, y2) (corners)
	
	// (JGM) Not sure if necessary
	public GridCell()
	{
		super();
	}
	
	public GridCell(String xCoordAttribute, String yCoordAttribute, int xCells, int yCells, int[] corners)
	{
		this.xCoordAttributeName = xCoordAttribute;
		this.yCoordAttributeName = yCoordAttribute;
		this.xCells = xCells;
		this.yCells = yCells;
		this.corners = new int[4];
		System.arraycopy(corners, 0, this.corners, 0, 4);
	}

	public void dump() {
		System.out.println("gridCellOp");
	}

//	public int getNumberOfOutputs() {
//		return numDestinationStreams;
//	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog) throws InvalidPlanException 
	{    
		this.xCoordAttributeName = e.getAttribute("xcoord");
		this.yCoordAttributeName = e.getAttribute("ycoord");
		this.xCells = Integer.parseInt(e.getAttribute("xcells"));
		this.yCells = Integer.parseInt(e.getAttribute("ycells"));
		String[] cornerStrings = parseInputAttrs(e.getAttribute("corners"));

		corners = new int[4];
		for (int i = 0; i < 4; i++) {
			corners[i] = Integer.parseInt(cornerStrings[i]);
		}
	}
	
	public String getXCoordAttributeName()
	{
		return this.xCoordAttributeName;
	}

	public String getYCoordAttributeName()
	{
		return this.yCoordAttributeName;
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
		result.addAttr(new Variable("cid", varType.ELEMENT_VAR));
		result.addAttr(new Variable("c_midx", varType.ELEMENT_VAR));
		result.addAttr(new Variable("c_midy", varType.ELEMENT_VAR));
		return result;
	}

	public Op opCopy() {
		final GridCell op = new GridCell(this.xCoordAttributeName,
			 this.yCoordAttributeName, this.xCells, this.yCells, this.corners);
		return op;
	}

	@Override
	public int hashCode()
	{
		// (JGM) Probably not a good hashcode.
		final String hashString = String.valueOf(xCells) + String.valueOf(yCells);
		return hashString.hashCode();
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof GridCell))
			return false;
		if (obj.getClass() != GridCell.class)
			return obj.equals(this); 
		GridCell other = (GridCell) obj;
		return (this.xCoordAttributeName.equals(other.xCoordAttributeName)) && (this.yCoordAttributeName.equals(other.yCoordAttributeName))
			 && (this.xCells == other.xCells) && (this.yCells == other.yCells) && Arrays.equals(this.corners, other.corners);
	}
}
