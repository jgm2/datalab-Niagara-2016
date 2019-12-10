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
 * The operator that performs sid-to-pid mapping and foot-combining at the bottom of the windows version of
 * the heatmap plan, for the DEBS 2013 heatmap challenge.
 * This is very specific to the DEBS heatmap challenge and is not generalized.
 * 
 * Based off of HeatmapMapSidToPid, used in the frames version of the DEBS heatmap query.
 * 
 */

public class HeatmapPreprocessForWindowsGridQuery extends UnaryOperator {

	//	private int numDestinationStreams;
	//
	//	public void addDestinationStreams() {
	//		numDestinationStreams++;
	//	};
	
	private String xGridAttributeName;         // x-direction attribute (x)
	private String yGridAttributeName; 			// y-direction attribute (y)
	
	// (JGM) Not sure if necessary
	public HeatmapPreprocessForWindowsGridQuery()
	{
		super();
	}
	
	public HeatmapPreprocessForWindowsGridQuery(String xGridAttribute, String yGridAttribute)
	{
		this.xGridAttributeName = xGridAttribute;
		this.yGridAttributeName = yGridAttribute;
	}

	public void dump() {
		System.out.println("heatmapPreprocessForWindowsGridQueryOp");
	}

//	public int getNumberOfOutputs() {
//		return numDestinationStreams;
//	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog) throws InvalidPlanException 
	{    
		this.xGridAttributeName = e.getAttribute("xgridattr");
		this.yGridAttributeName = e.getAttribute("ygridattr");
	}
	
	public String getXGridAttributeName()
	{
		return this.xGridAttributeName;
	}

	public String getYGridAttributeName()
	{
		return this.yGridAttributeName;
	}

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		LogicalProperty result = input[0].copy();
		result.addAttr(new Variable("pid", varType.ELEMENT_VAR));
		return result;
	}

	public Op opCopy() {
		final HeatmapPreprocessForWindowsGridQuery op = new HeatmapPreprocessForWindowsGridQuery(this.xGridAttributeName,
			 this.yGridAttributeName);
		return op;
	}

	@Override
	public int hashCode()
	{
		// (JGM) Probably not a good hashcode.
		final String hashString = xGridAttributeName;
		return hashString.hashCode();
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof HeatmapPreprocessForWindowsGridQuery))
			return false;
		if (obj.getClass() != HeatmapPreprocessForWindowsGridQuery.class)
			return obj.equals(this); 
		HeatmapPreprocessForWindowsGridQuery other = (HeatmapPreprocessForWindowsGridQuery) obj;
		return (this.xGridAttributeName.equals(other.xGridAttributeName)) && (this.yGridAttributeName.equals(other.yGridAttributeName));
	}
}
