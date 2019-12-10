package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.varType;

import org.w3c.dom.Element;

/**
 * Jim Miller (JGM), 04/2013
 * First attempt at the conversion operator at the bottom of the heatmap plan, for the DEBS 2013 heatmap challenge.
 * This is very specific to the DEBS heatmap challenge and is not generalized.
 */

public class HeatmapMapSidToPid extends UnaryOperator {

//	private int numDestinationStreams;
//
//	public void addDestinationStreams() {
//		numDestinationStreams++;
//	};

	public void dump() {
		System.out.println("heatmapMapSidToPidOp");
	}

//	public int getNumberOfOutputs() {
//		return numDestinationStreams;
//	}

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		LogicalProperty result = input[0].copy();
		Attribute newattr = new Variable("pid", NodeDomain.getDOMNode());
		result.addAttr(newattr);
		return result;
	}

	public Op opCopy() {
		HeatmapMapSidToPid op = new HeatmapMapSidToPid();
//		op.numDestinationStreams = numDestinationStreams;
		return op;
	}

	public int hashCode() {
//		return numDestinationStreams;
		return 0;
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof HeatmapMapSidToPid))
			return false;
		if (obj.getClass() != HeatmapMapSidToPid.class)
			return obj.equals(this); 
		HeatmapMapSidToPid other = (HeatmapMapSidToPid) obj;
//		return numDestinationStreams == other.numDestinationStreams;
		return true;
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
//		String branchAttr = e.getAttribute("branch");
		// XXX vpapad: catch format exception, check that we really have
		// that many output streams - why do we have to specify this here?
/*		numDestinationStreams = Integer.parseInt(branchAttr);*/
	}
}
