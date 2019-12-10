package niagara.logical;

import java.util.ArrayList;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.LogicalProperty;

import org.w3c.dom.Element;

/**
 * Jim Miller (JGM), 05/21/2013
 * WindowSum, implemented for the 2013 DEBS Heatmap challenge.
 * 
 * Based off of WindowMax.
 */

@SuppressWarnings("unchecked")
public class WindowSum extends WindowAggregate {

	public void dump() {
		super.dump("windowSumOp");
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		ArrayList aggrAttrNames = new ArrayList();
		aggrAttrNames.add("sumattr");

		super.loadFromXML(e, inputProperties, aggrAttrNames);
	}

	protected WindowAggregate getInstance() {
		return new WindowSum();
	}
}
