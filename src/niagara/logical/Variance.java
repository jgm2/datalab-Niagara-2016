package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.utils.BaseAttr;

import org.w3c.dom.Element;

/**
 * This is the class for the Variance operator, that is a type of group operator.
 * 
 * @version 1.0
 * @author moorthy
 * 
 */

public class Variance extends Aggregate {

	public void dump() {
		super.dump("VarianceOp");
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		/*String name = e.
		if(inputProperties[0].getAttrs().getAttr(name).getDataType()!=BaseAttr.getDataTypeFromString("integer")
				&& inputProperties[0].getAttrs().getAttr(name).getDataType()!=BaseAttr.getDataTypeFromString("long")){
			;
		}*/
		super.loadFromXML(e, inputProperties, "varianceattr");
	}

	protected Aggregate getInstance() {
		return new Variance();
	}
}
