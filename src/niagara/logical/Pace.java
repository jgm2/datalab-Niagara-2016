package niagara.logical;

import java.lang.reflect.Array;
import java.util.ArrayList;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Domain;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.DOMHelper;

import org.w3c.dom.Element;

/**
 * This class is used to represent the Pace operator.
 */
public class Pace extends LogicalOperator {

	private int arity;
	private Attrs outputAttrs;
	private Attrs[] inputAttrs;
	private int numMappings;

	private Boolean propagate = false;
	private Boolean exploit = false;
	private Boolean logging = false;
	private String fattrsL = "";
	private String fattrsR = "";
	private String divergence;
	private String monitorAttr;

	public String getFAttrsL() {
		return fattrsL;
	}

	public String getFAttrsR() {
		return fattrsR;
	}

	
	public Boolean getPropagate(){
		return propagate;
	}
	
	public Boolean getExploit(){
		return exploit;
	}

	public Boolean getLogging() {
		return logging;
	}
	
	public void setArity(int arity) {
		this.arity = arity;
	}

	public int getArity() {
		return arity;
	}

	public Attrs getOutputAttrs() {
		return outputAttrs;
	}

	public Attrs[] getInputAttrs() {
		return inputAttrs;
	}

	public int numMappings() {
		return numMappings;
	}

	/**
	 * print the operator to the standard output
	 */
	public void dump() {
		System.out.println(this);
	}

	/**
	 * 
	 * @return the String representation of the operator
	 */
	public String toString() {
		return "Union";
	}

	/**
	 * @see niagara.optimizer.colombia.Op#copy()
	 */
	public Op opCopy() {
		Pace op = new Pace();
		op.setArity(arity);
		op.inputAttrs = inputAttrs;
		op.outputAttrs = outputAttrs;
		op.numMappings = numMappings;
		op.propagate = propagate;
		op.exploit = exploit;
		op.logging = logging;
		op.fattrsL = fattrsL;
		op.fattrsR = fattrsR;
		op.divergence = this.divergence;
		op.monitorAttr = this.monitorAttr;
		
		return op;
	}

	/**
	 * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog,
	 *      ArrayList)
	 */
	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		// We only propagate the variables from the first input
		// XXX vpapad: We should be very careful when pushing
		// project through union: before projecting out a variable from
		// the first input we should make sure to project out the
		// respective variables from all the other inputs

		// KT - change to support mappings - loadFromXML is
		// always called before this function
		if (numMappings == 0) {
			return input[0].copy();
		} else {
			LogicalProperty outLogProp = new LogicalProperty(numMappings,
					outputAttrs, true);

			// set up remote and local access
			boolean hasLocal = false;
			boolean hasRemote = false;
			for (int i = 0; i < arity
					&& (hasLocal == false || hasRemote == false); i++) {
				if (input[i].hasRemote())
					hasRemote = true;
				if (input[i].hasLocal())
					hasLocal = true;
			}
			outLogProp.setHasRemote(hasRemote);
			outLogProp.setHasLocal(hasLocal);
			return outLogProp;
		}
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Pace))
			return false;
		if (obj.getClass() != Pace.class)
			return obj.equals(this);
		if(((Pace)obj).propagate != propagate)
			return false;
		if(((Pace)obj).exploit != exploit)
			return false;
		if(((Pace)obj).logging != logging)
			return false;
		if(((Pace)obj).fattrsR != fattrsR)
			return false;
		if(((Pace)obj).logging != logging)
			return false;
		if(((Pace)obj).monitorAttr != monitorAttr)
			return false;
		if(((Pace)obj).divergence != divergence)
			return false;
		
		Pace other = (Pace) obj;
		return arity == other.arity && numMappings == other.numMappings
				&& outputAttrs.equals(other.outputAttrs)
				&& inputAttrs.equals(other.inputAttrs);
	}

	public int hashCode() {
		if (numMappings > 0)
			return arity ^ outputAttrs.hashCode() ^ inputAttrs.hashCode();
		else
			return arity;
	}

	/**
	 * @see niagara.optimizer.colombia.Op#matches(Op)
	 */
	public boolean matches(Op other) {
		if (arity == 0) // Special case arity = 0 => match any Union
			return (other instanceof Pace);
		return super.matches(other);
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		arity = inputProperties.length;

		outputAttrs = new Attrs(); // len will be numMappings, if have mappings
		inputAttrs = new Attrs[arity];

		
		String p = e.getAttribute("propagate");
		if(p.equals("yes"))
			propagate = true;
		else propagate = false;

		String ex = e.getAttribute("exploit");
		if(ex.equals("yes"))
			exploit = true;
		else exploit = false;
		
		String l = e.getAttribute("log");
		if(l.equals("yes"))
			logging = true;
		else logging = false;
		
		fattrsL = e.getAttribute("fattrsL");
		fattrsR = e.getAttribute("fattrsR");
		

		divergence = e.getAttribute("divergence");
		monitorAttr = e.getAttribute("monitor_attr");
		
		// get first child that is an element
		Element mapping = DOMHelper.getFirstChildElement(e);

		if (mapping == null) {
			numMappings = 0;
			// no mapping, verify union compatibility
			for (int i = 0; i < arity; i++) {
				if (inputProperties[i].getDegree() != inputProperties[0]
						.getDegree()) {
					throw new InvalidPlanException(
							"Union inputs are not union-compatible and no mapping specified");
				}
			}
		} else { // have mapping
			assert mapping.getNodeName().equals("mapping");
			numMappings = 1;
			while (mapping != null) {
				String outputAttrName = mapping.getAttribute("outputattr");
				String[] inputAttrNames = parseInputAttrs(mapping
						.getAttribute("inputattrs"));

				if (arity != Array.getLength(inputAttrNames))
					throw new InvalidPlanException("Bad arity in mapping");

				Domain outputDom = null;
				for (int i = 0; i < arity; i++) {
					// Get attribute for input - this also serves
					// to verify that the input attr is valid
					// findVariable throws InvalidPlan if it can't find variable
					Attribute attr;
					if (inputAttrNames[i].equalsIgnoreCase("NONE")) {
						attr = null;
					} else {
						attr = Variable.findVariable(inputProperties[i],
								inputAttrNames[i]);
					}
					if (inputAttrs[i] == null)
						inputAttrs[i] = new Attrs();
					inputAttrs[i].add(attr);
					if (outputDom == null && attr != null) {
						outputDom = attr.getDomain();
					} else {
						if (attr != null && outputDom != null
								&& !outputDom.equals(attr.getDomain())) {
						}
						// throw new
						// InvalidPlanException("Input types are not union compatible");
					}
				}
				outputAttrs.add(new Variable(outputAttrName, outputDom));
				// get next sibling, but get only elements
				mapping = DOMHelper.getNextSiblingElement(mapping);
				numMappings++;
			}
		}
	}
	
	public String getDivergence(){
		return divergence;
	}
	
	public String getMonitorAttr(){
		return monitorAttr;
	}

}
