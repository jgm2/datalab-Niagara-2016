package niagara.logical;

import java.util.ArrayList;
import java.util.Vector;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.logical.predicates.Predicate;
import niagara.logical.predicates.True;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.varType;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * The <code>Append2</code> operator applies a predicate to a tuple and appends
 * a true/false attribute flag to the tuple indicating if the predicate was
 * satisfied or not. Implementation based on the Select operator. This
 * operator does not drop any tuples.
 */
@SuppressWarnings("unchecked")
public class Apply extends UnaryOperator {

	/** Number of tuples to allow through */
	private Predicate pred;
	private String applyattr; 

	public Apply() {
	}

	public Apply(Predicate pred, String applyattr) {
		this.pred = pred;
		this.applyattr = applyattr;
	}

	public Apply(Apply applyOp) {
		this(applyOp.pred, applyOp.applyattr);
	}

	public Op opCopy() {
		return new Apply(this);
	}

	/**
	 * print the operator to the standard output
	 */
	public void dump() {
		System.out.println("Append2 :");
		pred.dump(1);
	}

	public String toString() {
		return " apply " + pred.toString();
	}

	public String getApplyAttr()
	{
	  return this.applyattr;
	}
	
	public void dumpAttributesInXML(StringBuffer sb) {
		sb.append(" ");
	}

	public void dumpChildrenInXML(StringBuffer sb) {
		sb.append(">");
		pred.toXML(sb);
		sb.append("</apply>");
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {

	  applyattr = e.getAttribute("applyattr");
	  
		NodeList children = e.getChildNodes();
		Element predElt = null;
		for (int i = 0; i < children.getLength(); i++) {
			if (children.item(i) instanceof Element) {
				predElt = (Element) children.item(i);
				break;
			}
		}
		pred = Predicate.loadFromXML(predElt, inputProperties);
	}

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		LogicalProperty result = input[0].copy();
		result.setCardinality(result.getCardinality() * pred.selectivity());
		result.addAttr(new Variable("appliedattr", varType.ELEMENT_VAR));
		return result;
	}

	/**
	 * @see niagara.xmlql_parser.op_tree.op#requiredInputAttributes(Attrs)
	 */
	public Attrs requiredInputAttributes(Attrs inputAttrs) {
		ArrayList al = new ArrayList();
		pred.getReferencedVariables(al);
		Attrs reqd = new Attrs(al);
		assert inputAttrs.contains(reqd);
		return reqd;
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof Apply))
			return false;
		if (o.getClass() != Apply.class)
			return o.equals(this);
		Apply s = (Apply) o;
		return pred.equals(s.pred);
	}

	public int hashCode() {
		return pred.hashCode();
	}

	public Predicate getPredicate() {
		return pred;
	}

	public boolean isEmpty() {
		return pred.equals(True.getTrue());
	}

	/* Can we combine the apply selection with this unnesting? */
	public boolean isPushableInto(Unnest unnest) {
		ArrayList al = new ArrayList();
		pred.getReferencedVariables(al);
		return (al.size() == 1 && ((Variable) al.get(0)).equals(unnest
				.getVariable()));
	}

	/**
	 * used to set the predicate of Append2 operator. A list of predicate are
	 * ANDed to produce single predicate for this operator
	 * 
	 * @param list
	 *            of predicates KT - this is used by the trigger_engine *@#$*
	 */
	public void setApply(Vector _preds) {
		pred = niagara.xmlql_parser.Util.andPredicates(_preds);
	}

}
