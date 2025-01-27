package niagara.physical.predicates;

import niagara.logical.Variable;
import niagara.logical.predicates.Constant;
import niagara.logical.predicates.VarToConstComparison;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.physical.SimpleAtomicEvaluator;
import niagara.query_engine.TupleSchema;
import niagara.utils.Tuple;

import org.w3c.dom.Node;

public class VarToConstComparisonImpl extends ComparisonImpl {
	// XXX vpapad: Changed it to use SimpleAtomicEvaluator instead of
	// full blown AtomicEvaluator (which also unnests paths)
	// Original code in PathToConstComparisonImpl.java

	private SimpleAtomicEvaluator leftAV;
	private String rightValue;
	private double sel;

	public VarToConstComparisonImpl(VarToConstComparison pred) {
		super(pred.getOperator());
		Variable left = (Variable) pred.getLeft();
		Constant right = (Constant) pred.getRight();
		sel = pred.selectivity();

		rightValue = right.getValue();
		leftAV = left.getSimpleEvaluator();
	}

	public boolean evaluate(Tuple t1, Tuple t2) {
		String av = leftAV.getAtomicValue(t1, t2);

		if (av == null)
			return false;

		return compareAtomicValues(av, rightValue);
	}

	public boolean evaluate(Node n) {
		String av = SimpleAtomicEvaluator.getAtomicValue(n);
		return compareAtomicValues(av, rightValue);
	}

	public void resolveVariables(TupleSchema ts, int streamId) {
		leftAV.resolveVariables(ts, streamId);
	}

	/**
	 * @see niagara.query_engine.PredicateImpl#getCost(ICatalog)
	 */
	public Cost getCost(ICatalog catalog) {
		// XXX vpapad: just use one blanket predicate cost for now
		return new Cost(catalog.getDouble("predicate_cost"));
	}

	/**
	 * @see niagara.query_engine.PredicateImpl#selectivity()
	 */
	public double selectivity() {
		return sel;
	}

}
