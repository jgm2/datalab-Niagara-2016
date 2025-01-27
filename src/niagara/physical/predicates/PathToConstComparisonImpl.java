package niagara.physical.predicates;

import java.util.ArrayList;

import niagara.logical.Variable;
import niagara.logical.predicates.Constant;
import niagara.logical.predicates.PathToConstComparison;
import niagara.logical.predicates.VarToConstComparison;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.physical.AtomicEvaluator;
import niagara.query_engine.TupleSchema;
import niagara.utils.Tuple;

import org.w3c.dom.Node;

/**
 * Start with a tuple attribute, unnest a path, and compare the resulting values
 * with a constant - succeed if even one comparison succeeds.
 */
@SuppressWarnings("unchecked")
public class PathToConstComparisonImpl extends ComparisonImpl {
	private AtomicEvaluator leftAV;
	private String rightValue;
	private double sel;

	private ArrayList leftValues;

	public PathToConstComparisonImpl(VarToConstComparison pred) {
		super(pred.getOperator());
		Variable left = (Variable) pred.getLeft();
		Constant right = (Constant) pred.getRight();
		sel = pred.selectivity();

		rightValue = right.getValue();
		leftAV = left.getEvaluator(pred.getPath());
		leftValues = new ArrayList();
	}

	public PathToConstComparisonImpl(PathToConstComparison pred) {
		super(pred.getOperator());
		Variable left = (Variable) pred.getLeft();
		Constant right = (Constant) pred.getRight();
		sel = pred.selectivity();

		rightValue = right.getValue();
		leftAV = left.getEvaluator(pred.getPath());
		leftValues = new ArrayList();
	}

	public boolean evaluate(Tuple t1, Tuple t2) {
		// Get the vector of atomic values to be compared
		leftValues.clear();

		leftAV.getAtomicValues(t1, t2, leftValues);

		// Loop over every combination of values and check whether
		// predicate holds
		int numLeft = leftValues.size();

		for (int left = 0; left < numLeft; ++left) {
			if (compareAtomicValues((String) leftValues.get(left), rightValue))
				return true;
		}

		// The comparison failed - return false
		return false;
	}

	public boolean evaluate(Node n) {
		// Get the vector of atomic values to be compared
		leftValues.clear();

		leftAV.getAtomicValues(n, leftValues);

		// Loop over every combination of values and check whether
		// predicate holds
		//
		int numLeft = leftValues.size();

		for (int left = 0; left < numLeft; ++left) {
			if (compareAtomicValues((String) leftValues.get(left), rightValue))
				return true;
		}

		// The comparison failed - return false
		return false;

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
