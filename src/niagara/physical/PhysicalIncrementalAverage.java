package niagara.physical;

import java.util.ArrayList;

import niagara.logical.IncrementalAverage;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.Op;
import niagara.utils.Tuple;

import org.w3c.dom.Node;

@SuppressWarnings("unchecked")
public class PhysicalIncrementalAverage extends PhysicalIncrementalGroup {
	private Attribute avgAttribute;
	private AtomicEvaluator ae;
	private ArrayList values;

	class GroupStatistics {
		int count;
		double sum;
	}

	public void opInitFrom(LogicalOp logicalOperator) {
		super.opInitFrom(logicalOperator);
		// Get the averaging attribute from the logical operator
		avgAttribute = ((IncrementalAverage) logicalOperator).getAvgAttribute();
	}

	public void opInitialize() {
		super.opInitialize();
		ae = new AtomicEvaluator(avgAttribute.getName());
		ae.resolveVariables(inputTupleSchemas[0], 0);
		values = new ArrayList();
	}

	/**
	 * @see niagara.query_engine.PhysicalIncrementalGroup#processTuple(Tuple,
	 *      Object)
	 */
	public Object processTuple(Tuple tuple, Object previousGroupInfo) {
		GroupStatistics prevAverage = (GroupStatistics) previousGroupInfo;
		GroupStatistics newAverage;

		ae.getAtomicValues(tuple, values);
		try {
			double newValue = Double.parseDouble((String) values.get(0));
			values.clear();
			newAverage = new GroupStatistics();

			// New group
			if (prevAverage == null) {
				newAverage.count = 1;
				newAverage.sum = newValue;
				return newAverage;
			}
			// We already have statistics for the group
			newAverage.count = prevAverage.count + 1;
			newAverage.sum = prevAverage.sum + newValue;
			double newAvg = newAverage.sum / newAverage.count;
			double prevAvg = prevAverage.sum / prevAverage.count;
			// We're messing with floating point arithmetic here,
			// this test may fail...
			if (newAvg != prevAvg)
				return newAverage;
			else
				// No change in group
				return prevAverage;
		} catch (NumberFormatException nfe) {
			throw new RuntimeException("XXX vpapad what do we do here?!");
		}
	}

	/**
	 * @see niagara.query_engine.PhysicalIncrementalGroup#emptyGroupValue()
	 */
	public Object emptyGroupValue() {
		return null;
	}

	/**
	 * @see niagara.query_engine.PhysicalIncrementalGroup#constructOutput(Object)
	 */
	public Node constructOutput(Object groupInfo) {
		GroupStatistics gs = (GroupStatistics) groupInfo;
		double avg = gs.sum / gs.count;
		return doc.createTextNode(String.valueOf(avg));
	}

	public Op opCopy() {
		PhysicalIncrementalAverage op = new PhysicalIncrementalAverage();
		if (logicalGroupOperator != null)
			op.initFrom(logicalGroupOperator);
		return op;
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof PhysicalIncrementalAverage))
			return false;
		if (o.getClass() != PhysicalIncrementalAverage.class)
			return o.equals(this);
		return logicalGroupOperator
				.equals(((PhysicalIncrementalAverage) o).logicalGroupOperator);
	}

	public int hashCode() {
		return logicalGroupOperator.hashCode();
	}
}
