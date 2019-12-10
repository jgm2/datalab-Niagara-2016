package niagara.physical;

import niagara.utils.BaseAttr;
import niagara.utils.LongAttr;
import niagara.utils.Tuple;

/**
 * Jim Miller (JGM), 05/21/2013
 * PhysicalWindowSum, implemented for the 2013 DEBS Heatmap challenge.
 * 
 * Based off of PhysicalWindowMax.
 */

public class PhysicalWindowLongSum extends PhysicalWindowLongAggregate {

	/**
	 * This function updates the statistics with a value
	 * 
	 * @param newValue
	 *            The value by which the statistics are to be updated
	 */
	public void updateAggrResult(PhysicalWindowLongAggregate.AggrResult result,
			Object ungroupedResult) {
		// Increment the number of values
		// KT - is this correct??
		// code from old mrege results:
		// finalResult.updateStatistics(((Integer) ungroupedResult).intValue());
		result.count++;
		// double newValue =
		// ((Double)((Vector)ungroupedResult).get(0)).doubleValue();
		result.longVal += ((Double)ungroupedResult).longValue();
	}

	// ///////////////////////////////////////////////////////////////////////
	// These functions are the hooks that are used to implement specific //
	// Count operator (specializing the group operator) //
	// ///////////////////////////////////////////////////////////////////////

	/**
	 * This function constructs a ungrouped result from a tuple
	 * 
	 * @param tupleElement
	 *            The tuple to construct the ungrouped result from
	 * 
	 * @return The constructed object; If no object is constructed, returns null
	 */

	protected final Object constructUngroupedResult(Tuple tupleElement) {

		// First get the atomic values
		atomicValues.clear();
		ae.get(0).getAtomicValues(tupleElement, atomicValues);

		assert atomicValues.size() == 1 : "Must have exactly one atomic value";
		return new Double(((BaseAttr) atomicValues.get(0)).toASCII());
	}

	/**
	 * This function returns an empty result in case there are no groups
	 * 
	 * @return The result when there are no groups. Returns null if no result is
	 *         to be constructed
	 */

	protected final BaseAttr constructEmptyResult() {
		return null;
	}

	/**
	 * This function constructs a result from the grouped partial and final
	 * results of a group. Both partial result and final result cannot be null
	 * 
	 * @param partialResult
	 *            The partial results of the group (this can be null)
	 * @param finalResult
	 *            The final results of the group (this can be null)
	 * 
	 * @return A results merging partial and final results; If no such result,
	 *         returns null
	 */

	protected final BaseAttr constructAggrResult(
			PhysicalWindowLongAggregate.AggrResult partialResult,
			PhysicalWindowLongAggregate.AggrResult finalResult) {

		// Create number of values and sum of values variables
		int numValues = 0;
		long sum = 0;

		if (partialResult != null) {
			numValues += partialResult.count;
			sum += (long)partialResult.longVal;
		}
		if (finalResult != null) {
			numValues += finalResult.count;
			sum += (long)finalResult.longVal;
		}

		// If the number of values is 0, sum does not make sense
		if (numValues == 0) {
			assert false : "KT don't think returning null is ok";
			return null;
		}

		LongAttr resultElement = new LongAttr(sum);
		return resultElement;
	}

	protected PhysicalWindowLongAggregate getInstance() {
		return new PhysicalWindowLongSum();
	}
}

