package niagara.physical;

import niagara.utils.Arithmetics;
import niagara.utils.BaseAttr;
import niagara.utils.DoubleAttr;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

import org.w3c.dom.Node;

/**
 * This is the <code>PhysicalVarianceOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of variance (a form
 * of grouping)
 * 
 * @version 1.0
 * @author moorthy
 * 
 */

public class PhysicalVariance extends PhysicalAggregate {

	/**
	 * This function updates the statistics with a value
	 * 
	 * @param newValue
	 *            The value by which the statistics are to be updated
	 */
	public void updateAggrResult(PhysicalAggregate.AggrResult result,
			BaseAttr ungroupedResult) {
		// Object ungroupedResult) {
		
		result.count++;
		
		if (null != result.value) {
			result.value = ((Arithmetics) result.value).plus(ungroupedResult);
		} else {
			result.value = ungroupedResult;
		}
		
		BaseAttr squareUngroupedResult = ((Arithmetics) ungroupedResult).multiply(ungroupedResult);
		if(null != result.secondValue){
			result.secondValue = ((Arithmetics) result.secondValue).plus(squareUngroupedResult);
		} else {
			result.secondValue = squareUngroupedResult;
		}
		
	}

	// ///////////////////////////////////////////////////////////////////////
	// These functions are the hooks that are used to implement specific //
	// average operator (specializing the group operator) //
	// ///////////////////////////////////////////////////////////////////////

	/**
	 * This function constructs a ungrouped result from a tuple
	 * 
	 * @param tupleElement
	 *            The tuple to construct the ungrouped result from
	 * 
	 * @return The constructed object; If no object is constructed, returns null
	 */

	protected final BaseAttr constructUngroupedResult(Tuple tupleElement)
			throws ShutdownException {
		return getValue(tupleElement);
	}

	/**
	 * This function returns an empty result in case there are no groups
	 * 
	 * @return The result when there are no groups. Returns null if no result is
	 *         to be constructed
	 */
	protected final Node constructEmptyResult() {
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
			PhysicalAggregate.AggrResult partialResult,
			PhysicalAggregate.AggrResult finalResult) {

		// Create number of values and number list of values variables
		int numValues = 0;
		BaseAttr sum = null;
		BaseAttr squaredSum = null;
		BaseAttr variance = null;

		if (null != partialResult) {
			numValues += partialResult.count;
			sum = partialResult.value;
			squaredSum = partialResult.secondValue;
		}
		
		if (null != finalResult) {
			numValues += finalResult.count;
			if (null != sum){
				sum = ((Arithmetics) sum).plus(finalResult.value);
			} else {
				sum = finalResult.value;
			}
			//BaseAttr squaredFinalResult = ((Arithmetics) finalResult.value).multiply(finalResult.value);
			if(null != squaredSum){
				squaredSum = ((Arithmetics) squaredSum).plus(finalResult.secondValue);
			} else {
				squaredSum = finalResult.secondValue;
			}
		}
		
		if(null != squaredSum){
			
			//find variance
			DoubleAttr varianceDbl = new DoubleAttr();
			
			double squaredSumVal =  ( (Double) ((DoubleAttr)squaredSum).attrVal()).doubleValue();
			double sumVal =  ( (Double) ((DoubleAttr)sum).attrVal()).doubleValue();
			double varianceVal = (squaredSumVal - ((sumVal*sumVal)/numValues))/numValues;
			
			varianceDbl.loadFromValue(varianceVal);
			variance = varianceDbl;
	
			// If the number of values is 0, average does not make sense
			if (numValues == 0) {
				assert false : "KT don't think returning null is ok";
				// return null;
			}
		}

		// Create an variance result element and return it
		return variance;
	}

	protected PhysicalAggregate getInstance() {
		return new PhysicalVariance();
	}
		
}
