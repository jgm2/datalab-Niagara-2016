package niagara.logical;

import java.util.StringTokenizer;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.connection_server.NiagraServer;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.utils.SerializableToXML;

import org.w3c.dom.Element;

/**
 * This class is used as a base class for all the classes that represents
 * different logical operators.
 * 
 */

@SuppressWarnings("unchecked")
public abstract class LogicalOperator extends LogicalOp implements
		SerializableToXML {
	// This is the index of the selected algorithm
	int selectedAlgorithmIndex;

	public LogicalOperator() {
		// Initially, no algorithm is selected
		selectedAlgorithmIndex = -1;
	}

	/**
	 * @return name of the operator
	 */
	public String getName() {
		return NiagraServer.getCatalog().getOperatorName(getClass());
	}

	/**
	 * @return the list of Physical Operator Classes that implements this
	 *         operator
	 */
	public Class[] getListOfAlgo() {
		return NiagraServer.getCatalog().getPhysical(getClass().getName());
	}

	/**
	 * This function sets sets the selected algorithm index
	 * 
	 * @param Index
	 *            of selected algorithm in list of algorithms
	 */

	public void setSelectedAlgoIndex(int index) {
		// Set the index
		selectedAlgorithmIndex = index;
	}

	/**
	 * Select the physical algorithm implemented by this class
	 * 
	 * @param className
	 *            name of the physical algorithm class
	 */

	public void setSelectedAlgorithm(String className)
			throws ClassNotFoundException, InvalidAlgorithmException {
		Class c = Class.forName(className);

		Class[] algoList = getListOfAlgo();
		for (int i = 0; i < algoList.length; i++) {
			if (algoList[i] == c) {
				setSelectedAlgoIndex(i);
				return;
			}
		}
	}

	@SuppressWarnings("serial")
	public class InvalidAlgorithmException extends Exception {
	}

	/**
	 * @return the index of the selected algorithm
	 */

	public int getSelectedAlgoIndex() {
		return selectedAlgorithmIndex;
	}

	/**
	 * This function returns the selected algorithm class. If no algorithm is
	 * selected, it returns null
	 * 
	 * @return Returns the class of selected algorithm; if no selected
	 *         algorithm, returns null
	 */

	public Class getSelectedAlgo() {
		// If there is no selected algorithm return null
		if (selectedAlgorithmIndex < 0) {
			return null;
		} else {
			// Return selected algorith
			return getListOfAlgo()[selectedAlgorithmIndex];
		}
	}

	/**
	 * to print the information on the screen
	 */
	public void dump() {
		System.out.println(getName());
	}

	public void dumpAttributesInXML(StringBuffer sb) {
	}

	/**
	 * Close the element tag, append the children of this operator to the string
	 * buffer, append the end element tag if necessary
	 */
	public void dumpChildrenInXML(StringBuffer sb) {
		sb.append("/>");
	}

	// source ops are those that read off an input source and
	// feed that source into the operator tree, sourceOps have
	// different control structures than normal operators and
	// are not inherited from PhysicalOperator, sourceOps can
	// not be at the head of a query
	public boolean isSourceOp() {
		return false; // default is false
	}

	/** Is this operator schedulable locally? */
	public boolean isSchedulable() {
		return true;
	}

	/** Set up this logical operator from an XML description */
	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		// XXX vpapad: make this method abstract once
		// every operator implements it
		throw new InvalidPlanException(
				"This operator does not implement loadFromXML yet");
	}

	/** Attributes required by this operator locally */
	public Attrs requiredInputAttributes(Attrs inputAttrs) {
		// Default: the operator requires all input attributes
		return inputAttrs;
	}

	/**
	 * Implementations of this operator are free to project away any attribute
	 * that is not included in outputAttrs.
	 */
	public void projectedOutputAttributes(Attrs outputAttrs) {
		// Default: do nothing
	}

	protected String[] parseInputAttrs(String inputAttrString) {
		StringTokenizer tokenizer = new StringTokenizer(inputAttrString, ", ");
		String[] parsedAttrs = new String[tokenizer.countTokens()];
		int i = 0;
		while (tokenizer.hasMoreTokens()) {
			parsedAttrs[i] = tokenizer.nextToken();
			i++;
		}
		return parsedAttrs;
	}
}
