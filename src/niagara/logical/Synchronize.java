package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;


/**
 * This class is used to represent the synchronize operator.
 *
 * @version 1.0
 * @author moorthy
 * 
 */
@SuppressWarnings("unchecked")
public class Synchronize extends BinaryOperator {

	private int numDestinationStreams = 2;
	
	private String left_tsAttributeName;      // timestamp attribute of left input stream
	private String right_tsAttributeName;     // timestamp attribute of right input stream
	
	public Synchronize(String left_tsAttributeName, 
					 String right_tsAttributeName){
		this.left_tsAttributeName = left_tsAttributeName;
		this.right_tsAttributeName = right_tsAttributeName;
	}
	
	public Synchronize(){
		super();
	}
	
	public int getNumberOfOutputs() {
		return numDestinationStreams;
	}
	
	public String getLeft_tsAttributeName() {
		return left_tsAttributeName;
	}

	public String getRight_tsAttributeName() {
		return right_tsAttributeName;
	}
	
	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		this.left_tsAttributeName = e.getAttribute("lefttimestamp");
		this.right_tsAttributeName = e.getAttribute("righttimestamp");
	}

	/**
	 * moorthy:
	 * The logical property/schema of the output stream is assumed to be the same for both the input streams. For now synchronize only
	 * supports same schema input streams. 
	 */
	@Override
	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		LogicalProperty left = input[0]; // left input stream
		LogicalProperty right = input[1]; // right input stream
		
		LogicalProperty result = left.copy();
	    
		return result;
	}

	@Override
	public Op opCopy() {
		final Synchronize op = new Synchronize(this.left_tsAttributeName, 
				 						   this.right_tsAttributeName);
		op.numDestinationStreams = numDestinationStreams;
		return op;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Synchronize))
			return false;
		if (obj.getClass() != Synchronize.class)
			return obj.equals(this);
		final Synchronize other = (Synchronize) obj;
		return this.left_tsAttributeName.equals(other.left_tsAttributeName) && 
			   this.right_tsAttributeName.equals(other.right_tsAttributeName) &&
			   this.numDestinationStreams == other.numDestinationStreams;
	}

	@Override
	public int hashCode() {
		// Combining left_tsAttributeName and right_tsAttributeName strings and hashing over that good
	    // enough?
	    final String hashString = this.left_tsAttributeName+this.right_tsAttributeName;
	    return hashString.hashCode();
	}

}
