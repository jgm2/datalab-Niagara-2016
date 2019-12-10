package niagara.logical;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.varType;


/**
 * This class is used to represent the fill frame operator.
 *
 * @version 1.0
 * @author moorthy
 * 
 */
@SuppressWarnings("unchecked")
public class FillFrame extends BinaryOperator {

	private String fs_groupByAttributeName;        // Grouping attribute (detectorid) in frame stream
	private String fs_frameStartAttributeName;     // frame start attribute in frame stream
	private String fs_frameEndAttributeName;       // frame end attribute in fill stream
	private String fs_frameIdAttributeName;        // frame id attribute in fill stream
	protected String fidName;				   // To form fid_from_*fidName* attribute to group tuples on frame id
	private String ff_groupByAttributeName;        // Grouping attribute (detectorid) in fill frame stream
	private String ff_timeAttributeName;		   // time attribute in fill stream 
	//protected String name;						   // Assigned id of the operator
	
	public FillFrame(String fs_groupByAttributeName, 
					 String fs_frameStartAttributeName, 
					 String fs_frameEndAttributeName,
					 String fs_frameIdAttributeName,
					 String ff_groupByAttributeName,
					 String ff_timeAttributeName){
		this.fs_groupByAttributeName = fs_groupByAttributeName;
		this.fs_frameStartAttributeName = fs_frameStartAttributeName;
		this.fs_frameEndAttributeName = fs_frameEndAttributeName;
		this.fs_frameIdAttributeName = fs_frameIdAttributeName;
		this.ff_groupByAttributeName = ff_groupByAttributeName;
		this.ff_timeAttributeName = ff_timeAttributeName;
	}
	
	public FillFrame(){
		super();
	}
	
	public String getFs_groupByAttributeName() {
		return fs_groupByAttributeName;
	}

	public String getFs_frameStartAttributeName() {
		return fs_frameStartAttributeName;
	}

	public String getFs_frameEndAttributeName() {
		return fs_frameEndAttributeName;
	}

	public String getFs_frameIdAttributeName() {
		return fs_frameIdAttributeName;
	}

	public String getFidName() {
		return fidName;
	}

	public String getFf_groupByAttributeName() {
		return ff_groupByAttributeName;
	}

	public String getFf_timeAttributeName() {
		return ff_timeAttributeName;
	}

	/*public String getName() {
		return name;
	}*/

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		//this.name = e.getAttribute("id");
		this.fidName = e.getAttribute("fid");
		this.fs_groupByAttributeName = e.getAttribute("fsgroupby");
		this.fs_frameStartAttributeName = e.getAttribute("fsframestart");
		this.fs_frameEndAttributeName = e.getAttribute("fsframeend");
		this.fs_frameIdAttributeName = e.getAttribute("fsframeid");
		this.ff_groupByAttributeName = e.getAttribute("ffgroupby");
		this.ff_timeAttributeName = e.getAttribute("fftimeattr");
	}

	@Override
	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		LogicalProperty left = input[0]; // fill frame stream
		LogicalProperty right = input[1]; // frame stream
		
		//result if fill frame stream with frame id appended
		LogicalProperty result = left.copy();
	    
	   /* if (!fidName.equals("")) {
			result.addAttr(new Variable("fid_from_" + fidName,
					varType.ELEMENT_VAR));
			result.addAttr(new Variable("fid_to_" + fidName,
					varType.ELEMENT_VAR));
		} else {
			result
					.addAttr(new Variable("fid_from_" + name,
							varType.ELEMENT_VAR));
			result.addAttr(new Variable("fid_to_" + name, varType.ELEMENT_VAR));
		}*/
		
		result.addAttr(new Variable("ff_frameId",varType.ELEMENT_VAR));
	    
		return result;
	}

	@Override
	public Op opCopy() {
		final FillFrame op = new FillFrame(this.fs_groupByAttributeName, 
				 						   this.fs_frameStartAttributeName, 
				 						   this.fs_frameEndAttributeName,
				 						   this.fs_frameIdAttributeName,
				 						   this.ff_groupByAttributeName,
				 						   this.ff_timeAttributeName);
		return op;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof FillFrame))
			return false;
		if (obj.getClass() != FillFrame.class)
			return obj.equals(this);
		final FillFrame other = (FillFrame) obj;
		return this.fs_groupByAttributeName.equals(other.fs_groupByAttributeName) && 
			   this.fs_frameStartAttributeName.equals(other.fs_frameStartAttributeName) && 
			   this.fs_frameEndAttributeName.equals(other.fs_frameEndAttributeName) &&
			   this.fs_frameIdAttributeName.equals(other.fs_frameIdAttributeName) &&
			   this.ff_groupByAttributeName.equals(other.ff_groupByAttributeName) &&
			   this.ff_timeAttributeName.equals(other.ff_timeAttributeName);
	}

	@Override
	public int hashCode() {
		// Combining groupby and frameattribute strings and hashing over that good
	    // enough?
	    final String hashString = this.fs_groupByAttributeName+this.fs_frameIdAttributeName;
	    return hashString.hashCode();
	}

}
