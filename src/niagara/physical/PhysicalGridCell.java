package niagara.physical;

import java.util.Arrays;
import java.util.HashMap;

import niagara.logical.GridFrame;
import niagara.logical.GridCell;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalProperty;
import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.DoubleAttr;
import niagara.utils.IntegerAttr;
import niagara.utils.LongAttr;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.StringAttr;
import niagara.utils.TSAttr;
import niagara.utils.Tuple;

/**
 * Jim Miller (JGM), 06/2013, 11/2013
 * The operator that performs cid-determination in the windows version of
 * the heatmap plan, for the DEBS 2013 heatmap challenge.
 * 
 * This is a cell-determiner for a 2-D grid, as a component of a series of operators which calculate "2-D windows".
 * 
 * Based off of PhysicalHeatmapMapSidToPid, used in the frames version of the DEBS heatmap query, and augmented with code from
 * PhysicalGridFrame, used in the same query.
 * 
 */

public class PhysicalGridCell extends PhysicalOperator {
//	private int numOutputStreams;
//	// No blocking input streams
	private static final boolean[] blockingSourceStreams = { false };
	private int outSize; // save a fcn call each time through
	private int pid;

	public String FRAME_PREDICATE_FALSE = "0";
	public String FRAME_PREDICATE_TRUE  = "1";
	private final boolean                   OVERRIDE_DEBUG                 = true;
	private String xCoordAttributeName;
	private String yCoordAttributeName;
	private int xCells;
	private int yCells;
	private int[] corners;
	// private int maxCid;
	private CellMetrics[][] cells;
	private int xCoordAttributeInputSchemaPosition;
	private int yCoordAttributeInputSchemaPosition;
	private boolean inputTupleSchemaLocationsKnown;
	private int cid, lastCid;
	private double c_midx, c_midy;

	private int gridLeft, gridBottom, gridRight, gridTop;
	private double gridWidth, gridHeight;
	private double cellWidth, cellHeight;

	private int TSCount;


	public PhysicalGridCell() {
		// (JGM) super() as in logical operator???
		setBlockingSourceStreams(blockingSourceStreams);
		inputTupleSchemaLocationsKnown = false;
		corners = new int[4];
	}
	
	public PhysicalGridCell(String xCoordAttribute, String yCoordAttribute, int xCells, int yCells, int[] corners)
	{
		setBlockingSourceStreams(blockingSourceStreams);
		inputTupleSchemaLocationsKnown = false;
		this.xCoordAttributeName = xCoordAttribute;
		this.yCoordAttributeName = yCoordAttribute;
		this.xCells = xCells;
		this.yCells = yCells;
		this.corners = new int[4];
		System.arraycopy(corners, 0, this.corners, 0, 4);
	}

	protected void opInitFrom(LogicalOp logicalOperator) {
		xCoordAttributeName = ((GridCell) logicalOperator).getXCoordAttributeName();
		yCoordAttributeName = ((GridCell) logicalOperator).getYCoordAttributeName();
		xCells = ((GridCell) logicalOperator).getXCells();
		yCells = ((GridCell) logicalOperator).getYCells();
		corners = ((GridCell) logicalOperator).getCorners();
	}

	public void opInitialize() {
		outSize = outputTupleSchema.getLength();
		
		gridLeft = corners[0]; gridBottom = corners[1]; gridRight = corners[2]; gridTop = corners[3];
		gridWidth = gridRight - gridLeft;
		gridHeight = gridTop - gridBottom;
		cells = new CellMetrics[xCells][yCells];
		cellWidth = gridWidth / xCells;
		cellHeight = gridHeight / yCells;
		for (int i = 0; i < xCells; i++)
			for (int j = 0; j < yCells; j++) {
				cells[i][j] = new CellMetrics();
				cells[i][j].l = gridLeft + i * cellWidth;
				cells[i][j].r = gridLeft + (i + 1) * cellWidth;
				cells[i][j].b = gridTop - (j + 1) * cellHeight;
				cells[i][j].t = gridTop - j * cellHeight;
				cells[i][j].m.x = (double)(gridLeft + (i + .5) * cellWidth);
				cells[i][j].m.y = (double)(gridTop - (j + .5) * cellHeight);
			}
	}
	
	/**
	 * This function processes a tuple element read from a source stream when
	 * the operator is non-blocking. This over-rides the corresponding function
	 * in the base class.
	 * 
	 * @param tupleElement
	 *            The tuple element read from a source stream
	 * @param streamId
	 *            The source stream from which the tuple was read
	 * 
	 * @exception ShutdownException
	 *                query shutdown by user or execution error
	 */

	protected void processTuple(Tuple inputTuple, int streamId)
			throws ShutdownException, InterruptedException {

		Tuple outputTuple = inputTuple.copy(outSize);
		//displayTupleAlways(inputTuple, "PhysicalGridCell::processStuple, start");
		pid = (Integer)(((IntegerAttr)inputTuple.getAttribute(0)).attrVal());
		getNewCid(pid, inputTuple);
		outputTuple.appendAttribute(new IntegerAttr(cid));
		outputTuple.appendAttribute(new DoubleAttr(c_midx));
		outputTuple.appendAttribute(new DoubleAttr(c_midy));
		putTuple(outputTuple, 0);
	}

//	protected void processPunctuation(Punctuation tupleElement, int streamId)
//			throws ShutdownException, InterruptedException {
//		// Copy the input tuple to all the sink streams
//		for (int dest = 0; dest < numSinkStreams; ++dest) {
//			putTuple(tupleElement, dest);
//		}
//	}
	
	/**
	 * This function processes a punctuation element read from a source stream
	 * when the operator is non-blocking. This over-rides the corresponding
	 * function in the base class.
	 * 
	 * Punctuations can simply be sent to the next operator from Select
	 * 
	 * @param inputTuple
	 *            The tuple element read from a source stream
	 * @param streamId
	 *            The source stream from which the tuple was read
	 * 
	 * @exception ShutdownException
	 *                query shutdown by user or execution error
	 */
	protected void processPunctuation(Punctuation inputTuple, int streamId)
			throws ShutdownException, InterruptedException {

		StringAttr starAttr = new StringAttr("*");

		inputTuple.appendAttribute(starAttr);  
		inputTuple.appendAttribute(starAttr); 
		inputTuple.appendAttribute(starAttr); 

		putTuple(inputTuple, 0);

		if(logging){
			punctsOut++; // Count the input punctuations for this operator
			log.Update("PunctsOut", String.valueOf(punctsOut));
		}		
		//System.out.println(this.getName() + "punctuation");
		//displayTupleAlways(inputTuple, "PhysicalGridCell::processPunctuation end");
	}

	private int getNewCid(int pid, Tuple inputTuple) {
		//		  // (JGM) Randomly generate cell boundary crossings for now.
		//		  int newCid = generator.nextInt(numCells) + 1;

		int newCid, newXCid, newYCid;
		//System.out.println(xCoordAttributeInputSchemaPosition);
		//System.out.println(yCoordAttributeInputSchemaPosition);
		//System.out.println(((StringAttr)inputTuple.getAttribute(xCoordAttributeInputSchemaPosition)).toString());
		//System.out.println(((StringAttr)inputTuple.getAttribute(yCoordAttributeInputSchemaPosition)).attrVal());
		double newX = Double.parseDouble(((StringAttr)inputTuple.getAttribute(xCoordAttributeInputSchemaPosition)).toString());
		double newY = Double.parseDouble(((StringAttr)inputTuple.getAttribute(yCoordAttributeInputSchemaPosition)).toString());

		newXCid = (int)((newX - gridLeft) * xCells / gridWidth);	// (JGM) Converting mm value to cell index value!
		// (JGM) *** Maybe change this to fix edge ownership problem for vertical.  Then follow up at other edge code areas.
		newYCid = (int)((gridTop - newY) * yCells / gridHeight);	// (JGM) Converting mm value to cell index value!
		if (newX >= gridLeft && newX < gridRight && newY > gridBottom && newY <= gridTop)		// (JGM) New x, y are now located in the grid
			newCid = newYCid * xCells + newXCid + 1;														// (JGM) Convert to cid
		else
			newCid = 0;
		cid = newCid;
		if (cid == 0)
			c_midx = c_midy = Integer.MIN_VALUE;
		else {
			c_midx = cells[newXCid][newYCid].m.x;
			c_midy = cells[newXCid][newYCid].m.y;
		}
		return newCid;
	}
	
	public boolean isStateful() {
		return true;		// (JGM) Should be false, as in PhysicalHeatmapMapSidToPid??
	}

	public boolean equals(Object other) {
		if ((other == null) || !(other instanceof PhysicalGridCell))
			return false;
		if (other.getClass() != PhysicalGridCell.class)
			return other.equals(this);
		final PhysicalGridCell  f = (PhysicalGridCell) other;
		return (this.xCoordAttributeName.equals(f.xCoordAttributeName)) && (this.yCoordAttributeName.equals(f.yCoordAttributeName))
				&& (this.xCells == f.xCells) && (this.yCells == f.yCells) && Arrays.equals(this.corners, f.corners);
	}

	public int hashCode() {
//		return numOutputStreams;
	// (JGM) Probably not a good hashcode.
		return 0;
	}

	public Op opCopy()
	// (JGM) I'm not sure which fields need to be initialized in an opCopy()....
	{
		final PhysicalGridCell op = new PhysicalGridCell(this.xCoordAttributeName,
			 this.yCoordAttributeName, this.xCells, this.yCells, this.corners);
		return op;
	}

	/**
	 * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
	 */
	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		//inputTupleSchemas = inputSchemas;
		//outputTupleSchema = inputTupleSchemas[0];
		super.constructTupleSchema(inputSchemas);

		if (inputTupleSchemaLocationsKnown == false)
		{
			lookupTupleSchemaLocations();
		}  
	}

	private void lookupTupleSchemaLocations()
	{
		//System.out.println(xCoordAttributeName +  ", PhysicalGridCell::lookupTupleSchemaLocations");
		xCoordAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], xCoordAttributeName);
		yCoordAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], yCoordAttributeName);
		inputTupleSchemaLocationsKnown = true;
	}

	private String getValue(Tuple tuple, int attributeLoc)
	{
		String value;
		value = ((BaseAttr) tuple.getAttribute(attributeLoc)).attrVal().toString();
		return value;
	}

	private int getAttributePosition(TupleSchema schema, String attributeName)
	{ 
		return schema.getPosition(attributeName);
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#findLocalCost(ICatalog,
	 *      LogicalProperty[])
	 */
	
	// (JGM) This cost function copied from PhysicalGridFrame; not sure if it is an appropriate cost for this operator.
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] inputLogProp) {
		float inputCard = inputLogProp[0].getCardinality();
		float outputCard = logProp.getCardinality();

		double cost = inputCard * catalog.getDouble("tuple_reading_cost");
		cost += inputCard * catalog.getDouble("tuple_hashing_cost");
		cost += outputCard * catalog.getDouble("tuple_construction_cost");
		return new Cost(cost);
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#findPhysProp(PhysicalProperty[])
	 */
//	public PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props) {
//		return input_phys_props[0];
//	}

	/**
	 * @see niagara.optimizer.colombia.Op#getNumberOfOutputs()
	 */
//	public int getNumberOfOutputs() {
//		return numOutputStreams;
//	}

	public void print(String output)
	{
		System.out.println(output);
	}

	private class CellMetrics {
		double l, r, t, b;
		Pair<Double, Double> m;

		CellMetrics() {
			l = r = t = b = -1;
			m = new Pair<Double, Double>();
		}
	}

	private class Pair<X, Y> {
		X x; 
		Y y;

		Pair() {
		}

		Pair(X x, Y y) { 
			this.x = x; this.y = y;
		}
	}
}
