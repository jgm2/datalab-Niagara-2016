package niagara.physical;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import niagara.logical.SplitFrameOnWindowBoundary;
import niagara.logical.Variable;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.FrameDetectorX;
import niagara.utils.FrameOutputX;
import niagara.utils.FrameTupleX;
import niagara.utils.IntegerAttr;
import niagara.utils.LongAttr;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.TSAttr;
import niagara.utils.Tuple;


/**
 * Jim Miller (JGM), 05/17/2013
 * An operator to split existing frames along regular window boundaries, for the DEBS 2013 heatmap challenge.
 * 
 * Based off of PhysicalGridFrame.
 */

public class PhysicalSplitFrameOnWindowBoundary extends PhysicalOperator
{
	public String FRAME_PREDICATE_FALSE = "0";
	public String FRAME_PREDICATE_TRUE  = "1";

	// No blocking source streams
	private static final boolean[]          blockingSourceStreams          = { false };
	private final boolean                   OVERRIDE_DEBUG                 = true;

	private String time1AttributeName;
	private String time2AttributeName;
	private long startTime;
	private long boundaryGap;

	private int time1AttributeInputSchemaPosition;
	private int time2AttributeInputSchemaPosition;
	private boolean inputTupleSchemaLocationsKnown;

	private int outSize;

	private long newSt_TS, end_TS;

	public PhysicalSplitFrameOnWindowBoundary()
	{
		// (JGM) super() as in logical operator???
		this.setBlockingSourceStreams(PhysicalSplitFrameOnWindowBoundary.blockingSourceStreams);
		inputTupleSchemaLocationsKnown = false;
	}

	public PhysicalSplitFrameOnWindowBoundary(String time1Attribute, String time2Attribute, long startTime, long boundaryGap)
	{
		this.setBlockingSourceStreams(PhysicalSplitFrameOnWindowBoundary.blockingSourceStreams);
		inputTupleSchemaLocationsKnown = false;

		this.time1AttributeName = time1Attribute;
		this.time2AttributeName = time2Attribute;
		this.startTime = startTime;
		this.boundaryGap = boundaryGap;
	}

	protected void opInitFrom(LogicalOp logicalOperator) {
		time1AttributeName = ((SplitFrameOnWindowBoundary)logicalOperator).getTime1AttributeName();
		time2AttributeName = ((SplitFrameOnWindowBoundary)logicalOperator).getTime2AttributeName();
		startTime = ((SplitFrameOnWindowBoundary)logicalOperator).getStartTime();
		boundaryGap = ((SplitFrameOnWindowBoundary)logicalOperator).getBoundaryGap();
	}

	public void opInitialize() {
		outSize = outputTupleSchema.getLength();
	}

	// (JGM) I'm not sure this is necessary for PhysicalGridFrame.
	public void constructTupleSchema(TupleSchema[] inputSchemas)
	{
		super.constructTupleSchema(inputSchemas);

		if (inputTupleSchemaLocationsKnown == false)
		{
			lookupTupleSchemaLocations();
		}     
	}

	@Override
	protected void processTuple(final Tuple inputTuple, final int streamId)
			throws ShutdownException, InterruptedException
			{
		Tuple outputTuple;
		long offsetFromWindowBoundary;

		// Extract information from input tuple.
		newSt_TS = (Long)(((LongAttr)inputTuple.getAttribute(time1AttributeInputSchemaPosition)).attrVal());
		end_TS = (Long)(((LongAttr)inputTuple.getAttribute(time2AttributeInputSchemaPosition)).attrVal());
		offsetFromWindowBoundary = (newSt_TS - startTime) % boundaryGap;

		 	 // (JGM) Shave and output a frame from the front of the input frame, up to the next window boundary.
		    if (end_TS - newSt_TS + offsetFromWindowBoundary >= boundaryGap) {
		       outputTuple = inputTuple.copy(outSize);
		       outputTuple.setAttribute(time1AttributeInputSchemaPosition, new TSAttr(newSt_TS));
		       outputTuple.setAttribute(time2AttributeInputSchemaPosition, new TSAttr(newSt_TS += (boundaryGap - offsetFromWindowBoundary)));
		       putTuple(outputTuple, 0);
		    }
		    // (JGM) If necessary, repeat shaving and outputting frames from the front of the input frame, now in alignment with the window boundary gap.
		    while (end_TS - newSt_TS >= boundaryGap) {
		       outputTuple = inputTuple.copy(outSize);
		       outputTuple.setAttribute(time1AttributeInputSchemaPosition, new TSAttr(newSt_TS));
		       outputTuple.setAttribute(time2AttributeInputSchemaPosition, new TSAttr(newSt_TS += boundaryGap));
		       putTuple(outputTuple, 0);
		    }
		    // (JGM) Belt out any last tuple (will have length < boundaryGap and will not cross a window boundary).
		    if (end_TS - newSt_TS > 0) {
		   	 outputTuple = inputTuple.copy(outSize);
		   	 outputTuple.setAttribute(time1AttributeInputSchemaPosition, new TSAttr(newSt_TS));
		   	 outputTuple.setAttribute(time2AttributeInputSchemaPosition, new TSAttr(end_TS));
		       putTuple(outputTuple, 0);
		    }

		//displayTuple(inputTuple, "PhysicalSplitFrameOnWindowBoundary::processTuple end"); 
	}

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

		putTuple(inputTuple, 0);

		if(logging){
			punctsOut++; // Count the input punctuations for this operator
			log.Update("PunctsOut", String.valueOf(punctsOut));
		}		
		displayTuple(inputTuple, "end");
	}

	private void lookupTupleSchemaLocations()
	{
		time1AttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], time1AttributeName);
		time2AttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], time2AttributeName);
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

	public void print(String output)
	{
		System.out.println(output);
	}

	@Override
	public boolean isStateful()
	{
		return true;
	}

	/**
	 * vpapad's code, jmw no changes
	 */
	//(JGM) I'm not sure if this should be altered.
	public Cost findLocalCost(final ICatalog catalog,
			final LogicalProperty[] InputLogProp)
	{
		// XXX vpapad: really naive. Only considers the hashing cost
		final float inpCard = InputLogProp[0].getCardinality();
		final float outputCard = this.logProp.getCardinality();

		double cost = inpCard * catalog.getDouble("tuple_reading_cost");
		cost += inpCard * catalog.getDouble("tuple_hashing_cost");
		cost += outputCard * catalog.getDouble("tuple_construction_cost");
		return new Cost(cost);
	}

	@Override
	public Op opCopy()
	// (JGM) I'm not sure which fields need to be initialized in an opCopy()....
	{
		final PhysicalSplitFrameOnWindowBoundary op = new PhysicalSplitFrameOnWindowBoundary(this.time1AttributeName,
				this.time2AttributeName, this.startTime, this.boundaryGap);
		return op;
	}

	@Override
	public boolean equals(final Object other)
	{
		if ((other == null) || !(other instanceof PhysicalSplitFrameOnWindowBoundary))
			return false;
		if (other.getClass() != PhysicalSplitFrameOnWindowBoundary.class)
			return other.equals(this);
		final PhysicalSplitFrameOnWindowBoundary f = (PhysicalSplitFrameOnWindowBoundary) other;
		return (this.time1AttributeName.equals(f.time1AttributeName)) && (this.time2AttributeName.equals(f.time2AttributeName))
				&& (this.startTime == f.startTime) && (this.boundaryGap == f.boundaryGap);
	}

	@Override
	public int hashCode()
	{
		//    return this.framesHashMap.hashCode();
		// (JGM) Probably not a good hashcode.
		return 0;
	}
}
