package niagara.physical;

import java.util.Arrays;
import java.util.HashMap;

import niagara.logical.GridFrame;
import niagara.logical.HeatmapPreprocessForWindowsGridQuery;
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
 * The operator that performs sid-to-pid mapping and foot-combining at the bottom of the windows version of
 * the heatmap plan, for the DEBS 2013 heatmap challenge.
 * This is very specific to the DEBS heatmap challenge and is not generalized.
 * 
 * Based off of PhysicalHeatmapMapSidToPid, used in the frames version of the DEBS heatmap query, and augmented with code from
 * PhysicalGridFrame, used in the same query.
 * 
 */

public class PhysicalHeatmapPreprocessForWindowsGridQuery extends PhysicalOperator {
//	private int numOutputStreams;
//	// No blocking input streams
	private static final boolean[] blockingSourceStreams = { false };
	private static final long GAME_START_PICOS = 13086639146403495L;
	private static final int NUM_PERSONS = 17;
	private int outSize; // save a fcn call each time through
	private int sid;
	private int pid;
	private int x, y;
	int numSensors;
	private HashMap<Integer, Integer> sensorToPlayerMap;
	private HashMap<Integer, Integer> sensorToIndexMap;
	private PlayerInfo[] players;
	private SensorInfo[] sensors;

	public String FRAME_PREDICATE_FALSE = "0";
	public String FRAME_PREDICATE_TRUE  = "1";
	private final boolean                   OVERRIDE_DEBUG                 = true;
	private String groupByAttributeName;
	private String timeAttributeName;
	private String xGridAttributeName;
	private String yGridAttributeName;
	// private int maxCid;
	private int xGridAttributeInputSchemaPosition;
	private int yGridAttributeInputSchemaPosition;
	private boolean inputTupleSchemaLocationsKnown;

	private int TSCount;


	public PhysicalHeatmapPreprocessForWindowsGridQuery() {
		// (JGM) super() as in logical operator???
		setBlockingSourceStreams(blockingSourceStreams);
		inputTupleSchemaLocationsKnown = false;
	}
	
	public PhysicalHeatmapPreprocessForWindowsGridQuery(String xGridAttribute, String yGridAttribute)
	{
		setBlockingSourceStreams(blockingSourceStreams);
		inputTupleSchemaLocationsKnown = false;
		this.xGridAttributeName = xGridAttribute;
		this.yGridAttributeName = yGridAttribute;
	}

	protected void opInitFrom(LogicalOp logicalOperator) {
		xGridAttributeName = ((HeatmapPreprocessForWindowsGridQuery) logicalOperator).getXGridAttributeName();
		yGridAttributeName = ((HeatmapPreprocessForWindowsGridQuery) logicalOperator).getYGridAttributeName();
	}

	public void opInitialize() {
		outSize = outputTupleSchema.getLength();
		
		// (JGM) I have a gut feeling that my player and sensor structures are horribly inefficient.
		numSensors = 0;
		players = new PlayerInfo[NUM_PERSONS];
	   for (int i = 0; i < NUM_PERSONS; i++)
	   	players[i] = new PlayerInfo();
		sensors = new SensorInfo[NUM_PERSONS * 2];
	   for (int i = 0; i < NUM_PERSONS * 2; i++)
	   	sensors[i] = new SensorInfo();
		
		sensorToPlayerMap = new HashMap<Integer, Integer>();
		sensorToIndexMap = new HashMap<Integer, Integer>();

		sensorToPlayerMap.put(13, 0); sensorToPlayerMap.put(14, 0);                                // Team A goalie, left and right feet
		sensorToIndexMap.put(13, numSensors++); sensorToIndexMap.put(14, numSensors++);
		players[0].sidL = 13; players[0].sidR = 14;
		sensorToPlayerMap.put(47, 1); sensorToPlayerMap.put(16, 1);
		sensorToIndexMap.put(47, numSensors++); sensorToIndexMap.put(16, numSensors++);
		players[1].sidL = 47; players[1].sidR = 16;
		sensorToPlayerMap.put(49, 2); sensorToPlayerMap.put(88, 2);
		sensorToIndexMap.put(49, numSensors++); sensorToIndexMap.put(88, numSensors++);
		players[2].sidL = 49; players[2].sidR = 88;
		sensorToPlayerMap.put(19, 3); sensorToPlayerMap.put(52, 3);
		sensorToIndexMap.put(19, numSensors++); sensorToIndexMap.put(52, numSensors++);
		players[3].sidL = 19; players[3].sidR = 52;
		sensorToPlayerMap.put(53, 4); sensorToPlayerMap.put(54, 4);
		sensorToIndexMap.put(53, numSensors++); sensorToIndexMap.put(54, numSensors++);
		players[4].sidL = 53; players[4].sidR = 54;
		sensorToPlayerMap.put(23, 5); sensorToPlayerMap.put(24, 5);
		sensorToIndexMap.put(23, numSensors++); sensorToIndexMap.put(24, numSensors++);
		players[5].sidL = 23; players[5].sidR = 24;
		sensorToPlayerMap.put(57, 6); sensorToPlayerMap.put(58, 6);
		sensorToIndexMap.put(57, numSensors++); sensorToIndexMap.put(58, numSensors++);
		players[6].sidL = 57; players[6].sidR = 58;
		sensorToPlayerMap.put(59, 7); sensorToPlayerMap.put(28, 7);
		sensorToIndexMap.put(59, numSensors++); sensorToIndexMap.put(28, numSensors++);
		players[7].sidL = 59; players[7].sidR = 28;

		sensorToPlayerMap.put(61, 8); sensorToPlayerMap.put(62, 8);                                // Team B goalie
		sensorToIndexMap.put(61, numSensors++); sensorToIndexMap.put(62, numSensors++);
		players[8].sidL = 61; players[8].sidR = 62;
		sensorToPlayerMap.put(63, 9); sensorToPlayerMap.put(64, 9);
		sensorToIndexMap.put(63, numSensors++); sensorToIndexMap.put(64, numSensors++);
		players[9].sidL = 63; players[9].sidR = 64;
		sensorToPlayerMap.put(65, 10); sensorToPlayerMap.put(66, 10);
		sensorToIndexMap.put(65, numSensors++); sensorToIndexMap.put(66, numSensors++);
		players[10].sidL = 65; players[10].sidR = 66;
		sensorToPlayerMap.put(67, 11); sensorToPlayerMap.put(68, 11);
		sensorToIndexMap.put(67, numSensors++); sensorToIndexMap.put(68, numSensors++);
		players[11].sidL = 67; players[11].sidR = 68;
		sensorToPlayerMap.put(69, 12); sensorToPlayerMap.put(38, 12);
		sensorToIndexMap.put(69, numSensors++); sensorToIndexMap.put(38, numSensors++);
		players[12].sidL = 69; players[12].sidR = 38;
		sensorToPlayerMap.put(71, 13); sensorToPlayerMap.put(40, 13);
		sensorToIndexMap.put(71, numSensors++); sensorToIndexMap.put(40, numSensors++);
		players[13].sidL = 71; players[13].sidR = 40;
		sensorToPlayerMap.put(73, 14); sensorToPlayerMap.put(74, 14);
		sensorToIndexMap.put(73, numSensors++); sensorToIndexMap.put(74, numSensors++);
		players[14].sidL = 73; players[14].sidR = 74;
		sensorToPlayerMap.put(75, 15); sensorToPlayerMap.put(44, 15);
		sensorToIndexMap.put(75, numSensors++); sensorToIndexMap.put(44, numSensors++);
		players[15].sidL = 75; players[15].sidR = 44;

		sensorToPlayerMap.put(105, 16); sensorToPlayerMap.put(106, 16);                            // Referee
		sensorToIndexMap.put(105, numSensors++); sensorToIndexMap.put(106, numSensors++);
		players[16].sidL = 105; players[16].sidR = 106;
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
		sid = (Integer)(((IntegerAttr)inputTuple.getAttribute(0)).attrVal());
		mapSidToPidAndFindPidPosition(sid, outputTuple);
		if (pid != -1) {
			outputTuple.setAttribute(xGridAttributeInputSchemaPosition, new IntegerAttr(x));
			outputTuple.setAttribute(yGridAttributeInputSchemaPosition, new IntegerAttr(y));
			outputTuple.appendAttribute(new IntegerAttr(pid));
			putTuple(outputTuple, 0);
		}
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

		putTuple(inputTuple, 0);

		if(logging){
			punctsOut++; // Count the input punctuations for this operator
			log.Update("PunctsOut", String.valueOf(punctsOut));
		}		
		//System.out.println(this.getName() + "punctuation");
		displayTuple(inputTuple, "PhysicalHeatmapPreprocessForWindowsGridQuery::processPunctuation end");
	}

	// (JGM) Calculates the pid.
	// *** This method also calculates current player-as-a-whole position (midpoint of leg sensors) and stores it in
	// players[pid] ***
	// (JGM) I have a gut feeling that my player and sensor structures are horribly inefficient.
	private void mapSidToPidAndFindPidPosition(int sid, Tuple outputTuple) {
		int currPidSidL, currPidSidR;
		int currPidOtherSidX, currPidOtherSidY;

		if (sensorToPlayerMap.containsKey(sid)) {
			pid = sensorToPlayerMap.get(sid);
			currPidSidL = players[pid].sidL;
			currPidSidR = players[pid].sidR;
			x = (Integer)(((IntegerAttr)outputTuple.getAttribute(xGridAttributeInputSchemaPosition)).attrVal());
			y = (Integer)(((IntegerAttr)outputTuple.getAttribute(yGridAttributeInputSchemaPosition)).attrVal());
			if (sid == currPidSidL) {
				currPidOtherSidX = sensors[sensorToIndexMap.get(currPidSidR)].x;
				currPidOtherSidY = sensors[sensorToIndexMap.get(currPidSidR)].y;
				if (x != sensors[sensorToIndexMap.get(currPidSidL)].x || y != sensors[sensorToIndexMap.get(currPidSidL)].y) {
					sensors[sensorToIndexMap.get(currPidSidL)].x = x;
					if (currPidOtherSidX != Integer.MIN_VALUE)
						x = (currPidOtherSidX + x) / 2;	// (JGM) Integer division introduces truncation error!!; 11/14/13 CONCERN:  Wasteful if x-value hasn't changed.
					players[pid].x = x;
					sensors[sensorToIndexMap.get(currPidSidL)].y = y;
					if (currPidOtherSidY != Integer.MIN_VALUE)
						y = (currPidOtherSidY + y) / 2;				// 11/14/13 CONCERN:  Ditto, and same in else block below!
					players[pid].y = y;
				}
				else {
					x = players[pid].x;
					y = players[pid].y;
				}
			}
			else {
				currPidOtherSidX = sensors[sensorToIndexMap.get(currPidSidL)].x;
				currPidOtherSidY = sensors[sensorToIndexMap.get(currPidSidL)].y;
				if (x != sensors[sensorToIndexMap.get(currPidSidR)].x || y != sensors[sensorToIndexMap.get(currPidSidR)].y) {
					sensors[sensorToIndexMap.get(currPidSidR)].x = x;
					if (currPidOtherSidX != Integer.MIN_VALUE)
						x = (currPidOtherSidX + x) / 2;
					players[pid].x = x;
					sensors[sensorToIndexMap.get(currPidSidR)].y = y;
					if (currPidOtherSidY != Integer.MIN_VALUE)
						y = (currPidOtherSidY + y) / 2;
					players[pid].y = y;
				}
				else {
					x = players[pid].x;
					y = players[pid].y;
				}
			}
		}
		else
			pid = -1;
	}

	public boolean isStateful() {
		return true;		// (JGM) Should be false, as in PhysicalHeatmapMapSidToPid??
	}

	public boolean equals(Object other) {
		if ((other == null) || !(other instanceof PhysicalHeatmapPreprocessForWindowsGridQuery))
			return false;
		if (other.getClass() != PhysicalHeatmapPreprocessForWindowsGridQuery.class)
			return other.equals(this);
		final PhysicalHeatmapPreprocessForWindowsGridQuery  f = (PhysicalHeatmapPreprocessForWindowsGridQuery ) other;
		return (this.xGridAttributeName.equals(f.xGridAttributeName)) && (this.yGridAttributeName.equals(f.yGridAttributeName));
	}

	public int hashCode() {
//		return numOutputStreams;
	// (JGM) Probably not a good hashcode.
		return 0;
	}

	public Op opCopy()
	// (JGM) I'm not sure which fields need to be initialized in an opCopy()....
	{
		final PhysicalHeatmapPreprocessForWindowsGridQuery op = new PhysicalHeatmapPreprocessForWindowsGridQuery(this.xGridAttributeName,
			 this.yGridAttributeName);
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
		xGridAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], xGridAttributeName);
		yGridAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], yGridAttributeName);
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

	
	private class PlayerInfo {
		int x, y;
		int xCid, yCid, cid;
		int sidL, sidR;
		long lastUpdateTS;
		long lastCellEntryTS;

		PlayerInfo() {
			x = y = Integer.MIN_VALUE;
			xCid = yCid = Integer.MIN_VALUE;
			cid = Integer.MIN_VALUE;
			//pid = -1;
			sidL = sidR = -1;
			lastUpdateTS = GAME_START_PICOS;
			lastCellEntryTS = GAME_START_PICOS;
		}
	}
	
	private class SensorInfo {
		int x, y;

		SensorInfo() {
			x = y = Integer.MIN_VALUE;
		}
	}
}
