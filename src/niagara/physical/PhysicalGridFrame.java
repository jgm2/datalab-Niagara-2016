package niagara.physical;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
//import java.util.Random;
import java.util.Vector;

import niagara.connection_server.NiagraServer;
import niagara.logical.GridFrame;
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
import niagara.utils.DoubleAttr;
import niagara.utils.IntegerAttr;
import niagara.utils.LongAttr;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.StringAttr;
import niagara.utils.TSAttr;
import niagara.utils.Tuple;


/**
 * Jim Miller (JGM), 05/09/2013
 * First attempt at a boundary frame operator for a 2-D grid, for the DEBS 2013 heatmap challenge.
 * v1:	Does not actually calculate cell boundary crossings; instead generates random crossing events.
 * v2:	Implements actual grid cell boundary crossing.
 * 
 * Based off of PhysicalFrameX_WithoutFragments.
 */

public class PhysicalGridFrame extends PhysicalOperator
{
  public String FRAME_PREDICATE_FALSE = "0";
  public String FRAME_PREDICATE_TRUE  = "1";
  
  // No blocking source streams
  private static final boolean[]          blockingSourceStreams          = { false };
  private final boolean                   OVERRIDE_DEBUG                 = true;
  
  private String groupByAttributeName;
  private String timeAttributeName;
  private String xGridAttributeName;
  private String yGridAttributeName;
  private int xCells;
  private int yCells;
  private int[] corners;
  
//  private int maxCid;
  private CellMetrics[][] cells;
  
  private int groupByAttributeInputSchemaPosition;
  private int timeAttributeInputSchemaPosition;
  private int xGridAttributeInputSchemaPosition;
  private int yGridAttributeInputSchemaPosition;
  private boolean inputTupleSchemaLocationsKnown;
  
  private int outSize;
  
  // (JGM) Should probably be generalized out via operator attribute
  private static final long GAME_START_PICOS = 13086639146403495L;
  // (JGM) Should probably be generalized out via operator attribute, or use a dynamic mapping rather than an array
  private static final int NUM_PERSONS = 17;
  private static final long NULL_TS = -1L;
  private int pid;
  private int x, y;
  private long TS, st_TS, end_TS;
  private int cid, lastCid;
  private double c_midx, c_midy;
  
  private int gridLeft, gridBottom, gridRight, gridTop;
  private double gridWidth, gridHeight;
  private double cellWidth, cellHeight;
  
  private int TSCount;
  
  private PlayerInfo[] playersInfo;
//  Random generator;

  AtomicEvaluator ae;
  
  public PhysicalGridFrame()
  {
	 // (JGM) super() as in logical operator???
    this.setBlockingSourceStreams(PhysicalGridFrame.blockingSourceStreams);
    inputTupleSchemaLocationsKnown = false;

    corners = new int[4];
  }
  
  public PhysicalGridFrame(String groupByAttribute, String timeAttribute, String xGridAttribute, String yGridAttribute, int xCells, int yCells, int[] corners)
  {
	  this.setBlockingSourceStreams(PhysicalGridFrame.blockingSourceStreams);
	  inputTupleSchemaLocationsKnown = false;

    this.groupByAttributeName = groupByAttribute;
    this.timeAttributeName = timeAttribute;
    this.xGridAttributeName = xGridAttribute;
    this.yGridAttributeName = yGridAttribute;
    this.xCells = xCells;
    this.yCells = yCells;
    this.corners = new int[4];
    System.arraycopy(corners, 0, this.corners, 0, 4);
  }
  
  protected void opInitFrom(LogicalOp logicalOperator) {
    groupByAttributeName = ((GridFrame) logicalOperator).getGroupByAttributeName();
    timeAttributeName = ((GridFrame) logicalOperator).getTimeAttributeName();
    xGridAttributeName = ((GridFrame) logicalOperator).getXGridAttributeName();
    yGridAttributeName = ((GridFrame) logicalOperator).getYGridAttributeName();
    xCells = ((GridFrame) logicalOperator).getXCells();
    yCells = ((GridFrame) logicalOperator).getYCells();
    corners = ((GridFrame) logicalOperator).getCorners();
    ae = new AtomicEvaluator(timeAttributeName);
  }
  
  public void opInitialize() {
		outSize = outputTupleSchema.getLength();
		playersInfo = new PlayerInfo[NUM_PERSONS];
	   for (int i = 0; i < NUM_PERSONS; i++)
	   	playersInfo[i] = new PlayerInfo();
//		generator = new Random();
//		maxCid = xCells * yCells - 1;
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
	   TSCount = 0;
	   
	   ae.resolveVariables(inputTupleSchemas[0], 0);
	}
  
  // (JGM) I'm not sure this is necessary for PhysicalGridFrame.
  public void constructTupleSchema(TupleSchema[] inputSchemas)
  {

    // Frame Output Tuple Schema
    // Int:  ID
    // Long: Start Time
    // Long: End Time
    // Int:  Group By Attribute
    
	 super.constructTupleSchema(inputSchemas);
    //outputTupleSchema = new TupleSchema();
    //outputTupleSchema.addMapping(new Variable("st_ts"));
    //outputTupleSchema.addMapping(new Variable("end_ts"));
    //outputTupleSchema.addMapping(new Variable("cid"));
    
    if (inputTupleSchemaLocationsKnown == false)
    {
      lookupTupleSchemaLocations();
    }     
  }
  
  @Override
  protected void processTuple(final Tuple inputTuple, final int streamId)
      throws ShutdownException, InterruptedException
  {
    Tuple outputTuple = inputTuple.copy(outSize);
    
    // Extract information from tuple    
    pid = Integer.parseInt(getValue(inputTuple, groupByAttributeInputSchemaPosition));
    
    if (getNewCid(pid, inputTuple) != -1) {		// Pid has moved to a new Cid, and the Cid is NOT its FIRST Cid.
   	 outputTuple.appendAttribute(new LongAttr(st_TS));
   	 outputTuple.appendAttribute(new LongAttr(end_TS));
   	 outputTuple.appendAttribute(new IntegerAttr(cid));
   	 outputTuple.appendAttribute(new DoubleAttr(c_midx));
   	 outputTuple.appendAttribute(new DoubleAttr(c_midy));
   	 putTuple(outputTuple, 0);
    }
    TSCount++;
  }
  
  protected void processPunctuation(Punctuation inputTuple, int streamId)
			throws ShutdownException, InterruptedException {

	  // get the punctuation tuple
	  // we will (mostly) append to the existing punctuation tuple 
	  // get ts value from punctuation tuple
	  // modify ts attribute to be a *
	  // append according to above schema...
	  
	  // get ts value
	  ArrayList values = new ArrayList();
	  ae.getAtomicValues(inputTuple, values); 
	  
	  // assume ts occurs at position 1 , get ts value
	  String tsVal = ((BaseAttr) values.get(0)).toASCII().trim();
			  
	  // here is st_ts, which has value of ts
	  StringAttr starAttr = new StringAttr("*");
	  int tsAttrInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], timeAttributeName);
	  inputTuple.setAttribute(tsAttrInputSchemaPosition, starAttr); 
	  
	  StringAttr tsValAttr = new StringAttr(tsVal); 
	  inputTuple.appendAttribute(tsValAttr);
	  
	  // here are the other *s being appended
 	 	inputTuple.appendAttribute(starAttr); 
 	 	inputTuple.appendAttribute(starAttr);
 	 	inputTuple.appendAttribute(starAttr);
 	 	inputTuple.appendAttribute(starAttr);
	  
		putTuple(inputTuple, streamId);
		
		if(logging){
			punctsOut++; // Count the input punctuations for this operator
			log.Update("PunctsOut", String.valueOf(punctsOut));
		}
		displayTuple(inputTuple, "end");
	}
  
  private int getNewCid(int pid, Tuple inputTuple) {
//	  // (JGM) Randomly generate cell boundary crossings for now.
//	  int newCid = generator.nextInt(numCells) + 1;
	  
	  int currCid = playersInfo[pid].cid;
	  int currXCid = playersInfo[pid].xCid;
	  int currYCid = playersInfo[pid].yCid;
	  int newCid, newXCid, newYCid;
	  int newX = (int)(((IntegerAttr)inputTuple.getAttribute(xGridAttributeInputSchemaPosition)).attrVal());
	  int newY = (int)(((IntegerAttr)inputTuple.getAttribute(yGridAttributeInputSchemaPosition)).attrVal());
	  //int newX = playersInfo[pid].x;
	  //int newY = playersInfo[pid].y;
	  
	  if (currCid <= 0) {	// (JGM) Pid was not currently located in the grid ("out of bounds") (0), or was not located anywhere yet (Integer.MIN_VALUE).
		  newXCid = (int)((newX - gridLeft) * xCells / gridWidth);	// (JGM) Converting mm value to cell index value!
		  // (JGM) *** Maybe change this to fix edge ownership problem for vertical.  Then follow up at other edge code areas.
		  newYCid = (int)((gridTop - newY) * yCells / gridHeight);	// (JGM) Converting mm value to cell index value!
		  if (newX >= gridLeft && newX < gridRight && newY > gridBottom && newY <= gridTop)		// (JGM) New x, y are now located in the grid
			  newCid = newYCid * xCells + newXCid + 1;														// (JGM) Convert to cid
		  else
			  newCid = 0;
	  }  
//	  else {
//		  int currXCid = (currCid - 1) % xCells;
//		  int currYCid = (currCid - 1) / xCells;
//		  CellMetrics currCell = cells[currXCid][currYCid];
//
//		  if (newX < currCell.l)
//			  if (newY > currCell.t)
//				  newCid = (newX >= gridLeft && newY <= gridTop) ? currCid - xCells - 1 : 0;
//			  else if (newY <= currCell.b)
//				  newCid = (newX >= gridLeft && newY > gridBottom) ? currCid + xCells - 1 : 0;
//			  else
//				  newCid = (newX >= gridLeft) ? currCid - 1 : 0;
//		  else if (newX >= currCell.r)
//			  if (newY > currCell.t)
//				  newCid = (newX < gridRight && newY <= gridTop) ? currCid - xCells + 1 : 0;
//			  else if (newY <= currCell.b)
//				  newCid = (newX < gridRight && newY > gridBottom) ? currCid + xCells + 1 : 0;
//			  else
//				  newCid = (newX < gridRight) ? currCid + 1 : 0;
//		  else
//			  if (newY > currCell.t)
//				  newCid = (newY <= gridTop) ? currCid - xCells : 0;
//			  else if (newY <= currCell.b)
//				  newCid = (newY > gridBottom) ? currCid + xCells : 0;
//			  else
//				  newCid = currCid;
//	  }
	  else {
		  CellMetrics currCell = cells[currXCid][currYCid];

		  if (newX < currCell.l)
			  if (newY > currCell.t)
				  if (newX >= gridLeft && newY <= gridTop) {
					  newCid = currCid - xCells - 1;
					  newXCid = currXCid - 1;
					  newYCid = currYCid - 1;
				  }
				  else {
					  newCid = 0;
					  newXCid = newYCid = -1;
				  }
//				  newCid = (newX >= gridLeft && newY <= gridTop) ? currCid - xCells - 1 : 0;
			  else if (newY <= currCell.b)
				  if (newX >= gridLeft && newY > gridBottom) {
					  newCid = currCid + xCells - 1;
					  newXCid = currXCid - 1;
					  newYCid = currYCid + 1;
				  }
				  else {
					  newCid = 0;
					  newXCid = newYCid = -1;
				  }
//				  newCid = (newX >= gridLeft && newY > gridBottom) ? currCid + xCells - 1 : 0;
			  else
				  if (newX >= gridLeft) {
					  newCid = currCid - 1;
					  newXCid = currXCid - 1;
					  newYCid = currYCid;
				  }
				  else {
					  newCid = 0;
					  newXCid = newYCid = -1;
				  }
//				  newCid = (newX >= gridLeft) ? currCid - 1 : 0;
		  else if (newX >= currCell.r)
			  if (newY > currCell.t)
				  if (newX < gridRight && newY <= gridTop) {
					  newCid = currCid - xCells + 1;
					  newXCid = currXCid + 1;
					  newYCid = currYCid - 1;
				  }
				  else {
					  newCid = 0;
					  newXCid = newYCid = -1;
				  }
//				  newCid = (newX < gridRight && newY <= gridTop) ? currCid - xCells + 1 : 0;
			  else if (newY <= currCell.b)
				  if (newX < gridRight && newY > gridBottom) {
					  newCid = currCid + xCells + 1;
					  newXCid = currXCid + 1;
					  newYCid = currYCid + 1;
				  }
				  else {
					  newCid = 0;
					  newXCid = newYCid = -1;
				  }
//				  newCid = (newX < gridRight && newY > gridBottom) ? currCid + xCells + 1 : 0;
			  else
				  if (newX < gridRight) {
					  newCid = currCid + 1;
					  newXCid = currXCid + 1;
					  newYCid = currYCid;
				  }
				  else {
					  newCid = 0;
					  newXCid = newYCid = -1;
				  }
//				  newCid = (newX < gridRight) ? currCid + 1 : 0;
		  else
			  if (newY > currCell.t)
				  if (newY <= gridTop) {
					  newCid = currCid - xCells;
					  newXCid = currXCid;
					  newYCid = currYCid - 1;
				  }
				  else {
					  newCid = 0;
					  newXCid = newYCid = -1;
				  }
//				  newCid = (newY <= gridTop) ? currCid - xCells : 0;
			  else if (newY <= currCell.b)
				  if (newY > gridBottom) {
					  newCid = currCid + xCells;
					  newXCid = currXCid;
					  newYCid = currYCid + 1;
				  }
				  else {
					  newCid = 0;
					  newXCid = newYCid = -1;
				  }
//				  newCid = (newY > gridBottom) ? currCid + xCells : 0;
			  else {
				  newCid = currCid;
				  newXCid = currXCid;
				  newYCid = currYCid;
			  }
	  }
	  
	  if (currCid == Integer.MIN_VALUE) {		// Pid has moved to its FIRST Cid.
		  playersInfo[pid].cid = newCid;
		  playersInfo[pid].xCid = newXCid;
		  playersInfo[pid].yCid = newYCid;
		  playersInfo[pid].lastCellEntryTS = (Long)(((TSAttr)inputTuple.getAttribute(timeAttributeInputSchemaPosition)).attrVal());
		  newCid = -1;
	  }
	  else if (newCid != currCid) {				// Pid has changed Cid (possibly into/out of oob (Cid 0)).
		  st_TS = playersInfo[pid].lastCellEntryTS;
		  end_TS = (Long)(((TSAttr)inputTuple.getAttribute(timeAttributeInputSchemaPosition)).attrVal());
		  playersInfo[pid].cid = newCid;
		  playersInfo[pid].xCid = newXCid;
		  playersInfo[pid].yCid = newYCid;
		  playersInfo[pid].lastCellEntryTS = end_TS;
		  cid = currCid;
		  if (cid == 0)
			  c_midx = c_midy = Integer.MIN_VALUE;
		  else {
			  c_midx = cells[currXCid][currYCid].m.x;
			  c_midy = cells[currXCid][currYCid].m.y;
		  }
	  }
	  else	// Pid is still in the same Cid.
		  newCid = -1;
	  return newCid;
  }
  
  private void lookupTupleSchemaLocations()
  {
    groupByAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], groupByAttributeName);
    timeAttributeInputSchemaPosition = getAttributePosition(inputTupleSchemas[0], timeAttributeName);
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
  
  //(JGM) I'm not sure if this can be safely commented out for PhysicalGridFrame.
//  private void insertTupleIntoFrameBuilder(FrameTupleX fTupleX)
//  {
//    // See if we already have a frame for this groupby attribute
//    if (this.framesHashMap.containsKey(fTupleX.getGroupbyAttr()))
//    {
//      FrameDetectorX fDetectorX = this.framesHashMap.get(fTupleX.getGroupbyAttr());
//      fDetectorX.insertTupleIntoFrame(fTupleX);
//    }
//    else
//    {
//      FrameDetectorX new_fBuilderX = new FrameDetectorX(fTupleX, frameSize);
//      this.framesHashMap.put(fTupleX.getGroupbyAttr(), new_fBuilderX);
//    }    
//  }
  
//  private void outputFrame(FrameOutputX frameOutX)
//  {    
//    Tuple outputTuple = new Tuple(false, 4);
//    
//    outputTuple.appendAttribute(new IntegerAttr(frameOutX.frameId));
//    outputTuple.appendAttribute(new TSAttr(frameOutX.startTime));
//    outputTuple.appendAttribute(new TSAttr(frameOutX.endTime));
//    outputTuple.appendAttribute(new IntegerAttr(frameOutX.groupAttr));
//            
//    try
//    {
//      putTuple(outputTuple, 0);
//    }
//    catch (InterruptedException e)
//    {
//      e.printStackTrace();
//    }
//    catch (ShutdownException e)
//    {
//      e.printStackTrace();
//    }
//
//  }
  
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
  //(JGM) I'm not sure if this should be altered for PhysicalGridFrame.
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
//  {
//    final PhysicalGridFrame  frame = new PhysicalGridFrame ();
//    frame.groupByAttributeName = this.groupByAttributeName;
//    frame.timeAttributeName = this.timeAttributeName;
//    frame.framePredicateAttributeName = this.framePredicateAttributeName;
//    frame.frameSize = this.frameSize;
//    frame.segmentationFlag = this.segmentationFlag;
//    return frame;
//  }
  
  // (JGM) I'm not sure which fields need to be initialized in an opCopy()....
  {
	  final PhysicalGridFrame op = new PhysicalGridFrame(this.groupByAttributeName, this.timeAttributeName,
			  this.xGridAttributeName, this.yGridAttributeName, this.xCells, this.yCells, this.corners);
	  op.ae = ae;
	  return op;
  }

  @Override
  public boolean equals(final Object other)
  {
    if ((other == null) || !(other instanceof PhysicalGridFrame))
      return false;
    if (other.getClass() != PhysicalGridFrame.class)
      return other.equals(this);
    final PhysicalGridFrame  f = (PhysicalGridFrame ) other;
    return (this.groupByAttributeName.equals(f.groupByAttributeName)) && (this.timeAttributeName.equals(f.timeAttributeName))
          && (this.xGridAttributeName.equals(f.xGridAttributeName)) && (this.yGridAttributeName.equals(f.yGridAttributeName))
          && (this.xCells == f.xCells) && (this.yCells == f.yCells) && Arrays.equals(this.corners, f.corners);
  }

  @Override
  public int hashCode()
  {
//    return this.framesHashMap.hashCode();
	  // (JGM) Probably not a good hashcode.
	  return 0;
  }
  
  // (JGM) I'm not sure if this can be safely commented out for PhysicalGridFrame.
  //when stream is closed, then flush any frames still open in memory
//  public void streamClosed(int streamId) throws ShutdownException {
//	  FrameOutputX framesToOutputX = new FrameOutputX();
//	  long punctuatedUpToTime = -1;
//	  for (Map.Entry<Integer, FrameDetectorX> entry: framesHashMap.entrySet()) 
//      {       
//        framesToOutputX = entry.getValue().flushOpenFrame(segmentationFlag);
//        
//        if(null != framesToOutputX)
//        {
//            outputFrame(framesToOutputX);
//            punctuatedUpToTime = framesToOutputX.endTime;
//            // Pass on punctuation.
//        	outputPunctuation(streamId, punctuatedUpToTime);
//          } else 
//        	  framecount++;
//        }
//	
//	//System.out.println(framecount+" frames. (frame fragements count)");
//      }
	
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
