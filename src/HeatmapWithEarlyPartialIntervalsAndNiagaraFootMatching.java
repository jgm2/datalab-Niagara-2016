/* CS 406, Prof. Kristin Tufte
 * By:       Jim Miller
 * Date:     03/07/13
 * Updated:  03/08/13 - 03/10/13, 03/21/13 - 03/22/13
 * 
 * Updated:  October and early November, 2013, for final reference results to match against Niagara heatmap queries.
 * 			 Ignores balls.
 * 			 Calculates only 16x25 granularity.
 *
 * 2013 DEBS soccer challenges:  Heatmap
 * Generate results for reference.
 */


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.util.HashMap;


public class HeatmapWithEarlyPartialIntervalsAndNiagaraFootMatching {
	//private static final long GAME_START_PICOS =  10753295594424116L;   // First half, according to FAQ #10 (in picoseconds)
	private static final long GAME_START_PICOS = 13086639146403495L;    // Second half, according to FAQ #10
	private static final long RESULTS_UPDATE_FREQ = 1000000000000L;                                   // 1 trillion picoseconds
	private static final int FIELD_L = 0, FIELD_R = 52483, FIELD_T = 33965, FIELD_B = -33960;         // In mm (smallest rectangle
	// enclosing all 4 field corners)
	// According to FAQ #1
	private static final int FIELD_W = FIELD_R - FIELD_L, FIELD_H = FIELD_T - FIELD_B;                // In mm
	//private static final int MAX_GRAN_X = 64, MAX_GRAN_Y = 100;
	private static final int MAX_GRAN_X = 16, MAX_GRAN_Y = 25;
	private static final float MIN_CELL_W = (float)FIELD_W / MAX_GRAN_X, MIN_CELL_H = (float)FIELD_H / MAX_GRAN_Y;
	private static final float OOB_COORD = Integer.MIN_VALUE;                                         // Arbitrary, for OOB "coordinates"
	private static final int OOB_INDEX = Integer.MIN_VALUE;     												  // Arbitrary, for OOB index
	private static final int DATA_LENGTH = 60;
	//private static final String IN_STREAM_PATH = "D:/Heatmap-input/";
	private static final String IN_STREAM_PATH = "";
	private static final String IN_STREAM_FILE = IN_STREAM_PATH + "2nd-half_to_end-" + DATA_LENGTH + "s.csv";
	//private static final String IN_STREAM_FILE = IN_STREAM_PATH + "2nd-half_to_end-" + DATA_LENGTH + "s - coarsened_by_100.csv";
	private static final String RESULT_STREAM_PATH = "D:/Reference program output/";
	private static final String RESULT_STREAM_FILE_PREFIX = "HeatResultWithPartialsStream_";
	private static final String RESULT_STREAM_PREFIX = RESULT_STREAM_PATH + DATA_LENGTH + "/" + RESULT_STREAM_FILE_PREFIX;
	//private static final String RESULT_STREAM_PREFIX = RESULT_STREAM_PATH + DATA_LENGTH + " - coarsened_by_100/" + RESULT_STREAM_FILE_PREFIX;
	private static final String VISUAL_RESULT_STREAM_FILE_PREFIX = "ExcelHeatResultWithPartialsStream_";
	private static final String VISUAL_RESULT_STREAM_PREFIX = RESULT_STREAM_PATH + DATA_LENGTH + "/"  + VISUAL_RESULT_STREAM_FILE_PREFIX;
	//private static final long GAME_END_PICOS = GAME_START_PICOS + GAME_UPDATE_INTERVALS * RESULTS_UPDATE_FREQ;  // End of half, not game
	//private static final long GAME_END_PICOS = 12553295594424116L;  // Artificial end of first half (1800 seconds)
	private static final long GAME_END_PICOS =  14886639146403495L;  // Artificial end of second half (1800 seconds)
	//private static final long GAME_END_PICOS = 12557295594424116L;  // Actual end of first half, per FAQ #10 (1804 seconds!!  Why??)
	//private static final long GAME_END_PICOS = 14879639146403495L;  // Actual end of second half, per FAQ #10 (1793 seconds!!  Why??)
	// In units of RESULTS_UPDATE_FREQ
	private static final int GAME_UPDATE_INTERVALS = (int)((GAME_END_PICOS - GAME_START_PICOS) / RESULTS_UPDATE_FREQ);
	private static final int NUM_PERSONS = 17;
	//private static final int NUM_BALLS = 4;
	//private static final int NUM_SENSORS = NUM_PERSONS * 2 + NUM_BALLS;                               // Assumes exactly 2 sensors per person
	private static final int NUM_SENSORS = NUM_PERSONS * 2;
	private int curUpdateInterval;
	private int outTupleCount;
	private ArrayList<Pair<Integer, Integer>> gridGrans;                                              // Must always evenly divide MAX_GRAN_X
	// and MAX_GRAN_Y
	private int[] windowLengths;
	private HashMap<Integer, Integer> sensorMap;
	private SensorInfo[] sensors;
	//  private PlayerInfo[] players;
	
	
	// (JGM) From PhysicalHeatmapMapSidToPid in Niagara.
	int numSensors;
	private PlayerInfo[] players;

	
	
	//  private BallInfo[] balls;
	private ClassLoader loader;
	private InputStream inData;
	private BufferedReader gameDataReader;
	private BufferedWriter[] resultWriters;
	private BufferedWriter[] visualResultWriters;
	private boolean recording;      // True during game time.
	private long nextUpdateBound;
	private gameDataInTuple curTuple;
	private int curX, curY, curSid;
	private long curTS;
	private long lastUpdateTS;
	private boolean curTupleIsSplit;
	private long newIntervalTupleSplitTS;
	private Pair<Integer, Integer> curMaxGranCell;
	private CellDims[][][] cells;
	private float[][][][] heats;      // BIG array
	private float[][] oobHeats;
	private LatestUpdateDataPerWindow[][][][] cumes;
	private LatestUpdateDataPerWindow[][] oobCumes;
	private LatestUpdateDataPerResultsUpdateInterval[][][] subUpdateCumes;
	private LatestUpdateDataPerResultsUpdateInterval[] subUpdateOobCumes;
	
	// (JGM) From PhysicalHeatmapMapSidToPid in Niagara.
	private HashMap<Integer, Integer> sensorToPlayerMap;
	private HashMap<Integer, Integer> sensorToIndexMap;

	public HeatmapWithEarlyPartialIntervalsAndNiagaraFootMatching() throws IOException {
		curUpdateInterval = 0;
		outTupleCount = 0;
		//gridGrans = new ArrayList<Pair<Integer, Integer>>(Arrays.asList(new Pair<Integer, Integer>(16, 25),
		//		new Pair<Integer, Integer>(32, 50), new Pair<Integer, Integer>(64, 100)));                  // do 8x13 separately!
		
		// (JGM) Using only 16x25 granularity for generating final paper results (11/4/13). 
		gridGrans = new ArrayList<Pair<Integer, Integer>>(Arrays.asList(new Pair<Integer, Integer>(16, 25)));
		windowLengths = new int[]{60, 300, 600, 1800};                                                  // In units of RESULTS_UPDATE_FREQ
		sensorMap = new HashMap<Integer, Integer>();
		sensors = new SensorInfo[NUM_SENSORS];
		for (int i = 0; i < NUM_SENSORS; i++)
			sensors[i] = new SensorInfo();
		//    players = new PlayerInfo[NUM_PERSONS];
		//    for (int i = 0; i < NUM_PERSONS; i++)
		//      players[i] = new PlayerInfo();
		//    balls = new BallInfo[NUM_BALLS];
		//    for (int i = 0; i < NUM_BALLS; i++)
		//      balls[i] = new BallInfo();
		
		
		
		// (JGM) From PhysicalHeatmapMapSidToPid in Niagara.
		numSensors = 0;
		players = new PlayerInfo[NUM_PERSONS];
	   for (int i = 0; i < NUM_PERSONS; i++)
	   	players[i] = new PlayerInfo();
	   
	   

		recording = false;
		nextUpdateBound = GAME_START_PICOS + RESULTS_UPDATE_FREQ;

		curMaxGranCell = new Pair<Integer, Integer>();

		float cellW, cellH;
		cells = new CellDims[MAX_GRAN_X][MAX_GRAN_Y][gridGrans.size()];     // Higher array members not used at coarser granularities.
		for (int k = 0; k < gridGrans.size(); k++) {
			cellW = (float)FIELD_W / gridGrans.get(k).x;
			cellH = (float)FIELD_H / gridGrans.get(k).y;
			for (int i = 0; i < gridGrans.get(k).x; i++)
				for (int j = 0; j < gridGrans.get(k).y; j++) {
					cells[i][j][k] = new CellDims();
					cells[i][j][k].m.x = FIELD_L + cellW * (i + .5f);
					cells[i][j][k].m.y = FIELD_B + cellH * (j + .5f);
				}
		}
//		heats = new float[MAX_GRAN_X][MAX_GRAN_Y][NUM_SENSORS][GAME_UPDATE_INTERVALS];
//		oobHeats = new float[NUM_SENSORS][GAME_UPDATE_INTERVALS];
//		cumes = new LatestUpdateDataPerWindow[MAX_GRAN_X][MAX_GRAN_Y][NUM_SENSORS][windowLengths.length];
//		for (int i = 0; i < MAX_GRAN_X; i++)
//			for (int j = 0; j < MAX_GRAN_Y; j++)
//				for (int k = 0; k < NUM_SENSORS; k++)
//					for (int l = 0; l < windowLengths.length; l++)
//						cumes[i][j][k][l] = new LatestUpdateDataPerWindow();
//		oobCumes = new LatestUpdateDataPerWindow[NUM_SENSORS][windowLengths.length];
//		for (int i = 0; i < NUM_SENSORS; i++)
//			for (int j = 0; j < windowLengths.length; j++)
//				oobCumes[i][j] = new LatestUpdateDataPerWindow();
//		subUpdateCumes = new LatestUpdateDataPerResultsUpdateInterval[MAX_GRAN_X][MAX_GRAN_Y][NUM_SENSORS];
//		for (int i = 0; i < MAX_GRAN_X; i++)
//			for (int j = 0; j < MAX_GRAN_Y; j++)
//				for (int k = 0; k < NUM_SENSORS; k++)
//					subUpdateCumes[i][j][k] = new LatestUpdateDataPerResultsUpdateInterval();
//		subUpdateOobCumes = new LatestUpdateDataPerResultsUpdateInterval[NUM_SENSORS];
//		for (int i = 0; i < NUM_SENSORS; i++)
//			subUpdateOobCumes[i] = new LatestUpdateDataPerResultsUpdateInterval();
		
		heats = new float[MAX_GRAN_X][MAX_GRAN_Y][NUM_PERSONS][GAME_UPDATE_INTERVALS];
		oobHeats = new float[NUM_PERSONS][GAME_UPDATE_INTERVALS];
		cumes = new LatestUpdateDataPerWindow[MAX_GRAN_X][MAX_GRAN_Y][NUM_PERSONS][windowLengths.length];
		for (int i = 0; i < MAX_GRAN_X; i++)
			for (int j = 0; j < MAX_GRAN_Y; j++)
				for (int k = 0; k < NUM_PERSONS; k++)
					for (int l = 0; l < windowLengths.length; l++)
						cumes[i][j][k][l] = new LatestUpdateDataPerWindow();
		oobCumes = new LatestUpdateDataPerWindow[NUM_PERSONS][windowLengths.length];
		for (int i = 0; i < NUM_PERSONS; i++)
			for (int j = 0; j < windowLengths.length; j++)
				oobCumes[i][j] = new LatestUpdateDataPerWindow();
		subUpdateCumes = new LatestUpdateDataPerResultsUpdateInterval[MAX_GRAN_X][MAX_GRAN_Y][NUM_PERSONS];
		for (int i = 0; i < MAX_GRAN_X; i++)
			for (int j = 0; j < MAX_GRAN_Y; j++)
				for (int k = 0; k < NUM_PERSONS; k++)
					subUpdateCumes[i][j][k] = new LatestUpdateDataPerResultsUpdateInterval();
		subUpdateOobCumes = new LatestUpdateDataPerResultsUpdateInterval[NUM_PERSONS];
		for (int i = 0; i < NUM_PERSONS; i++)
			subUpdateOobCumes[i] = new LatestUpdateDataPerResultsUpdateInterval();

//		// sensorMap values must be consecutive, with all players before all balls
//		sensorMap.put(13, 0); sensorMap.put(14, 1);                                                     // Team A goalie, left and right feet
//		sensorMap.put(47, 2); sensorMap.put(16, 3);
//		sensorMap.put(49, 4); sensorMap.put(88, 5);
//		sensorMap.put(19, 6); sensorMap.put(52, 7);
//		sensorMap.put(53, 8); sensorMap.put(54, 9);
//		sensorMap.put(23, 10); sensorMap.put(24, 11);
//		sensorMap.put(57, 12); sensorMap.put(58, 13);
//		sensorMap.put(59, 14); sensorMap.put(28, 15);
//
//		sensorMap.put(61, 16); sensorMap.put(62, 17);                                                   // Team B goalie
//		sensorMap.put(63, 18); sensorMap.put(64, 19);
//		sensorMap.put(65, 20); sensorMap.put(66, 21);
//		sensorMap.put(67, 22); sensorMap.put(68, 23);
//		sensorMap.put(69, 24); sensorMap.put(38, 25);
//		sensorMap.put(71, 26); sensorMap.put(40, 27);
//		sensorMap.put(73, 28); sensorMap.put(74, 29);
//		sensorMap.put(75, 30); sensorMap.put(44, 31);
//
//		sensorMap.put(105, 32); sensorMap.put(106, 33);                                                 // Referee
//
//		sensorMap.put(4, 34);                                                                           // Balls
//		sensorMap.put(8, 35);
//		sensorMap.put(10, 36);
//		sensorMap.put(12, 37);
		
		
		
		// (JGM) From PhysicalHeatmapMapSidToPid in Niagara.
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
		
	

		loader = ClassLoader.getSystemClassLoader();
		openReaders();
		resultWriters = new BufferedWriter[gridGrans.size() * windowLengths.length];
		visualResultWriters = new BufferedWriter[gridGrans.size() * windowLengths.length];
		openResultWriters();
	}

	public static void main(final String[] argv) throws IOException {
		final long startMillis, endMillis;
		String runTime;

		System.out.printf("RUN START TIME:  " + new Date().toString() + "\n\n");
		startMillis = System.currentTimeMillis();

		HeatmapWithEarlyPartialIntervalsAndNiagaraFootMatching heat = new HeatmapWithEarlyPartialIntervalsAndNiagaraFootMatching();
		heat.processGameData(heat.gameDataReader);
		heat.gameDataReader.close();
		heat.closeResultWriters();

		endMillis = System.currentTimeMillis() - startMillis;
		System.out.printf("\nRUN END TIME:  " + new Date().toString() + "\n");
		runTime = String.format("%d min, %d sec", TimeUnit.MILLISECONDS.toMinutes(endMillis),
				TimeUnit.MILLISECONDS.toSeconds(endMillis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(endMillis)));
		System.out.printf("TOTAL RUN TIME:  " + runTime  + "\n");
	}

	private void openReaders() throws IOException {
		inData = loader.getResourceAsStream(IN_STREAM_FILE);
		gameDataReader = new BufferedReader(new InputStreamReader(inData));
	}

	private void processGameData(BufferedReader gameD) throws IOException {
		getNextLineTokens(gameD, ",");

		// DO!:  Deal with ygran=13 case separately in this method, before main array processing.
		// new Pair<Integer, Integer>(8, 13), in gridGrans  ---Possibly just y value needs dealing with!
		// ***** But output streams' data will be out of order!! *****

		curTuple = readGameDataTuple(gameD, ",");
		for (curTS = curTuple.ts; (curTuple != null) && ((curTS = curTuple.ts) < GAME_END_PICOS);
				curTuple = readGameDataTuple(gameD, ",")) {   // Stop the read loop when last tuple in file reached, or end of game ts reached.
			if (!recording)
				if (curTS >= GAME_START_PICOS)
					recording = true;
				else
					continue;                       // Ignore tuples with timestamps before the start of official game time (for whichever half).
			curSid = curTuple.sid;
			//if (!sensorMap.containsKey(curSid)) // Ignore tuples for objects we are not currently tracking (such as goalie arms).
			if (!sensorToIndexMap.containsKey(curSid))
				continue;
			if (curTS >= nextUpdateBound) {
				splitTupleAcrossUpdateBound();  // Sets newIntervalTupleSplitTS, and recalculates timestamp in curTuple (for old interval).
				processInputTuple();    // On the old interval's (last) fractional tuple.  Create new values for the sub-update arrays.
				processResultTuples();  // First process the old interval, creating new values for the heats arrays and the cumes array,
				// then clear the sub-update cumes and set the ts for the fractional tuple (using newIntervalTupleSplitTS)
				// in the new interval.  Then, create results tuples (adding together players' feet and dividing by
				// window lengths to do so).
			}
			processInputTuple();  // Default handling if update boundary not reached; otherwise processes first fraction of NEW interval.
		}
		recording = false;
		if (curTS >= GAME_END_PICOS) {
			splitTupleAcrossUpdateBound();  // For last partial tuple in game
			if (sensorMap.containsKey(curTuple.sid))
				processInputTuple();          // For last partial tuple in game
			processResultTuples();          // For last boundary in game
		} 
	}

	private void splitTupleAcrossUpdateBound() {
		curTupleIsSplit = true;
		newIntervalTupleSplitTS = curTuple.ts;
		curTS = curTuple.ts = nextUpdateBound;
	}

	private void processInputTuple() {
		int pid;
		
		//int sensorNum = sensorMap.get(curTuple.sid);
		//int sensorNum = sensorToIndexMap.get(curTuple.sid);

		//curX = curTuple.x;
		//curY = curTuple.y;
		pid = mapSidToPidAndFindPidPosition(curTuple.sid, curTuple);
		if (players[pid].x >= FIELD_L && players[pid].x < FIELD_R && players[pid].y >= FIELD_B && players[pid].y < FIELD_T) {    // In-bounds
			getCurMaxGranCell(pid);
			// Assign the ENTIRE subinterval, between sensor readings for the current sensor, to the current grid cell (via curTS) - questionable policy?
			subUpdateCumes[curMaxGranCell.x][curMaxGranCell.y][pid].percentT += ((float)(curTS - players[pid].lastUpdateTS) / RESULTS_UPDATE_FREQ);
		}
		else                                                                             // Out-of-bounds
			// Assign the ENTIRE subinterval, between sensor readings for the current sensor, to oob - questionable policy?
			subUpdateOobCumes[pid].percentT += ((float)(curTS - players[pid].lastUpdateTS) / RESULTS_UPDATE_FREQ);
		players[pid].lastUpdateTS = curTS;
		lastUpdateTS = curTS;
	}
	
	

	// (JGM) Mostly from PhysicalHeatmapMapSidToPid in Niagara.
	// Here it returns the calculated pid.
	// *** This method also calculates current player-as-a-whole position (midpoint of leg sensors) and stores it in
	// players[pid] ***
	private int mapSidToPidAndFindPidPosition(int sid, gameDataInTuple curTuple) {
		int currPidSidL, currPidSidR;
		float currPidOtherSidX, currPidOtherSidY;
		int pid;
		float x, y;

		if (sensorToPlayerMap.containsKey(sid)) {
			pid = sensorToPlayerMap.get(sid);
			currPidSidL = players[pid].sidL;
			currPidSidR = players[pid].sidR;
			x = curTuple.x;
			y = curTuple.y;
			if (sid == currPidSidL) {
				currPidOtherSidX = sensors[sensorToIndexMap.get(currPidSidR)].x;
				currPidOtherSidY = sensors[sensorToIndexMap.get(currPidSidR)].y;
				if (x != sensors[sensorToIndexMap.get(currPidSidL)].x || y != sensors[sensorToIndexMap.get(currPidSidL)].y) {
					sensors[sensorToIndexMap.get(currPidSidL)].x = x;
					if (currPidOtherSidX != Integer.MIN_VALUE)
						x = (currPidOtherSidX + x) / 2;
					players[pid].x = x;
					sensors[sensorToIndexMap.get(currPidSidL)].y = y;
					if (currPidOtherSidY != Integer.MIN_VALUE)
						y = (currPidOtherSidY + y) / 2;
					players[pid].y = y;
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
			}
			return pid;
		}
		else
			return -1;
	}
	
	
	

	private void processResultTuples() throws IOException {
		for (int k = 0; k < NUM_PERSONS; k++) {
			for (int i = 0; i < MAX_GRAN_X; i++)
				for (int j = 0; j < MAX_GRAN_Y; j++) {
					heats[i][j][k][curUpdateInterval] = subUpdateCumes[i][j][k].percentT;      // In-bounds, update heats array
					for (int l = 0; l < windowLengths.length; l++) {                           // Update cumes array
						if (curUpdateInterval >= windowLengths[l]) {    // Window length was already filled out, so must drop a unit from the front
							cumes[i][j][k][l].percentT -= heats[i][j][k][curUpdateInterval - windowLengths[l]];
						}
						cumes[i][j][k][l].percentT += heats[i][j][k][curUpdateInterval];        // Always add a unit from the back (current interval)
					}
				}
			oobHeats[k][curUpdateInterval] = subUpdateOobCumes[k].percentT;                  // Out-of-bounds, update oob heats array
			for (int l = 0; l < windowLengths.length; l++) {                                 // Update oob cumes array
				if (curUpdateInterval >= windowLengths[l]) {        // Window length was already filled out, so must drop a unit from the front
					oobCumes[k][l].percentT -= oobHeats[k][curUpdateInterval - windowLengths[l]];
				}
				oobCumes[k][l].percentT += oobHeats[k][curUpdateInterval];                    // Always add a unit from the back (current interval)
			}
		}

		for (int i = 0; i < MAX_GRAN_X; i++)                                                // Clear all of the cumes from the sub-update arrays
			for (int j = 0; j < MAX_GRAN_Y; j++)
				for (int k = 0; k < NUM_PERSONS; k++)
					subUpdateCumes[i][j][k].reset();
		for (int i = 0; i < NUM_PERSONS; i++)
			subUpdateOobCumes[i].reset();

		if (curTupleIsSplit) {                                                              // Prep the remaining partial tuple, if necessary
			curTS = curTuple.ts = newIntervalTupleSplitTS;
			curTupleIsSplit = false;
		}

		heatStreamOutTuple outTuple;
		visualHeatStreamOutTuple visualOutTuple;
		int resultWriterNum;
		float aggPlayerInSupercellPerWindowLengthT;                                         // Aggregate percentage time for aggregate supercell
		int xGran, yGran;
		int supercellWidthAtGridGran, supercellHeightAtGridGran;
		ArrayList<Float> visualYPercentsT = new ArrayList<Float>();
		float percentOfPartialWindow;																	      // PARTIAL INTERVALS CODE ADDED 10/15/13
		float windowLengthsltimespercentOfPartialWindow;											   // PARTIAL INTERVALS CODE ADDED 10/15/13
		float twotimeswindowLengthsltimespercentOfPartialWindow;								      // PARTIAL INTERVALS CODE ADDED 10/15/13

		for (int l = 0; l < windowLengths.length; l++) {                                    // **HERE WE GO**...write tuples to the result streams
			//      if (curUpdateInterval < windowLengths[l] - 1)                            // Skip if we have an incomplete window
			//        continue;
			if (curUpdateInterval < windowLengths[l] - 1)   											// PARTIAL INTERVALS CODE ADDED 10/15/13
				percentOfPartialWindow = ((float)curUpdateInterval + 1) / windowLengths[l];  	// PARTIAL INTERVALS CODE ADDED 10/15/13
			else  																									// PARTIAL INTERVALS CODE ADDED 10/15/13
				percentOfPartialWindow = 1.0f;  																// PARTIAL INTERVALS CODE ADDED 10/15/13
			windowLengthsltimespercentOfPartialWindow = windowLengths[l] * percentOfPartialWindow;					// PARTIAL INTERVALS CODE ADDED 10/15/13
			//twotimeswindowLengthsltimespercentOfPartialWindow = 2 * windowLengths[l] * percentOfPartialWindow;	// PARTIAL INTERVALS CODE ADDED 10/15/13
			for (int m = 0; m < gridGrans.size(); m++) {
				resultWriterNum = m * windowLengths.length + l;
				xGran = gridGrans.get(m).x;
				yGran = gridGrans.get(m).y;
				supercellWidthAtGridGran = MAX_GRAN_X / xGran;                                // Expressed in number of MAX_GRAN_X-granularity cells
				supercellHeightAtGridGran = MAX_GRAN_Y / yGran;                               // Expressed in number of MAX_GRAN_Y-granularity cells
				int maxGranXIndexAtGridGran, maxGranYIndexAtGridGran;                         // Corresponding indices in the cell matrix at max gran
				//for (int k = 0; k < NUM_PERSONS + NUM_BALLS; k++) {
				for (int k = 0; k < NUM_PERSONS; k++) {
					aggPlayerInSupercellPerWindowLengthT = 0.0f;                               // Write tuple for the out-of-bounds area
					if (k < NUM_PERSONS) {                                                     // Dealing with a person (2 sensors)
						//aggPlayerInSupercellPerWindowLengthT += (oobCumes[k * 2][l].percentT + oobCumes[k * 2 + 1][l].percentT);
						aggPlayerInSupercellPerWindowLengthT += oobCumes[k][l].percentT;
						//      		  aggPlayerInSupercellPerWindowLengthT /= (2 * windowLengths[l]);     	  // Get the % of time relative to length of window
						//aggPlayerInSupercellPerWindowLengthT /= twotimeswindowLengthsltimespercentOfPartialWindow;			// PARTIAL INTERVALS CODE ADDED 10/15/13
						aggPlayerInSupercellPerWindowLengthT /= windowLengthsltimespercentOfPartialWindow;
					}
//					else {                                                                     // Dealing with a ball (1 sensor)
//						aggPlayerInSupercellPerWindowLengthT += oobCumes[NUM_PERSONS + k][l].percentT;
//						//      		  aggPlayerInSupercellPerWindowLengthT /= windowLengths[l];               // Get the % of time relative to length of window
//						aggPlayerInSupercellPerWindowLengthT /= windowLengthsltimespercentOfPartialWindow;				// PARTIAL INTERVALS CODE ADDED 10/15/13
//					}
					outTuple = new heatStreamOutTuple(lastUpdateTS, k, new Pair<Float, Float>(OOB_COORD, OOB_COORD), aggPlayerInSupercellPerWindowLengthT * 100);
					writeResultTuple(outTuple, resultWriters[resultWriterNum], ",");
					//      	  visualYPercentsT.add(aggPlayerInSupercellPerWindowLengthT * 100);
					//      	  visualOutTuple = new visualHeatStreamOutTuple(lastUpdateTS, k, OOB_INDEX, visualYPercentsT);
					//      	  writeVisualResultTuple(visualOutTuple, visualResultWriters[resultWriterNum], ",");
					//      	  visualYPercentsT.clear();
					for (int n = 0; n < xGran; n++) {                               				// Now write tuples for the grid cells
						for (int o = 0; o < yGran; o++) {
							aggPlayerInSupercellPerWindowLengthT = 0.0f;
							maxGranXIndexAtGridGran = n * supercellWidthAtGridGran;
							maxGranYIndexAtGridGran = o * supercellHeightAtGridGran;
							for (int p = 0; p < supercellWidthAtGridGran; p++)                   // Loop through component subcells of current supercell
								for (int q = 0; q < supercellHeightAtGridGran; q++)
									// ***** OPTIMIZE ORDER OF NESTING BETTER!!! *****
									if (k < NUM_PERSONS) {                                         // Dealing with a person (2 sensors)
										//aggPlayerInSupercellPerWindowLengthT += (cumes[maxGranXIndexAtGridGran + p][maxGranYIndexAtGridGran + q][k * 2][l].percentT
										//		+ cumes[maxGranXIndexAtGridGran + p][maxGranYIndexAtGridGran + q][k * 2 + 1][l].percentT);
										aggPlayerInSupercellPerWindowLengthT += cumes[maxGranXIndexAtGridGran + p][maxGranYIndexAtGridGran + q][k][l].percentT;
									}
//									else                                                           // Dealing with a ball (1 sensor)
//										aggPlayerInSupercellPerWindowLengthT += cumes[maxGranXIndexAtGridGran + p][maxGranYIndexAtGridGran + q][NUM_PERSONS + k][l].percentT;
							if (k < NUM_PERSONS)
								//      				  aggPlayerInSupercellPerWindowLengthT /= (2 * windowLengths[l]);
								//aggPlayerInSupercellPerWindowLengthT /= twotimeswindowLengthsltimespercentOfPartialWindow;	// PARTIAL INTERVALS CODE ADDED 10/15/13
								aggPlayerInSupercellPerWindowLengthT /= windowLengthsltimespercentOfPartialWindow;
//							else
//								//      				  aggPlayerInSupercellPerWindowLengthT /= windowLengths[l];
//								aggPlayerInSupercellPerWindowLengthT /= windowLengthsltimespercentOfPartialWindow;		// PARTIAL INTERVALS CODE ADDED 10/15/13
							//      			  visualYPercentsT.add(aggPlayerInSupercellPerWindowLengthT * 100);
							outTuple = new heatStreamOutTuple(lastUpdateTS, k, cells[n][o][m].m, aggPlayerInSupercellPerWindowLengthT * 100);
							writeResultTuple(outTuple, resultWriters[resultWriterNum], ",");
						}
						//      		  visualOutTuple = new visualHeatStreamOutTuple(lastUpdateTS, k, n, visualYPercentsT);
						//      		  writeVisualResultTuple(visualOutTuple, visualResultWriters[resultWriterNum], ",");
						//      		  visualYPercentsT.clear();
					}
				}
			}
		}

		System.out.printf(curUpdateInterval + "\n");
		System.out.printf("Total output tuples so far:  " + outTupleCount + "\n");

		nextUpdateBound += RESULTS_UPDATE_FREQ;                                          // Prep the next interval
		curUpdateInterval++;
	}

	private void getCurMaxGranCell(int pid) {
		curMaxGranCell.x = (int)((players[pid].x - FIELD_L) / MIN_CELL_W);
		curMaxGranCell.y = (int)((players[pid].y - FIELD_B) / MIN_CELL_H);
	}

	private gameDataInTuple readGameDataTuple(BufferedReader reader, String delimiter) throws IOException {
		String[] tokens = getNextLineTokens(reader, delimiter);
		if (tokens != null) {
			gameDataInTuple tuple = new gameDataInTuple(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]),
					Integer.parseInt(tokens[2]), Integer.parseInt(tokens[3]));
			return tuple;
		}
		else
			return null;
	}

	//If returns null (blank line), then done with file.
	private String[] getNextLineTokens (BufferedReader reader, String delimiter) throws IOException {
		String line = null;

		if ((line = reader.readLine()) != null)
			return line.split(delimiter);
		else
			return null;
	}

	private void openResultWriters() throws IOException {
		int i = 0;

		for (Pair<Integer, Integer> gran : gridGrans)
			for (Integer wLen : windowLengths) {
				resultWriters[i] = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(RESULT_STREAM_PREFIX + DATA_LENGTH + "s_" + gran.x +
						"," + gran.y + "_" + wLen + ".csv")));
				visualResultWriters[i] = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(VISUAL_RESULT_STREAM_PREFIX + DATA_LENGTH + "s_" + gran.x +
						"," + gran.y + "_" + wLen + ".csv")));
				i++;
			}
	}

	private void writeResultTuple(heatStreamOutTuple tuple, BufferedWriter writer, String delimiter) throws IOException {
		String line = Long.toString(tuple.lastUpdateTS) + delimiter + Integer.toString(tuple.pid) + delimiter +
				tuple.cellM.x + delimiter + tuple.cellM.y + delimiter + tuple.percentT;
		writer.write(line);
		writer.newLine();
		outTupleCount++;
	}

	private void writeVisualResultTuple(visualHeatStreamOutTuple tuple, BufferedWriter writer, String delimiter) throws IOException {
		String line = Long.toString(tuple.lastUpdateTS) + delimiter + Integer.toString(tuple.pid) + delimiter +
				Integer.toString(tuple.xCell);
		for (int i = 0; i < tuple.yPercentsT.size(); i++)
			line += (delimiter + tuple.yPercentsT.get(i));
		writer.write(line);
		writer.newLine();
	}

	private void closeResultWriters() throws IOException {
		for (int i = 0; i < gridGrans.size() * windowLengths.length; i++) {
			resultWriters[i].close();
			visualResultWriters[i].close();
		}
	}

	// For reading the "game interruptions" times - not used
	//  private long eventTimeToAbsolutePicos(final String eventTime) {
	//    long picos = GAME_START_PICOS;
	//    
	//    if (!eventTime.equals("0"))
	//      picos += (Integer.parseInt(eventTime.substring(0, 1)) * 60000000000000L
	//             + Integer.parseInt(eventTime.substring(3, 4)) * 1000000000000L
	//             + Integer.parseInt(eventTime.substring(6, 6)) * 100000000000L);
	//    return picos; 
	//  }

	private class CellDims {
		Pair<Float, Float> m;

		CellDims() {
			m = new Pair<Float, Float>();
		}
	}

	private class LatestUpdateDataPerResultsUpdateInterval {
		float percentT;

		void reset() {
			percentT = 0.0f;
		}
	}

	private class LatestUpdateDataPerWindow {
		//    long windowStartTS;
		float percentT;

		LatestUpdateDataPerWindow() {
			//      windowStartTS = GAME_START_PICOS;
		}
	}

//	private class SensorInfo {
//		//    int x, y;
//		//    int xCell, yCell, sid;
//		long lastUpdateTS;
//
//		SensorInfo() {
//			//      x = y = Integer.MIN_VALUE;
//			//      xCell = yCell = -1;
//			//      sid = 0;
//			this.lastUpdateTS = GAME_START_PICOS;    // Assume all sensors are running by the start of the game
//		}
//	}
	
	
	
	// (JGM) Mostly from PhysicalHeatmapMapSidToPid in Niagara.
	private class PlayerInfo {
		float x, y;
		int sidL, sidR;
		long lastUpdateTS;

		PlayerInfo() {
			x = y = Integer.MIN_VALUE;
			sidL = sidR = -1;
			this.lastUpdateTS = GAME_START_PICOS;    // Assume all players' sensors are running by the start of the game
		}
	}
	
	// (JGM) Mostly from PhysicalHeatmapMapSidToPid in Niagara.
	private class SensorInfo {
		float x, y;
		long lastUpdateTS;

		SensorInfo() {
			x = y = Integer.MIN_VALUE;
			this.lastUpdateTS = GAME_START_PICOS;    // Assume all sensors are running by the start of the game
		}
	}
	
	
	

	//  private class PlayerInfo {
	//    int x, y;
	//    int xCell, yCell, pid;
	//      int sidL, sidR;
	//    long lastUpdateTS;
	//    
	//    PlayerInfo() {
	//      x = y = Integer.MIN_VALUE;
	//      xCell = yCell = -1;
	//      pid = 0;
	//      sidL = sidR = 0;
	//      this.lastUpdateTS = GAME_START_PICOS;
	//    }
	//  }

	//  private class BallInfo {
	//    int x, y;
	//    int xCell, yCell, bid;
	//    int sid;
	//    long lastUpdateTS;
	//    
	//    BallInfo() {
	//      x = y = Integer.MIN_VALUE;
	//      xCell = yCell = -1;
	//      bid = 0;
	//      sid = 0;
	//      this.lastUpdateTS = GAME_START_PICOS;
	//    }
	//  }

	private class gameDataInTuple {
		long ts;
		int sid, x, y;

		gameDataInTuple(int sid, long ts, int x, int y) {
			this.sid = sid; this.ts = ts; this.x = x; this.y = y;
		}
	}

	private class heatStreamOutTuple {
		long lastUpdateTS;
		int pid;
		Pair<Float, Float> cellM;
		float percentT;

		heatStreamOutTuple(long ts, int pid, Pair<Float, Float> cellM, float percentT) {
			lastUpdateTS = ts; this.pid = pid; this.cellM = cellM; this.percentT = percentT;
		}
	}

	private class visualHeatStreamOutTuple {
		long lastUpdateTS;
		int pid;
		int xCell;
		ArrayList<Float> yPercentsT;

		visualHeatStreamOutTuple(long ts, int pid, int xCell, ArrayList<Float> yPercentsT) {
			lastUpdateTS = ts; this.pid = pid; this.xCell = xCell; this.yPercentsT = yPercentsT;
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
