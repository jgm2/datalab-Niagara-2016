package niagara.query_engine;

import java.util.Vector;

import niagara.connection_server.NiagraServer;
import niagara.connection_server.ResultTransmitter;
import niagara.connection_server.ServerQueryInfo;
import niagara.data_manager.DataManager;
import niagara.utils.PageStream;
import niagara.utils.ShutdownException;

/**
 * The QueryEngine class executes queries written in XML-QL and returns an XML
 * result. It has the public function <code>executeQuery(string)</code> for
 * executing queries. This function returns an object of type QueryResult to the
 * caller, which is used for subsequent interactions between the caller and the
 * query. A QueryThread object runs each query (each of which uses multiple
 * PhysicalOperatorThreads)
 * 
 * @version 1.0
 * 
 * 
 * @see QueryResult
 * @see QueryThread
 * @see PhysicalOperatorThread
 */
@SuppressWarnings("unchecked")
public class QueryEngine {
	// The data manager associated with the query engine
	private DataManager dataManager;

	// The queue of queries to be run. Query Threads block on get()
	private QueryQueue queryQueue;

	// The queue of operators
	private PhysicalOperatorQueue opQueue;

	// A hashtable of active queries
	private ActiveQueryList activeQueries;

	// Group of threads waiting for queries to enter the system
	private Vector queryThreads;

	// Group of operator threads waiting to process operators
	private Vector opThreads;

	// The execution scheduler that schedules operators
	private ExecutionScheduler scheduler;

	// A query id counter, accessed by a synchronized method
	private int lastQueryId;

	// private NiagraServer server;

	/**
	 * Constructor() - Create the query engine. Initialize all thread queues and
	 * other query engine data members
	 * 
	 * @param server
	 *            the niagara server we're running in
	 * @param maxOperators
	 *            the maximum number of operator threads
	 * @param maxQueries
	 *            the maximum number of query threads
	 * @param useConnectionManager
	 *            flag to allow connection/no connection mngr
	 */
	public QueryEngine(NiagraServer server, int maxQueries, int maxOperators) {

		System.out.println("Query engine starts with " + maxQueries
				+ " query threads and " + maxOperators + " operator threads");

		// this.server = server;

		// Initialize the data manager
		//
		// server.getCatalog() -> NiagaraServer.getCatalog()
		dataManager = new DataManager(NiagraServer.getCatalog(), ".", // Path
																		// for
																		// temp
				// files
				10000, // Disk space
				0, //
				10, // Fetch threads
				5); // URL Threads

		// Create a vector for operators scheduled for execution
		opQueue = new PhysicalOperatorQueue(maxOperators);

		// Create operator thread vector and fill it with operator threads
		opThreads = new Vector(maxOperators);

		for (int opthread = 0; opthread < maxOperators; ++opthread) {
			opThreads.addElement(new PhysicalOperatorThread(opQueue));
		}

		// Create the query scheduler
		//
		scheduler = new ExecutionScheduler(server, dataManager, opQueue);

		// Create the active query list
		activeQueries = new ActiveQueryList();

		// Create the query queue
		queryQueue = new QueryQueue(maxQueries);

		// Create query thread vector and fill it with query threads
		queryThreads = new Vector(maxQueries);

		for (int qthread = 0; qthread < maxQueries; ++qthread) {
			queryThreads.addElement(new QueryThread(dataManager, queryQueue,
					scheduler));
		}

		// Inform that Query Engine is ready for processing queries
		System.out.println("Query Engine Ready");
	}

	/**
	 * The function called by all clients of the query engine who want to run a
	 * query. A new query information object is created, entered into the query
	 * queue and active query list, and wrapped in a query result object which
	 * is returned to the client
	 * 
	 * @param query
	 *            the query to execute
	 * 
	 * @return qid a query id or a negative err code
	 */
	public synchronized void executeQuery(ResultTransmitter transmitter,
			ServerQueryInfo sqi, String query) {
		// Get the next qid
		int qid = getNextQueryId();

		// Generate the output stream
		PageStream resultStream = new PageStream("To QueryResult");

		// Create a query information object
		QueryInfo queryInfo;

		try {
			queryInfo = new QueryInfo(query, qid, resultStream, activeQueries,
					true);
		} catch (ActiveQueryList.QueryIdAlreadyPresentException e) {
			System.err.println("Error in Assigning Unique Query Ids!");
			return;
		}

		// Add it to the query queue FIX:: May have to make this non-blocking
		/*
		 * Note, this puts the query info in a queue for a QueryThread to pick
		 * up. The Query Thread takes care of the parsing and scheduling of this
		 * query
		 */
		queryQueue.addQuery(queryInfo);

		sqi.setQueryResult(new QueryResult(qid, resultStream));

		// Get the result transmitter going
		opQueue.putOperator(transmitter);
	}

	/**
	 * Originally from trigger code. Used by query plan queries now. A new query
	 * information object is created, entered into the query queue and active
	 * query list, and wrapped in a query result object which is returned
	 * 
	 * This interface is used to run a already optimized plan
	 * 
	 * @param optimized
	 *            logicalPlan root
	 * 
	 * @return QueryResult
	 */

	// KT broke executeOptimizedQuery into two functions, this
	// function MUST return the queryResult object quickly
	// otherwise short queries will be killed before queryInfo.transmitter
	// is set
	public synchronized QueryResult getNewQueryResult()
			throws ShutdownException {

		// Get the next qid
		int qid = getNextQueryId();

		// Generate the output streams for each query root
		PageStream resultStream = new PageStream("To Query Result");

		// Create the query result object to return to the caller
		QueryResult queryResult = new QueryResult(qid, resultStream);
		return queryResult;
	}

	public synchronized void execOptimizedQuery(ResultTransmitter transmitter,
			SchedulablePlan planRoot, QueryResult queryResult)
			throws ShutdownException {

		try {
			// Get the result transmitter going
			opQueue.putOperator(transmitter);

			// Create a query information object
			QueryInfo queryInfo;

			queryInfo = new QueryInfo("", queryResult.getQueryId(), queryResult
					.getOutputPageStream(), activeQueries, true);

			if (planRoot.getPlanID() != null)
				queryInfo.setPlanID(planRoot.getPlanID());

			// call Execution Scheduler to generate the physical
			// plan and execute the group plan.
			scheduler.executeOperators(planRoot, queryInfo);

		} catch (ActiveQueryList.QueryIdAlreadyPresentException e) {
			assert false : "Error in Assigning Unique Query Ids!";
		}
	}

	/**
	 * Dumps the query engine and its components as a string for debugging
	 * 
	 * @return a string representation of the query engine
	 */

	public synchronized String toString() {

		String retStr = new String(
				"\n**********************************************************************\n");
		retStr += "**    Q u e r y      E n g i n e      D u m p                       **\n";
		retStr += "**********************************************************************\n";
		retStr += "lastQueryId: " + lastQueryId + "\n";
		retStr += "+++++++++++++++ Waiting Queries ++++++++++++++++++++++++++++++++++++++\n";
		retStr += queryQueue.toString() + "\n\n";
		retStr += "+++++++++++++++ Active Queries +++++++++++++++++++++++++++++++++++++++\n";
		retStr += activeQueries.toString() + "\n";
		retStr += "+++++++++++++++ Threads ++++++++++++++++++++++++++++++++++++++++++++++\n\n";
		retStr += queryThreads.toString() + "\n\n";
		retStr += opThreads.toString() + "\n\n";
		retStr += "+++++++++++++++ Operators ++++++++++++++++++++++++++++++++++++++++++++\n";
		retStr += opQueue.toString() + "\n";
		retStr += "+++++++++++++++ Scheduler ++++++++++++++++++++++++++++++++++++++++++++\n";
		retStr += scheduler.toString() + "\n";
		retStr += "**********************************************************************\n";

		return retStr;
	}

	/**
	 * Gracefully shutdown the query engine.
	 * 
	 */
	public void shutdown() {
		dataManager.shutdown();
	}

	/**
	 * return the dataManager instance in the query engine.
	 */
	public DataManager getDataManager() {
		return dataManager;
	}

	public ExecutionScheduler getScheduler() {
		return scheduler;
	}

	/**
	 * getNextQueryId() - Increments and returns the next unique queryId
	 * 
	 * @return int - the next query id
	 */
	private int getNextQueryId() {
		return lastQueryId++;
	}
}
