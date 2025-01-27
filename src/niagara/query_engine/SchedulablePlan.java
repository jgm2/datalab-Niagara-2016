package niagara.query_engine;

import niagara.data_manager.DataManager;
import niagara.physical.PhysicalOperator;
import niagara.utils.ShutdownException;
import niagara.utils.SinkTupleStream;

/** A SchedulablePlan is an executable DAG of query operators */
public interface SchedulablePlan {
	PhysicalOperator getPhysicalOperator();

	int getArity();

	SchedulablePlan getInput(int i);

	void setIsHead();

	boolean isHead();

	/** A unique identifier for this plan node */
	String getName();

	boolean isSchedulable();

	boolean isSource();

	void processSource(SinkTupleStream stream, DataManager dm,
			PhysicalOperatorQueue opQueue) throws ShutdownException;

	int getNumberOfOutputs();

	boolean isSendImmediate();

	void setSendImmediate();

	void setPlanID(String planID);

	String getPlanID();
}
