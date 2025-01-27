package niagara.client;

public class KillPreparedQPQuery extends Query {
	String text;

	public KillPreparedQPQuery(String text) {
		this.text = text;
	}

	public String getText() {
		return text;
	}

	public String getCommand() {
		return "kill_prepared_query";
	}

	public String getDescription() {
		return "KillPrepared";
	}

	public int getType() {
		return QueryType.KILL_PREPARED;
	}
}
