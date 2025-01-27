package niagara.logical.predicates;

public class StringConstant extends Constant {
	private String value;

	public StringConstant(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	// XXX vpapad: If the value contains quotes or any
	// other invalid characters, this conversion fails.
	public void toXML(StringBuffer sb) {
		sb.append("<string value='");
		sb.append(getValue());
		sb.append("'/>");
	}
}
