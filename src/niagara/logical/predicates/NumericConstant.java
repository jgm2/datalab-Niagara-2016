package niagara.logical.predicates;

public class NumericConstant extends Constant {
	double value;

	public NumericConstant(double value) {
		this.value = value;
	}

	public NumericConstant(String value) {
		this.value = Double.parseDouble(value);
	}

	public String getValue() {
		return Double.toString(value);
	}

	public void toXML(StringBuffer sb) {
		sb.append("<number value='");
		sb.append(getValue());
		sb.append("'/>");
	}
}
