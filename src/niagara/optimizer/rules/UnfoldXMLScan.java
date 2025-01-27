package niagara.optimizer.rules;

import niagara.logical.FileScan;
import niagara.logical.FileScanSpec;
import niagara.logical.Project;
import niagara.logical.Unnest;
import niagara.logical.Variable;
import niagara.logical.XMLScan;
import niagara.logical.path.Constant;
import niagara.logical.path.Wildcard;
import niagara.optimizer.AnyLogicalOp;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Expr;
import niagara.optimizer.colombia.MExpr;
import niagara.optimizer.colombia.PhysicalProperty;

/** Turn XMLScan into a FileScan and a series of Unnests */
public class UnfoldXMLScan extends CustomRule {
	public UnfoldXMLScan(String name) {
		super(name, 0, new Expr(new XMLScan()),
		// XXX vpapad: The output pattern is meaningless
				new Expr(new AnyLogicalOp()));
	}

	public Expr nextSubstitute(Expr before, MExpr mexpr,
			PhysicalProperty ReqdProp) {

		XMLScan x = (XMLScan) before.getOp();
		Attribute a = x.getVariable();
		FileScanSpec fss = (FileScanSpec) x.getSpec();
		Attrs projectedAttrs = x.getProjectedAttrs();

		FileScan fs = new FileScan(fss, a);
		Expr expr = new Expr(fs);
		Attribute tupAttr = new Variable("_" + a.getName() + "_unnest_top");
		Unnest firstu = new Unnest(tupAttr, a, new Wildcard(), null,
				Unnest.OuterBehavior.STRICT);
		expr = new Expr(firstu, expr);

		Attrs as = x.getAttrs();
		for (int i = 0; i < as.size(); i++) {
			a = as.get(i);
			Unnest u = new Unnest(a, tupAttr, new Constant(a.getName()), null,
					Unnest.OuterBehavior.STRICT);
			expr = new Expr(u, expr);
		}

		if (projectedAttrs != null)
			expr = new Expr(new Project(projectedAttrs), expr);

		return expr;
	}
}
