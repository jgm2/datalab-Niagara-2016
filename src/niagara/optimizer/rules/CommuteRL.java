package niagara.optimizer.rules;

import niagara.optimizer.colombia.Expr;
import niagara.optimizer.colombia.MExpr;
import niagara.optimizer.colombia.PhysicalProperty;

/** Commute a remote subplan on the left ith a local subplan on the right */
public class CommuteRL extends CommuteJoin {
	public CommuteRL(String name) {
		super(name);
	}

	public boolean condition(Expr before, MExpr mexpr, PhysicalProperty ReqdProp) {
		if (!mexpr.getGroup().getLogProp().isMixed())
			return false;

		// Left input group must be R(emote), Right must be L(ocal)
		return mexpr.getInput(0).getLogProp().isRemote()
				&& mexpr.getInput(1).getLogProp().isLocal();
	}
}
