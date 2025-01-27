package niagara.logical.path;

public class Wildcard implements RE {
	public void attachToNFA(NFA nfa) {
		State s = nfa.addState(false);
		nfa.addTransition(s, Transition.wildcard, nfa.startState);
		nfa.setStartState(s);
	}

	public boolean equals(Object other) {
		if (other == null || !(other instanceof Wildcard))
			return false;
		if (other.getClass() != Wildcard.class)
			return other.equals(this);
		return true;
	}

	public int hashCode() {
		return 1;
	}

	public boolean generatesDuplicates() {
		return true;
	}
}