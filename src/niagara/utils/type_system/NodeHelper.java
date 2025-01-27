package niagara.utils.type_system;

import org.w3c.dom.Node;

/**
 * Interface for classes which will help give this system a very limted form of
 * typing.
 * 
 * @version 1.0
 * 
 * @author Kristin Tufte
 */

@SuppressWarnings("unchecked")
public interface NodeHelper {
	public boolean nodeEquals(Node lNode, Node rNode);

	public Class getNodeClass();

	/* converts a Node into an object of the appropriate type */
	public Object valueOf(Node node);

	public String getName();
}
