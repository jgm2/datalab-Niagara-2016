package niagara.ndom.saxdom;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * We don't support DTDs yet - every method returns null.
 * 
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */

public class DocumentTypeImpl extends NodeImpl {

	// private Document doc;

	public DocumentTypeImpl(DocumentImpl doc, int index) {
		super(doc, index);
	}

	public short getNodeType() {
		return Node.DOCUMENT_TYPE_NODE;
	}

	public String getNodeName() {
		// XXX vpapad: What are we supposed to return here?
		return null;
	}

	public String getName() {
		return null;
	}

	public NamedNodeMap getEntities() {
		return null;
	}

	public NamedNodeMap getNotations() {
		return null;
	}

	public String getPublicId() {
		return null;
	}

	public String getSystemId() {
		return null;
	}

	public String getInternalSubset() {
		return null;
	}

}
