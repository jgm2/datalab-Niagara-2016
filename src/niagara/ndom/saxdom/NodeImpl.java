package niagara.ndom.saxdom;

import niagara.utils.PEException;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.UserDataHandler;

/**
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */

public abstract class NodeImpl implements Node {

	protected DocumentImpl doc;
	protected int index;

	public NodeImpl(DocumentImpl doc, int index) {
		this.doc = doc;
		this.index = index;
	}

	public void pin() {
		doc.pin();
	}

	public void unpin() {
		doc.unpin();
	}

	public boolean equals(Object other) {
		if (!(other instanceof NodeImpl))
			return false;
		NodeImpl no = (NodeImpl) other;
		return (no.doc == doc && no.index == index);
	}

	public int hashCode() {
		if (this instanceof DocumentImpl)
			return super.hashCode();

		return doc.hashCode() ^ index;
	}

	/**
	 * SAXDOM specific method for serializing a node and all its descendants in
	 * a StringBuffer
	 */
	public void flatten(StringBuffer sb, boolean prettyprint) {
		BufferManager.flatten(index, sb, prettyprint);
	}

	public abstract String getNodeName();

	public String getNodeValue() throws DOMException {
		// Default implementation - overriden in some subclasses
		return null;
	}

	public int getIndex() {
		return index;
	}

	public void setNodeValue(String nodeValue) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	public abstract short getNodeType();

	public Node getParentNode() {
		return BufferManager.getParentNode(doc, index);
	}

	public NodeList getChildNodes() {
		return BufferManager.getChildNodes(doc, index);
	}

	public Node getFirstChild() {
		return BufferManager.getFirstChild(doc, index);
	}

	public Node getLastChild() {
		return BufferManager.getLastChild(doc, index);
	}

	public Node getPreviousSibling() {
		return BufferManager.getPreviousSibling(doc, index);
	}

	public Node getNextSibling() {
		return BufferManager.getNextSibling(doc, index);
	}

	public NamedNodeMap getAttributes() {
		return BufferManager.getAttributes(doc, index);
	}

	public Document getOwnerDocument() {
		return doc;
	}

	public Node insertBefore(Node newChild, Node refChild) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	public Node replaceChild(Node newChild, Node oldChild) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	public Node removeChild(Node oldChild) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	public Node appendChild(Node newChild) throws DOMException {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"This node does not support appendChild");
	}

	public boolean hasChildNodes() {
		return BufferManager.hasChildNodes(index);
	}

	public Node cloneNode(boolean deep) {
		return null; // We can't do this, and the spec does not allow us
		// to throw an exception. Oh well.
	}

	public void normalize() {
		// Do nothing, the SAXDOM parser code should have taken care of this.
	}

	public boolean isSupported(String feature, String version) {
		// XXX vpapad: This is a prudent choice...
		return false;
	}

	public String getNamespaceURI() {
		return BufferManager.getNamespaceURI(index);
	}

	public String getPrefix() {
		return BufferManager.getPrefix(index);
	}

	public void setPrefix(String prefix) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	public String getLocalName() {
		return BufferManager.getLocalName(index);
	}

	public boolean hasAttributes() {
		return BufferManager.hasAttributes(index);
	}

	// XXX vpapad:
	// The following methods are from DOM Level 3, but we either have
	// to "implement" them, or go through major hoops to compile two
	// different DOM interface definitions (the standard one, and the
	// one included in Xerces) in the same project...

	public Node adoptNode(Node n) throws org.w3c.dom.DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"SAXDOM objects are read-only.");
	}

	public String getEncoding() {
		return null;
	}

	public void setEncoding(String s) {
	}

	public boolean getStandalone() {
		return true;
	}

	public void setStandalone(boolean b) {
	}

	public boolean getStrictErrorChecking() {
		return false;
	}

	public void setStrictErrorChecking(boolean b) {
	}

	public String getVersion() {
		return null;
	}

	public void setVersion(String s) {
	}

	public short compareDocumentPosition(Node other) throws DOMException {
		throw new PEException("Not implemented yet");
	}

	public String getBaseURI() {
		throw new PEException("Not implemented yet");
	}

	public Object getFeature(String feature, String version) {
		throw new PEException("Not implemented yet");
	}

	public String getTextContent() throws DOMException {
		throw new PEException("Not implemented yet");
	}

	public Object getUserData(String key) {
		throw new PEException("Not implemented yet");
	}

	public boolean isDefaultNamespace(String namespaceURI) {
		throw new PEException("Not implemented yet");
	}

	public boolean isEqualNode(Node arg) {
		throw new PEException("Not implemented yet");
	}

	public boolean isSameNode(Node other) {
		throw new PEException("Not implemented yet");
	}

	public String lookupNamespaceURI(String prefix) {
		throw new PEException("Not implemented yet");
	}

	public String lookupPrefix(String namespaceURI) {
		throw new PEException("Not implemented yet");
	}

	public void setTextContent(String textContent) throws DOMException {
		throw new PEException("Not implemented yet");
	}

	public Object setUserData(String key, Object data, UserDataHandler handler) {
		throw new PEException("Not implemented yet");
	}
}
