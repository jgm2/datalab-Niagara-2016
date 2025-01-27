package niagara.utils;

import niagara.physical.NodeConsumer;

import org.w3c.dom.Node;

/**
 * Class to implement a simple list of nodes
 * 
 * @version 1.0
 * 
 * @author Kristin Tufte
 */

public class NodeVector implements NodeConsumer {

	/** Contains a list of the key value lists */
	Node[] list;
	/** number of spots alloced in list */
	int allocSize;
	/** number of elts in list */
	int listSize;
	public static int count = 0;
	static private int DEFAULT_SIZE = 10;

	/**
	 * Constructor - does nothing
	 */
	public NodeVector() {
		allocSize = DEFAULT_SIZE;
		list = new Node[allocSize];
		listSize = 0;
	}

	/**
	 * Constructor - does nothing
	 */
	public NodeVector(int initialSize) {
		allocSize = initialSize;
		list = new Node[allocSize];
		listSize = 0;
	}

	public void clear() {
		fullReset();
	}

	public void quickReset() {
		listSize = 0;
	}

	public void fullReset() {
		for (int i = 0; i < allocSize; i++)
			list[i] = null;
		listSize = 0;
	}

	public void setSize(int size) {
		listSize = size;
	}

	public int hashCode() {
		int hashCode = 1;
		for (int i = 0; i < listSize; i++) {
			hashCode = 31 * hashCode + list[i].hashCode();
		}
		return hashCode;
	}

	/**
	 * getListSize - returns the size of the list
	 * 
	 * @return the size of the list
	 */
	public int size() {
		return listSize;
	}

	/**
	 * add - adds a node to the list
	 * 
	 * @param str
	 *            the string to be appended
	 */
	public void add(Node obj) {
		if (listSize == allocSize) {
			expandList();
		}
		list[listSize] = obj;
		listSize++;
	}

	public Node pop() {
		assert listSize > 0;
		int newSize = listSize - 1;
		Node n = list[newSize];
		list[newSize] = null;
		listSize = newSize;
		return n;
	}

	public void consume(Node n) {
		add(n);
	}

	/**
	 * add - adds a node to the list
	 * 
	 * @param str
	 *            the string to be appended
	 */
	public void add(int idx, Node obj) {
		while (idx >= allocSize) {
			expandList();
		}
		list[idx] = obj;
		if (idx >= listSize)
			listSize = idx + 1;
	}

	/**
	 * get - returns a value from the list
	 * 
	 * @return the Node
	 */
	public Node get(int i) {
		return list[i];
	}

	/**
	 * determines if two Lists are equal
	 * 
	 * @param other
	 *            the List to be compared with
	 * @return true if equal, false otherwise
	 */
	public boolean equals(Object obj) {
		NodeVector other = (NodeVector) obj;
		if (listSize != other.listSize) {
			return false;
		}

		/*
		 * assume here that differences are likely to be near the top of the
		 * list (this is certainly what I want for the rooted key value stuff)
		 */
		for (int i = listSize - 1; i >= 0; i--) {
			if (!(list[i] == null ? other.list[i] == null : list[i]
					.equals(other.list[i]))) {
				return false;
			}
		}
		return true;
	}

	public Object clone() {
		count++;
		NodeVector clone = new NodeVector(allocSize);
		for (int i = 0; i < listSize; i++) {
			clone.list[i] = list[i];
		}
		clone.listSize = listSize;
		return clone;
	}

	private void expandList() {
		Node[] newList = new Node[allocSize * 2];
		for (int i = 0; i < listSize; i++) {
			newList[i] = list[i];
		}
		list = newList;
		allocSize = allocSize * 2;
	}
}
