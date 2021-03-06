package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.datatypes.IntDataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * A B+ tree leaf node. A leaf node header contains the page number of the
 * parent node (or -1 if no parent exists), the page number of the previous leaf
 * node (or -1 if no previous leaf exists), and the page number of the next leaf
 * node (or -1 if no next leaf exists). A leaf node contains LeafEntry's.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class LeafNode extends BPlusNode {

  public LeafNode(BPlusTree tree) {
    super(tree, true);
    getPage().writeByte(0, (byte) 1);
    setPrevLeaf(-1);
    setParent(-1);
    setNextLeaf(-1);
  }
  
  public LeafNode(BPlusTree tree, int pageNum) {
    super(tree, pageNum, true);
    if (getPage().readByte(0) != (byte) 1) {
      throw new BPlusTreeException("Page is not Leaf Node!");
    }
  }

  @Override
  public boolean isLeaf() {
    return true;
  }

  /**
   * See BPlusNode#locateLeaf documentation.
   */
  @Override
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
    if (findFirst) {
      if (this.getPrevLeaf() != -1) {
        LeafNode prevPage = (LeafNode) BPlusNode.getBPlusNode(this.getTree(), this.getPrevLeaf());
        Iterator<RecordID> prev = prevPage.scanForKey(key);
        if (prev.hasNext()) {
          return prevPage.locateLeaf(key, findFirst);
        } else {
          return this;
        }
      } else {
        return this;
      }
    } else {
      if (this.getNextLeaf() != -1) {
        LeafNode nextPage = (LeafNode) BPlusNode.getBPlusNode(this.getTree(), this.getNextLeaf());
        Iterator<RecordID> next = nextPage.scanForKey(key);
        if (next.hasNext()) {
          return nextPage.locateLeaf(key, findFirst);
        } else {
          return this;
        }
      } else {
        return this;
      }
    }
  }

  /**
   * Splits this node and copies up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full leaf node of 2d entries will be split
   * into a left node with d entries and a right node with d entries, with the
   * leftmost key of the right node copied up.
   */
  @Override
  public void splitNode() {
    LeafNode extraNode = new LeafNode(this.getTree());
    int extraNodePgNum = extraNode.getPageNum();
    int d = this.numEntries / 2;

    //Here we generate correct list of leafEntries
    List<BEntry> leftEntries = new ArrayList<BEntry>();
    List<BEntry> rightEntries = new ArrayList<BEntry>();
    List<BEntry> allEntries = this.getAllValidEntries();
    DataType copyOfKey = null;
    for (int i=0; i < this.numEntries; i++) {
      if (i == d) {
        copyOfKey = allEntries.get(i).getKey();
      }
      if (i <= d-1) {
        leftEntries.add(i, allEntries.get(i));
      } else {
        rightEntries.add(i-d, allEntries.get(i));
      }
    }

    //Now two leaf nodes have correct entries
    this.overwriteBNodeEntries(leftEntries);
    extraNode.overwriteBNodeEntries(rightEntries);

    //Here we set up pointers with 2 cases:
    if (this.getNextLeaf() == -1) {
      extraNode.setNextLeaf(this.getNextLeaf());
      extraNode.setPrevLeaf(this.getPageNum());
      this.setNextLeaf(extraNodePgNum);
    } else {
      LeafNode adjacentNode = (LeafNode) BPlusNode.getBPlusNode(this.getTree(), this.getNextLeaf());
      adjacentNode.setPrevLeaf(extraNodePgNum);
      extraNode.setNextLeaf(adjacentNode.getPageNum());
      this.setNextLeaf(extraNodePgNum);
      extraNode.setPrevLeaf(this.getPageNum());
    }

    //Create InnerEntry and add to InnerNode
    InnerEntry newInnerEntry = new InnerEntry(copyOfKey,extraNodePgNum);
    InnerNode parentNode;
    if (this.getParent() == -1) {
      parentNode = new InnerNode(this.getTree());
      this.getTree().updateRoot(parentNode.getPageNum());
      parentNode.setFirstChild(this.getPageNum());
    } else {
      parentNode = (InnerNode) BPlusNode.getBPlusNode(this.getTree(), this.getParent());
    }
    this.setParent(parentNode.getPageNum());
    extraNode.setParent(parentNode.getPageNum());
    parentNode.insertBEntry(newInnerEntry);

    /*
    //If InnerNode becomes full then we need to split the InnerNode
    if (parentNode.getAllValidEntries().size() == this.numEntries) {
      parentNode.splitNode();
    }*/

  }
  
  public int getPrevLeaf() {
    return getPage().readInt(5);
  }

  public int getNextLeaf() {
    return getPage().readInt(9);
  }
  
  public void setPrevLeaf(int val) {
    getPage().writeInt(5, val);
  }

  public void setNextLeaf(int val) {
    getPage().writeInt(9, val);
  }

  /**
   * Creates an iterator of RecordID's for all entries in this node.
   *
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scan() {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      rids.add(le.getRecordID());
    }
    return rids.iterator();
  }

  /**
   * Creates an iterator of RecordID's whose keys are greater than or equal to
   * the given start value key.
   *
   * @param startValue the start value key
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scanFrom(DataType startValue) {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      if (startValue.compareTo(le.getKey()) < 1) {
        rids.add(le.getRecordID());
      }
    }
    return rids.iterator();
  }

  /**
   * Creates an iterator of RecordID's that correspond to the given key.
   *
   * @param key the search key
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scanForKey(DataType key) {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      if (key.compareTo(le.getKey()) == 0) { 
        rids.add(le.getRecordID());
      }
    }

    return rids.iterator();
  }

}
