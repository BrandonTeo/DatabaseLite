package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;

import java.util.List;
import java.util.ArrayList;

/**
 * A B+ tree inner node. An inner node header contains the page number of the
 * parent node (or -1 if no parent exists), and the page number of the first
 * child node (or -1 if no child exists). An inner node contains InnerEntry's.
 * Note that an inner node can have duplicate keys if a key spans multiple leaf
 * pages.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class InnerNode extends BPlusNode {

  public InnerNode(BPlusTree tree) {
    super(tree, false);
    getPage().writeByte(0, (byte) 0);
    setFirstChild(-1);
    setParent(-1);
  }
  
  public InnerNode(BPlusTree tree, int pageNum) {
    super(tree, pageNum, false);
    if (getPage().readByte(0) != (byte) 0) {
      throw new BPlusTreeException("Page is not Inner Node!");
    }
  }

  @Override
  public boolean isLeaf() {
    return false;
  }

  public int getFirstChild() {
    return getPage().readInt(5);
  }
  
  public void setFirstChild(int val) {
    getPage().writeInt(5, val);
  }

  /**
   * See BPlusNode#locateLeaf documentation.
   */
  @Override
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
    List<BEntry> validEntries = this.getAllValidEntries();
    int numOfEntries = validEntries.size();
    for (int i = 0; i < numOfEntries; i++) {
      int compareVal = validEntries.get(i).getKey().compareTo(key);
      if (compareVal > 0) {
        if (i == 0) {
          return BPlusNode.getBPlusNode(this.getTree(), getFirstChild()).locateLeaf(key, findFirst);
        } else {
          return BPlusNode.getBPlusNode(this.getTree(), validEntries.get(i - 1).getPageNum()).locateLeaf(key, findFirst);
        }
      } else if (compareVal == 0 && i+1 < numOfEntries) {
        DataType nextKey = validEntries.get(i+1).getKey();
        if (nextKey.compareTo(key) == 0) {
          return BPlusNode.getBPlusNode(this.getTree(), validEntries.get(i).getPageNum()).locateLeaf(key, findFirst);
        }
      }
    }
    return BPlusNode.getBPlusNode(this.getTree(), validEntries.get(numOfEntries - 1).getPageNum()).locateLeaf(key, findFirst);
  }

  /**
   * Splits this node and pushes up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full inner node of 2d entries will be split
   * into a left node with d entries and a right node with d-1 entries, with the
   * middle key pushed up.
   */
  @Override
  public void splitNode() {
    InnerNode extraNode = new InnerNode(this.getTree());
    int extraNodePgNum = extraNode.getPageNum();
    int d = this.numEntries / 2;

    //Generate correct list of leafEntries
    List<BEntry> leftEntries = new ArrayList<BEntry>();
    List<BEntry> rightEntries = new ArrayList<BEntry>();
    List<BEntry> allEntries = this.getAllValidEntries();
    InnerEntry copyOfMiddle = null;
    for (int i=0; i < this.numEntries; i++) {
      if (i == d) {
        copyOfMiddle = (InnerEntry) allEntries.get(i);
        continue;
      }
      if (i <= d-1) {
        leftEntries.add(i, allEntries.get(i));
      } else {
        rightEntries.add(i-d-1, allEntries.get(i));
      }
    }

    //Two InnerNodes will have correct entries
    this.overwriteBNodeEntries(leftEntries);
    extraNode.overwriteBNodeEntries(rightEntries);
    extraNode.setFirstChild(copyOfMiddle.getPageNum());
    BPlusNode a = BPlusNode.getBPlusNode(this.getTree(), copyOfMiddle.getPageNum());
    a.setParent(extraNodePgNum);
    copyOfMiddle.updatePageNum(extraNodePgNum);

    //Change children's pointers for the extra node
    List<BEntry> someEntries = extraNode.getAllValidEntries();
    for (int i=0; i<someEntries.size(); i++) {
      BPlusNode.getBPlusNode(this.getTree(), someEntries.get(i).getPageNum()).setParent(extraNodePgNum);
    }

    //Insert this middleCopy according to 2 cases
    if (this.isRoot()) {
      InnerNode newRoot = new InnerNode(this.getTree());
      newRoot.setFirstChild(this.getPageNum());
      this.getTree().updateRoot(newRoot.getPageNum());
      this.setParent(newRoot.getPageNum());
      extraNode.setParent(newRoot.getPageNum());
      newRoot.insertBEntry(copyOfMiddle);
    } else {
      InnerNode parentNode = (InnerNode) BPlusNode.getBPlusNode(this.getTree(), this.getParent());
      extraNode.setParent(parentNode.getPageNum());
      parentNode.insertBEntry(copyOfMiddle);
    }

  }
}
