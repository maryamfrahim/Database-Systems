package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Collections;
import java.util.List;

/**
 * An inner node of a B+ tree. An InnerNode header contains an `isLeaf` flag
 * set to 0 and the page number of the first child node (or -1 if no child
 *
 * exists). An InnerNode contains InnerEntries.
 * Inherits all the properties of a BPlusNode.
 */

public class InnerNode extends BPlusNode {
    public static int headerSize = 5;       // isLeaf + pageNum of first child

    public InnerNode(BPlusTree tree) {
        super(tree, false);
        tree.incrementNumNodes();
        getPage().writeByte(0, (byte) 0);   // isLeaf = 0
        setFirstChild(-1);
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
        return getPage().readInt(1);
    }

    public void setFirstChild(int val) {
        getPage().writeInt(1, val);
    }

    /**
     * Finds the correct child of this InnerNode whose subtree contains the
     * given key.
     *
     * @param key the given key
     * @return page number of the child of this InnerNode whose subtree
     * contains the given key
     */
    public int findChildFromKey(DataBox key) {
        int keyPage = getFirstChild();  // Default keyPage
        List<BEntry> entries = getAllValidEntries();
        for (BEntry ent : entries) {
            if (key.compareTo(ent.getKey()) < 0) {
                break;
            }
            keyPage = ent.getPageNum();
        }
        return keyPage;
    }

    /**
     * Inserts a LeafEntry into the corresponding LeafNode in this subtree.
     *
     * @param ent the LeafEntry to be inserted
     * @return the InnerEntry to be pushed/copied up to this InnerNode's parent
     * as a result of this InnerNode being split, null otherwise
     */
    public InnerEntry insertBEntry(LeafEntry ent) {
        // Implement me!
        int addedToHere = this.findChildFromKey(ent.getKey());
        BPlusNode here = this.getBPlusNode(this.getTree(), addedToHere); //this or BPlusNode
        InnerEntry ret = here.insertBEntry(ent);
        if (ret == null) {
            return null;
        } else {
            if (this.hasSpace()) {
//                this.getAllValidEntries().add(addedToHere, ret);
                List<BEntry> current = this.getAllValidEntries();
                current.add(ret); //adding ret not ent omggg
                Collections.sort(current);
                this.overwriteBNodeEntries(current);
                return null;
            } else {
                InnerEntry outputInsertBEntry = splitNode(ret); //ret not entry
                return outputInsertBEntry;
            }
        }


    }

    /**
     * Splits this InnerNode and returns the resulting InnerEntry to be
     * pushed/copied up to this InnerNode's parent as a result of the split.
     * The left node should contain d entries and the right node should contain
     * d entries.
     *
     * @param newEntry the BEntry that is being added to this InnerNode
     * @return the resulting InnerEntry to be pushed/copied up to this
     * InnerNode's parent as a result of this InnerNode being split
     */
    @Override
    public InnerEntry splitNode(BEntry newEntry) {
        List<BEntry> current = this.getAllValidEntries();
        current.add(newEntry);
        Collections.sort(current);
        BEntry middle = current.get(current.size()/2);

        InnerNode second = new InnerNode(this.getTree()); //allocate pag
        second.overwriteBNodeEntries(current.subList(current.size()/2 + 1, current.size())); //flip order
        second.setFirstChild(newEntry.getPageNum()); ///settting first pointer to the new entries page num CHANGED

        this.overwriteBNodeEntries(current.subList(0, current.size()/2));

        int pageSecond = second.getPageNum();
        InnerEntry copyUp = new InnerEntry(middle.getKey(), pageSecond);

        return copyUp;
    }
}
