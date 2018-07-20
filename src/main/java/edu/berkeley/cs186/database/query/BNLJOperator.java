package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class BNLJOperator extends JoinOperator {

    private int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getNumMemoryPages();
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new BNLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        int numPagesLeft = this.getLeftSource().getStats().getNumPages();
        int numPagesRight = this.getRightSource().getStats().getNumPages();
        int r = (int) Math.ceil(((double)numPagesLeft/(numBuffers-2)));

        return numPagesLeft + (r*numPagesRight);
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class BNLJIterator implements Iterator<Record> {
        private String leftTableName;
        private Iterator<Page> leftIterator;
        private Page currLeftPage;
        private int currLeftPageSlotNum;
        private int currLeftPageNumRecordPerPage;
        private Record currLeftRecord;

        private String rightTableName;
        private Iterator<Page> rightIterator;
        private Page currRightPage;
        private int currRightPageSlotNum;
        private int currRightPageNumRecordPerPage;
        private Record currRightRecord;

        private Page[] block;
        private int blockNextPagePtr;
        private int numPagesInBlock;
        private Record nextRecord;
        private boolean finish; //This is to indicate whether we reached the end of the join

        public BNLJIterator() throws QueryPlanException, DatabaseException {
            if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
                this.leftTableName = ((SequentialScanOperator) BNLJOperator.this.getLeftSource()).getTableName();
            } else {
                this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
                BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
                Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
                while (leftIter.hasNext()) {
                    BNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
                }
            }
            if (BNLJOperator.this.getRightSource().isSequentialScan()) {
                this.rightTableName = ((SequentialScanOperator) BNLJOperator.this.getRightSource()).getTableName();
            } else {
                this.rightTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getRightColumnName() + "Right";
                BNLJOperator.this.createTempTable(BNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
                Iterator<Record> rightIter = BNLJOperator.this.getRightSource().iterator();
                while (rightIter.hasNext()) {
                    BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
                }
            }

            //Here we set up leftIterator and rightIterator to be the corresponding page iterators of the left and right source data
            this.leftIterator = BNLJOperator.this.getPageIterator(this.leftTableName);
            this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);
            this.block = new Page[numBuffers-2];
            this.numPagesInBlock = 0;

            //Locks in the current first page of right source data with skipping header page
            if (this.rightIterator.hasNext()) {
                this.rightIterator.next();
                if (this.rightIterator.hasNext()) {
                    this.currRightPage = this.rightIterator.next();
                    this.currRightPageNumRecordPerPage = BNLJOperator.this.getNumEntriesPerPage(this.rightTableName);
                    this.currRightPageSlotNum = 0;
                }
            }

            //Locks in the current first page of left source data with skipping header page
            if (this.leftIterator.hasNext()) {
                this.leftIterator.next();
                //Skipped header page here
                int count = 0;
                //Loop to populate block with pages up to b-2
                while (this.leftIterator.hasNext() && count < numBuffers-2) {
                    this.block[count] = this.leftIterator.next();
                    this.numPagesInBlock++;
                    count++;
                }
                this.currLeftPage = this.block[0];
                this.currLeftPageNumRecordPerPage = BNLJOperator.this.getNumEntriesPerPage(this.leftTableName);
                this.currLeftPageSlotNum = 0;
                this.blockNextPagePtr = 1;
            }

            this.finish = false;
            this.nextRecord = null;
            this.currLeftRecord = getNextLeftRecord();
            this.currRightRecord = getNextRightRecord();
        }

        //Searches for the next leftRecord and moves to the next page if needed
        public Record getNextLeftRecord() {
            while (true) {
                //This if block addresses the need to page change
                if (this.currLeftPageSlotNum >= this.currLeftPageNumRecordPerPage) {
                    //This case means we're still page changing within our block
                    if (this.blockNextPagePtr < this.numPagesInBlock) {
                        this.currLeftPage = this.block[this.blockNextPagePtr];
                        this.blockNextPagePtr++;
                        this.currLeftPageSlotNum = 0;
                        this.currRightPageSlotNum = 0;
                        this.currRightRecord = getNextRightRecord();
                        return getNextLeftRecord();

                    } else { //We don't have a next page in our block so its either we can switch right page or we need to move to next block
                        //Try to move right page
                        if (this.rightIterator.hasNext()) {
                            this.currRightPage = this.rightIterator.next();
                            //Reset to front of the block
                            this.currLeftPage = this.block[0];
                            this.blockNextPagePtr = 1;
                            this.currLeftPageSlotNum = 0;
                            this.currRightPageSlotNum = 0;
                            this.currRightRecord = getNextRightRecord();
                            return getNextLeftRecord();
                        } else { //Can't move right page, check to see if we can make another block
                            if (this.leftIterator.hasNext()) {
                                int count = 0;
                                this.numPagesInBlock = 0;
                                //Loop to populate block with pages up to b-2
                                while (this.leftIterator.hasNext() && count < numBuffers-2) {
                                    this.block[count] = this.leftIterator.next();
                                    this.numPagesInBlock++;
                                    count++;
                                }
                                this.currLeftPage = this.block[0];
                                try {
                                    this.currLeftPageNumRecordPerPage = BNLJOperator.this.getNumEntriesPerPage(this.leftTableName);
                                } catch (DatabaseException e) {
                                    return null;//dummy

                                }
                                this.currLeftPageSlotNum = 0;
                                this.blockNextPagePtr = 1;

                                //Here we reset our right table to the first page by resetting rightIterator
                                try {
                                    //Resets the right page to the first by resetting the iterator
                                    this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);
                                } catch (DatabaseException e) {
                                    return null;//dummy

                                }
                                //Sets the current right page to by skipping the header page
                                if (this.rightIterator.hasNext()) {
                                    this.rightIterator.next();
                                    if (this.rightIterator.hasNext()) {
                                        this.currRightPage = this.rightIterator.next();
                                    }
                                }

                                //Resets our records to the first one on the current pages
                                this.currLeftPageSlotNum = 0;
                                this.currRightPageSlotNum = 0;
                                this.currRightRecord = getNextRightRecord();
                                return getNextLeftRecord();

                            } else {
                                //Can't even make another block, so we're done
                                this.finish = true;
                                return null;
                            }

                        }

                    }
                }

                //No page change or anything, just searching for next record in this page
                byte[] header;
                try {
                    header = BNLJOperator.this.getPageHeader(this.leftTableName, this.currLeftPage);
                } catch (DatabaseException e) {
                    return null;//dummy

                }
                int byteOffset = this.currLeftPageSlotNum / 8;
                int bitOffset = 7 - (this.currLeftPageSlotNum % 8);
                byte targetByte = header[byteOffset];
                if (((targetByte >> bitOffset) & 1) == 1) { //We have a valid record
                    int headerSize;
                    int entrySize;
                    try {
                        headerSize = BNLJOperator.this.getHeaderSize(this.leftTableName);
                        entrySize = BNLJOperator.this.getEntrySize(this.leftTableName);
                    } catch (DatabaseException e) {
                        return null;//dummy

                    }
                    byte[] bytes = this.currLeftPage.readBytes(headerSize+(this.currLeftPageSlotNum*entrySize), entrySize);
                    this.currLeftPageSlotNum++;
                    return BNLJOperator.this.getLeftSource().getOutputSchema().decode(bytes);
                } else {
                    //Not a valid record, need to try next one
                    //So we increment slot num and re-loop
                    this.currLeftPageSlotNum++;
                }
            }
        }

        public Record getNextRightRecord() {
            while (true) {
                //This if block is to address page change
                if (this.currRightPageSlotNum >= this.currRightPageNumRecordPerPage) {
                    this.currLeftRecord = getNextLeftRecord();
                    this.currRightPageSlotNum = 0;
                    return getNextRightRecord();
                }

                //Normal search for next record on the same page
                byte[] header;
                try {
                    header = BNLJOperator.this.getPageHeader(this.rightTableName, this.currRightPage);
                } catch (DatabaseException e) {
                    return null;//dummy

                }
                int byteOffset = this.currRightPageSlotNum / 8;
                int bitOffset = 7 - (this.currRightPageSlotNum % 8);
                byte targetByte = header[byteOffset];
                if (((targetByte >> bitOffset) & 1) == 1) { //We have a valid record at this slot
                    int headerSize;
                    int entrySize;
                    try {
                        headerSize = BNLJOperator.this.getHeaderSize(this.rightTableName);
                        entrySize = BNLJOperator.this.getEntrySize(this.rightTableName);
                    } catch (DatabaseException e) {
                        return null;//dummy

                    }
                    byte[] bytes = this.currRightPage.readBytes(headerSize+(this.currRightPageSlotNum*entrySize), entrySize);
                    this.currRightPageSlotNum++;
                    return BNLJOperator.this.getRightSource().getOutputSchema().decode(bytes);
                } else {
                    //Not a valid record, need to try next one
                    //So we increment slot num and re-loop
                    this.currRightPageSlotNum++;
                }
            }
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        //This hasNext method must be able to prepare a nextRecord if it returns true
        public boolean hasNext() {
            while (true) {
                //To avoid "double" hasNext calls
                if (this.nextRecord != null) {
                    return true;
                }

                //If we finish all records, then we're done
                if (this.finish) {
                    return false;
                }

                //Check join conditions
                if (this.currLeftRecord != null && this.currRightRecord != null) {
                    DataType leftJoinValue = this.currLeftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                    DataType rightJoinValue = this.currRightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());

                    //We have a successful join
                    if (leftJoinValue.equals(rightJoinValue)) {
                        List<DataType> leftValues = new ArrayList<DataType>(this.currLeftRecord.getValues());
                        List<DataType> rightValues = new ArrayList<DataType>(this.currRightRecord.getValues());

                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);

                        //Get next rightRecord
                        this.currRightRecord = getNextRightRecord();

                        return true;
                    } else { //Not successful join so we search for the next one
                        //Get next rightRecord
                        this.currRightRecord = getNextRightRecord();
                        //Re-loop to check on next pair of records
                    }
                }
            }
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public Record next() {
            //This assumes that hasNext() method will take care of absolutely everything
            if (this.hasNext()) {
                Record r = this.nextRecord;
                this.nextRecord = null;
                return r;
            } else {
                throw new NoSuchElementException();
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
