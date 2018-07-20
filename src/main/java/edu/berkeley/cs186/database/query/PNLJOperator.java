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
import edu.berkeley.cs186.database.table.RecordID;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class PNLJOperator extends JoinOperator {

    public PNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.PNLJ);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new PNLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        int numPagesLeft = this.getLeftSource().getStats().getNumPages();
        int numPagesRight = this.getRightSource().getStats().getNumPages();

        return numPagesLeft + (numPagesLeft*numPagesRight);
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class PNLJIterator implements Iterator<Record> {

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

        private Record nextRecord;
        private boolean finish; //This is to indicate whether we reached the end of the join


        public PNLJIterator() throws QueryPlanException, DatabaseException {
            if (PNLJOperator.this.getLeftSource().isSequentialScan()) {
                this.leftTableName = ((SequentialScanOperator) PNLJOperator.this.getLeftSource()).getTableName();
            } else {
                this.leftTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getLeftColumnName() + "Left";
                PNLJOperator.this.createTempTable(PNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
                Iterator<Record> leftIter = PNLJOperator.this.getLeftSource().iterator();
                while (leftIter.hasNext()) {
                    PNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
                }
            }

            if (PNLJOperator.this.getRightSource().isSequentialScan()) {
                this.rightTableName = ((SequentialScanOperator) PNLJOperator.this.getRightSource()).getTableName();
            } else {
                this.rightTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getRightColumnName() + "Right";
                PNLJOperator.this.createTempTable(PNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
                Iterator<Record> rightIter = PNLJOperator.this.getRightSource().iterator();
                while (rightIter.hasNext()) {
                    PNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
                }
            }

            //Here we set up leftIterator and rightIterator to be the corresponding page iterators of the left and right source data
            this.leftIterator = PNLJOperator.this.getPageIterator(this.leftTableName);
            this.rightIterator = PNLJOperator.this.getPageIterator(this.rightTableName);

            //Locks in the current first page of left source data with skipping header page
            if (this.leftIterator.hasNext()) {
                this.leftIterator.next();
                if (this.leftIterator.hasNext()) {
                    this.currLeftPage = this.leftIterator.next();
                    this.currLeftPageNumRecordPerPage = PNLJOperator.this.getNumEntriesPerPage(this.leftTableName);
                    this.currLeftPageSlotNum = 0;
                }
            }

            //Locks in the current first page of right source data with skipping header page
            if (this.rightIterator.hasNext()) {
                this.rightIterator.next();
                if (this.rightIterator.hasNext()) {
                    this.currRightPage = this.rightIterator.next();
                    this.currRightPageNumRecordPerPage = PNLJOperator.this.getNumEntriesPerPage(this.rightTableName);
                    this.currRightPageSlotNum = 0;
                }
            }

            this.finish = false;
            this.nextRecord = null;
            this.currLeftRecord = getNextLeftRecord();
            this.currRightRecord = getNextRightRecord();
        }


        //Searches for the next leftRecord and moves to the next page if needed
        public Record getNextLeftRecord() {

            while (true) {
                //This if block is to address the need for page change
                if (this.currLeftPageSlotNum >= this.currLeftPageNumRecordPerPage) {
                    //Tries to move right page
                    if (this.rightIterator.hasNext()) {
                        //Entering here means we can move to next right page
                        this.currRightPage = this.rightIterator.next();
                        this.currLeftPageSlotNum = 0;
                        this.currRightPageSlotNum = 0;
                        this.currRightRecord = getNextRightRecord();
                        return getNextLeftRecord();
                    } else {
                        //Case when finished up right pages
                        //So we try to move left page
                        if (this.leftIterator.hasNext()) {
                            //Entering here means we can move left page
                            this.currLeftPage = this.leftIterator.next(); //Move to next left page
                            try {
                                //Resets the right page to the first by resetting the iterator
                                this.rightIterator = PNLJOperator.this.getPageIterator(this.rightTableName);
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
                            //Case when we can't move left page
                            //This means we are at the end of the whole thing
                            this.finish = true;
                            return null;
                        }
                    }
                }

                //No page change or anything, just searching for next record in this page
                byte[] header;
                try {
                    header = PNLJOperator.this.getPageHeader(this.leftTableName, this.currLeftPage);
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
                        headerSize = PNLJOperator.this.getHeaderSize(this.leftTableName);
                        entrySize = PNLJOperator.this.getEntrySize(this.leftTableName);
                    } catch (DatabaseException e) {
                        return null;//dummy

                    }
                    byte[] bytes = this.currLeftPage.readBytes(headerSize+(this.currLeftPageSlotNum*entrySize), entrySize);
                    this.currLeftPageSlotNum++;
                    return PNLJOperator.this.getLeftSource().getOutputSchema().decode(bytes);
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
                    header = PNLJOperator.this.getPageHeader(this.rightTableName, this.currRightPage);
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
                        headerSize = PNLJOperator.this.getHeaderSize(this.rightTableName);
                        entrySize = PNLJOperator.this.getEntrySize(this.rightTableName);
                    } catch (DatabaseException e) {
                        return null;//dummy

                    }
                    byte[] bytes = this.currRightPage.readBytes(headerSize+(this.currRightPageSlotNum*entrySize), entrySize);
                    this.currRightPageSlotNum++;
                    return PNLJOperator.this.getRightSource().getOutputSchema().decode(bytes);
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
                    DataType leftJoinValue = this.currLeftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                    DataType rightJoinValue = this.currRightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());

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
