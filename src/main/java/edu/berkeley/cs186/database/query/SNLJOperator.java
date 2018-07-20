package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class SNLJOperator extends JoinOperator {

    public SNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.SNLJ);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new SNLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        int numPagesLeft = this.getLeftSource().getStats().getNumPages();
        int numRecordsLeft = this.getLeftSource().getStats().getNumRecords();
        int numPagesRight = this.getRightSource().getStats().getNumPages();

        return numPagesLeft + (numRecordsLeft*numPagesRight);
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class SNLJIterator implements Iterator<Record> {
        private Iterator<Record> leftIterator;
        private Iterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;

        public SNLJIterator() throws QueryPlanException, DatabaseException {
            this.leftIterator = SNLJOperator.this.getLeftSource().iterator();
            this.rightIterator = null;
            this.leftRecord = null;
            this.nextRecord = null;
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            if (this.nextRecord != null) {
                return true;
            }

            //Case when nextRecord is null
            while (true) {
                //Case when leftRecord is null
                if (this.leftRecord == null) {
                    if (this.leftIterator.hasNext()) {
                        //Able to generate next leftRecord
                        this.leftRecord = this.leftIterator.next();
                        try {
                            //For each leftRecord, we generate a whole iterator for right records
                            this.rightIterator = SNLJOperator.this.getRightSource().iterator();
                            //After this we will step into the next while loop
                            //This means a leftRecord will be accompanied by a right iterator
                        } catch (QueryPlanException q) {
                            return false;
                        } catch (DatabaseException e) {
                            return false;
                        }
                    } else {
                        //If leftRecord is null and cannot generate next leftRecord which means iterator ended
                        return false;
                    }
                }
                while (this.rightIterator.hasNext()) {
                    Record rightRecord = this.rightIterator.next();

                    DataType leftJoinValue = this.leftRecord.getValues().get(SNLJOperator.this.getLeftColumnIndex());
                    DataType rightJoinValue = rightRecord.getValues().get(SNLJOperator.this.getRightColumnIndex());

                    if (leftJoinValue.equals(rightJoinValue)) {
                        List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());
                        List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());

                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                        return true;
                    }
                }//If we do exhaust this while loop, it means that nothing in this right iterator matches with the current leftRecord
                //This means we need to move leftRecord to the next one
                this.leftRecord = null;
            }
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public Record next() {
            if (this.hasNext()) {
                Record r = this.nextRecord;
                this.nextRecord = null;
                return r;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
