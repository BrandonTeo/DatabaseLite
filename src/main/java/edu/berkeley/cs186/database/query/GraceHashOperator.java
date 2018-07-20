package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;


public class GraceHashOperator extends JoinOperator {

    private int numBuffers;

    public GraceHashOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.GRACEHASH);

        this.numBuffers = transaction.getNumMemoryPages();
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new GraceHashIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        int numPagesLeft = this.getLeftSource().getStats().getNumPages();
        int numPagesRight = this.getRightSource().getStats().getNumPages();

        return 3*(numPagesLeft+numPagesRight);
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class GraceHashIterator implements Iterator<Record> {
        private Iterator<Record> leftIterator;
        private Iterator<Record> rightIterator;
        private Iterator<Record> rightPIterator;
        private Iterator<Record> spitIterator;
        private Record rightRecord;
        private Record nextRecord;
        private String[] leftPartitions;
        private String[] rightPartitions;
        private int currentPartition;
        private Map<DataType, ArrayList<Record>> inMemoryHashTable;


        public GraceHashIterator() throws QueryPlanException, DatabaseException {
            this.leftIterator = getLeftSource().iterator();
            this.rightIterator = getRightSource().iterator();
            leftPartitions = new String[numBuffers - 1];
            rightPartitions = new String[numBuffers - 1];
            String leftTableName;
            String rightTableName;
            for (int i = 0; i < numBuffers - 1; i++) {
                leftTableName = "Temp HashJoin Left Partition " + Integer.toString(i);
                rightTableName = "Temp HashJoin Right Partition " + Integer.toString(i);
                GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
                GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
                //These 2 arrays hold the tempTable names of the partitions
                leftPartitions[i] = leftTableName;
                rightPartitions[i] = rightTableName;
            }

            //Add records into the appropriate tempTables which are basically partitions
            while (this.leftIterator.hasNext()) {
                Record r = this.leftIterator.next();
                DataType leftJoinValue = r.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
                int hc = leftJoinValue.hashCode();
                int bucket = hc % (numBuffers-1);
                addRecord(leftPartitions[bucket], r.getValues());
            }

            //Add records into the appropriate tempTables which are basically partitions
            while (this.rightIterator.hasNext()) {
                Record r = this.rightIterator.next();
                DataType rightJoinValue = r.getValues().get(GraceHashOperator.this.getRightColumnIndex());
                int hc = rightJoinValue.hashCode();
                int bucket = hc % (numBuffers-1);
                addRecord(rightPartitions[bucket], r.getValues());
            }

            //Loads leftPartitions[0] into the hashMap
            this.currentPartition = 0;
            this.inMemoryHashTable = new HashMap<DataType, ArrayList<Record>>();
            Iterator<Record> lpartitionIter = GraceHashOperator.this.getTableIterator(leftPartitions[this.currentPartition]);
            while (lpartitionIter.hasNext()) {
                Record r = lpartitionIter.next();
                DataType k = r.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
                if (this.inMemoryHashTable.containsKey(k)) {
                    ArrayList<Record> aList = this.inMemoryHashTable.get(k);
                    aList.add(r);
                    this.inMemoryHashTable.put(k, aList);
                } else {
                    ArrayList<Record> aList = new ArrayList<Record>();
                    aList.add(r);
                    this.inMemoryHashTable.put(k,aList);
                }
            }

            //Loads rightPartitions[0] as an iterator
            this.rightPIterator = GraceHashOperator.this.getTableIterator(rightPartitions[this.currentPartition]);
            this.nextRecord = null;
            //Use the iterator to get first rightPartitions[0] record
            if (this.rightPIterator.hasNext()) {
                this.rightRecord = this.rightPIterator.next();
            } else {
                this.rightRecord = null;
            }
            this.spitIterator = null;
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            //This is to avoid "double" hasNexts
            if (this.nextRecord != null) {
                return true;
            }

            while (true) {
                //This case happens when the current right partition is empty
                //So we need to increment partition number and switch hashMap content and rightPartition iterator
                if (this.rightRecord == null) {
                    this.currentPartition++;

                    //This is the case when we finished all partitions
                    if (this.currentPartition >= numBuffers-1) {
                        return false;
                    }

                    //New hashMap for new partition number
                    this.inMemoryHashTable = new HashMap<DataType, ArrayList<Record>>();
                    Iterator<Record> lpartitionIter;
                    try {
                        lpartitionIter = GraceHashOperator.this.getTableIterator(leftPartitions[this.currentPartition]);
                    } catch (DatabaseException e) {
                        return false;//dummy
                    }
                    while (lpartitionIter.hasNext()) {
                        Record r = lpartitionIter.next();
                        DataType k = r.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
                        if (this.inMemoryHashTable.containsKey(k)) {
                            ArrayList<Record> aList = this.inMemoryHashTable.get(k);
                            aList.add(r);
                            this.inMemoryHashTable.put(k, aList);
                        } else {
                            ArrayList<Record> aList = new ArrayList<Record>();
                            aList.add(r);
                            this.inMemoryHashTable.put(k,aList);
                        }
                    }

                    //New rightPartition iterator
                    try {
                        this.rightPIterator = GraceHashOperator.this.getTableIterator(rightPartitions[this.currentPartition]);
                    } catch (DatabaseException e) {
                        return false;//dummy
                    }
                    this.nextRecord = null;
                    if (this.rightPIterator.hasNext()) {
                        this.rightRecord = this.rightPIterator.next();
                    } else {
                        this.rightRecord = null;
                    }
                    this.spitIterator = null;

                } else if (this.spitIterator == null) {
                    //This case happens when we have a rightRecord but not yet a spitIterator
                    //So we try to match it with keys in our hashMap
                    DataType toTest = this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
                    //Check if the rightRecord matches any key in the same partition
                    if (this.inMemoryHashTable.containsKey(toTest)) {
                        this.spitIterator = this.inMemoryHashTable.get(toTest).iterator();
                    } else { //If nothing matches we try the next rightRecord
                        if (this.rightPIterator.hasNext()) {
                            this.rightRecord = this.rightPIterator.next();
                        } else {
                            this.rightRecord = null;
                        }
                    }

                } else { //Enter this case when spitIterator and rightRecord are not null
                    //Which means we have a bunch of matches
                    if (this.spitIterator.hasNext()) {
                        Record left = this.spitIterator.next();
                        Record right = this.rightRecord;
                        List<DataType> leftValues = new ArrayList<DataType>(left.getValues());
                        List<DataType> rightValues = new ArrayList<DataType>(right.getValues());
                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                        return true;
                    } else { //This means we finished pairing current spitIterator and rightRecord
                        this.spitIterator = null;
                        //Test next rightRecord in this partition
                        if (this.rightPIterator.hasNext()) { //We have a next rightRecord to test
                            this.rightRecord = this.rightPIterator.next();
                        } else { //We ran out of rightRecords, which means we have to move to next partition
                            this.currentPartition++;

                            //Case where we used up all the partitions
                            if (this.currentPartition >= numBuffers-1) {
                                return false;
                            }
                            //New HashTable for new partition
                            this.inMemoryHashTable = new HashMap<DataType, ArrayList<Record>>();
                            Iterator<Record> lpartitionIter;
                            try {
                                lpartitionIter = GraceHashOperator.this.getTableIterator(leftPartitions[this.currentPartition]);
                            } catch (DatabaseException e) {
                                return false;//dummy
                            }
                            while (lpartitionIter.hasNext()) { //Populate hashMap
                                Record r = lpartitionIter.next();
                                DataType k = r.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
                                if (this.inMemoryHashTable.containsKey(k)) {
                                    ArrayList<Record> aList = this.inMemoryHashTable.get(k);
                                    aList.add(r);
                                    this.inMemoryHashTable.put(k, aList);
                                } else {
                                    ArrayList<Record> aList = new ArrayList<Record>();
                                    aList.add(r);
                                    this.inMemoryHashTable.put(k,aList);
                                }
                            }

                            //This creates rightRecord iterator for new partition
                            try {
                                this.rightPIterator = GraceHashOperator.this.getTableIterator(rightPartitions[this.currentPartition]);
                            } catch (DatabaseException e) {
                                return false;//dummy
                            }
                            this.nextRecord = null;
                            if (this.rightPIterator.hasNext()) {
                                this.rightRecord = this.rightPIterator.next();
                            } else {
                                this.rightRecord = null;
                            }
                            this.spitIterator = null;
                        }
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
            //Everything is done in hasNext
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
