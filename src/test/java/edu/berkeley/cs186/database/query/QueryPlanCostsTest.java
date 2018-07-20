package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.datatypes.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import edu.berkeley.cs186.database.TestUtils;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;
import edu.berkeley.cs186.database.table.stats.StringHistogram;
import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP2;

import static org.junit.Assert.*;

public class QueryPlanCostsTest {
    private Database database;
    private Random random = new Random();
    private String alphabet = StringHistogram.alphaNumeric;
    private String defaulTableName = "testAllTypes";
    private int defaultNumRecords = 1000;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("db");
        this.database = new Database(tempDir.getAbsolutePath());
        this.database.deleteAllTables();
    }

    @Test(timeout = 1000)
    public void testIndexScanOperatorCost() throws DatabaseException, QueryPlanException {
        List<String> intTableNames = new ArrayList<String>();
        intTableNames.add("int");

        List<DataType> intTableTypes = new ArrayList<DataType>();
        intTableTypes.add(new IntDataType());

        String tableName = "tempIntTable";

        this.database.createTableWithIndices(
                new Schema(intTableNames, intTableTypes), tableName, intTableNames);

        Database.Transaction transaction = this.database.beginTransaction();

        for (int i = 0; i < 300; i++) {
            List<DataType> values = new ArrayList<DataType>();
            values.add(new IntDataType(i));

            transaction.addRecord(tableName, values);
        }

        QueryOperator indexScanOperator;

        indexScanOperator = new IndexScanOperator(
                transaction, tableName, "int", PredicateOperator.GREATER_THAN_EQUALS, new IntDataType(200));

        assertEquals(115, indexScanOperator.estimateIOCost());

        for (int i = 0; i < 700; i++) {
            List<DataType> values = new ArrayList<DataType>();
            values.add(new IntDataType(i));

            transaction.addRecord(tableName, values);
        }

        indexScanOperator = new IndexScanOperator(
                transaction, tableName, "int", PredicateOperator.GREATER_THAN_EQUALS, new IntDataType(500));

        assertEquals(380, indexScanOperator.estimateIOCost());

        transaction.end();
    }


    @Test(timeout = 2000)
    public void testSNLJOperatorCost() throws DatabaseException, QueryPlanException {
        List<DataType> values = TestUtils.createRecordWithAllTypes().getValues();
        this.database.createTable(TestUtils.createSchemaWithAllTypes(), "tempIntTable1");
        this.database.createTable(TestUtils.createSchemaWithAllTypes(), "tempIntTable2");

        Database.Transaction transaction = this.database.beginTransaction();
        int numEntries = transaction.getNumEntriesPerPage("tempIntTable1");

        for (int i = 0; i < 2 * numEntries; i++) {
            transaction.addRecord("tempIntTable1", values);
        }
        for (int i = 0; i < 4 * numEntries; i++) {
            transaction.addRecord("tempIntTable2", values);
        }

        QueryOperator left = new SequentialScanOperator(transaction, "tempIntTable1");
        QueryOperator right = new SequentialScanOperator(transaction, "tempIntTable2");

        JoinOperator leftJoinRight = new SNLJOperator(left, right, "int", "int", transaction);
        assertEquals(2306, leftJoinRight.estimateIOCost());

        JoinOperator rightJoinLeft = new SNLJOperator(right, left, "int", "int", transaction);
        assertEquals(2308, rightJoinLeft.estimateIOCost());
    }


    @Test(timeout = 2000)
    public void testPNLJOperatorCost() throws DatabaseException, QueryPlanException {
        List<DataType> values = TestUtils.createRecordWithAllTypes().getValues();
        this.database.createTable(TestUtils.createSchemaWithAllTypes(), "tempIntTable1");
        this.database.createTable(TestUtils.createSchemaWithAllTypes(), "tempIntTable2");

        Database.Transaction transaction = this.database.beginTransaction();
        int numEntries = transaction.getNumEntriesPerPage("tempIntTable1");

        for (int i = 0; i < 2 * numEntries; i++) {
            transaction.addRecord("tempIntTable1", values);
        }
        for (int i = 0; i < 3 * numEntries + 10; i++) {
            transaction.addRecord("tempIntTable2", values);
        }

        QueryOperator left = new SequentialScanOperator(transaction, "tempIntTable1");
        QueryOperator right = new SequentialScanOperator(transaction, "tempIntTable2");

        JoinOperator leftJoinRight = new PNLJOperator(left, right, "int", "int", transaction);
        assertEquals(10, leftJoinRight.estimateIOCost());

        JoinOperator rightJoinLeft = new PNLJOperator(right, left, "int", "int", transaction);
        assertEquals(12, rightJoinLeft.estimateIOCost());
    }


    @Test(timeout = 2000)
    public void testBNLJOperatorCost() throws DatabaseException, QueryPlanException {
        List<DataType> values = TestUtils.createRecordWithAllTypes().getValues();
        this.database.createTable(TestUtils.createSchemaWithAllTypes(), "tempIntTable1");
        this.database.createTable(TestUtils.createSchemaWithAllTypes(), "tempIntTable2");

        Database.Transaction transaction = this.database.beginTransaction();
        int numEntries = transaction.getNumEntriesPerPage("tempIntTable1");

        for (int i = 0; i < 17 * numEntries + 100; i++) {
            transaction.addRecord("tempIntTable1", values);
        }
        for (int i = 0; i < 4 * numEntries; i++) {
            transaction.addRecord("tempIntTable2", values);
        }

        QueryOperator left = new SequentialScanOperator(transaction, "tempIntTable1");
        QueryOperator right = new SequentialScanOperator(transaction, "tempIntTable2");

        JoinOperator leftJoinRight = new BNLJOperator(left, right, "int", "int", transaction);
        assertEquals(42, leftJoinRight.estimateIOCost());

        JoinOperator rightJoinLeft = new BNLJOperator(right, left, "int", "int", transaction);
        assertEquals(40, rightJoinLeft.estimateIOCost());
    }


    @Test(timeout = 2000)
    public void testGraceHashOperatorCost() throws DatabaseException, QueryPlanException {
        List<DataType> values = TestUtils.createRecordWithAllTypes().getValues();
        this.database.createTable(TestUtils.createSchemaWithAllTypes(), "tempIntTable1");
        this.database.createTable(TestUtils.createSchemaWithAllTypes(), "tempIntTable2");

        Database.Transaction transaction = this.database.beginTransaction();
        int numEntries = transaction.getNumEntriesPerPage("tempIntTable1");

        for (int i = 0; i < 18 * numEntries; i++) {
            transaction.addRecord("tempIntTable1", values);
        }
        for (int i = 0; i < 3 * numEntries + 287; i++) {
            transaction.addRecord("tempIntTable2", values);
        }

        QueryOperator left = new SequentialScanOperator(transaction, "tempIntTable1");
        QueryOperator right = new SequentialScanOperator(transaction, "tempIntTable2");

        JoinOperator leftJoinRight = new GraceHashOperator(left, right, "int", "int", transaction);
        assertEquals(66, leftJoinRight.estimateIOCost());

        JoinOperator rightJoinLeft = new GraceHashOperator(right, left, "int", "int", transaction);
        assertEquals(66, rightJoinLeft.estimateIOCost());
    }

    @Test
    @Category(StudentTestP2.class)
    public void mySNLJCostEasyTest() throws DatabaseException, QueryPlanException {
        List<DataType> recordValues = new ArrayList<DataType>();
        recordValues.add(new IntDataType(17));
        recordValues.add(new StringDataType("Harambe", 7));
        Record r = new Record(recordValues);

        List<DataType> dataTypes = new ArrayList<DataType>();
        List<String> fieldNames = new ArrayList<String>();
        dataTypes.add(new IntDataType());
        dataTypes.add(new StringDataType(7));
        fieldNames.add("age");
        fieldNames.add("name");
        Schema s = new Schema(fieldNames, dataTypes);

        this.database.createTable(s, "DearHarambe");
        this.database.createTable(s, "HarambeIsBae");

        Database.Transaction transaction = this.database.beginTransaction();
        int entries = transaction.getNumEntriesPerPage("DearHarambe");

        for (int i = 0; i < 5 * entries; i++) {
            transaction.addRecord("DearHarambe", recordValues);
        }
        for (int i = 0; i < 9 * entries; i++) {
            transaction.addRecord("HarambeIsBae", recordValues);
        }

        QueryOperator leftOp = new SequentialScanOperator(transaction, "DearHarambe");
        QueryOperator rightOp = new SequentialScanOperator(transaction, "HarambeIsBae");

        JoinOperator leftJoinRight = new SNLJOperator(leftOp, rightOp, "age", "age", transaction);
        assertEquals(16565, leftJoinRight.estimateIOCost());
    }

    @Test
    @Category(StudentTestP2.class)
    public void myPNLJCostEasyTest() throws DatabaseException, QueryPlanException {
        List<DataType> recordValues = new ArrayList<DataType>();
        recordValues.add(new IntDataType(17));
        recordValues.add(new StringDataType("Harambe", 7));
        Record r = new Record(recordValues);

        List<DataType> dataTypes = new ArrayList<DataType>();
        List<String> fieldNames = new ArrayList<String>();
        dataTypes.add(new IntDataType());
        dataTypes.add(new StringDataType(7));
        fieldNames.add("age");
        fieldNames.add("name");
        Schema s = new Schema(fieldNames, dataTypes);

        this.database.createTable(s, "DearHarambe");
        this.database.createTable(s, "HarambeIsBae");

        Database.Transaction transaction = this.database.beginTransaction();
        int entries = transaction.getNumEntriesPerPage("DearHarambe");

        for (int i = 0; i < 5 * entries; i++) {
            transaction.addRecord("DearHarambe", recordValues);
        }
        for (int i = 0; i < 9 * entries; i++) {
            transaction.addRecord("HarambeIsBae", recordValues);
        }

        QueryOperator leftOp = new SequentialScanOperator(transaction, "DearHarambe");
        QueryOperator rightOp = new SequentialScanOperator(transaction, "HarambeIsBae");

        JoinOperator leftJoinRight = new PNLJOperator(leftOp, rightOp, "age", "age", transaction);
        assertEquals(50, leftJoinRight.estimateIOCost());
    }

    @Test
    @Category(StudentTestP2.class)
    public void myBNLJCostEasyTest() throws DatabaseException, QueryPlanException {
        List<DataType> recordValues = new ArrayList<DataType>();
        recordValues.add(new IntDataType(17));
        recordValues.add(new StringDataType("Harambe", 7));
        Record r = new Record(recordValues);

        List<DataType> dataTypes = new ArrayList<DataType>();
        List<String> fieldNames = new ArrayList<String>();
        dataTypes.add(new IntDataType());
        dataTypes.add(new StringDataType(7));
        fieldNames.add("age");
        fieldNames.add("name");
        Schema s = new Schema(fieldNames, dataTypes);

        this.database.createTable(s, "DearHarambe");
        this.database.createTable(s, "HarambeIsBae");

        Database.Transaction transaction = this.database.beginTransaction();
        int entries = transaction.getNumEntriesPerPage("DearHarambe");

        for (int i = 0; i < 5 * entries; i++) {
            transaction.addRecord("DearHarambe", recordValues);
        }
        for (int i = 0; i < 9 * entries; i++) {
            transaction.addRecord("HarambeIsBae", recordValues);
        }

        QueryOperator leftOp = new SequentialScanOperator(transaction, "DearHarambe");
        QueryOperator rightOp = new SequentialScanOperator(transaction, "HarambeIsBae");

        JoinOperator leftJoinRight = new BNLJOperator(leftOp, rightOp, "age", "age", transaction);
        assertEquals(23, leftJoinRight.estimateIOCost());
    }

    @Test
    @Category(StudentTestP2.class)
    public void myINLJCostEasyTest() throws DatabaseException, QueryPlanException {
        List<String> intTableNames = new ArrayList<String>();
        intTableNames.add("age");

        List<DataType> intTableTypes = new ArrayList<DataType>();
        intTableTypes.add(new IntDataType());

        String tableName = "Harambe4Life";

        this.database.createTableWithIndices(new Schema(intTableNames, intTableTypes), tableName, intTableNames);
        Database.Transaction transaction = this.database.beginTransaction();

        for (int i = 0; i < 300; i++) {
            List<DataType> values = new ArrayList<DataType>();
            values.add(new IntDataType(i));
            transaction.addRecord(tableName, values);
        }
        QueryOperator indexScanOperator;
        indexScanOperator = new IndexScanOperator(transaction, tableName, "age", PredicateOperator.EQUALS, new IntDataType(13));
        assertEquals(2, indexScanOperator.estimateIOCost());
        transaction.end();
    }

    @Test
    @Category(StudentTestP2.class)
    public void myGHJCostEasyTest() throws DatabaseException, QueryPlanException {
        List<DataType> recordValues = new ArrayList<DataType>();
        recordValues.add(new IntDataType(17));
        recordValues.add(new StringDataType("Harambe", 7));
        Record r = new Record(recordValues);

        List<DataType> dataTypes = new ArrayList<DataType>();
        List<String> fieldNames = new ArrayList<String>();
        dataTypes.add(new IntDataType());
        dataTypes.add(new StringDataType(7));
        fieldNames.add("age");
        fieldNames.add("name");
        Schema s = new Schema(fieldNames, dataTypes);

        this.database.createTable(s, "DearHarambe");
        this.database.createTable(s, "HarambeIsBae");

        Database.Transaction transaction = this.database.beginTransaction();
        int entries = transaction.getNumEntriesPerPage("DearHarambe");

        for (int i = 0; i < 5 * entries; i++) {
            transaction.addRecord("DearHarambe", recordValues);
        }
        for (int i = 0; i < 9 * entries; i++) {
            transaction.addRecord("HarambeIsBae", recordValues);
        }

        QueryOperator leftOp = new SequentialScanOperator(transaction, "DearHarambe");
        QueryOperator rightOp = new SequentialScanOperator(transaction, "HarambeIsBae");

        JoinOperator leftJoinRight = new GraceHashOperator(leftOp, rightOp, "age", "age", transaction);
        assertEquals(42, leftJoinRight.estimateIOCost());
    }

    @Test
    @Category(StudentTestP2.class)
    public void mySNLJCostEdgeTest() throws DatabaseException, QueryPlanException {
        List<DataType> recordValues = new ArrayList<DataType>();
        recordValues.add(new IntDataType(17));
        recordValues.add(new StringDataType("Harambe", 7));
        Record r = new Record(recordValues);

        List<DataType> dataTypes = new ArrayList<DataType>();
        List<String> fieldNames = new ArrayList<String>();
        dataTypes.add(new IntDataType());
        dataTypes.add(new StringDataType(7));
        fieldNames.add("age");
        fieldNames.add("name");
        Schema s = new Schema(fieldNames, dataTypes);

        this.database.createTable(s, "DearHarambe");
        this.database.createTable(s, "HarambeIsBae");

        Database.Transaction transaction = this.database.beginTransaction();
        int entries = transaction.getNumEntriesPerPage("DearHarambe");

        for (int i = 0; i < 5 * entries + 1; i++) {
            transaction.addRecord("DearHarambe", recordValues);
        }
        for (int i = 0; i < 9 * entries + 1; i++) {
            transaction.addRecord("HarambeIsBae", recordValues);
        }

        QueryOperator leftOp = new SequentialScanOperator(transaction, "DearHarambe");
        QueryOperator rightOp = new SequentialScanOperator(transaction, "HarambeIsBae");

        JoinOperator leftJoinRight = new SNLJOperator(leftOp, rightOp, "age", "age", transaction);
        assertEquals(18416, leftJoinRight.estimateIOCost());
    }

    @Test
    @Category(StudentTestP2.class)
    public void myPNLJCostEdgeTest() throws DatabaseException, QueryPlanException {
        List<DataType> recordValues = new ArrayList<DataType>();
        recordValues.add(new IntDataType(17));
        recordValues.add(new StringDataType("Harambe", 7));
        Record r = new Record(recordValues);

        List<DataType> dataTypes = new ArrayList<DataType>();
        List<String> fieldNames = new ArrayList<String>();
        dataTypes.add(new IntDataType());
        dataTypes.add(new StringDataType(7));
        fieldNames.add("age");
        fieldNames.add("name");
        Schema s = new Schema(fieldNames, dataTypes);

        this.database.createTable(s, "DearHarambe");
        this.database.createTable(s, "HarambeIsBae");

        Database.Transaction transaction = this.database.beginTransaction();
        int entries = transaction.getNumEntriesPerPage("DearHarambe");

        for (int i = 0; i < 5 * entries + 1; i++) {
            transaction.addRecord("DearHarambe", recordValues);
        }
        for (int i = 0; i < 9 * entries + 1; i++) {
            transaction.addRecord("HarambeIsBae", recordValues);
        }

        QueryOperator leftOp = new SequentialScanOperator(transaction, "DearHarambe");
        QueryOperator rightOp = new SequentialScanOperator(transaction, "HarambeIsBae");

        JoinOperator leftJoinRight = new PNLJOperator(leftOp, rightOp, "age", "age", transaction);
        assertEquals(66, leftJoinRight.estimateIOCost());
    }

    @Test
    @Category(StudentTestP2.class)
    public void myBNLJCostEdgeTest() throws DatabaseException, QueryPlanException {
        List<DataType> recordValues = new ArrayList<DataType>();
        recordValues.add(new IntDataType(17));
        recordValues.add(new StringDataType("Harambe", 7));
        Record r = new Record(recordValues);

        List<DataType> dataTypes = new ArrayList<DataType>();
        List<String> fieldNames = new ArrayList<String>();
        dataTypes.add(new IntDataType());
        dataTypes.add(new StringDataType(7));
        fieldNames.add("age");
        fieldNames.add("name");
        Schema s = new Schema(fieldNames, dataTypes);

        this.database.createTable(s, "DearHarambe");
        this.database.createTable(s, "HarambeIsBae");

        Database.Transaction transaction = this.database.beginTransaction();
        int entries = transaction.getNumEntriesPerPage("DearHarambe");

        for (int i = 0; i < 5 * entries + 1; i++) {
            transaction.addRecord("DearHarambe", recordValues);
        }
        for (int i = 0; i < 9 * entries + 1; i++) {
            transaction.addRecord("HarambeIsBae", recordValues);
        }

        QueryOperator leftOp = new SequentialScanOperator(transaction, "DearHarambe");
        QueryOperator rightOp = new SequentialScanOperator(transaction, "HarambeIsBae");

        JoinOperator leftJoinRight = new BNLJOperator(leftOp, rightOp, "age", "age", transaction);
        assertEquals(26, leftJoinRight.estimateIOCost());
    }

    @Test
    @Category(StudentTestP2.class)
    public void myINLJCostEdgeTest() throws DatabaseException, QueryPlanException {
        List<String> intTableNames = new ArrayList<String>();
        intTableNames.add("age");

        List<DataType> intTableTypes = new ArrayList<DataType>();
        intTableTypes.add(new IntDataType());

        String tableName = "Harambe4Life";

        this.database.createTableWithIndices(new Schema(intTableNames, intTableTypes), tableName, intTableNames);
        Database.Transaction transaction = this.database.beginTransaction();

        for (int i = 0; i < 300; i++) {
            List<DataType> values = new ArrayList<DataType>();
            values.add(new IntDataType(i));
            transaction.addRecord(tableName, values);
        }
        QueryOperator indexScanOperator;
        indexScanOperator = new IndexScanOperator(transaction, tableName, "age", PredicateOperator.GREATER_THAN, new IntDataType(0));
        assertEquals(302, indexScanOperator.estimateIOCost());
        transaction.end();
    }

    @Test
    @Category(StudentTestP2.class)
    public void myGHJCostEdgeTest() throws DatabaseException, QueryPlanException {
        List<DataType> recordValues = new ArrayList<DataType>();
        recordValues.add(new IntDataType(17));
        recordValues.add(new StringDataType("Harambe", 7));
        Record r = new Record(recordValues);

        List<DataType> dataTypes = new ArrayList<DataType>();
        List<String> fieldNames = new ArrayList<String>();
        dataTypes.add(new IntDataType());
        dataTypes.add(new StringDataType(7));
        fieldNames.add("age");
        fieldNames.add("name");
        Schema s = new Schema(fieldNames, dataTypes);

        this.database.createTable(s, "DearHarambe");
        this.database.createTable(s, "HarambeIsBae");

        Database.Transaction transaction = this.database.beginTransaction();
        int entries = transaction.getNumEntriesPerPage("DearHarambe");

        for (int i = 0; i < 5 * entries + 1; i++) {
            transaction.addRecord("DearHarambe", recordValues);
        }
        for (int i = 0; i < 9 * entries + 1; i++) {
            transaction.addRecord("HarambeIsBae", recordValues);
        }

        QueryOperator leftOp = new SequentialScanOperator(transaction, "DearHarambe");
        QueryOperator rightOp = new SequentialScanOperator(transaction, "HarambeIsBae");

        JoinOperator leftJoinRight = new GraceHashOperator(leftOp, rightOp, "age", "age", transaction);
        //RIP no hybrid hashing
        assertEquals(48, leftJoinRight.estimateIOCost());
    }
}
