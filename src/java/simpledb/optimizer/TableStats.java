package simpledb.optimizer;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();
    static final int IOCOSTPERPAGE = 1000;
    private int tableId;
    private int ioCostPerPage;

    private BufferPool bufferPool = Database.getBufferPool();
    private Catalog catalog = Database.getCatalog();
    private TupleDesc tupleDesc;
    private DbFileIterator dbFileIterator;  //所有tuple
    private int numPages;
    private int total = 0;

    private int[] max;  //每一个字段的max
    private int[] min;  //每一个字段的min
    private IntHistogram[] intHistograms;  //每一个字段的直方图

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        HeapFile heapFile = (HeapFile) catalog.getDatabaseFile(tableid);
        this.tupleDesc  = heapFile.getTupleDesc();
        this.numPages = heapFile.numPages();
        this.dbFileIterator = heapFile.iterator(new TransactionId());

        this.max  = new int[tupleDesc.numFields()];
        this.min = new int[tupleDesc.numFields()];
        this.intHistograms = new IntHistogram[tupleDesc.numFields()];
        Arrays.fill(min,Integer.MAX_VALUE);
        Arrays.fill(max,Integer.MIN_VALUE);

        try {
            this.dbFileIterator.open();
            while(dbFileIterator.hasNext()){
                this.total++;
                Tuple tuple = dbFileIterator.next();
                for(int i=0;i<max.length;i++){
                    Type fieldType = tuple.getField(i).getType();
                    if(fieldType.equals(Type.INT_TYPE)){
                        IntField field = (IntField)tuple.getField(i);
                        int value = field.getValue();
                        if(value>max[i]){
                            max[i] = value;
                        }
                        if(value<min[i]){
                            min[i]  =value;
                        }
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }


        for(int i=0;i<tupleDesc.numFields();i++){
            Type fieldType = tupleDesc.getFieldType(i);
            if(fieldType.equals(Type.STRING_TYPE)){
                continue;
            }
            this.intHistograms[i]  =new IntHistogram(100,min[i],max[i]);
            try {
                this.dbFileIterator.rewind();
                while(dbFileIterator.hasNext()){
                    Tuple tuple = dbFileIterator.next();
                    IntField field = (IntField)tuple.getField(i);
                    this.intHistograms[i].addValue(field.getValue());
                }
            } catch (DbException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            }

        }

        this.dbFileIterator.close();
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.numPages*ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(totalTuples()*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type type = constant.getType();
        if(type.equals(Type.STRING_TYPE)){
            StringHistogram stringHistogram = new StringHistogram(100);
            return stringHistogram.estimateSelectivity(op,((StringField)constant).getValue());
        }else if(type.equals(Type.INT_TYPE)){
            return this.intHistograms[field].estimateSelectivity(op,((IntField)constant).getValue());
        }
        return -1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return total;
    }

}
