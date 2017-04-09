package edu.berkeley.cs186.database.query;

import java.awt.font.NumericShaper;
import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.databox.DataBox;
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
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new GraceHashIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class GraceHashIterator implements Iterator<Record> {
    private Iterator<Record> leftIterator;
    private Iterator<Record> rightIterator;
    private String[] leftPartitions;
    private String[] rightPartitions;
    private Record leftRecord;
    private Record rightRecord;
    private Record nextRecord;
    private int currPartition;
    private HashMap<DataBox, ArrayList<Record>> solution_buffer;
    private Iterator<Record> rightPartitionIterater;
//    private Record returnable;
    private List<Record> output_buffer;
    /* TODO: Implement the GraceHashOperator */

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
        leftPartitions[i] = leftTableName;
        rightPartitions[i] = rightTableName;
      }
      /* TODO */
      //Build the two partitions
      while (this.leftIterator.hasNext()) {
        this.leftRecord = this.leftIterator.next();
        DataBox key = this.leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
        int index = key.hashCode() % (numBuffers-1);
        GraceHashOperator.this.addRecord(this.leftPartitions[index], this.leftRecord.getValues());
      }
      while (this.rightIterator.hasNext()) {
        this.rightRecord = this.rightIterator.next();
        DataBox key = this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
        int index = key.hashCode() % (numBuffers -1);
        GraceHashOperator.this.addRecord(this.rightPartitions[index], this.rightRecord.getValues());
      }

      this.output_buffer = new ArrayList<Record>(); //RECORD THAT WE ARE RETURNING
      this.currPartition = 0;
      this.leftIterator = null;
      this.rightIterator = null;
      this.solution_buffer = new HashMap<DataBox, ArrayList<Record>>();
      this.nextRecord = null;
      this.rightPartitionIterater = null;
    }

    /**
     * Makes the Hash Table from the Partitions we made.
     *
     * @return the Hash Table
     */
//    public Hashtable HashTableCreater() throws DatabaseException {
//      Hashtable<DataBox, List<Record>> solution_buffer = new Hashtable<DataBox, List<Record>>();
//
////      go through left partiiton and go through all and get databox and put in hash map. thats your key. array listof record. in the end, all key and have same key.
//
//      for (String partition : leftPartitions) {
//        Iterator<Record> currTable = GraceHashOperator.this.getTableIterator(partition);
//        while (currTable.hasNext()) {
//          Record currRecord = currTable.next();
//          DataBox key = currRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
//          if (solution_buffer.containsKey(key)) {
//            List<Record> temp = solution_buffer.get(key);
//            temp.add(currRecord);
//            solution_buffer.put(key, temp);
//          } else {
//            List<Record> temp = new ArrayList<Record>();
//            temp.add(currRecord);
//            solution_buffer.put(key, temp);
//          }
//        }
//        ////right partiition find corresponding key and the list of left.
//      }
//      return null;
//    }


    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() { //ONE WHILE
      /* TODO */
      //go through left partiton and go through all and get databox and put in hash map. thats your key. array listof record. in the end, all key and have same key.
      //right partition find corresponding key and the list of left.
      if (this.nextRecord != null) {
        return true;
      }
      try {
        for (; this.currPartition < this.leftPartitions.length; this.currPartition++) {
          this.leftIterator = GraceHashOperator.this.getTableIterator(this.leftPartitions[currPartition]);
          while (this.leftIterator.hasNext()) {
//          for ( ; currTable.hasNext() ;) {
            this.leftRecord = this.leftIterator.next();
            DataBox key = this.leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
            if (this.solution_buffer.containsKey(key)) {
              List<Record> temp = solution_buffer.get(key);
              temp.add(this.leftRecord);
//              solution_buffer.put(key, temp);
            } else {
              List<Record> temp = new ArrayList<Record>();
              temp.add(this.leftRecord);
//              solution_buffer.put(key, temp);
            }
          }

          this.rightIterator = GraceHashOperator.this.getTableIterator(this.rightPartitions[currPartition]);

          if (this.rightPartitionIterater == null && this.rightIterator.hasNext()) {
            if (this.rightRecord == null) {
              this.rightRecord = this.rightIterator.next();
            }
            DataBox key = this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
            if (solution_buffer.containsKey(key)) { //s == hash
              List<Record> listing = solution_buffer.get(key);
            } else {
              this.rightRecord = null;
            }
          }
          if (this.rightIterator.hasNext() && this.rightPartitionIterater != null) {
            Record rightRecord = this.rightIterator.next();
            DataBox key = rightRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
            if (solution_buffer.containsKey(key)) { //s == hash
              List<Record> listing = solution_buffer.get(key);
              for (Record leftRecord : listing) {
                DataBox leftJoinValue = leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
                DataBox rightJoinValue = rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
                if (leftJoinValue.equals(rightJoinValue)) {
                  List<DataBox> leftValues = new ArrayList<DataBox>(leftRecord.getValues());
                  List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
                  leftValues.addAll(rightValues);
                  Record joined = new Record(leftValues);
                  this.output_buffer.add(joined);
                }
              }
              this.nextRecord = this.output_buffer.get(0);
              return true;
            }
          }
        }
        return false;
      } catch (DatabaseException database) {
        System.out.println("Opps");
      }
      return false;
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
