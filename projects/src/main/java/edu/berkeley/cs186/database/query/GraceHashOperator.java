package edu.berkeley.cs186.database.query;

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
    private Iterator<Record> leftIterator; //A singular table from Left Partition
    private Iterator<Record> rightIterator; //A singular table from Right Partition
    private String[] leftPartitions; //Array with all the strings of name Table
    private String[] rightPartitions;
    private Record leftRecord;
    private Record rightRecord;
    private Record nextRecord;
    private List<Record> output_buffer; //The list of records that have a match in the left partition
    private HashMap<DataBox, ArrayList<Record>> solution_buffer; //HM of left partition
    private Iterator<Record> rightJoinedElem; //The sucessful elements to be joined yay
    private DataBox key; //Key
    private List<Record> toJoin; //Element from right partition to be joined
    private int currPartition;

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
      while (this.leftIterator.hasNext()) {
        this.leftRecord = this.leftIterator.next();
        DataBox leftJoinValue = this.leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
        int leftBucket = leftJoinValue.hashCode() % (numBuffers - 1);
        GraceHashOperator.this.addRecord(leftPartitions[leftBucket], leftRecord.getValues());
      }
      while (this.rightIterator.hasNext()) {
        this.rightRecord = this.rightIterator.next();
        DataBox rightJoinValue = this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
        int rightBucket = rightJoinValue.hashCode() % (numBuffers - 1);
        GraceHashOperator.this.addRecord(rightPartitions[rightBucket], rightRecord.getValues());
      }

      this.output_buffer = new ArrayList<Record>(); //RECORD THAT WE ARE RETURNING
      this.currPartition = 0;
            this.solution_buffer = new HashMap<DataBox, ArrayList<Record>>();
      this.toJoin = new ArrayList<Record>();
      this.key = null;

      this.nextRecord = null;
      this.rightJoinedElem = null;

      this.leftIterator = null;
      this.rightIterator = null;
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
      try {
//        for (; this.currPartition < this.leftPartitions.length; this.currPartition++) {
          while (true) {
          if (this.currPartition >= this.leftPartitions.length) { //out of partitions ie B-2 times
            return false;
          }
          if(this.solution_buffer.size() == 0){ //Only need to put another left partiiton into hashmap if empty
            this.leftIterator = GraceHashOperator.this.getTableIterator(this.leftPartitions[this.currPartition]);
            this.rightIterator = GraceHashOperator.this.getTableIterator(this.rightPartitions[this.currPartition]);
            while (this.leftIterator.hasNext()) {
              //          for ( ; currTable.hasNext() ;) {
              this.leftRecord = this.leftIterator.next();
              DataBox key = this.leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
              if (this.solution_buffer.containsKey(key)) {
                Boolean temp = this.solution_buffer.get(key).add(this.leftRecord);
                output_buffer.add(this.leftRecord);
              } else {
                ArrayList<Record> temp = new ArrayList<Record>();
                temp.add(this.leftRecord);
                this.solution_buffer.put(key, temp);
                output_buffer.add(this.leftRecord);
                //              solution_buffer.put(key, temp);
              }
            }
          }

          if (this.rightJoinedElem != null) { //RIGHT
            if (!this.rightJoinedElem.hasNext()) {
              this.rightJoinedElem = null;
              this.rightRecord = null;

                        }else {
              Record rightRecords = this.rightRecord;
              this.toJoin.add(this.rightRecord);
              List<DataBox> rightVal = new ArrayList<DataBox>();
              rightVal.addAll(rightRecords.getValues());

              Record leftRecords = this.rightJoinedElem.next();
              this.toJoin.add(leftRecords);
              List<DataBox> leftVal = new ArrayList<DataBox>();
              leftVal.addAll(leftRecords.getValues());
              leftVal.addAll(rightVal);
              this.nextRecord = new Record(leftVal);
              return true;
            }
                    }

          else if (this.rightIterator.hasNext()) {
            if (this.rightRecord != null) {
              this.key = this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
            } else  {
              this.rightRecord = this.rightIterator.next();
//              this.toJoin.add(this.rightRecord);
              this.key = this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
            }
            if (!this.solution_buffer.containsKey(this.key)) {
              this.rightRecord = null;
            } else {
              ArrayList<Record> listing = this.solution_buffer.get(this.key);
              this.rightJoinedElem = listing.iterator();
            }
            this.toJoin.add(this.rightRecord);
          }
          else {
            this.currPartition++;
            this.toJoin.add(this.rightRecord);
            this.rightRecord = null;
            this.solution_buffer.clear();

          }
           }
      }
      catch (DatabaseException database) {
        System.out.println("Got a databaseException");
        return false;
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
