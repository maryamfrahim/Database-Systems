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
    private Iterator<Record> leftIterator;
    private Iterator<Record> rightIterator;
    private String[] leftPartitions;
    private String[] rightPartitions;
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
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      /* TODO */
      while (this.leftIterator.hasNext()) {
        Record current = leftIterator.next();
//        leftPartitions[current.getValues().get(SNLJOperator.this.getLeftColumnIndex()).hashCode()] = current.toString(); //hashmap
      }
      while (this.rightIterator.hasNext()) {
        Record current = rightIterator.next();
        rightPartitions[current.getValues().hashCode()] = current.toString();
      }

//      for each j bucket,
//      for each record r in Rj
//        insert into hash table mm
//      for each record s in Sj
//      for each item in hash table
//        if r = s
//          probe the hash table and put in output


      return false;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      /* TODO */
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
