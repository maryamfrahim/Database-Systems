package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;
import org.omg.CORBA.INTERNAL;

import java.lang.reflect.Array;
import java.util.*;
import java.lang.*;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  /**
  * An implementation of Iterator that provides an iterator interface for this operator.
  */
  private class SortMergeIterator implements Iterator<Record> {
    /* TODO: Implement the SortMergeIterator */
    private String leftTableName;
    private String rightTableName;
    private Iterator<Page> leftIterator;
    private Iterator<Page> rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Page leftPage;
    private Page rightPage;
    private byte[] leftHeader;
    private byte[] rightHeader;
    private int leftEntryNum;
    private int rightEntryNum;
    private int pageCurrent;
    private int pageBlock;

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      /* TODO */
      if (SortMergeOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator) SortMergeOperator.this.getLeftSource()).getTableName();
      } else {
        int hellog;
        this.leftTableName = "Temp" + SortMergeOperator.this.getJoinType().toString() + "Operator" + SortMergeOperator.this.getLeftColumnName() + "Left";
        SortMergeOperator.this.createTempTable(SortMergeOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = SortMergeOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          SortMergeOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (SortMergeOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator) SortMergeOperator.this.getRightSource()).getTableName();
      } else {
        int myself;
        this.rightTableName = "Temp" + SortMergeOperator.this.getJoinType().toString() + "Operator" + SortMergeOperator.this.getRightColumnName() + "Right";
        SortMergeOperator.this.createTempTable(SortMergeOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = SortMergeOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          SortMergeOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }
      //Sorting the tables and putting them in temp tables.
      //temp table and then sort....
      this.leftIterator = BNLJOperator.this.getPageIterator(this.leftTableName);
      this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);


      if (this.leftIterator.hasNext()) {
        this.leftIterator.next();
      } else {
        this.leftPage = null;
      }

      if (this.rightIterator.hasNext()) {
        this.rightIterator.next();
      } else {
        this.rightPage = null;
      }

      if (this.leftIterator.hasNext() ) {
        this.leftPage = this.leftIterator.next(); //hmm edge case is if the empty table is passed in
        this.pageCurrent = this.leftPage.getPageNum();
        this.leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
        this.leftEntryNum = 0;
        this.leftRecord = this.getNextLeftRecordInPage();

        this.pageBlock = 0;
      }
      if (this.rightIterator.hasNext()) {
        this.rightPage = this.rightIterator.next();
        this.rightHeader = BNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
        this.rightEntryNum = 0;
        this.rightRecord = this.getNextRightRecordInPage();

      }
      else {
        this.leftPage = null;
        this.leftHeader = null;
        this.leftEntryNum = 0;
        this.leftRecord = null;
        this.pageCurrent = 0;
        this.pageBlock = 0;

        this.rightPage = null;
        this.rightHeader = null;
        this.rightEntryNum = 0;
        this.rightRecord = null;

        this.nextRecord = null;
      }

      this.nextRecord = null;

      //sort


    }

    /**
    * Checks if there are more record(s) to yield
    *
    * @return true if this iterator has another record to yield, otherwise false
    */
    public boolean hasNext() {
      /* TODO */
      while (leftIterator.hasNext() && rightIterator.hasNext()) {
        Page leftPage = leftIterator.next();
        Page rightpage = rightIterator.next();

        DataBox leftKey = leftPage.getValues().get(SortMergeOperator.this.getLeftColumnIndex());

        while (leftKey < rightKey) {
          advanceLeftTable();
        }
        while (leftKey > rightKey) {
          rightIterator.next();
          rightKey = ;
        }

        //mark s. save the entry number
        while (leftKey == rightKey) {
          //Outer loop over r
          while (leftKey == rightKey) {
            //inner loop over s
            return leftKey, rightKey;
            rightKey.next();

          }
          using pages, rightKey to mark.
          leftKey.next
        }
      }
      return false;
    }

    private boolean advanceLeftTable() {
      /* TODO */
      while (leftKey < rightKey) {
        leftIterator.next();
        leftKey =;
      }
      return leftTable.next();
    }

    private boolean advanceRightTable() {
      /* TODO */
//      rightTable.next();
      return rightTable.next();
      while (leftKey > rightKey) {
        rightIterator.next();
        rightKey = ;
      }
    }

    private Record getNextLeftRecordInPage() {
      while (this.leftEntryNum < PNLJOperator.this.getNumEntriesPerPage(this.leftTableName)) {
        byte b = this.leftHeader[this.leftEntryNum / 8];
        int bitOffset = 7 - (this.leftEntryNum % 8);
        byte mask = (byte) (1 << bitOffset);

        byte value = (byte) (b & mask);
        if (value != 0) {
          int entrySize = PNLJOperator.this.getSchema(this.leftTableName).getEntrySize();

          int offset = PNLJOperator.this.getHeaderSize(this.leftTableName) + (entrySize * this.leftEntryNum);
          byte[] bytes = this.leftPage.readBytes(offset, entrySize);

          Record toRtn = PNLJOperator.this.getSchema(this.leftTableName).decode(bytes);
          this.leftEntryNum++;
          return toRtn;
        }
        this.leftEntryNum++;
      }
      return null;
    }

    private Record getNextRightRecordInPage() {
      while (this.rightEntryNum < PNLJOperator.this.getNumEntriesPerPage(this.rightTableName)) {
        byte b = this.rightHeader[this.rightEntryNum / 8];
        int bitOffset = 7 - (this.rightEntryNum % 8);
        byte mask = (byte) (1 << bitOffset);

        byte value = (byte) (b & mask);
        if (value != 0) {
          int entrySize = PNLJOperator.this.getEntrySize(this.rightTableName);

          int offset = PNLJOperator.this.getHeaderSize(this.rightTableName) + (entrySize * this.rightEntryNum);
          byte[] bytes = this.rightPage.readBytes(offset, entrySize);

          Record toRtn = PNLJOperator.this.getSchema(this.rightTableName).decode(bytes);
          this.rightEntryNum++;
          return toRtn;
        }
        this.rightEntryNum++;
      }
      return null;
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


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
            o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
            o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }

  }

}
