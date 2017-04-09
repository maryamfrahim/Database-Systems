package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BNLJOperator extends JoinOperator {

  private int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);
    this.numBuffers = transaction.getNumMemoryPages();

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class BNLJIterator implements Iterator<Record> {
    /* TODO: Implement the BNLJIterator */
    /* Suggested Fields */
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

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      /* TODO */
      if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator) BNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
        int hello;
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
        int hello2;
        while (rightIter.hasNext()) {
          BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }
          /* TODO */
      this.leftIterator = BNLJOperator.this.getPageIterator(this.leftTableName);
      this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);
//            this.leftIterator.next();
//            this.rightIterator.next();

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
        this.leftRecord = this.getNextLeftRecordInBlock();

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
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {

      if (this.nextRecord != null) {
        return true;
      } while (true) {
        try {
          //Out of the left side
          if (this.rightRecord == null) {

            if (this.leftRecord != null) {
              this.leftRecord = this.getNextLeftRecordInBlock();
              this.rightEntryNum = 0; //LOL
              this.rightRecord = this.getNextRightRecordInPage();
            } else if (this.rightPage != null) {
              if (!this.rightIterator.hasNext()) {
                this.rightPage = null;
                if (this.pageCurrent - this.pageBlock == numBuffers - 2) { //block done so reset the block MAYBEEE HEREREEER
                  this.leftRecord = null;
                  this.pageBlock = this.pageCurrent;
                }
              } else {
                this.rightPage = this.rightIterator.next(); //we in a new page DO WE NEED TO CALL NEXT TWICE

                this.rightHeader = BNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);

                this.rightEntryNum = 0;
                this.rightRecord = this.getNextRightRecordInPage();
                //reset leftRec all the way to the top of first left page. offset 0
                this.leftEntryNum = 0;
                this.leftRecord = this.getNextLeftRecordInBlock();
              }
            } else if (this.leftPage != null) {
              if (this.pageCurrent - this.pageBlock == numBuffers - 2) { //block done so reset the block MAYBEEE HEREREEER
                this.leftRecord = null;
                this.pageBlock = this.pageCurrent;
              }
              if (!this.leftIterator.hasNext()) {
                this.leftPage = null;
                this.pageCurrent = 0;
              } else {
                this.leftPage = this.leftIterator.next();
                this.pageCurrent = this.leftPage.getPageNum();
                this.leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
                this.leftEntryNum = 0;
                this.leftRecord = this.getNextLeftRecordInBlock();

                this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);
                this.rightIterator.next();
                this.rightPage = this.rightIterator.next();
                this.rightEntryNum = 0;
                this.rightHeader = BNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
                this.rightRecord = this.getNextRightRecordInPage();
              }
            } else {
              return false;
            }
          } else
          if (this.leftRecord != null && this.rightRecord != null) {
            //Situation 1 - Normal nothing is null
//                            this.rightRecord = getNextRightRecordInPage();
            DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
            DataBox rightJoinValue = this.rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
            if (leftJoinValue.equals(rightJoinValue)) {
              List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
              List<DataBox> rightValues = new ArrayList<DataBox>(this.rightRecord.getValues());
              leftValues.addAll(rightValues);
              int hello;
              this.nextRecord = new Record(leftValues);
              this.rightRecord = this.getNextRightRecordInPage(); //advance the right pointer
              return true;
            } else {
              this.rightRecord = this.getNextRightRecordInPage(); //advance the right pointer
            }
          }
          else {
            this.rightRecord = null;
          }









        } catch (DatabaseException arresting) {
          System.out.println("Caught an exception");
          return false;
        }
      }
    }


    private Record getNextLeftRecordInBlock() throws DatabaseException {
      //reach end of block then switch
      //return null if end of page, but now call get next left ..check if null, more block, call again save that record
//      while (true) {
      while (this.leftEntryNum < BNLJOperator.this.getNumEntriesPerPage(this.leftTableName)) {
        byte b = this.leftHeader[this.leftEntryNum / 8];
        int bitOffset = 7 - (this.leftEntryNum % 8);
        byte mask = (byte) (1 << bitOffset);

        byte value = (byte) (b & mask);
        if (value != 0) {
          int entrySize = BNLJOperator.this.getSchema(this.leftTableName).getEntrySize();
          int hello;
          int offset = BNLJOperator.this.getHeaderSize(this.leftTableName) + (entrySize * this.leftEntryNum);
          byte[] bytes = this.leftPage.readBytes(offset, entrySize);


          Record toRtn = BNLJOperator.this.getSchema(this.leftTableName).decode(bytes);
          this.leftEntryNum++;
          return toRtn;
        }
        this.leftEntryNum++;
      }

      if (this.pageCurrent >= this.pageBlock && this.pageCurrent < this.pageBlock + numBuffers - 2) { //block is not done
//          return  getNextLeftRecordInBlock();
        if (this.leftIterator.hasNext()) {
          this.leftPage = this.leftIterator.next(); ///////////////HMMMM
          this.pageCurrent = this.leftPage.getPageNum();
          System.out.println("in here");
        }
        else {
          this.leftPage = null;
          this.pageCurrent = 0;
          System.out.println("polo");
          this.leftRecord = null;
          return null;
        }
        this.leftRecord = this.getNextLeftRecordInBlock();
        return this.leftRecord;
      }
//    }

      this.leftRecord = null;
      return this.leftRecord;
    }

    private Record getNextRightRecordInPage() throws DatabaseException {
      while (this.rightEntryNum < BNLJOperator.this.getNumEntriesPerPage(this.rightTableName)) {
        byte b = this.rightHeader[this.rightEntryNum / 8];
        int bitOffset = 7 - (this.rightEntryNum % 8);
        byte mask = (byte) (1 << bitOffset);

        byte value = (byte) (b & mask);
        if (value != 0) {
          int entrySize = BNLJOperator.this.getEntrySize(this.rightTableName);

          int offset = BNLJOperator.this.getHeaderSize(this.rightTableName) + (entrySize * this.rightEntryNum);
          byte[] bytes = this.rightPage.readBytes(offset, entrySize);
          int hello;
          Record toRtn = BNLJOperator.this.getSchema(this.rightTableName).decode(bytes);
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
  }
}
