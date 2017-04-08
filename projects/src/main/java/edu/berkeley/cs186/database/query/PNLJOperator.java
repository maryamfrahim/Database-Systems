package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new PNLJIterator();
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class PNLJIterator implements Iterator<Record> {
        /* TODO: Implement the PNLJIterator */
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

        public PNLJIterator() throws QueryPlanException, DatabaseException {
          /* Suggested Starter Code: get table names. */
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
          /* TODO */
            this.leftIterator = PNLJOperator.this.getPageIterator(this.leftTableName);
            this.rightIterator = PNLJOperator.this.getPageIterator(this.rightTableName);

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

            if (this.leftIterator.hasNext()) {
                this.leftPage = this.leftIterator.next(); //hmm edge case is if the empty table is passed in
                this.leftHeader = PNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
                this.leftRecord = this.getNextLeftRecordInPage();
            } else {
                this.leftPage = null;
                this.leftHeader = null;
                this.leftRecord = null;

            }
            if (this.rightIterator.hasNext()) {
                this.rightPage = this.rightIterator.next();
                this.rightHeader = PNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
                this.rightRecord = this.getNextRightRecordInPage();
            } else {
                this.rightPage = null;
                this.rightHeader = null;
                this.rightRecord = null;
            }

            this.leftEntryNum = 0;
            this.rightEntryNum = 0;


            this.nextRecord = null;
        }

        public boolean hasNext() {
            if (this.nextRecord != null) {
                return true;
            }
            while (true) {
                try {
                    //Out of the left side
                    if (this.rightRecord == null) {
                        if (this.leftRecord != null) {
                            this.leftRecord = this.getNextLeftRecordInPage();
                            this.rightEntryNum = 0; //LOL
                            this.rightRecord = this.getNextRightRecordInPage();
                        }
                        else if (this.rightPage != null) {
                            if (!this.rightIterator.hasNext()) {
                                this.rightPage = null;
                            } else {
                                this.rightPage = this.rightIterator.next(); //we in a new page DO WE NEED TO CALL NEXT TWICE

                                this.rightHeader = PNLJOperator.this.getPageHeader(rightTableName, rightPage);

                                this.rightEntryNum = 0;
                                this.rightRecord = this.getNextRightRecordInPage();
                                //reset leftRec all the way to the top of first left page. offset 0
                                this.leftEntryNum = 0;
                                this.leftRecord = this.getNextLeftRecordInPage();
                            }
                        }
                        else if (this.leftPage != null){
                            if (!this.leftIterator.hasNext()) {
                                this.leftPage = null;
                            } else {
                                this.leftPage = this.leftIterator.next();
                                this.leftHeader = PNLJOperator.this.getPageHeader(leftTableName, leftPage);
                                this.leftEntryNum = 0;
                                this.leftRecord = this.getNextLeftRecordInPage();

                                this.rightIterator = PNLJOperator.this.getPageIterator(this.rightTableName);
                                this.rightIterator.next();
                                this.rightPage = this.rightIterator.next();
                                this.rightEntryNum = 0;
                                this.rightHeader = PNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
                                this.rightRecord = this.getNextRightRecordInPage();
                            }
                        }
                        else {
                            return false;
                        }
                    } else if (this.leftRecord != null) {
                        //Situation 1 - Normal nothing is null DO I USE THE WHILE LOOP THINK
//                        while (this.rightEntryNum < PNLJOperator.this.getNumEntriesPerPage(this.rightTableName)){
//                            this.rightRecord = getNextRightRecordInPage();
                            DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                            DataBox rightJoinValue = this.rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
                            if (leftJoinValue.equals(rightJoinValue)) {
                                List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
                                List<DataBox> rightValues = new ArrayList<DataBox>(this.rightRecord.getValues());
                                leftValues.addAll(rightValues);
                                this.nextRecord = new Record(leftValues);
                                this.rightRecord = this.getNextRightRecordInPage(); //advance the right pointer
                                return true;
                            } else {
                                this.rightRecord = this.getNextRightRecordInPage(); //advance the right pointer
                            }
//                        }

                    } else {
                        this.rightRecord = null;
                    }

//                    else if (this.leftRecord == null) { //if left record is null, but right record is not
//                        //this.rightRecord = null;
//                        if (!this.rightIterator.hasNext()) {
//                            this.rightPage = null;
//                        } else {
//                            this.rightPage = this.rightIterator.next(); //we in a new page DO WE NEED TO CALL NEXT TWICE
//                            this.rightEntryNum = 0;
//                            this.rightHeader = PNLJOperator.this.getPageHeader(rightTableName, rightPage);
//                            this.rightRecord = this.getNextRightRecordInPage();
//                            //reset leftRec all the way to the top of first left page. offset 0
//                            this.leftEntryNum = 0;
//                            this.leftRecord = this.getNextLeftRecordInPage();
//                        }
//                    } else if(this.rightPage == null) {
//                        if (!this.leftIterator.hasNext()) {
//                            this.leftPage = null;
//                        } else {
//                            this.leftPage = this.leftIterator.next();
//                            this.leftEntryNum = 0;//DOES HEADER and en
//                            this.leftRecord = this.getNextLeftRecordInPage();
//
//                            this.rightIterator = PNLJOperator.this.getPageIterator(this.rightTableName);
//                            this.rightIterator.next();
//                            this.rightPage = this.rightIterator.next();
//                            this.rightEntryNum = 0;
//                            this.rightHeader = PNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
//                            this.rightRecord = this.getNextRightRecordInPage();
//                        }
//                    } else if (this.leftPage == null) {
//                        return false;
//                    }

                } catch (DatabaseException arresting) {
                    System.out.println("Caught an exception");
                    return false;
                }
//                System.out.println("got through one iter of while");
            }
        }

        private Record getNextLeftRecordInPage() throws DatabaseException {
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
                    //          this.recordCount++;
                    this.leftEntryNum++;
                    return toRtn;
                }

                this.leftEntryNum++;
            }
            return null;
        }

        private Record getNextRightRecordInPage() throws DatabaseException {
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
    }
}
