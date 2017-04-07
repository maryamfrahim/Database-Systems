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
//            leftTableName = get;
//            rightTableName = ;
            this.leftIterator = getPageIterator(leftTableName);
            this.rightIterator = getPageIterator(rightTableName);
            this.leftRecord = getNextLeftRecordInPage();
            this.nextRecord = null;
            this.rightRecord = getNextRightRecordInPage();
            this.leftPage = leftIterator.next(); //hmm edge case is if the empty table is passed in
            this.rightPage = rightIterator.next();//hmm
            this.leftHeader = getPageHeader(leftTableName, leftPage);
            this.rightHeader = getPageHeader(rightTableName, rightPage);
            this.leftEntryNum = 0; //hmm
            this.rightEntryNum = 0; //my bet on this

        }

        public boolean hasNext() {
            if (this.nextRecord != null) {
                return true;
            }
            while (true) {
                try {
                    //Out of the left side
                    if (leftPage == null) {
                        return false;
                    }

                    if (rightRecord == null) {
                        if (leftRecord != null) {

                            leftRecord = getNextLeftRecordInPage();
                            rightRecord; //reset rightRec
                        }
                        if (rightPage != null) {

                            rightPage = rightIterator.next();
                            leftRecord; //reset leftRec
                        }
                        if (leftPage != null){
                            leftPage = leftIterator.next();
                            rightPage = getPageIterator(rightTableName).next();
                        }
                        else {
                            return false;
                        }
                    } else {
                        //Situation 1 - Normal nothing is null
                        while (this.rightIterator.hasNext()) {
                            //Record rightRecord = this.rightIterator.next();
                            Record rightRecord = getNextRightRecordInPage();
                            DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                            DataBox rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
                            if (leftJoinValue.equals(rightJoinValue)) {
                                List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
                                List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
                                leftValues.addAll(rightValues);
                                this.nextRecord = new Record(leftValues);
                                return true;
                            }
                        }
                        this.rightRecord = getNextRightRecordInPage(); //advance the right pointer
                    }

                } catch (DatabaseException arresting) {
                    System.out.println("Caught an exception");
                }
                return false;
            }
        }

        private Record getNextLeftRecordInPage() throws DatabaseException {
            while (this.leftEntryNum < getNumEntriesPerPage(leftTableName)) {
                byte b = leftHeader[this.leftEntryNum/8];
                int bitOffset = 7 - (this.leftEntryNum % 8);
                byte mask = (byte) (1 << bitOffset);

                byte value = (byte) (b & mask);
                if (value != 0) {
                    int entrySize = getSchema(leftTableName).getEntrySize();

                    int offset = getHeaderSize(leftTableName) + (entrySize * leftEntryNum);
                    byte[] bytes = this.leftPage.readBytes(offset, entrySize);

                    Record toRtn = getSchema(leftTableName).decode(bytes);
                    //          this.recordCount++;
                    this.leftEntryNum++;
                    return toRtn;
                }

                this.leftEntryNum++;
            }

            if (this.hasNext()) {
                this.leftEntryNum = 0;
                this.leftPage = this.leftIterator.next();
                leftHeader = getPageHeader(leftTableName, this.leftPage);
            }
            return null;
        }

        private Record getNextRightRecordInPage() throws DatabaseException {
            while (this.rightEntryNum < getNumEntriesPerPage(rightTableName)) {
                byte b = rightHeader[this.rightEntryNum/8];
                int bitOffset = 7 - (this.rightEntryNum % 8);
                byte mask = (byte) (1 << bitOffset);

                byte value = (byte) (b & mask);
                if (value != 0) {
                    int entrySize = getEntrySize(rightTableName);

                    int offset = getHeaderSize(rightTableName) + (entrySize * rightEntryNum);
                    byte[] bytes = this.rightPage.readBytes(offset, entrySize);

                    Record toRtn = getSchema(rightTableName).decode(bytes);
                    //          this.recordCount++;
                    this.rightEntryNum++;
                    return toRtn;
                }

                this.rightEntryNum++;
            }

            if (this.hasNext()) {
                this.rightEntryNum = 0;
                this.rightPage = this.rightIterator.next();
                rightHeader = getPageHeader(rightTableName, this.rightPage);
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
