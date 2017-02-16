package edu.berkeley.cs186.database.table;

import com.sun.org.apache.xpath.internal.operations.Bool;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.table.stats.TableStats;

import javax.xml.crypto.Data;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import java.util.NoSuchElementException;
import java.util.Iterator;
import java.io.Closeable;

/**
 * A database table. Allows the user to add, delete, update, and get records.
 * A table has an associated schema, stats, and page allocator. The first page
 * in the page allocator is a header page that serializes the schema, and each
 * subsequent page is a data page containing the table records.
 *
 * Properties:
 * `schema`: the Schema (column names and column types) for this table
 * `freePages`: a set of page numbers that correspond to allocated pages with free space
 * `stats`: the TableStats for this table
 * `allocator`: the PageAllocator for this table
 * `tableName`: name of this table
 * `numEntriesPerPage`: number of records a data page of this table can hold
 * `pageHeaderSize`: physical size (in bytes) of a page header slot bitmap
 * `numRecords`: number of records currently contained in this table
 */
public class Table implements Iterable<Record>, Closeable {
    public static final String FILENAME_PREFIX = "db";
    public static final String FILENAME_EXTENSION = ".table";

    private Schema schema;
    private TreeSet<Integer> freePages;

    private TableStats stats;

    private PageAllocator allocator;
    private String tableName;

    private int numEntriesPerPage;
    private int pageHeaderSize;
    private long numRecords;

    public Table(String tableName) {
        this(tableName, FILENAME_PREFIX);
    }

    public Table(String tableName, String filenamePrefix) {
        this.tableName = tableName;

        String pathname = Paths.get(filenamePrefix, tableName + FILENAME_EXTENSION).toString();
        this.allocator = new PageAllocator(pathname, false);
        this.readHeaderPage();

        this.stats = new TableStats(this.schema);

        this.freePages = new TreeSet<Integer>();
        this.setEntryCounts();
        Iterator<Page> pIter = this.allocator.iterator();
        pIter.next();

        long freshCountRecords = 0;

        while(pIter.hasNext()) {
            Page p = pIter.next();

            // add all records in this page to TableStats
            int entryNum = 0;
            byte[] header = this.readPageHeader(p);
            while (entryNum < this.getNumEntriesPerPage()) {
                byte b = header[entryNum/8];
                int bitOffset = 7 - (entryNum % 8);
                byte mask = (byte) (1 << bitOffset);

                byte value = (byte) (b & mask);
                if (value != 0) {

                    int entrySize = this.schema.getEntrySize();

                    int offset = this.pageHeaderSize + (entrySize * entryNum);
                    byte[] bytes = p.readBytes(offset, entrySize);

                    Record record = this.schema.decode(bytes);
                    this.stats.addRecord(record);
                }

                entryNum++;
            }

            if (spaceOnPage(p)) {
                this.freePages.add(p.getPageNum());
            }

            freshCountRecords += numValidEntries(p);
        }

        this.numRecords = freshCountRecords;
    }

    public Table(Schema schema, String tableName) {
        this(schema, tableName, FILENAME_PREFIX);
    }

    /**
     * This constructor is used for creating a table in some specified directory.
     *
     * @param schema the schema for this table
     * @param tableName the name of the table
     * @param filenamePrefix the prefix where the table's files will be created
     */
    public Table(Schema schema, String tableName, String filenamePrefix) {
        this.schema = schema;
        this.tableName = tableName;
        this.stats = new TableStats(this.schema);

        this.freePages = new TreeSet<Integer>();
        String pathname = Paths.get(filenamePrefix, tableName + FILENAME_EXTENSION).toString();
        this.allocator = new PageAllocator(pathname, true);

        this.setEntryCounts();

        this.writeHeaderPage();
    }

    public void close() {
        allocator.close();
    }

    public Iterator<Record> iterator() {
        return new TableIterator();
    }

    /**
     * Returns where the first entry location is available to add bytes to on a page.
     *
     * @param P Page we are trying to find location for
     * @return the entry location
     */
    private int getEntryLoc(Page P) {

        byte[] header = this.readPageHeader(P);
        int entryNum = 0;

        for (byte b : header) {
            for (int mask = 0x80; mask != 0x00; mask >>>= 1) {
                if ((b & (byte) mask) == 0) {
                    return entryNum;
                }
                entryNum++;
            }
        }
        return entryNum;
    }

    /**
     * Adds a new record to this table. The record should be added to the first
     * free slot of the first free page if one exists, otherwise a new page should
     * be allocated and the record should be placed in the first slot of that
     * page. Recall that a free slot in the slot bitmap means the bit is set to 0.
     * Make sure to update this.stats, this.freePages, and this.numRecords as
     * necessary.
     *
     * @param values the values of the record being added
     * @return the RecordID of the added record
     * @throws DatabaseException if the values passed in to this method do not
     *         correspond to the schema of this table
     */
    public RecordID addRecord(List<DataBox> values) throws DatabaseException {
        int pageNum = 0;
        Record record;

        try {
            record = schema.verify(values);
        } catch (SchemaException exception) {
            throw new DatabaseException("You got 99 problems and this is one. Ya got the wrong schema");
        }

        if (this.freePages.isEmpty()) {
            pageNum = this.allocator.allocPage();
            this.freePages.add(pageNum);
        } else {
            pageNum = this.freePages.first();
        }
        Page currPage = this.allocator.fetchPage(pageNum);
        byte[] header = this.readPageHeader(currPage);
        int entryNum = 0;

        secondforloop:
        for (byte b : header) {
            for (int mask = 0x80; mask != 0x00; mask >>>= 1) {
                if ((b & (byte) mask) == 0) {
                    break secondforloop;
                }
                entryNum++;
            }
        }

        byte bytw = 1;
        writeBitToHeader(currPage, entryNum, bytw);

        //update everything after the fact
        byte[] recc = schema.encode(record);
        int pos = schema.getEntrySize() * entryNum + pageHeaderSize;
        currPage.writeBytes(pos, schema.getEntrySize(), recc);

        RecordID returnable = new RecordID(pageNum, entryNum);
        this.numRecords += 1;
        this.stats.addRecord(record); //Why the hell did I do this

        if (!spaceOnPage(currPage)) {
            freePages.remove(pageNum);
        }

        return returnable;
        }

//    In addRecord, how can we add a record's data into a block? Or flipping the bit to 1 is enough?
//    Page.java has a writeBytes() method which should allow you to write bytes to a page.
//
    /**
     * Deletes the record specified by rid from the table. Make sure to update
     * this.stats, this.freePages, and this.numRecords as necessary.
     *
     * @param rid the RecordID of the record to delete
     * @return the Record referenced by rid that was removed
     * @throws DatabaseException if rid does not correspond to a valid record
     */
    public Record deleteRecord(RecordID rid) throws DatabaseException {
        // TODO: implement me!
        Record record;
        try {
            record = getRecord(rid);
        } catch (DatabaseException ran) {
            throw new DatabaseException("im to tired for your bs");
        }

//        List<DataBox> values = new ArrayList<DataBox>();
//        updateRecord(values, rid);

        int entryNum = rid.getEntryNumber();
        int pageNum = rid.getPageNum();

        Page currPage = this.allocator.fetchPage(pageNum);
        byte bytw = 0;

        this.writeBitToHeader(currPage, entryNum, bytw);

//        byte[] recc = schema.encode(record);
//        byte[] empty = new byte[schema.getEntrySize()];
//        int pos = schema.getEntrySize() * entryNum + pageHeaderSize;
//        currPage.writeBytes(pos, schema.getEntrySize(), empty);

        this.numRecords -= 1;
        this.stats.removeRecord(record);

        if (!freePages.contains(pageNum)) {
            freePages.add(pageNum);
        }



        return record;
    }

    /**
     * Retrieves a record from the table.
     *
     * @param rid the RecordID of the record to retrieve
     * @return the Record referenced by rid
     * @throws DatabaseException if rid does not correspond to a valid record
     */
    public Record getRecord(RecordID rid) throws DatabaseException {
        // TODO: implement me!
        Boolean hmm = checkRecordIDValidity(rid);
        if (!hmm) {
            throw new DatabaseException ("get your ish together");
        }

        int pageNum = rid.getPageNum();
        Page headerPage = this.allocator.fetchPage(pageNum);
        int entrySize = this.schema.getEntrySize();
        int entryNum = rid.getEntryNumber();
        int offset = this.pageHeaderSize + (entrySize * entryNum);
        byte[] header = headerPage.readBytes(offset, entrySize);
        Record record = this.schema.decode(header);
        try {
            return this.schema.verify(record.getValues());
        } catch (SchemaException name) {
            throw new DatabaseException("hello");
        }
        //return null;
    }

    /**
     * Updates an existing record with new values and returns the old version of the record.
     * Make sure to update this.stats as necessary.
     *
     * @param values the new values of the record
     * @param rid the RecordID of the record to update
     * @return the old version of the record
     * @throws DatabaseException if rid does not correspond to a valid record or
     *         if the values do not correspond to the schema of this table
     */
    public Record updateRecord(List<DataBox> values, RecordID rid) throws DatabaseException {
        // TODO: implement me!
        Record record;
        try {
            record = getRecord(rid);
        } catch (DatabaseException ran) {
            throw new DatabaseException("im to tired for your bs");
        }

        int currPage = rid.getPageNum();
        Page Page = this.allocator.fetchPage(currPage);
        int entryNum = rid.getEntryNumber();
        byte bytw = 1;
        writeBitToHeader(Page, entryNum, bytw);

        Record newRec = new Record(values);
        byte[] recc = schema.encode(newRec);
        int pos = schema.getEntrySize() * entryNum + pageHeaderSize;
        Page.writeBytes(pos, schema.getEntrySize(), recc);

        this.numRecords += 1;
        this.stats.removeRecord(record);
        this.stats.addRecord(newRec);

        return record;
//        return null;
    }

    public int getNumEntriesPerPage() {
        return this.numEntriesPerPage;
    }

    public Schema getSchema() {
        return this.schema;
    }

    /**
     * Checks whether a RecordID is valid or not. That is, check to see if the slot
     * in the page specified by the RecordID contains a valid record (i.e. whether
     * the bit in the slot bitmap is set to 1).
     *
     * @param rid the record id to check
     * @return true if rid corresponds to a valid record, otherwise false
     * @throws DatabaseException if rid does not reference an existing data page slot
     */

    private boolean checkRecordIDValidity(RecordID rid) throws DatabaseException {
        // TODO: implement me!
        Page page = null;
        try {
            int pageNum = rid.getPageNum();
            page = this.allocator.fetchPage(pageNum);
        } catch (PageException something) {
            throw new DatabaseException(" you suck");
        }

        int checkingpagenumber = rid.getPageNum();
        int entrynumber = rid.getEntryNumber();

        if (checkingpagenumber == 0 || entrynumber >= this.numEntriesPerPage) {
            throw new DatabaseException ("not valid");
        }

        byte[] header = this.readPageHeader(page);
        int byteOffset = entrynumber / 8;
        int bitOffset = 7 - (entrynumber % 8);

//        byte mask = (byte) ((double)bitOffset * (double)bitOffset);
        byte mask = (byte) (1 << bitOffset);
        header[byteOffset] = (byte) (header[byteOffset] & mask);
        byte checking = header[byteOffset];

        if (checking != 0) {
            return true;
        }
        return false;
    }

    /**
     * Based on the Schema known to this table, calculates the number of record
     * entries a data page can hold and the size (in bytes) of the page header.
     * The page header only contains the slot bitmap and takes up no other space.
     * For ease of calculations and to prevent header byte splitting, ensure that
     * `numEntriesPerPage` is a multiple of 8 (this may waste some space).
     *
     * Should set this.pageHeaderSize and this.numEntriesPerPage.
     */

    private void setEntryCounts() {
        float size = schema.getEntrySize() + (float) 0.125;
        float buf = (Page.pageSize / size) % 8;
        this.numEntriesPerPage = (int) ((Page.pageSize / size) - buf);

        this.pageHeaderSize = this.numEntriesPerPage / 8;
    }

    /**
     * Checks if there is any free space on the given page.
     *
     * @param p the page to check
     * @return true if there exists free space, otherwise false
     */
    private boolean spaceOnPage(Page p) {
        byte[] header = this.readPageHeader(p);

        for (byte b : header) {
            if (b != (byte) 0xFF) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks how many valid record entries are in the given page.
     *
     * @param p the page to check
     * @return number of record entries in p
     */
    private int numValidEntries(Page p) {
        byte[] header = this.readPageHeader(p);
        int count = 0;

        for (byte b : header) {
            for (int mask = 0x01; mask != 0x100; mask <<= 1) {
                if ((b & (byte) mask) != 0) {
                    count++;
                }
            }
        }

        return count;
    }

    /**
     * Utility method to write the header page of the table. The only information written into
     * the header page is the table's schema.
     */
    private void writeHeaderPage() {
        int numBytesWritten = 0;
        Page headerPage = this.allocator.fetchPage(this.allocator.allocPage());

        assert(0 == headerPage.getPageNum());

        List<String> fieldNames = this.schema.getFieldNames();
        headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(fieldNames.size()).array());
        numBytesWritten += 4;

        for (String fieldName : fieldNames) {
            headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(fieldName.length()).array());
            numBytesWritten += 4;
        }

        for (String fieldName : fieldNames) {
            headerPage.writeBytes(numBytesWritten, fieldName.length(), fieldName.getBytes(Charset.forName("UTF-8")));
            numBytesWritten += fieldName.length();
        }

        for (DataBox field : this.schema.getFieldTypes()) {
            headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(field.type().ordinal()).array());
            numBytesWritten += 4;

            if (field.type().equals(DataBox.Types.STRING)) {
                headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(field.getSize()).array());
                numBytesWritten += 4;
            }
        }
    }

    /**
     * Utility method to read the header page of the table.
     */
    private void readHeaderPage() {
        int numBytesRead = 0;
        Page headerPage = this.allocator.fetchPage(0);

        int numFields = ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt();
        numBytesRead += 4;

        List<Integer> fieldNameLengths = new ArrayList<Integer>();
        for (int i = 0; i < numFields; i++) {
            fieldNameLengths.add(ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt());
            numBytesRead += 4;
        }

        List<String> fieldNames = new ArrayList<String>();
        for (int fieldNameLength : fieldNameLengths) {
            byte[] bytes = headerPage.readBytes(numBytesRead, fieldNameLength);

            fieldNames.add(new String(bytes, Charset.forName("UTF-8")));
            numBytesRead += fieldNameLength;
        }

        List<DataBox> fieldTypes = new ArrayList<DataBox>();
        for (int i = 0; i < numFields; i++) {
            int ordinal = ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt();
            DataBox.Types type = DataBox.Types.values()[ordinal];
            numBytesRead += 4;

            switch(type) {
                case INT:
                    fieldTypes.add(new IntDataBox());
                    break;
                case STRING:
                    int len = ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt();
                    numBytesRead += 4;

                    fieldTypes.add(new StringDataBox(len));
                    break;
                case BOOL:
                    fieldTypes.add(new BoolDataBox());
                    break;
                case FLOAT:
                    fieldTypes.add(new FloatDataBox());
                    break;
            }
        }

        this.schema = new Schema(fieldNames, fieldTypes);

    }

    /**
     * Utility method to write a particular bit into the header of a particular page.
     *
     * @param page the page to modify
     * @param entryNum the header slot to modify
     * @param value the value of the bit to write (should either be 0 or 1)
     */
    private void writeBitToHeader(Page page, int entryNum, byte value) {
        byte[] header = this.readPageHeader(page);
        int byteOffset = entryNum / 8;
        int bitOffset = 7 - (entryNum % 8);

        if (value == 0) {
            byte mask = (byte) ~((1 << bitOffset));

            header[byteOffset] = (byte) (header[byteOffset] & mask);
            page.writeBytes(0, this.pageHeaderSize, header);
        } else {
            byte mask = (byte) (1 << bitOffset);

            header[byteOffset] = (byte) (header[byteOffset] | mask);
        }

        page.writeBytes(0, this.pageHeaderSize, header);
    }

    /**
     * Read the slot header of a page.
     *
     * @param page the page to read from
     * @return a byte[] with the slot header
     */
    private byte[] readPageHeader(Page page) {
        return page.readBytes(0, this.pageHeaderSize);
    }

    /**
     * An implementation of Iterator that provides an iterator interface over all
     * of the records in this table.
     */
    private class TableIterator implements Iterator<Record> {
        private Page page; // readPageHeader(Page page) to get the byte[] if want it
        RecordID rid;
        int numRecord;
        private Iterator<Page> pageIterator;

        public TableIterator() {
            this.pageIterator = Table.this.allocator.iterator();
            int pageNum = this.pageIterator.next().getPageNum();

            int entryNum = 0;
            if (pageNum != 0) {
                return;
            }
            if (Table.this.allocator.iterator().hasNext()) {
                Page page = Table.this.allocator.iterator().next();
                byte[] header = Table.this.readPageHeader(page);
//                pageNum = this.pageIterator.next().getPageNum()
//                Page currPage = Table.this.allocator.fetchPage(pageNum);
            }
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            // TODO: implement me!
//            if (this.next != null) {
//                return true;
//            }
            return false;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public Record next() {
            // TODO: implement me

//            try {
//                return getRecord(next);
//            } catch (DatabaseException e) {
//                throw new NoSuchElementException("hope im not messing everything up");
//            }

            return null;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
