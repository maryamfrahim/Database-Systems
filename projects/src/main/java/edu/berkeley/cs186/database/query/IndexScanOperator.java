package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class IndexScanOperator extends QueryOperator {
  private Database.Transaction transaction;
  private String tableName;
  private String columnName;
  private QueryPlan.PredicateOperator predicate;
  private DataBox value;

  private int columnIndex;




  /**
   * An index scan operator.
   *
   * @param transaction the transaction containing this operator
   * @param tableName the table to iterate over
   * @param columnName the name of the column the index is on
   * @throws QueryPlanException
   * @throws DatabaseException
   */
  public IndexScanOperator(Database.Transaction transaction,
                           String tableName,
                           String columnName,
                           QueryPlan.PredicateOperator predicate,
                           DataBox value) throws QueryPlanException, DatabaseException {
    super(OperatorType.INDEXSCAN);
    this.tableName = tableName;
    this.transaction = transaction;
    this.columnName = columnName;
    this.predicate = predicate;
    this.value = value;
    this.setOutputSchema(this.computeSchema());
    columnName = this.checkSchemaForColumn(this.getOutputSchema(), columnName);
    this.columnIndex = this.getOutputSchema().getFieldNames().indexOf(columnName);
  }

  public String toString() {
    return "type: " + this.getType() +
        "\ntable: " + this.tableName +
        "\ncolumn: " + this.columnName +
        "\noperator: " + this.predicate +
        "\nvalue: " + this.value;
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new IndexScanIterator();

  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class IndexScanIterator implements Iterator<Record> {
    /* TODO: Implement the IndexScanIterator */
    private Record nextRecord;
    private Iterator<Record> recIter;

    public IndexScanIterator() throws QueryPlanException, DatabaseException {
      /* TODO */
      nextRecord = null;
      recIter = null;
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      /* TODO */
      if (this.nextRecord != null) {
        return true;
      }

      if (recIter != null && recIter.hasNext()) {
        nextRecord = recIter.next();
        return true;
      }
      if (recIter != null && !recIter.hasNext()) {
//        nextRecord = null;
        recIter = null;
        return false;
      }
//      else {
//        return false;
//      }
      try {
        if (IndexScanOperator.this.transaction.indexExists(IndexScanOperator.this.tableName, IndexScanOperator.this.columnName)) {

          Iterator<Record> later = IndexScanOperator.this.transaction.getRecordIterator(IndexScanOperator.this.tableName);

          if (later.hasNext()) {
            Record now  = later.next();
            List<DataBox> yolo = now.getValues();

            DataBox compare = yolo.get(IndexScanOperator.this.columnIndex);
            if (compare.compareTo(IndexScanOperator.this.value) >= 0) {
              recIter = IndexScanOperator.this.transaction.sortedScanFrom(IndexScanOperator.this.tableName, IndexScanOperator.this.columnName, IndexScanOperator.this.value);
              if (recIter.hasNext()) {
                this.nextRecord = this.recIter.next();
                return true;
              } else {
                return false;
              }
            }
            if (compare.compareTo(IndexScanOperator.this.value) > 0) {
              recIter = IndexScanOperator.this.transaction.sortedScanFrom(IndexScanOperator.this.tableName, IndexScanOperator.this.columnName, IndexScanOperator.this.value);
              if (recIter.hasNext()) {
                this.recIter.next();
                this.nextRecord = this.recIter.next();
                return true;
              } else {
                return false;
              }
            }


            if (compare.compareTo(IndexScanOperator.this.value) == 0) {
              recIter = IndexScanOperator.this.transaction.lookupKey(IndexScanOperator.this.tableName, IndexScanOperator.this.columnName, IndexScanOperator.this.value);
              if (recIter.hasNext()) {
                this.nextRecord = this.recIter.next();
                return true;
              } else {
                return false;
              }
            }

            if (compare.compareTo(IndexScanOperator.this.value) <= 0) {
              Iterator<Record> FullRecIter = IndexScanOperator.this.transaction.sortedScan(IndexScanOperator.this.tableName, IndexScanOperator.this.columnName);
              Iterator<Record> HalfRecIter = IndexScanOperator.this.transaction.sortedScanFrom(IndexScanOperator.this.tableName, IndexScanOperator.this.columnName, IndexScanOperator.this.value);
              while (FullRecIter.hasNext()) {
                if (FullRecIter.next().equals(HalfRecIter.next())) {
                  FullRecIter.remove();
                }
              }
              recIter = FullRecIter;
              if (recIter.hasNext()) {
                this.nextRecord = this.recIter.next();
                return true;
              } else {
                return false;
              }
            }

            if (compare.compareTo(IndexScanOperator.this.value) < 0) {
              Iterator<Record> FullRecIter  = IndexScanOperator.this.transaction.sortedScan(IndexScanOperator.this.tableName, IndexScanOperator.this.columnName);
              Iterator<Record> HalfRecIter = IndexScanOperator.this.transaction.sortedScanFrom(IndexScanOperator.this.tableName, IndexScanOperator.this.columnName, IndexScanOperator.this.value);
              while (FullRecIter.hasNext()) {
                if (FullRecIter.next().equals(HalfRecIter.next())) {
                  FullRecIter.remove();
                }
              }
              recIter = FullRecIter;
              if (recIter.hasNext()) {
                this.recIter.next();
                this.nextRecord = this.recIter.next();
                return true;
              } else {
                return false;
              }
            }




          }




        }
        return false;
      } catch (DatabaseException hey) {
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
