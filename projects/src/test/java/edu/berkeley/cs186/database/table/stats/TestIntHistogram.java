package edu.berkeley.cs186.database.table.stats;

import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP2;

import edu.berkeley.cs186.database.StudentTestP4;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;

public class TestIntHistogram {



  @Test(timeout=1000)
  @Category(StudentTestP4.class)
  public void testIntSimpleHistogram1() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 20; i++) {
      histogram.addValue(i);
    }

    assertEquals(20, histogram.getEntriesInRange(0, 20));
  }

  @Test(timeout=1000)
  @Category(StudentTestP4.class)
  public void testIntSimpleHistogram2() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 30; i++) {
      histogram.addValue(i);
    }

    assertEquals(30, histogram.getEntriesInRange(0, 30));
  }

  @Test(timeout=1000)
  @Category(StudentTestP4.class)
  public void testIntSimpleHistogram3() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 40; i++) {
      histogram.addValue(i);
    }

    assertEquals(40, histogram.getEntriesInRange(0, 40));
  }

  @Test(timeout=1000)
  @Category(StudentTestP4.class)
  public void testIntSimpleHistogram4() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 50; i++) {
      histogram.addValue(i);
    }

    assertEquals(50, histogram.getEntriesInRange(0, 50));
  }

  @Test(timeout=1000)
  @Category(StudentTestP4.class)
  public void testIntSimpleHistogram5() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 60; i++) {
      histogram.addValue(i);
    }

    assertEquals(60, histogram.getEntriesInRange(0, 60));
  }

  @Test(timeout=1000)
  @Category(StudentTestP4.class)
  public void testIntSimpleHistogram6() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 120; i++) {
      histogram.addValue(i);
    }
    System.out.println(120);
    System.out.println(histogram.getEntriesInRange(0, 120));
    assertEquals(120, histogram.getEntriesInRange(0, 120));
  }

  @Test(timeout=1000)
  @Category(StudentTestP4.class)
  public void testIntSimpleHistogram7() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 80; i++) {
      histogram.addValue(i);
    }

    assertEquals(80, histogram.getEntriesInRange(0, 80));
  }

  @Test(timeout=1000)
  @Category(StudentTestP4.class)
  public void testIntSimpleHistogram89() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 15; i++) {
      histogram.addValue(i);
    }
    System.out.println(histogram.getEntriesInRange(0,10));
    assertEquals(10, histogram.getEntriesInRange(0,10));
  }

  @Test(timeout=1000)
  @Category(StudentTestP4.class)
  public void testIntSimpleHistogram10() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 100; i++) {
      histogram.addValue(i);
    }

    assertEquals(100, histogram.getEntriesInRange(0, 100));
  }


  @Test(timeout=1000)
  public void testIntSimpleHistogram() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 10; i++) {
      histogram.addValue(i);
    }

    assertEquals(10, histogram.getEntriesInRange(0, 10));
  }

  @Test(timeout=1000)
  public void testIntComplexHistogram() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 40; i++) {
      histogram.addValue(i);
    }

    assertEquals(10, histogram.getEntriesInRange(0, 10));
    assertEquals(10, histogram.getEntriesInRange(10, 20));
    assertEquals(10, histogram.getEntriesInRange(20, 30));
    assertEquals(10, histogram.getEntriesInRange(30, 40));
    assertEquals(20, histogram.getEntriesInRange(20, 40));
    assertEquals(10, histogram.getEntriesInRange(15, 25));
    assertEquals(5, histogram.getEntriesInRange(25, 30));
  }

  @Test(timeout=1000)
  public void testIntHistogramExpand() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 10; i++) {
      histogram.addValue(i);
    }

    histogram.addValue(99);

    assertEquals(10, histogram.getAllBuckets().get(5).getCount());
    assertEquals(1, histogram.getAllBuckets().get(9).getCount());
  }

  @Test(timeout=1000)
  public void testIntComputeReductionFactor() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 50; i++) {
      histogram.addValue(i);
      histogram.addValue(i);
    }

    assertEquals(50, histogram.getNumDistinct());

    IntDataBox equalsValue = new IntDataBox(3);
    assertEquals(0.02f,
                 histogram.computeReductionFactor(PredicateOperator.EQUALS,
                                                  equalsValue),
                 0.001f);

    IntDataBox lessThanValue = new IntDataBox(25);
    assertEquals(0.5,
                 histogram.computeReductionFactor(PredicateOperator.LESS_THAN,
                                                  lessThanValue),
                 0.001f);

    IntDataBox lessThanEqualsValue = new IntDataBox(25);
    assertEquals(0.52,
                 histogram.computeReductionFactor(PredicateOperator.LESS_THAN_EQUALS,
                                                  lessThanEqualsValue),
                 0.001f);

    IntDataBox greaterThanValue = new IntDataBox(9);
    assertEquals(0.82,
                 histogram.computeReductionFactor(PredicateOperator.GREATER_THAN,
                                                  greaterThanValue),
                 0.001f);

    IntDataBox greaterThanEqualsValue = new IntDataBox(10);
    assertEquals(0.82,
                 histogram.computeReductionFactor(PredicateOperator.GREATER_THAN_EQUALS,
                                                  greaterThanEqualsValue),
                 0.001f);
  }

  @Test(timeout=1000)
  public void testIntCopyWithReduction() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 100; i++) {
      histogram.addValue(i);
    }

    assertEquals(100, histogram.getNumDistinct());

    IntHistogram copyHistogram = histogram.copyWithReduction(0.7f);

    assertEquals(70, copyHistogram.getEntriesInRange(0, 100));
    assertEquals(70, copyHistogram.getNumDistinct());
  }

  @Test(timeout=1000)
  public void testIntCopyWithPredicate() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 500; i++) {
      histogram.addValue(i);
    }

    assertEquals(500, histogram.getNumDistinct());

    IntDataBox value = new IntDataBox(320);
    IntHistogram copyHistogram = histogram.copyWithPredicate(PredicateOperator.LESS_THAN,
                                                             value);

    assertEquals(320, copyHistogram.getEntriesInRange(0, 500));
    assertEquals(250, copyHistogram.getNumDistinct());
  }
}