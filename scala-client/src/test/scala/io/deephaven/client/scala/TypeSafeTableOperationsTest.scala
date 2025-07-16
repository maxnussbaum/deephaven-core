package io.deephaven.client.scala

import io.deephaven.api.filter.FilterComparison
import io.deephaven.api.{ColumnName, Selectable}
import io.deephaven.api.expression.Function
import io.deephaven.api.filter.Filter
import io.deephaven.api.literal.Literal
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import TypeSafeTableOperations._

/**
 * Tests for the type-safe table operations DSL.
 * 
 * These tests verify that the DSL generates the correct underlying
 * Deephaven API calls and expressions.
 */
class TypeSafeTableOperationsTest extends AnyFunSuite with Matchers {
  
  test("col() function creates TypeSafeColumn") {
    val column = col("TestColumn")
    column.name shouldEqual "TestColumn"
    column.toColumnName shouldEqual ColumnName.of("TestColumn")
  }
  
//  test("String to TypeSafeColumn conversion") {
//    val col: TypeSafeColumn = "TestColumn"
//    col.name shouldEqual "TestColumn"
//    col.toColumnName shouldEqual ColumnName.of("TestColumn")
//  }
  
  test("Column comparison operations") {
    val col1 = col("Column1")
    val col2 = col("Column2")
    
    val gtFilter = col1 > col2
    gtFilter.toFilter shouldBe a[FilterComparison]
    
    val eqFilter = col1 === col2
    eqFilter.toFilter shouldBe a[FilterComparison]
    
    val neqFilter = col1 !== col2
    neqFilter.toFilter shouldBe a[FilterComparison]
  }
  
  test("Null checks") {
    val testCol = col("TestColumn")
    
    val isNullFilter = testCol.isNull
    isNullFilter.toFilter shouldEqual Filter.isNull(testCol.toExpression)
    
    val isNotNullFilter = testCol.isNotNull
    isNotNullFilter.toFilter shouldEqual Filter.isNotNull(testCol.toExpression)
    
    val funcIsNull = isNull(testCol)
    funcIsNull.toFilter shouldEqual Filter.isNull(testCol.toExpression)
    
    val funcIsNotNull = isNotNull(testCol)
    funcIsNotNull.toFilter shouldEqual Filter.isNotNull(testCol.toExpression)
  }
  
  test("Arithmetic operations") {
    val col1 = col("Column1")
    val col2 = col("Column2")
    
    val addExpr = col1 + col2
    addExpr.toExpression shouldBe a[Function]
    
    val subExpr = col1 - col2
    subExpr.toExpression shouldBe a[Function]
    
    val mulExpr = col1 * col2
    mulExpr.toExpression shouldBe a[Function]
    
    val divExpr = col1 / col2
    divExpr.toExpression shouldBe a[Function]
  }
  
  test("Logical operations") {
    val filter1 = col("Column1") > col("Column2")
    val filter2 = col("Column3") < col("Column4")
    
    val andFilter = filter1 && filter2
    andFilter.toFilter shouldBe a[io.deephaven.api.filter.FilterAnd]
    
    val orFilter = filter1 || filter2
    orFilter.toFilter shouldBe a[io.deephaven.api.filter.FilterOr]
    
    val notFilter = !filter1
    notFilter.toFilter shouldBe a[io.deephaven.api.filter.FilterNot[?]]
  }
  
  test("Column assignment") {
    val newCol = col("NewColumn")
    val expr = col("Column1") + col("Column2")
    
    val assignment = newCol := expr
    assignment.toSelectable shouldBe a[Selectable]
    
    val selectable = assignment.toSelectable
    selectable.newColumn() shouldEqual ColumnName.of("NewColumn")
    selectable.expression() shouldBe a[Function]
  }
  
  test("Literal conversions") {
    val intLiteral: TypeSafeExpression = 42
    intLiteral.toExpression shouldEqual Literal.of(42)
    
    val stringLiteral: TypeSafeExpression = "test"
    stringLiteral.toExpression shouldEqual Literal.of("test")
    
    val boolLiteral: TypeSafeExpression = true
    boolLiteral.toExpression shouldEqual Literal.of(true)
    
    val doubleLiteral: TypeSafeExpression = 3.14
    doubleLiteral.toExpression shouldEqual Literal.of(3.14)
  }
  
  test("Aggregation functions - method syntax") {
    val testCol = col("TestColumn")
    
    val medianExpr = testCol.median()
    medianExpr.toExpression shouldBe a[Function]
    
    val sumExpr = testCol.sum()
    sumExpr.toExpression shouldBe a[Function]
    
    val maxExpr = testCol.max()
    maxExpr.toExpression shouldBe a[Function]
    
    val minExpr = testCol.min()
    minExpr.toExpression shouldBe a[Function]
  }
  
  test("Aggregation functions - function syntax") {
    val testCol = col("TestColumn")
    
    val medianExpr = median(testCol)
    medianExpr.toExpression shouldBe a[Function]
    
    val sumExpr = sum(testCol)
    sumExpr.toExpression shouldBe a[Function]
    
    val maxExpr = max(testCol)
    maxExpr.toExpression shouldBe a[Function]
    
    val minExpr = min(testCol)
    minExpr.toExpression shouldBe a[Function]
  }
  
  test("Math functions") {
    val testCol = col("TestColumn")
    
    val absExpr = abs(testCol)
    absExpr.toExpression shouldBe a[Function]
    
    val sqrtExpr = sqrt(testCol)
    sqrtExpr.toExpression shouldBe a[Function]
    
    val powExpr = pow(testCol, 2)
    powExpr.toExpression shouldBe a[Function]
    
    val logExpr = log(testCol)
    logExpr.toExpression shouldBe a[Function]
  }
  
  test("String functions") {
    val testCol = col("TestColumn")
    
    val upperExpr = upper(testCol)
    upperExpr.toExpression shouldBe a[Function]
    
    val lowerExpr = lower(testCol)
    lowerExpr.toExpression shouldBe a[Function]
    
//    val lengthExpr = size(testCol)
//    lengthExpr.toExpression shouldBe a[Function]
    
    val trimExpr = trim(testCol)
    trimExpr.toExpression shouldBe a[Function]
  }
  
  test("Date/time functions") {
    val dateCol = col("DateColumn")
    
    val yearExpr = year(dateCol)
    yearExpr.toExpression shouldBe a[Function]
    
    val monthExpr = month(dateCol)
    monthExpr.toExpression shouldBe a[Function]
    
    val dayExpr = day(dateCol)
    dayExpr.toExpression shouldBe a[Function]
    
    val nowExpr = now()
    nowExpr.toExpression shouldBe a[Function]
  }
  
  test("Conditional expressions") {
    val condition = col("Column1") > col("Column2")
    val thenExpr = col("Column1")
    val elseExpr = col("Column2")
    
    val conditionalExpr = when(condition, thenExpr).otherwise(elseExpr)
    conditionalExpr.toExpression shouldBe a[Function]
  }
  
  test("Complex filter expression matching ExpressionTest.java") {
    // This recreates the complex filter from the Java test:
    // or(gt(FOO, BAR), gt(FOO, BAZ), and(isNull(FOO), isNotNull(BAR), isNotNull(BAZ)))
    
    val FOO = col("Foo")
    val BAR = col("Bar")
    val BAZ = col("Baz")
    
    val complexFilter = or(
      FOO > BAR,
      FOO > BAZ,
      and(
        FOO.isNull,
        BAR.isNotNull,
        BAZ.isNotNull
      )
    )
    
    complexFilter.toFilter shouldBe a[io.deephaven.api.filter.FilterOr]
  }
  
  test("Method calls on columns") {
    val testCol = col("TestColumn")
    val arg1 = col("Arg1")
    val arg2 = col("Arg2")
    
    val methodCall = testCol.method("myMethod", arg1, arg2)
    methodCall.toExpression shouldBe a[io.deephaven.api.expression.Method]
  }
  
  test("Raw string expressions") {
    val rawExpr = raw("someComplexFunction(Column1, Column2)")
    rawExpr.toExpression shouldBe a[io.deephaven.api.RawString]
  }
  
  test("Constants") {
    TRUE.toFilter shouldEqual Filter.ofTrue()
    FALSE.toFilter shouldEqual Filter.ofFalse()
  }
  
  test("Mixed expression types") {
    val testCol = col("TestColumn")
    val literal = 42
    val function = sum(col("OtherColumn"))
    
    val mixedExpr = testCol + literal + function
    mixedExpr.toExpression shouldBe a[Function]
  }
  
  test("Precedence in complex expressions") {
    // Test that operator precedence works as expected
    val expr1 = col("A") + col("B") * col("C")  // Should be A + (B * C)
    val expr2 = (col("A") + col("B")) * col("C")  // Should be (A + B) * C
    
    // Both should compile and produce Function expressions
    expr1.toExpression shouldBe a[Function]
    expr2.toExpression shouldBe a[Function]
    
    // The expressions should be different
    expr1.toExpression should not equal expr2.toExpression
  }
  
  test("Type safety with comparisons") {
    val testCol = col("TestColumn")
    
    // These should all compile
    val intComp = testCol > 42
    val doubleComp = testCol >= 3.14
    val stringComp = testCol === "test"
    val boolComp = testCol !== true
    val colComp = testCol < col("OtherColumn")
    
    // All should produce FilterComparison objects
    intComp.toFilter shouldBe a[FilterComparison]
    doubleComp.toFilter shouldBe a[FilterComparison]
    stringComp.toFilter shouldBe a[FilterComparison]
    boolComp.toFilter shouldBe a[FilterComparison]
    colComp.toFilter shouldBe a[FilterComparison]
  }
}