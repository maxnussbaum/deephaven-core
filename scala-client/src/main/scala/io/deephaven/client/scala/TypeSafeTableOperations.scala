package io.deephaven.client.scala

import io.deephaven.api.*
import io.deephaven.api.expression.{Expression, Function, Method}
import io.deephaven.api.filter.*
import io.deephaven.api.literal.Literal
import io.deephaven.engine.table.Table

import scala.language.implicitConversions
import java.util.Collection as JCollection
import scala.annotation.targetName
import scala.jdk.CollectionConverters.*

/**
 * Type-safe Scala DSL for Deephaven table operations.
 * 
 * This class provides a high-level, type-safe interface for table operations
 * that replaces string-based expressions with compile-time checked constructs.
 * 
 * Example usage:
 * {{{
 * import TypeSafeTableOperations._
 * 
 * val result = table
 *   .where(col("MedianAverageDailyVolume").isNotNull && col("TargetEODDollars") > 0)
 *   .update(col("NewColumn") := col("Column1") + col("Column2"))
 *   .view(col("Column1"), col("Column2"), col("NewColumn"))
 * }}}
 */
class TypeSafeTableOperations(val table: Table) extends AnyVal {
  
  /**
   * Enhanced where method with type-safe filter expressions.
   */
  def where(filter: TypeSafeFilter): Table = {
    table.where(filter.toFilter)
  }
  
  /**
   * Enhanced update method with type-safe column assignments.
   */
  def update(assignments: TypeSafeSelectable*): Table = {
    table.update(assignments.map(_.toSelectable).asJava)
  }
  
  /**
   * Enhanced view method with type-safe column selections.
   */
  def view(selections: TypeSafeSelectable*): Table = {
    table.view(selections.map(_.toSelectable).asJava)
  }
  
  /**
   * Enhanced updateView method with type-safe column assignments.
   */
  def updateView(assignments: TypeSafeSelectable*): Table = {
    table.updateView(assignments.map(_.toSelectable).asJava)
  }
  
  /**
   * Enhanced select method with type-safe column selections.
   */
  def select(selections: TypeSafeSelectable*): Table = {
    table.select(selections.map(_.toSelectable).asJava)
  }
  
  /**
   * Enhanced groupBy method with type-safe column references.
   */
  def groupBy(columns: TypeSafeColumn*): Table = {
    table.groupBy(columns.map(_.toColumnName).asJava)
  }
  
  /**
   * Enhanced sort method with type-safe column references.
   */
  def sort(columns: TypeSafeColumn*): Table = {
    table.sort(columns.map(_.name).toArray*)
  }
  
  /**
   * Enhanced sortDescending method with type-safe column references.
   */
  def sortDescending(columns: TypeSafeColumn*): Table = {
    table.sortDescending(columns.map(_.name).toArray*)
  }
}

/**
 * Type-safe representation of a column reference.
 */
class TypeSafeColumn(val name: String) extends AnyVal {
  
  def toColumnName: ColumnName = ColumnName.of(name)
  def toExpression: Expression = toColumnName
  
  // Comparison operators
  def >(other: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(FilterComparison.gt(this.toExpression, other.toExpression))
  def >=(other: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(FilterComparison.geq(this.toExpression, other.toExpression))
  def <(other: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(FilterComparison.lt(this.toExpression, other.toExpression))
  def <=(other: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(FilterComparison.leq(this.toExpression, other.toExpression))
  def ===(other: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(FilterComparison.eq(this.toExpression, other.toExpression))
  def !==(other: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(FilterComparison.neq(this.toExpression, other.toExpression))
  
  // Null checks
  def isNull: TypeSafeFilter = TypeSafeFilter(Filter.isNull(this.toExpression))
  def isNotNull: TypeSafeFilter = TypeSafeFilter(Filter.isNotNull(this.toExpression))
  
  // Arithmetic operators
  def +(other: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("plus", this.toExpression, other.toExpression))
  def -(other: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("minus", this.toExpression, other.toExpression))
  def *(other: TypeSafeExpression): TypeSafeExpression =
    TypeSafeExpression(Function.of("multiply", this.toExpression, other.toExpression))
  def /(other: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("divide", this.toExpression, other.toExpression))
  
  // Method calls
  def method(methodName: String, args: TypeSafeExpression*): TypeSafeExpression = {
    TypeSafeExpression(Method.of(this.toExpression, methodName, args.map(_.toExpression)*))
  }
  
  // Common aggregation functions as methods
  def median(): TypeSafeExpression = TypeSafeExpression(Function.of("median", this.toExpression))
  def mean(): TypeSafeExpression = TypeSafeExpression(Function.of("mean", this.toExpression))
  def sum(): TypeSafeExpression = TypeSafeExpression(Function.of("sum", this.toExpression))
  def count(): TypeSafeExpression = TypeSafeExpression(Function.of("count", this.toExpression))
  def min(): TypeSafeExpression = TypeSafeExpression(Function.of("min", this.toExpression))
  def max(): TypeSafeExpression = TypeSafeExpression(Function.of("max", this.toExpression))
  def std(): TypeSafeExpression = TypeSafeExpression(Function.of("std", this.toExpression))
  def varr(): TypeSafeExpression = TypeSafeExpression(Function.of("var", this.toExpression))
  def first(): TypeSafeExpression = TypeSafeExpression(Function.of("first", this.toExpression))
  def last(): TypeSafeExpression = TypeSafeExpression(Function.of("last", this.toExpression))
  
  // Column assignment
  def :=(expr: TypeSafeExpression): TypeSafeSelectable = 
    TypeSafeSelectable(Selectable.of(this.toColumnName, expr.toExpression))
  
  // String interpolation support
  override def toString: String = name
}

/**
 * Type-safe representation of an expression.
 */
case class TypeSafeExpression(expression: Expression) {
  def toExpression: Expression = expression
  
  // Comparison operators
  def >(other: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(FilterComparison.gt(this.toExpression, other.toExpression))
  def >=(other: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(FilterComparison.geq(this.toExpression, other.toExpression))
  def <(other: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(FilterComparison.lt(this.toExpression, other.toExpression))
  def <=(other: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(FilterComparison.leq(this.toExpression, other.toExpression))
  def ===(other: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(FilterComparison.eq(this.toExpression, other.toExpression))
  def !==(other: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(FilterComparison.neq(this.toExpression, other.toExpression))
  
  // Arithmetic operators
  def +(other: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("plus", this.toExpression, other.toExpression))
  def -(other: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("minus", this.toExpression, other.toExpression))
  def *(other: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("multiply", this.toExpression, other.toExpression))
  def /(other: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("divide", this.toExpression, other.toExpression))
}

/**
 * Type-safe representation of a filter condition.
 */
case class TypeSafeFilter(filter: Filter) {
  def toFilter: Filter = filter
  def toExpression: Expression = filter
  
  // Logical operators
  @targetName("and")
  def &&(other: TypeSafeFilter): TypeSafeFilter = 
    TypeSafeFilter(Filter.and(this.toFilter, other.toFilter))
  @targetName("or")
  def ||(other: TypeSafeFilter): TypeSafeFilter = 
    TypeSafeFilter(Filter.or(this.toFilter, other.toFilter))
  @targetName("not")
  def unary_! : TypeSafeFilter = 
    TypeSafeFilter(Filter.not(this.toFilter))
}

/**
 * Type-safe representation of a selectable column assignment.
 */
case class TypeSafeSelectable(selectable: Selectable) {
  def toSelectable: Selectable = selectable
}

/**
 * Companion object with implicit conversions and helper functions.
 */
object TypeSafeTableOperations {
  
  // Implicit conversion from Table to TypeSafeTableOperations
  implicit def tableToTypeSafe(table: Table): TypeSafeTableOperations = 
    new TypeSafeTableOperations(table)
  
  // Column function
  def col(name: String): TypeSafeColumn = new TypeSafeColumn(name)
  
  // Literal value conversions
  implicit def intToExpression(value: Int): TypeSafeExpression = 
    TypeSafeExpression(Literal.of(value))
  implicit def longToExpression(value: Long): TypeSafeExpression = 
    TypeSafeExpression(Literal.of(value))
  implicit def floatToExpression(value: Float): TypeSafeExpression = 
    TypeSafeExpression(Literal.of(value))
  implicit def doubleToExpression(value: Double): TypeSafeExpression = 
    TypeSafeExpression(Literal.of(value))
  implicit def booleanToExpression(value: Boolean): TypeSafeExpression = 
    TypeSafeExpression(Literal.of(value))
  implicit def stringToExpression(value: String): TypeSafeExpression = 
    TypeSafeExpression(Literal.of(value))
  
  // Column to expression conversion
  implicit def columnToExpression(column: TypeSafeColumn): TypeSafeExpression = 
    TypeSafeExpression(column.toExpression)
  
  // Function-based null checks
  def isNull(expr: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(Filter.isNull(expr.toExpression))
  def isNotNull(expr: TypeSafeExpression): TypeSafeFilter = 
    TypeSafeFilter(Filter.isNotNull(expr.toExpression))
  
  // Function-based aggregations
  def median(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("median", expr.toExpression))
  def mean(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("mean", expr.toExpression))
  def sum(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("sum", expr.toExpression))
  def count(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("count", expr.toExpression))
  def min(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("min", expr.toExpression))
  def max(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("max", expr.toExpression))
  def std(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("std", expr.toExpression))
  def variance(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("var", expr.toExpression))
  def first(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("first", expr.toExpression))
  def last(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("last", expr.toExpression))
  
  // Math functions
  def abs(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("abs", expr.toExpression))
  def sqrt(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("sqrt", expr.toExpression))
  def pow(base: TypeSafeExpression, exponent: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("pow", base.toExpression, exponent.toExpression))
  def log(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("log", expr.toExpression))
  def exp(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("exp", expr.toExpression))
  
  // String functions
  def upper(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("upper", expr.toExpression))
  def lower(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("lower", expr.toExpression))
  def length(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("length", expr.toExpression))
  def trim(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("trim", expr.toExpression))
  def substr(expr: TypeSafeExpression, start: TypeSafeExpression, length: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("substr", expr.toExpression, start.toExpression, length.toExpression))
  
  // Date/time functions
  def now(): TypeSafeExpression = 
    TypeSafeExpression(Function.of("now"))
  def date(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("date", expr.toExpression))
  def year(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("year", expr.toExpression))
  def month(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("month", expr.toExpression))
  def day(expr: TypeSafeExpression): TypeSafeExpression = 
    TypeSafeExpression(Function.of("day", expr.toExpression))
  
  // Conditional expressions
  def when(condition: TypeSafeFilter, thenExpr: TypeSafeExpression): ConditionalBuilder = 
    new ConditionalBuilder(condition, thenExpr)
  
  class ConditionalBuilder(condition: TypeSafeFilter, thenExpr: TypeSafeExpression) {
    def otherwise(elseExpr: TypeSafeExpression): TypeSafeExpression = 
      TypeSafeExpression(Function.of("iif", condition.toFilter, thenExpr.toExpression, elseExpr.toExpression))
  }
  
  // Raw string expressions for complex cases
  def raw(expression: String): TypeSafeExpression = 
    TypeSafeExpression(RawString.of(expression))
  
  // Support for complex filter expressions using the same pattern as the test
  def and(filters: TypeSafeFilter*): TypeSafeFilter = 
    TypeSafeFilter(Filter.and(filters.map(_.toFilter).asJava))
  def or(filters: TypeSafeFilter*): TypeSafeFilter = 
    TypeSafeFilter(Filter.or(filters.map(_.toFilter).asJava))
  
  // Constants
  val TRUE: TypeSafeFilter = TypeSafeFilter(Filter.ofTrue())
  val FALSE: TypeSafeFilter = TypeSafeFilter(Filter.ofFalse())
}

/**
 * Enhanced table operations with fluent API support.
 */
object TableOps {
  
  /**
   * Creates a fluent query builder for complex table operations.
   */
  def from(table: Table): TableQueryBuilder = new TableQueryBuilder(table)
  
  class TableQueryBuilder(private var currentTable: Table) {
    
    def where(filter: TypeSafeFilter): TableQueryBuilder = {
      currentTable = currentTable.where(filter.toFilter)
      this
    }
    
    def update(assignments: TypeSafeSelectable*): TableQueryBuilder = {
      currentTable = currentTable.update(assignments.map(_.toSelectable).asJava)
      this
    }
    
    def view(selections: TypeSafeSelectable*): TableQueryBuilder = {
      currentTable = currentTable.view(selections.map(_.toSelectable).asJava)
      this
    }
    
    def updateView(assignments: TypeSafeSelectable*): TableQueryBuilder = {
      currentTable = currentTable.updateView(assignments.map(_.toSelectable).asJava)
      this
    }
    
    def select(selections: TypeSafeSelectable*): TableQueryBuilder = {
      currentTable = currentTable.select(selections.map(_.toSelectable).asJava)
      this
    }
    
    def groupBy(columns: TypeSafeColumn*): TableQueryBuilder = {
      currentTable = currentTable.groupBy(columns.map(_.toColumnName).asJava)
      this
    }
    
    def sort(columns: TypeSafeColumn*): TableQueryBuilder = {
      currentTable = currentTable.sort(columns.map(_.name).toArray*)
      this
    }
    
    def sortDescending(columns: TypeSafeColumn*): TableQueryBuilder = {
      currentTable = currentTable.sortDescending(columns.map(_.name).toArray*)
      this
    }
    
    def head(size: Long): TableQueryBuilder = {
      currentTable = currentTable.head(size)
      this
    }
    
    def tail(size: Long): TableQueryBuilder = {
      currentTable = currentTable.tail(size)
      this
    }
    
    def build(): Table = currentTable
  }
}