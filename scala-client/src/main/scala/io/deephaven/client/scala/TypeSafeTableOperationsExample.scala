package io.deephaven.client.scala

import io.deephaven.engine.table.Table
import TypeSafeTableOperations._
import TableOps._

/**
 * Comprehensive examples demonstrating the type-safe table operations DSL.
 * 
 * This shows how to replace string-based expressions with compile-time checked,
 * type-safe operations that provide better IDE support and error detection.
 */
object TypeSafeTableOperationsExample {
  
  /**
   * Example 1: Basic filtering with type-safe expressions
   * 
   * Before: table.where("MedianAverageDailyVolume != null && TargetEODDollars > 0")
   * After:  table.where("MedianAverageDailyVolume".isNotNull && "TargetEODDollars" > 0)
   */
  def basicFiltering(table: Table): Table = {
    table.where(col("MedianAverageDailyVolume").isNotNull && col("TargetEODDollars") > 0)
  }
  
  /**
   * Example 2: Complex filtering with multiple conditions
   * 
   * Before: table.where("(Foo > Bar) || (Foo > Baz) || (isNull(Foo) && !isNull(Bar) && !isNull(Baz))")
   * After:  Uses both operator precedence and explicit combinators
   */
  def complexFiltering(table: Table): Table = {
    // Using operator precedence (requires careful parentheses)
    table.where((col("Foo") > col("Bar")) || (col("Foo") > col("Baz")) || (col("Foo").isNull && col("Bar").isNotNull && col("Baz").isNotNull))
  }
  
  /**
   * Example 3: Alternative complex filtering with explicit combinators
   */
  def complexFilteringWithCombinators(table: Table): Table = {
    table.where(
      or(
        col("Foo") > col("Bar"),
        col("Foo") > col("Baz"),
        and(
          col("Foo").isNull,
          col("Bar").isNotNull,
          col("Baz").isNotNull
        )
      )
    )
  }
  
  /**
   * Example 4: Column updates with arithmetic operations
   * 
   * Before: table.update("NewColumn = Column1 + Column2", "Ratio = Column1 / Column2")
   * After:  Uses type-safe arithmetic operators
   */
  def columnUpdates(table: Table): Table = {
    table.update(
      col("NewColumn") := col("Column1") + col("Column2"),
      col("Ratio") := col("Column1") / col("Column2")
    )
  }
  
  /**
   * Example 5: Aggregation functions in updates
   * 
   * Before: table.update("MedianVolume = median(AverageDailyVolume)", "TotalValue = sum(Value)")
   * After:  Uses both method and function syntax
   */
  def aggregationUpdates(table: Table): Table = {
    table.update(
      col("MedianVolume") := col("AverageDailyVolume").median(),      // Method syntax
      col("TotalValue") := sum(col("Value")),                         // Function syntax
      col("MaxPrice") := col("Price").max(),                          // Method syntax
      col("MinPrice") := min(col("Price"))                            // Function syntax
    )
  }
  
  /**
   * Example 6: View operations with column selection
   * 
   * Before: table.view("Column1", "Column2", "NewColumn = Column1 + Column2")
   * After:  Type-safe column references and assignments
   */
//  def viewOperations(table: Table): Table = {
//    table.view(
//      col("Column1"),
//      col("Column2"),
//      col("NewColumn") := col("Column1") + col("Column2")
//    )
//  }
  
  /**
   * Example 7: Conditional expressions
   * 
   * Before: table.update("Status = isNull(Value) ? \"Unknown\" : Value > 100 ? \"High\" : \"Low\"")
   * After:  Uses when/otherwise construct
   */
  def conditionalExpressions(table: Table): Table = {
    table.update(
      col("Status") := when(col("Value").isNull, "Unknown")
        .otherwise(
          when(col("Value") > 100, "High").otherwise("Low")
      )
    )
  }
  
  /**
   * Example 8: String and math functions
   * 
   * Before: table.update("UpperName = upper(Name)", "AbsValue = abs(Value)", "SqrtValue = sqrt(Value)")
   * After:  Type-safe function calls
   */
  def functionCalls(table: Table): Table = {
    table.update(
      col("UpperName") := upper(col("Name")),
      col("AbsValue") := abs(col("Value")),
      col("SqrtValue") := sqrt(col("Value")),
      col("PowerValue") := pow(col("Value"), 2)
    )
  }
  
  /**
   * Example 9: Date/time operations
   * 
   * Before: table.update("Year = year(Date)", "CurrentTime = now()")
   * After:  Type-safe date functions
   */
  def dateTimeOperations(table: Table): Table = {
    table.update(
      col("Year") := year(col("Date")),
      col("Month") := month(col("Date")),
      col("Day") := day(col("Date")),
      col("CurrentTime") := now()
    )
  }
  
  /**
   * Example 10: Method calls on columns
   * 
   * Before: table.update("Result = Column.myMethod(Arg1, Arg2)")
   * After:  Type-safe method syntax
   */
  def methodCalls(table: Table): Table = {
    table.update(
      col("Result") := col("Column").method("myMethod", col("Arg1"), col("Arg2"))
    )
  }
  
  /**
   * Example 11: Fluent query builder for complex operations
   * 
   * Demonstrates chaining multiple operations in a readable way
   */
  def fluentQueryBuilder(table: Table): Table = {
    from(table)
      .where(col("MedianAverageDailyVolume").isNotNull && col("TargetEODDollars") > 0)
      .update(
        col("MedianVolume") := col("AverageDailyVolume").median(),
        col("TotalValue") := col("Price") * col("Volume")
      )
//      .view(col("Symbol"), col("MedianVolume"), col("TotalValue"))
      .sort(col("TotalValue"))
      .head(100)
      .build()
  }
  
  /**
   * Example 12: Grouping and sorting operations
   * 
   * Before: table.groupBy("Category").sort("Value")
   * After:  Type-safe column references
   */
  def groupingAndSorting(table: Table): Table = {
    table
      .groupBy(col("Category"), col("Region"))
      .sort(col("Value"))
  }
  
  /**
   * Example 13: Mixed column reference styles
   * 
   * Shows different ways to reference columns - explicit and implicit
   */
  def mixedColumnReferences(table: Table): Table = {
    table.update(
      col("StringBased") := col("Column1") + col("Column2"),                     // String-based (with explicit conversion)
      col("ExplicitBased") := col("Column3") + col("Column4")                    // Explicit col() calls
    )
  }
  
  /**
   * Example 14: Raw expressions for complex cases
   * 
   * For cases where the DSL doesn't cover specific functionality
   */
  def rawExpressions(table: Table): Table = {
    table.update(
      col("SimpleExpression") := col("Column1") + col("Column2"),
      col("ComplexExpression") := raw("someComplexFunction(Column1, Column2, specificParameter)")
    )
  }
  
  /**
   * Example 15: Null handling variations
   * 
   * Shows different ways to handle null values
   */
  def nullHandling(table: Table): Table = {
    table.where(
      and(
        col("Column1").isNotNull,                    // Method syntax
        isNotNull(col("Column2")),                   // Function syntax
        !(col("Column3").isNull)                     // Negation syntax
      )
    )
  }
  
  /**
   * Example 16: Comparison operations with literals
   * 
   * Shows how literals are automatically converted
   */
  def literalComparisons(table: Table): Table = {
    table.where(
      col("IntColumn") > 100 &&                      // Int literal
      col("DoubleColumn") >= 3.14 &&                 // Double literal
      col("StringColumn") === "test" &&              // String literal
      col("BooleanColumn") === true                  // Boolean literal
    )
  }
  
  /**
   * Example 17: Complete trading example
   * 
   * A realistic example showing a complete trading data analysis
   */
  def tradingAnalysis(tickData: Table): Table = {
    from(tickData)
      .where(
        col("Price") > 0 &&
        col("Volume") > 0 &&
        col("Symbol").isNotNull
      )
      .update(
        col("DollarVolume") := col("Price") * col("Volume"),
        col("PriceChange") := col("Price") - col("PrevPrice"),
        col("PriceChangePercent") := (col("Price") - col("PrevPrice")) / col("PrevPrice") * 100
      )
//      .view(
//        col("Symbol"),
//        col("Price"),
//        col("Volume"),
//        col("DollarVolume"),
//        col("PriceChange"),
//        col("PriceChangePercent")
//      )
      .where(col("DollarVolume") > 1000000)  // Filter for high-value trades
      .sort(col("DollarVolume"))
      .tail(50)  // Top 50 by dollar volume
      .build()
  }
  
  /**
   * Example 18: Error handling patterns
   * 
   * Shows how to handle potential errors in expressions
   */
  def errorHandling(table: Table): Table = {
    table.update(
      col("SafeRatio") := when(col("Denominator") === 0, 0.0).otherwise(col("Numerator") / col("Denominator")),
      col("SafeLog") := when(col("Value") > 0, log(col("Value"))).otherwise(Double.NaN)
    )
  }
}