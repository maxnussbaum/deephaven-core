# Type-Safe Table Operations DSL for Deephaven

This Scala DSL provides a type-safe, compile-time checked alternative to Deephaven's string-based table operations. It replaces error-prone string expressions with high-level, composable operations that offer better IDE support and catch errors at compile time.

## Overview

### Before (String-based)
```scala
table.where("(Foo > Bar) || (Foo > Baz) || (isNull(Foo) && !isNull(Bar) && !isNull(Baz))")
table.update("MedianAverageDailyVolume = median(AverageDailyVolume)")
table.view("Column1, Column2, NewColumn = Column1 + Column2")
```

### After (Type-safe)
```scala
import TypeSafeTableOperations._

table.where(("Foo" > "Bar") || ("Foo" > "Baz") || ("Foo".isNull && "Bar".isNotNull && "Baz".isNotNull))
table.update("MedianAverageDailyVolume" := median("AverageDailyVolume"))
table.view("Column1", "Column2", "NewColumn" := "Column1" + "Column2")
```

## Key Features

- **Compile-time safety**: Catch expression errors at compile time
- **IDE support**: Full autocomplete and refactoring support
- **Multiple syntax styles**: Support for both string implicit conversion (preferred) and explicit `col()` function calls
- **Implicit conversions**: Seamless integration with literals and existing code
- **Comprehensive operators**: Full support for arithmetic, logical, comparison, and function operations
- **Fluent API**: Optional query builder for complex operations

## Getting Started

### Import the DSL
```scala
import io.deephaven.client.scala.TypeSafeTableOperations._
```

### Column References

#### String-based (recommended)
```scala
val column: TypeSafeColumn = "ColumnName"
// Or directly in expressions:
"ColumnName".isNotNull
```

#### Explicit col() function
```scala
val column = col("ColumnName")
```

## Core Operations

### Filtering

#### Basic Comparisons
```scala
table.where("Price" > 100)
table.where("Symbol" === "AAPL")
table.where("Volume" >= 1000000)
```

#### Null Checks
```scala
// Method syntax
table.where("Column".isNotNull)
table.where("Column".isNull)

// Function syntax
table.where(isNotNull("Column"))
table.where(isNull("Column"))
```

#### Logical Operations
```scala
// Using operators
table.where("Price" > 100 && "Volume" > 1000)
table.where("Symbol" === "AAPL" || "Symbol" === "MSFT")
table.where(!("Column".isNull))

// Using explicit combinators
table.where(and("Price" > 100, "Volume" > 1000))
table.where(or("Symbol" === "AAPL", "Symbol" === "MSFT"))
```

#### Complex Expressions
```scala
// Recreating: "(Foo > Bar) || (Foo > Baz) || (isNull(Foo) && !isNull(Bar) && !isNull(Baz))"
table.where(
  or(
    "Foo" > "Bar",
    "Foo" > "Baz",
    and(
      "Foo".isNull,
      "Bar".isNotNull,
      "Baz".isNotNull
    )
  )
)
```

### Column Updates

#### Basic Assignments
```scala
table.update(
  "NewColumn" := "Column1" + "Column2",
  "Ratio" := "Numerator" / "Denominator"
)
```

#### Arithmetic Operations
```scala
table.update(
  "Sum" := "A" + "B",
  "Difference" := "A" - "B",
  "Product" := "A" * "B",
  "Quotient" := "A" / "B"
)
```

#### Function Calls
```scala
table.update(
  "MedianValue" := median("Value"),           // Function syntax
  "MaxValue" := "Value".max(),                // Method syntax
  "UpperName" := upper("Name"),               // String functions
  "AbsValue" := abs("Value")                  // Math functions
)
```

### View Operations

#### Column Selection
```scala
table.view("Column1", "Column2", "Column3")
```

#### With Calculations
```scala
table.view(
  "Symbol",
  "Price",
  "Volume",
  "DollarVolume" := "Price" * "Volume"
)
```

### Aggregation Functions

#### Method Syntax
```scala
table.update(
  col("MedianPrice") := col("Price").median(),
  col("TotalVolume") := col("Volume").sum(),
  col("MaxPrice") := col("Price").max(),
  col("MinPrice") := col("Price").min(),
  col("AvgPrice") := col("Price").mean(),
  col("StdPrice") := col("Price").std(),
  col("VarPrice") := col("Price").var(),
  col("FirstPrice") := col("Price").first(),
  col("LastPrice") := col("Price").last()
)
```

#### Function Syntax
```scala
table.update(
  col("MedianPrice") := median(col("Price")),
  col("TotalVolume") := sum(col("Volume")),
  col("MaxPrice") := max(col("Price")),
  col("MinPrice") := min(col("Price")),
  col("AvgPrice") := mean(col("Price")),
  col("StdPrice") := std(col("Price")),
  col("VarPrice") := variance(col("Price")),
  col("FirstPrice") := first(col("Price")),
  col("LastPrice") := last(col("Price"))
)
```

### Math Functions

```scala
table.update(
  col("AbsValue") := abs(col("Value")),
  col("SqrtValue") := sqrt(col("Value")),
  col("PowerValue") := pow(col("Value"), 2),
  col("LogValue") := log(col("Value")),
  col("ExpValue") := exp(col("Value"))
)
```

### String Functions

```scala
table.update(
  col("UpperName") := upper(col("Name")),
  col("LowerName") := lower(col("Name")),
  col("NameLength") := length(col("Name")),
  col("TrimmedName") := trim(col("Name")),
  col("SubName") := substr(col("Name"), 0, 5)
)
```

### Date/Time Functions

```scala
table.update(
  col("Year") := year(col("Date")),
  col("Month") := month(col("Date")),
  col("Day") := day(col("Date")),
  col("CurrentTime") := now()
)
```

### Conditional Expressions

```scala
table.update(
  col("Status") := when(col("Value") > 100, "High").otherwise("Low"),
  col("SafeRatio") := when(col("Denominator") === 0, 0.0).otherwise(col("Numerator") / col("Denominator"))
)
```

### Method Calls

```scala
table.update(
  col("Result") := col("Column").method("myMethod", col("Arg1"), col("Arg2"))
)
```

## Advanced Features

### Fluent Query Builder

For complex operations, use the fluent query builder:

```scala
import TableOps._

val result = from(table)
  .where(col("Price") > 0 && col("Volume") > 0)
  .update(
    col("DollarVolume") := col("Price") * col("Volume"),
    col("PriceChange") := col("Price") - col("PrevPrice")
  )
  .view(col("Symbol"), col("Price"), col("Volume"), col("DollarVolume"))
  .where(col("DollarVolume") > 1000000)
  .sort(col("DollarVolume"))
  .head(100)
  .build()
```

### Raw Expressions

For cases not covered by the DSL, use raw expressions:

```scala
table.update(
  col("StandardExpression") := col("Column1") + col("Column2"),
  col("ComplexExpression") := raw("someComplexFunction(Column1, Column2, specificParameter)")
)
```

### Literal Values

Literals are automatically converted:

```scala
table.where(
  col("IntColumn") > 100 &&                    // Int literal
  col("DoubleColumn") >= 3.14 &&               // Double literal
  col("StringColumn") === "test" &&            // String literal
  col("BooleanColumn") === true                // Boolean literal
)
```

## Complete Example

Here's a realistic trading analysis example:

```scala
import TypeSafeTableOperations._
import TableOps._

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
    .view(
      col("Symbol"),
      col("Price"),
      col("Volume"),
      col("DollarVolume"),
      col("PriceChange"),
      col("PriceChangePercent")
    )
    .where(col("DollarVolume") > 1000000)  // High-value trades only
    .sort(col("DollarVolume"))
    .tail(50)  // Top 50 by dollar volume
    .build()
}
```

## Error Handling

The DSL provides compile-time error checking, but you can also handle runtime errors:

```scala
table.update(
  col("SafeRatio") := when(col("Denominator") === 0, 0.0).otherwise(col("Numerator") / col("Denominator")),
  col("SafeLog") := when(col("Value") > 0, log(col("Value"))).otherwise(Double.NaN)
)
```

## Performance Considerations

- The DSL creates the same underlying Deephaven API calls as string expressions
- No runtime performance overhead compared to string-based operations
- Compile-time overhead is minimal
- Memory usage is equivalent to hand-written API calls

## Migration Guide

### From String Expressions

1. **Replace string filters with type-safe expressions**:
   ```scala
   // Before
   table.where("Column > 100")
   
   // After
   table.where(col("Column") > 100)
   ```

2. **Replace string updates with assignments**:
   ```scala
   // Before
   table.update("NewColumn = Column1 + Column2")
   
   // After
   table.update(col("NewColumn") := col("Column1") + col("Column2"))
   ```

3. **Replace string views with column references**:
   ```scala
   // Before
   table.view("Column1, Column2, NewColumn = Column1 + Column2")
   
   // After
   table.view(col("Column1"), col("Column2"), col("NewColumn") := col("Column1") + col("Column2"))
   ```

### Gradual Migration

You can migrate incrementally:

```scala
// Mixed approach during migration
table
  .where("OldStringExpression")  // Old style
  .update(col("NewColumn") := col("Column1") + col("Column2"))  // New style
  .where(col("NewColumn") > 100)  // New style
```

## Best Practices

1. **Use string implicit conversion for conciseness**: `"Column"` is preferred over `col("Column")`
2. **Prefer method syntax for single-column operations**: `"Column".sum()` instead of `sum("Column")`
3. **Use explicit combinators for complex logic**: `and()`, `or()` for clarity
4. **Leverage the fluent API** for complex multi-step operations
5. **Use raw expressions sparingly** only when the DSL doesn't cover your use case
6. **Handle potential division by zero** and other runtime errors explicitly

## Supported Operations

### Comparison Operators
- `>`, `>=`, `<`, `<=`, `===`, `!==`

### Arithmetic Operators
- `+`, `-`, `*`, `/`

### Logical Operators
- `&&`, `||`, `!`

### Null Checks
- `.isNull`, `.isNotNull`, `isNull()`, `isNotNull()`

### Aggregation Functions
- `median()`, `mean()`, `sum()`, `count()`, `min()`, `max()`, `std()`, `var()`, `first()`, `last()`

### Math Functions
- `abs()`, `sqrt()`, `pow()`, `log()`, `exp()`

### String Functions
- `upper()`, `lower()`, `length()`, `trim()`, `substr()`

### Date/Time Functions
- `year()`, `month()`, `day()`, `now()`, `date()`

### Conditional Expressions
- `when().otherwise()`

## Implementation Details

The DSL is implemented using:

- **Value classes** for zero-overhead wrappers
- **Implicit conversions** for seamless integration
- **Type-safe builders** for complex expressions
- **Scala's operator overloading** for natural syntax

## Testing

Run the test suite to verify functionality:

```bash
sbt test
```

The tests cover:
- All operator combinations
- Implicit conversions
- Complex expression building
- Integration with Deephaven API
- Error conditions

## Contributing

To extend the DSL:

1. Add new operators to the appropriate classes
2. Create implicit conversions for new types
3. Add comprehensive tests
4. Update documentation and examples

## License

This DSL is part of the Deephaven project and follows the same licensing terms.