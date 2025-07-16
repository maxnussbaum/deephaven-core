package io.deephaven.client.scala

import io.deephaven.api.expression.{Function, Method}
import io.deephaven.api.filter.*
import io.deephaven.api.filter.{ExtractAnds, FilterPattern}
import io.deephaven.api.filter.FilterPattern.Mode
import io.deephaven.api.literal.Literal
import io.deephaven.api.{ColumnName, RawString, Strings}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import TypeSafeTableOperations._

import java.util.ArrayList
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._

/**
 * Scala equivalent of FilterTest.java using the type-safe DSL.
 * 
 * This test demonstrates how filter operations can be expressed using
 * our Scala DSL with better type safety and more natural syntax.
 */
class FilterTestScala extends AnyFunSuite with Matchers {

  val FOO = col("Foo")
  val BAR = col("Bar")
  val BAZ = col("Baz")
  val L42 = Literal.of(42L)

  private def filterCount(): Int = {
    val methods = classOf[Filter.Visitor[?]].getMethods
    methods.count(method => 
      method.getName == "visit" && 
      method.getParameterCount == 1
    )
  }

  test("visitAll") {
    val visitor = new CountingVisitor()
    visitAll(visitor)
    visitor.count shouldEqual filterCount()
  }

  test("filterIsNull") {
    stringsOf(FOO.isNull, "isNull(Foo)")
    stringsOf(!(FOO.isNull), "!isNull(Foo)")
  }

  test("filterIsNotNull") {
    stringsOf(FOO.isNotNull, "!isNull(Foo)")
    stringsOf(!(FOO.isNotNull), "isNull(Foo)")
  }

  test("filterNot") {
    stringsOf(!(FOO.isNull), "!isNull(Foo)")
  }

  test("filterAnd") {
    stringsOf(FOO.isNotNull && BAR.isNotNull, "!isNull(Foo) && !isNull(Bar)")
    stringsOf(!(FOO.isNotNull && BAR.isNotNull), "isNull(Foo) || isNull(Bar)")
  }

  test("filterOr") {
    stringsOf(FOO.isNull || (FOO > BAR), "isNull(Foo) || (Foo > Bar)")
    stringsOf(!(FOO.isNull || (FOO > BAR)), "!isNull(Foo) && (Foo <= Bar)")
  }

  test("filterOfTrue") {
    stringsOf(TRUE, "true")
    stringsOf(!TRUE, "false")
  }

  test("filterOfFalse") {
    stringsOf(FALSE, "false")
    stringsOf(!FALSE, "true")
  }

  test("filterIsTrue") {
    stringsOf(Filter.isTrue(FOO.toExpression), "Foo == true")
    stringsOf(Filter.not(Filter.isTrue(FOO.toExpression)), "Foo != true")
  }

  test("filterIsFalse") {
    stringsOf(Filter.isFalse(FOO.toExpression), "Foo == false")
    stringsOf(Filter.not(Filter.isFalse(FOO.toExpression)), "Foo != false")
  }

  test("filterEqPrecedence") {
    val leftSide = or(TypeSafeFilter(Filter.isTrue(FOO.toExpression)), FOO === BAR)
    val rightSide = and(TypeSafeFilter(Filter.isTrue(FOO.toExpression)), FOO !== BAR)
    val eqFilter = FilterComparison.eq(leftSide.toFilter, rightSide.toFilter)
    
    stringsOf(TypeSafeFilter(eqFilter), "((Foo == true) || (Foo == Bar)) == ((Foo == true) && (Foo != Bar))")
    stringsOf(TypeSafeFilter(Filter.not(eqFilter)), "((Foo == true) || (Foo == Bar)) != ((Foo == true) && (Foo != Bar))")
  }

  test("filterFunction") {
    stringsOf(TypeSafeFilter(Function.of("MyFunction1")), "MyFunction1()")
    stringsOf(TypeSafeFilter(Function.of("MyFunction2", FOO.toExpression)), "MyFunction2(Foo)")
    stringsOf(TypeSafeFilter(Function.of("MyFunction3", FOO.toExpression, BAR.toExpression)), "MyFunction3(Foo, Bar)")

    stringsOf(TypeSafeFilter(Filter.not(Function.of("MyFunction1"))), "!MyFunction1()")
    stringsOf(TypeSafeFilter(Filter.not(Function.of("MyFunction2", FOO.toExpression))), "!MyFunction2(Foo)")
    stringsOf(TypeSafeFilter(Filter.not(Function.of("MyFunction3", FOO.toExpression, BAR.toExpression))), "!MyFunction3(Foo, Bar)")
  }

  test("filterMethod") {
    stringsOf(TypeSafeFilter(Method.of(FOO.toExpression, "MyFunction1")), "Foo.MyFunction1()")
    stringsOf(TypeSafeFilter(Method.of(FOO.toExpression, "MyFunction2", BAR.toExpression)), "Foo.MyFunction2(Bar)")
    stringsOf(TypeSafeFilter(Method.of(FOO.toExpression, "MyFunction3", BAR.toExpression, BAZ.toExpression)), "Foo.MyFunction3(Bar, Baz)")

    stringsOf(TypeSafeFilter(Filter.not(Method.of(FOO.toExpression, "MyFunction1"))), "!Foo.MyFunction1()")
    stringsOf(TypeSafeFilter(Filter.not(Method.of(FOO.toExpression, "MyFunction2", BAR.toExpression))), "!Foo.MyFunction2(Bar)")
    stringsOf(TypeSafeFilter(Filter.not(Method.of(FOO.toExpression, "MyFunction3", BAR.toExpression, BAZ.toExpression))), "!Foo.MyFunction3(Bar, Baz)")
  }

  test("filterPattern") {
    val pattern = Pattern.compile("myregex")
    stringsOf(TypeSafeFilter(FilterPattern.of(FOO.toExpression, pattern, Mode.FIND, false)),
      "FilterPattern(ColumnName(Foo), myregex, 0, FIND, false)")
    stringsOf(TypeSafeFilter(FilterPattern.of(FOO.toExpression, pattern, Mode.FIND, true)),
      "FilterPattern(ColumnName(Foo), myregex, 0, FIND, true)")

    stringsOf(TypeSafeFilter(Filter.not(FilterPattern.of(FOO.toExpression, pattern, Mode.FIND, false))),
      "!FilterPattern(ColumnName(Foo), myregex, 0, FIND, false)")
    stringsOf(TypeSafeFilter(Filter.not(FilterPattern.of(FOO.toExpression, pattern, Mode.FIND, true))),
      "!FilterPattern(ColumnName(Foo), myregex, 0, FIND, true)")
  }

  test("filterSerial") {
    stringsOf(TypeSafeFilter(Function.of("MySerialFunction").withSerial()), "invokeSerially(MySerialFunction())")
  }

  test("filterIn") {
    val filterIn = FilterIn.of(FOO.toExpression, Literal.of(40), Literal.of(42))
    stringsOf(TypeSafeFilter(filterIn),
      "FilterIn{expression=ColumnName(Foo), values=[LiteralInt{value=40}, LiteralInt{value=42}]}")
    stringsOf(TypeSafeFilter(Filter.not(filterIn)),
      "!FilterIn{expression=ColumnName(Foo), values=[LiteralInt{value=40}, LiteralInt{value=42}]}")
  }

  test("filterRawString") {
    stringsOf(TypeSafeFilter(RawString.of("this is a raw string")), "this is a raw string")
    stringsOf(TypeSafeFilter(Filter.not(RawString.of("this is a raw string"))), "!(this is a raw string)")
  }

  test("examplesStringsOf") {
    for (filter <- Examples.of()) {
      Strings.of(filter) // Should not throw
    }
  }

  // TODO: ExtractAnds is package-private, need to move test to io.deephaven.api.filter package
  // test("extractAnds") {
  //   for (filter <- Examples.of()) {
  //     val results = ExtractAnds.of(filter).asScala.toList
  //     filter match {
  //       case filterAnd: FilterAnd =>
  //         results should contain theSameElementsAs filterAnd.filters().asScala
  //       case f if Filter.ofTrue().equals(f) =>
  //         results shouldBe empty
  //       case _ =>
  //         results shouldEqual List(filter)
  //     }
  //   }
  // }

  // Additional tests using our DSL syntax
  test("dslFilterCombinations") {
    // Test complex DSL combinations
    val complexFilter = (FOO > BAR) && (BAZ.isNotNull || (FOO === 42))
    complexFilter.toFilter shouldBe a[FilterAnd]
    
    val orFilter = (FOO.isNull || BAR.isNotNull) && !(BAZ > 0)
    orFilter.toFilter shouldBe a[FilterAnd]
  }

  test("dslComparisonOperators") {
    (FOO > BAR).toFilter shouldBe a[FilterComparison]
    (FOO >= BAR).toFilter shouldBe a[FilterComparison]
    (FOO < BAR).toFilter shouldBe a[FilterComparison]
    (FOO <= BAR).toFilter shouldBe a[FilterComparison]
    (FOO === BAR).toFilter shouldBe a[FilterComparison]
    (FOO !== BAR).toFilter shouldBe a[FilterComparison]
  }

  test("dslNullChecks") {
    FOO.isNull.toFilter shouldBe a[FilterIsNull]
    FOO.isNotNull.toFilter shouldBe a[FilterNot[?]]
    
    // Function style
    isNull(FOO).toFilter shouldBe a[FilterIsNull]
    isNotNull(FOO).toFilter shouldBe a[FilterNot[?]]
  }

  private def stringsOf(filter: TypeSafeFilter, expected: String): Unit = {
    Strings.of(filter.toFilter) shouldEqual expected
    filter.toFilter.walk(FilterSpecificString) shouldEqual expected
  }

  private def stringsOf(filter: Filter, expected: String): Unit = {
    Strings.of(filter) shouldEqual expected
    filter.walk(FilterSpecificString) shouldEqual expected
  }

  def visitAll(visitor: Filter.Visitor[?]): Unit = {
    visitor.visit(null.asInstanceOf[FilterIsNull])
    visitor.visit(null.asInstanceOf[FilterComparison])
    visitor.visit(null.asInstanceOf[FilterIn])
    visitor.visit(null.asInstanceOf[FilterNot[?]])
    visitor.visit(null.asInstanceOf[FilterOr])
    visitor.visit(null.asInstanceOf[FilterAnd])
    visitor.visit(null.asInstanceOf[FilterPattern])
    visitor.visit(null.asInstanceOf[FilterSerial])
    visitor.visit(null.asInstanceOf[Function])
    visitor.visit(null.asInstanceOf[Method])
    visitor.visit(false)
    visitor.visit(null.asInstanceOf[RawString])
  }

  private object FilterSpecificString extends Filter.Visitor[String] {
    override def visit(isNull: FilterIsNull): String = Strings.of(isNull)
    override def visit(comparison: FilterComparison): String = Strings.of(comparison)
    override def visit(in: FilterIn): String = Strings.of(in)
    override def visit(not: FilterNot[?]): String = Strings.of(not)
    override def visit(ors: FilterOr): String = Strings.of(ors)
    override def visit(ands: FilterAnd): String = Strings.of(ands)
    override def visit(pattern: FilterPattern): String = Strings.of(pattern)
    override def visit(serial: FilterSerial): String = Strings.of(serial)
    override def visit(function: Function): String = Strings.of(function)
    override def visit(method: Method): String = Strings.of(method)
    override def visit(literal: Boolean): String = Strings.of(literal)
    override def visit(rawString: RawString): String = Strings.of(rawString)
  }

  private class CountingVisitor extends Filter.Visitor[CountingVisitor] {
    var count = 0

    override def visit(isNull: FilterIsNull): CountingVisitor = { count += 1; this }
    override def visit(comparison: FilterComparison): CountingVisitor = { count += 1; this }
    override def visit(serial: FilterSerial): CountingVisitor = { count += 1; null }
    override def visit(in: FilterIn): CountingVisitor = { count += 1; this }
    override def visit(not: FilterNot[?]): CountingVisitor = { count += 1; this }
    override def visit(ors: FilterOr): CountingVisitor = { count += 1; this }
    override def visit(ands: FilterAnd): CountingVisitor = { count += 1; this }
    override def visit(pattern: FilterPattern): CountingVisitor = { count += 1; this }
    override def visit(function: Function): CountingVisitor = { count += 1; this }
    override def visit(method: Method): CountingVisitor = { count += 1; this }
    override def visit(literal: Boolean): CountingVisitor = { count += 1; this }
    override def visit(rawString: RawString): CountingVisitor = { count += 1; this }
  }

  object Examples extends Filter.Visitor[Unit] {
    def of(): List[Filter] = {
      val visitor = Examples
      visitAll(visitor)
      val baseFilters = visitor.out.asScala.toList
      
      // Add negated versions as in the Java test
      val withNegations = baseFilters.flatMap { filter =>
        List(filter, Filter.not(filter), Filter.not(Filter.not(filter)))
      }
      
      visitor.out.asScala.toList
    }

    private val out = new ArrayList[Filter]()

    override def visit(isNull: FilterIsNull): Unit = {
      out.add(Filter.isNull(FOO.toExpression))
    }

    override def visit(comparison: FilterComparison): Unit = {
      out.add(FilterComparison.eq(FOO.toExpression, BAR.toExpression))
      out.add(FilterComparison.gt(FOO.toExpression, BAR.toExpression))
      out.add(FilterComparison.geq(FOO.toExpression, BAR.toExpression))
      out.add(FilterComparison.lt(FOO.toExpression, BAR.toExpression))
      out.add(FilterComparison.leq(FOO.toExpression, BAR.toExpression))
      out.add(FilterComparison.neq(FOO.toExpression, BAR.toExpression))
    }

    override def visit(in: FilterIn): Unit = {
      out.add(FilterIn.of(FOO.toExpression, Literal.of(40), Literal.of(42)))
    }

    override def visit(not: FilterNot[?]): Unit = {
      // All filter nots will be handled in of()
    }

    override def visit(ors: FilterOr): Unit = {
      out.add(FilterOr.of(Filter.isTrue(FOO.toExpression), Filter.isTrue(BAR.toExpression)))
    }

    override def visit(ands: FilterAnd): Unit = {
      out.add(FilterAnd.of(Filter.isTrue(FOO.toExpression), Filter.isTrue(BAR.toExpression)))
    }

    override def visit(pattern: FilterPattern): Unit = {
      out.add(FilterPattern.of(FOO.toExpression, Pattern.compile("somepattern"), Mode.FIND, false))
      out.add(FilterPattern.of(FOO.toExpression, Pattern.compile("somepattern"), Mode.FIND, true))
    }

    override def visit(function: Function): Unit = {
      out.add(Function.of("my_function", FOO.toExpression))
    }

    override def visit(method: Method): Unit = {
      out.add(Method.of(FOO.toExpression, "whats", BAR.toExpression))
    }

    override def visit(literal: Boolean): Unit = {
      out.add(Filter.ofFalse())
      out.add(Filter.ofTrue())
    }

    override def visit(serial: FilterSerial): Unit = {
      out.add(Function.of("my_serial_function", FOO.toExpression).withSerial())
    }

    override def visit(rawString: RawString): Unit = {
      out.add(RawString.of("Foo > Bar"))
      out.add(RawString.of("Foo >= Bar + 42"))
      out.add(RawString.of("aoeuaoeu"))
    }
  }
}