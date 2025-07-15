package io.deephaven.client.scala

import io.deephaven.api.expression.{Expression, Function, Method}
import io.deephaven.api.filter.Filter
import io.deephaven.api.literal.Literal
import io.deephaven.api.{ColumnName, RawString, Strings}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import TypeSafeTableOperations._

import java.util.ArrayList
import scala.jdk.CollectionConverters._

/**
 * Scala equivalent of ExpressionTest.java using the type-safe DSL.
 * 
 * This test demonstrates how the same expression concepts from Java
 * can be expressed using our Scala DSL with better type safety and conciseness.
 */
class ExpressionTestScala extends AnyFunSuite with Matchers {

  val FOO = "Foo"
  val BAR = "Bar"
  val BAZ = "Baz"

  private def expressionCount(): Int = {
    val methods = classOf[Expression.Visitor[_]].getMethods
    methods.count(method => 
      method.getName == "visit" && 
      method.getParameterCount == 1 &&
      classOf[Expression].isAssignableFrom(method.getParameterTypes()(0))
    )
  }

  test("visitAll") {
    val visitor = new CountingVisitor()
    visitAll(visitor)
    visitor.count shouldEqual expressionCount()
  }

  test("columnName") {
    stringsOf(FOO.toExpression, "Foo")
  }

  test("filter") {
    val complexFilter = or(
      FOO > BAR,
      FOO > BAZ,
      and(FOO.isNull, BAR.isNotNull, BAZ.isNotNull)
    )
    stringsOf(complexFilter.toExpression, "(Foo > Bar) || (Foo > Baz) || (isNull(Foo) && !isNull(Bar) && !isNull(Baz))")
  }

  test("expressionFunction") {
    stringsOf(Function.of("plus", FOO.toExpression, BAR.toExpression), "plus(Foo, Bar)")
    stringsOf(Function.of("plus", FOO.toExpression, Function.of("minus", BAR.toExpression, BAZ.toExpression)), "plus(Foo, minus(Bar, Baz))")
  }

  test("expressionFunctionThatTakesFilters") {
    val complexExpression = Function.of("some_func", 
      (FOO > BAR).toFilter, 
      BAZ.toExpression, 
      TRUE.toFilter, 
      FALSE.toFilter,
      and(FOO.isNull, BAR.isNotNull, or(FOO === BAR, FOO !== BAZ)).toFilter
    )
    stringsOf(complexExpression, "some_func(Foo > Bar, Baz, true, false, isNull(Foo) && !isNull(Bar) && ((Foo == Bar) || (Foo != Baz)))")
  }

  test("expressionMethod") {
    stringsOf(FOO.method("myMethod", BAR).toExpression, "Foo.myMethod(Bar)")
  }

  test("literals") {
    stringsOf(Literal.of(true), "true")
    stringsOf(Literal.of(false), "false")
    stringsOf(Literal.of(42), "(int)42")
    stringsOf(Literal.of(42L), "42L")
    stringsOf(Literal.of("foo bar"), "\"foo bar\"")
    stringsOf(Literal.of("\"foo bar\""), "\"\\\"foo bar\\\"\"")
  }

  test("rawString") {
    stringsOf(raw("Foo + Bar - 42").toExpression, "Foo + Bar - 42")
  }

  test("examplesStringsOf") {
    for (expression <- Examples.of()) {
      Strings.of(expression) // Should not throw
    }
  }

  private def stringsOf(expression: Expression, expected: String): Unit = {
    Strings.of(expression) shouldEqual expected
    expression.walk(SpecificMethod) shouldEqual expected
  }

  private object SpecificMethod extends Expression.Visitor[String] {
    override def visit(literal: Literal): String = Strings.of(literal)
    override def visit(columnName: ColumnName): String = Strings.of(columnName)
    override def visit(filter: Filter): String = Strings.of(filter)
    override def visit(function: Function): String = Strings.of(function)
    override def visit(method: Method): String = Strings.of(method)
    override def visit(rawString: RawString): String = Strings.of(rawString)
  }

  /**
   * Calls every single visit method of visitor with a null object.
   */
  def visitAll(visitor: Expression.Visitor[_]): Unit = {
    visitor.visit(null.asInstanceOf[Literal])
    visitor.visit(null.asInstanceOf[ColumnName])
    visitor.visit(null.asInstanceOf[Filter])
    visitor.visit(null.asInstanceOf[Function])
    visitor.visit(null.asInstanceOf[Method])
    visitor.visit(null.asInstanceOf[RawString])
  }

  private class CountingVisitor extends Expression.Visitor[CountingVisitor] {
    var count = 0

    override def visit(literal: Literal): CountingVisitor = {
      count += 1
      this
    }

    override def visit(columnName: ColumnName): CountingVisitor = {
      count += 1
      this
    }

    override def visit(filter: Filter): CountingVisitor = {
      count += 1
      this
    }

    override def visit(function: Function): CountingVisitor = {
      count += 1
      this
    }

    override def visit(method: Method): CountingVisitor = {
      count += 1
      this
    }

    override def visit(rawString: RawString): CountingVisitor = {
      count += 1
      this
    }
  }

  object Examples extends Expression.Visitor[Unit] {
    def of(): List[Expression] = {
      val visitor = Examples
      visitAll(visitor)
      visitor.out.toList
    }

    private val out = new ArrayList[Expression]()

    override def visit(literal: Literal): Unit = {
      out.addAll(LiteralExamples.of().asJava)
    }

    override def visit(columnName: ColumnName): Unit = {
      out.add(FOO.toColumnName)
      out.add(BAR.toColumnName)
      out.add(BAZ.toColumnName)
    }

    override def visit(filter: Filter): Unit = {
      out.addAll(FilterExamples.of().asJava)
    }

    override def visit(function: Function): Unit = {
      out.add(Function.of("my_function", FOO.toExpression))
    }

    override def visit(method: Method): Unit = {
      out.add(Method.of(FOO.toExpression, "whats", BAR.toExpression))
    }

    override def visit(rawString: RawString): Unit = {
      out.add(RawString.of("Foo + Bar"))
      out.add(RawString.of("Foo > Bar + 42"))
      out.add(RawString.of("!Foo - what_isTHIS(Bar)"))
      out.add(RawString.of("blerg9"))
    }
  }

  // Helper objects to generate example expressions
  object LiteralExamples {
    def of(): List[Expression] = List(
      Literal.of(true),
      Literal.of(false),
      Literal.of(42),
      Literal.of(42L),
      Literal.of(3.14),
      Literal.of("test string")
    )
  }

  object FilterExamples {
    def of(): List[Expression] = List(
      // Basic filters using our DSL
      FOO.isNull.toFilter,
      FOO.isNotNull.toFilter,
      (FOO > BAR).toFilter,
      (FOO >= BAR).toFilter,
      (FOO < BAR).toFilter,
      (FOO <= BAR).toFilter,
      (FOO === BAR).toFilter,
      (FOO !== BAR).toFilter,
      
      // Logical combinations
      (FOO.isNull && BAR.isNotNull).toFilter,
      (FOO > BAR || FOO > BAZ).toFilter,
      (!(FOO.isNull)).toFilter,
      
      // Complex expressions
      or(FOO > BAR, FOO > BAZ, and(FOO.isNull, BAR.isNotNull, BAZ.isNotNull)).toFilter,
      
      // Function and method filters
      Function.of("some_function", FOO.toExpression),
      Method.of(FOO.toExpression, "someMethod", BAR.toExpression),
      
      // Raw string filters
      RawString.of("Foo > Bar"),
      RawString.of("complex_condition(Foo, Bar)")
    )
  }
}