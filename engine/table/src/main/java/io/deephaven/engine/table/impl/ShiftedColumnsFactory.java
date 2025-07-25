//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import com.github.javaparser.ast.ArrayCreationLevel;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.PackageDeclaration;
import com.github.javaparser.ast.body.AnnotationDeclaration;
import com.github.javaparser.ast.body.AnnotationMemberDeclaration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.CompactConstructorDeclaration;
import com.github.javaparser.ast.body.ConstructorDeclaration;
import com.github.javaparser.ast.body.EnumConstantDeclaration;
import com.github.javaparser.ast.body.EnumDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.InitializerDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.body.ReceiverParameter;
import com.github.javaparser.ast.body.RecordDeclaration;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.comments.BlockComment;
import com.github.javaparser.ast.comments.JavadocComment;
import com.github.javaparser.ast.comments.LineComment;
import com.github.javaparser.ast.expr.*;
import com.github.javaparser.ast.modules.ModuleDeclaration;
import com.github.javaparser.ast.modules.ModuleExportsDirective;
import com.github.javaparser.ast.modules.ModuleOpensDirective;
import com.github.javaparser.ast.modules.ModuleProvidesDirective;
import com.github.javaparser.ast.modules.ModuleRequiresDirective;
import com.github.javaparser.ast.modules.ModuleUsesDirective;
import com.github.javaparser.ast.stmt.AssertStmt;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.stmt.BreakStmt;
import com.github.javaparser.ast.stmt.CatchClause;
import com.github.javaparser.ast.stmt.ContinueStmt;
import com.github.javaparser.ast.stmt.DoStmt;
import com.github.javaparser.ast.stmt.EmptyStmt;
import com.github.javaparser.ast.stmt.ExplicitConstructorInvocationStmt;
import com.github.javaparser.ast.stmt.ExpressionStmt;
import com.github.javaparser.ast.stmt.ForEachStmt;
import com.github.javaparser.ast.stmt.ForStmt;
import com.github.javaparser.ast.stmt.IfStmt;
import com.github.javaparser.ast.stmt.LabeledStmt;
import com.github.javaparser.ast.stmt.LocalClassDeclarationStmt;
import com.github.javaparser.ast.stmt.LocalRecordDeclarationStmt;
import com.github.javaparser.ast.stmt.ReturnStmt;
import com.github.javaparser.ast.stmt.SwitchEntry;
import com.github.javaparser.ast.stmt.SwitchStmt;
import com.github.javaparser.ast.stmt.SynchronizedStmt;
import com.github.javaparser.ast.stmt.ThrowStmt;
import com.github.javaparser.ast.stmt.TryStmt;
import com.github.javaparser.ast.stmt.UnparsableStmt;
import com.github.javaparser.ast.stmt.WhileStmt;
import com.github.javaparser.ast.stmt.YieldStmt;
import com.github.javaparser.ast.type.ArrayType;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.github.javaparser.ast.type.IntersectionType;
import com.github.javaparser.ast.type.PrimitiveType;
import com.github.javaparser.ast.type.TypeParameter;
import com.github.javaparser.ast.type.UnionType;
import com.github.javaparser.ast.type.UnknownType;
import com.github.javaparser.ast.type.VarType;
import com.github.javaparser.ast.type.VoidType;
import com.github.javaparser.ast.type.WildcardType;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.Pair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.filter.ExtractShiftedColumnDefinitions;
import io.deephaven.engine.table.impl.filter.TransformToFinalFormula;
import io.deephaven.engine.table.impl.lang.JavaExpressionParser;
import io.deephaven.engine.table.impl.lang.QueryLanguageParser;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.select.ShiftedColumnDefinition;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ShiftedColumnsFactory extends VoidVisitorAdapter<ShiftedColumnsFactory.ShiftedColumnAttributes> {

    public static final String SHIFTED_COL_PREFIX = "_Shifted";

    /**
     * Returns a Map from inner-formula-expression to ShiftedColumnDefinitions that are required to perform the where
     * operation on the source.
     *
     * @param expression expression to analyze for ShiftedColumnDefinitions
     * @return Mappings from inner-formula-expression to ShiftedColumnDefinition
     */
    public static Pair<String, Set<ShiftedColumnDefinition>> getShiftedColumnDefinitions(
            @NotNull Expression expression) {
        final ShiftedColumnAttributes formulaAttributes = new ShiftedColumnAttributes();
        expression.accept(new ShiftedColumnsFactory(), formulaAttributes);
        return formulaAttributes.shifted.isEmpty()
                ? null
                : new Pair(formulaAttributes.formulaBuilder.toString(), formulaAttributes.shifted);
    }

    /**
     * Returns the array access expressions in the formula with appropriate shifted column suffixes.
     *
     * @param formula to convert
     * @return returns the array access expressions in the formula with appropriate shifted column suffixes
     */
    public static String convertToShiftedFormula(@NotNull String formula) {
        // Convert backticks *before* converting single equals!
        // Backticks must be converted first in order to properly identify single-equals signs within
        // String and char literals, which should *not* be converted.
        formula = QueryLanguageParser.convertBackticks(formula);
        formula = QueryLanguageParser.convertSingleEquals(formula);

        Expression expression = JavaExpressionParser.parseExpression(formula);
        ShiftedColumnAttributes formulaAttributes = new ShiftedColumnAttributes();
        final ShiftedColumnsFactory visitor = new ShiftedColumnsFactory();
        expression.accept(visitor, formulaAttributes);
        return formulaAttributes.formulaBuilder.toString();
    }

    /**
     * Performs {@link Table#where(Filter)} operation on interim table built using shifted columns. Returns result table
     * with only the original display columns.
     *
     * @param source input table
     * @param filters filters list; at least one of which has a constant array access expression
     * @return result table of expected where operation
     */
    public static Table where(
            @NotNull Table source,
            @NotNull List<WhereFilter> filters) {
        String nuggetName = "shiftedWhere(" +
                filters.stream().map(Object::toString).collect(Collectors.joining(", ")) +
                ')';

        return QueryPerformanceRecorder.withNugget(nuggetName, source.sizeForInstrumentation(), () -> {
            final WhereFilter aggFilter = ConjunctiveFilter.of(filters.toArray(WhereFilter[]::new));
            final Set<ShiftedColumnDefinition> shifted = ExtractShiftedColumnDefinitions.of(aggFilter);

            final String[] columnsToDrop = shifted.stream()
                    .map(ShiftedColumnDefinition::getResultColumnName)
                    .toArray(String[]::new);

            return ShiftedColumnOperation
                    .addShiftedColumns(source, shifted)
                    .where(TransformToFinalFormula.of(aggFilter))
                    .dropColumns(columnsToDrop);
        });
    }

    /**
     * Returns new table that includes the columns from the source table plus additional column
     * resultColumnName=expression built using ShiftedColumnOperation.
     *
     * @param source the source table to use build the new ShiftedColumns table.
     * @param formulaColumn should be of type FormulaColumn
     * @return new table that includes the columns from the source table plus additional column
     *         "resultColumnName=expression" built using ShiftedColumnOperation.
     */
    public static Table getShiftedColumnsTable(
            @NotNull final Table source,
            @NotNull FormulaColumn formulaColumn,
            @NotNull SelectAndViewAnalyzer.UpdateFlavor updateFlavor) {
        String nuggetName = "getShiftedColumnsTable( " + formulaColumn + ", " + updateFlavor + ") ";
        return QueryPerformanceRecorder.withNugget(nuggetName, source.sizeForInstrumentation(), () -> {
            Table tableSoFar = source;
            final Set<ShiftedColumnDefinition> shiftedColumnDefinitions =
                    formulaColumn.getFormulaShiftedColumnDefinitions();

            final String[] columnsToDrop = shiftedColumnDefinitions.stream()
                    .map(ShiftedColumnDefinition::getResultColumnName)
                    .filter(name -> !name.equals(formulaColumn.getName()))
                    .toArray(String[]::new);

            // invoke SCO#addShiftedColumns with the shifted column definitions
            tableSoFar = ShiftedColumnOperation.addShiftedColumns(tableSoFar, shiftedColumnDefinitions);

            final String resultColFormula = formulaColumn.getName() + " = " + formulaColumn.getShiftedFormulaString();
            switch (updateFlavor) {
                case Select:
                case Update:
                    tableSoFar = tableSoFar.update(resultColFormula);
                    break;
                case View:
                case UpdateView:
                    tableSoFar = tableSoFar.updateView(resultColFormula);
                    break;
                case LazyUpdate:
                    tableSoFar = tableSoFar.lazyUpdate(resultColFormula);
            }
            return tableSoFar.dropColumns(columnsToDrop);
        });
    }

    /**
     * Returns null if the ArrayAccessExpression is NOT of type "i +/- &lt;constant&gt;" or "ii +/- &lt;constant&gt;"
     * Otherwise builds a ShiftedColumnDefinition containing shift and sourceColumn.
     *
     * @param expression is a ArrayAccessExpr
     * @return null or Pair containing shift value and sourceColumn.
     */
    private static ShiftedColumnDefinition parseForConstantArrayAccessAttributes(@NotNull ArrayAccessExpr expression) {
        final List<String> validLeftValues = List.of("i", "ii");
        if (expression.getIndex() instanceof NameExpr) {
            final String name = ((NameExpr) expression.getIndex()).getNameAsString();
            if (validLeftValues.contains(name)) {
                String sourceCol = expression.getName().toString();
                if (sourceCol.endsWith("_")) {
                    sourceCol = sourceCol.substring(0, sourceCol.length() - 1);
                    return new ShiftedColumnDefinition(sourceCol, 0);
                }
            }
            return null;
        }

        if (!(expression.getIndex() instanceof BinaryExpr)) {
            return null;
        }

        BinaryExpr binaryExpr = (BinaryExpr) expression.getIndex();

        boolean isExpectedLeftExpr = false;
        if (binaryExpr.getLeft() instanceof NameExpr) {
            final String leftName = ((NameExpr) binaryExpr.getLeft()).getNameAsString();
            isExpectedLeftExpr = validLeftValues.contains(leftName);
        }

        boolean isExpectedOperator =
                binaryExpr.getOperator() == BinaryExpr.Operator.PLUS ||
                        binaryExpr.getOperator() == BinaryExpr.Operator.MINUS;

        boolean isExpectedRightExpr = binaryExpr.getRight() instanceof IntegerLiteralExpr;

        if (!isExpectedLeftExpr || !isExpectedOperator || !isExpectedRightExpr) {
            return null;
        }

        String sourceCol = expression.getName().toString();
        if (!sourceCol.endsWith("_")) {
            return null;
        }

        sourceCol = sourceCol.substring(0, sourceCol.length() - 1);

        long rightValue = Long.parseLong(((IntegerLiteralExpr) (binaryExpr.getRight())).getValue());
        Long shift = binaryExpr.getOperator() == BinaryExpr.Operator.MINUS ? -rightValue : rightValue;

        return new ShiftedColumnDefinition(sourceCol, shift);
    }

    // GenericVisitor overrides below
    // ----------------------------------------------------------------------------------------------------------------
    @Override
    public void visit(ArrayAccessExpr expr, ShiftedColumnAttributes attributes) {
        ShiftedColumnDefinition shifted = parseForConstantArrayAccessAttributes(expr);
        if (shifted != null) {
            attributes.formulaBuilder.append(shifted.getResultColumnName());
            attributes.shifted.add(shifted);
            return;
        }

        attributes.formulaBuilder.append(expr);
    }

    @Override
    public void visit(EnclosedExpr currExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append("(");
        currExpr.getInner().accept(this, attributes);
        attributes.formulaBuilder.append(")");
    }

    @Override
    public void visit(BinaryExpr currExpr, ShiftedColumnAttributes attributes) {
        currExpr.getLeft().accept(this, attributes);
        attributes.formulaBuilder.append(' ');
        attributes.formulaBuilder.append(QueryLanguageParser.getOperatorSymbol(currExpr.getOperator())).append(' ');
        currExpr.getRight().accept(this, attributes);
    }

    @Override
    public void visit(ArrayCreationExpr arrayCreationExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append(arrayCreationExpr);
    }

    @Override
    public void visit(ArrayInitializerExpr initializerExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append('{');
        if (initializerExpr.getValues() != null) {
            attributes.formulaBuilder.append(' ');
            for (Iterator<Expression> i = initializerExpr.getValues().iterator(); i.hasNext();) {
                Expression expr = i.next();
                expr.accept(this, attributes);
                if (i.hasNext()) {
                    attributes.formulaBuilder.append(", ");
                }
            }
            attributes.formulaBuilder.append(' ');
        }
        attributes.formulaBuilder.append('}');
    }

    @Override
    public void visit(FieldAccessExpr fieldAccessExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append(fieldAccessExpr);
    }

    @Override
    public void visit(StringLiteralExpr literalExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append('"');
        attributes.formulaBuilder.append(literalExpr.getValue());
        attributes.formulaBuilder.append('"');
    }

    @Override
    public void visit(IntegerLiteralExpr literalExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append(literalExpr.getValue());
    }

    @Override
    public void visit(LongLiteralExpr literalExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append(literalExpr.getValue());
    }

    @Override
    public void visit(CharLiteralExpr literalExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append('\'');
        attributes.formulaBuilder.append(literalExpr.getValue());
        attributes.formulaBuilder.append('\'');
    }

    @Override
    public void visit(DoubleLiteralExpr literalExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append(literalExpr.getValue());
    }

    @Override
    public void visit(BooleanLiteralExpr literalExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append(literalExpr.getValue());
    }

    @Override
    public void visit(NullLiteralExpr literalExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append("null");
    }

    @Override
    public void visit(MethodCallExpr methodCallExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append(methodCallExpr);
    }

    @Override
    public void visit(NameExpr nameExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append(nameExpr);
    }

    @Override
    public void visit(ObjectCreationExpr objectCreationExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append(objectCreationExpr);
    }

    @Override
    public void visit(UnaryExpr unaryExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append(unaryExpr);
    }

    @Override
    public void visit(CastExpr castExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append(castExpr);
    }

    @Override
    public void visit(ClassExpr classExpr, ShiftedColumnAttributes attributes) {
        attributes.formulaBuilder.append(classExpr);
    }

    @Override
    public void visit(ConditionalExpr conditionalExpr, ShiftedColumnAttributes attributes) {
        conditionalExpr.getCondition().accept(this, attributes);
        attributes.formulaBuilder.append(" ? ");
        conditionalExpr.getThenExpr().accept(this, attributes);
        attributes.formulaBuilder.append(" : ");
        conditionalExpr.getElseExpr().accept(this, attributes);
    }

    @Override
    public void visit(AssignExpr assignExpr, ShiftedColumnAttributes attributes) {
        assignExpr.getTarget().accept(this, attributes);
        attributes.formulaBuilder
                .append(' ')
                .append(assignExpr.getOperator().asString())
                .append(' ');
        assignExpr.getValue().accept(this, attributes);
    }

    // ---------- UNSUPPORTED: ----------------------------------------------------------------------------------------

    @Override
    public void visit(AnnotationDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("AnnotationDeclaration Operation not supported");
    }

    @Override
    public void visit(AnnotationMemberDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("AnnotationMemberDeclaration Operation not supported");
    }

    @Override
    public void visit(AssertStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("AssertStmt Operation not supported");
    }

    @Override
    public void visit(BlockComment n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("BlockComment Operation not supported");
    }

    @Override
    public void visit(BlockStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("BlockStmt Operation not supported");
    }

    @Override
    public void visit(BreakStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("BreakStmt Operation not supported");
    }

    @Override
    public void visit(CatchClause n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("CatchClause Operation not supported");
    }

    @Override
    public void visit(ClassOrInterfaceDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ClassOrInterfaceDeclaration Operation not supported");
    }

    @Override
    public void visit(CompilationUnit n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("CompilationUnit Operation not supported");
    }

    @Override
    public void visit(ConstructorDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ConstructorDeclaration Operation not supported");
    }

    @Override
    public void visit(ContinueStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ContinueStmt Operation not supported");
    }

    @Override
    public void visit(DoStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("DoStmt Operation not supported");
    }

    @Override
    public void visit(EmptyStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("EmptyStmt Operation not supported");
    }

    @Override
    public void visit(EnumConstantDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("EnumConstantDeclaration Operation not supported");
    }

    @Override
    public void visit(EnumDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("EnumDeclaration Operation not supported");
    }

    @Override
    public void visit(ExplicitConstructorInvocationStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ExplicitConstructorInvocationStmt Operation not supported");
    }

    @Override
    public void visit(FieldDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("FieldDeclaration Operation not supported");
    }

    @Override
    public void visit(IfStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("IfStmt Operation not supported");
    }

    @Override
    public void visit(ImportDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ImportDeclaration Operation not supported");
    }

    @Override
    public void visit(InitializerDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("InitializerDeclaration Operation not supported");
    }

    @Override
    public void visit(InstanceOfExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("InstanceOfExpr Operation not supported");
    }

    @Override
    public void visit(JavadocComment n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("JavadocComment Operation not supported");
    }

    @Override
    public void visit(LabeledStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("LabeledStmt Operation not supported");
    }

    @Override
    public void visit(LambdaExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("LambdaExpr Operation not supported!");
    }

    @Override
    public void visit(LineComment n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("LineComment Operation not supported");
    }

    @Override
    public void visit(MarkerAnnotationExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("MarkerAnnotationExpr Operation not supported");
    }

    @Override
    public void visit(MemberValuePair n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("MemberValuePair Operation not supported");
    }

    @Override
    public void visit(MethodDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("MethodDeclaration Operation not supported");
    }

    @Override
    public void visit(MethodReferenceExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("MethodReferenceExpr Operation not supported");
    }

    @Override
    public void visit(PackageDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("PackageDeclaration Operation not supported");
    }

    @Override
    public void visit(Parameter n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("Parameter Operation not supported");
    }

    @Override
    public void visit(ReturnStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ReturnStmt Operation not supported");
    }

    @Override
    public void visit(SingleMemberAnnotationExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("SingleMemberAnnotationExpr Operation not supported");
    }

    @Override
    public void visit(SuperExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("SuperExpr Operation not supported");
    }

    @Override
    public void visit(SwitchStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("SwitchStmt Operation not supported");
    }

    @Override
    public void visit(SynchronizedStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("SynchronizedStmt Operation not supported");
    }

    @Override
    public void visit(ThisExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ThisExpr Operation not supported");
    }

    @Override
    public void visit(ThrowStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ThrowStmt Operation not supported");
    }

    @Override
    public void visit(TryStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("TryStmt Operation not supported");
    }

    @Override
    public void visit(TypeExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("TypeExpr Operation not supported");
    }

    @Override
    public void visit(TypeParameter n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("TypeParameter Operation not supported");
    }

    @Override
    public void visit(VariableDeclarationExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("VariableDeclarationExpr Operation not supported");
    }

    @Override
    public void visit(VariableDeclarator n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("VariableDeclarator Operation not supported");
    }

    @Override
    public void visit(VoidType n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("VoidType Operation not supported");
    }

    @Override
    public void visit(WhileStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("WhileStmt Operation not supported");
    }

    @Override
    public void visit(WildcardType n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("WildcardType Operation not supported");
    }

    @Override
    public void visit(ClassOrInterfaceType n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ClassOrInterfaceType Operation not supported");
    }

    @Override
    public void visit(ExpressionStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ExpressionStmt Operation not supported");
    }

    @Override
    public void visit(ForEachStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ForEachStmt Operation not supported");
    }

    @Override
    public void visit(ForStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ForStmt Operation not supported");
    }

    @Override
    public void visit(NormalAnnotationExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("NormalAnnotationExpr Operation not supported");
    }

    @Override
    public void visit(PrimitiveType n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("PrimitiveType Operation not supported");
    }

    @Override
    public void visit(Name n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("Name Operation not supported");
    }

    @Override
    public void visit(SimpleName n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("SimpleName Operation not supported");
    }

    @Override
    public void visit(ArrayType n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ArrayType Operation not supported");
    }

    @Override
    public void visit(ArrayCreationLevel n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ArrayCreationLevel Operation not supported");
    }

    @Override
    public void visit(IntersectionType n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("IntersectionType Operation not supported");
    }

    @Override
    public void visit(UnionType n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("UnionType Operation not supported");
    }

    @Override
    public void visit(SwitchEntry n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("SwitchEntry Operation not supported");
    }

    @Override
    public void visit(LocalClassDeclarationStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("LocalClassDeclarationStmt Operation not supported");
    }

    @Override
    public void visit(LocalRecordDeclarationStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("LocalRecordDeclarationStmt Operation not supported");
    }

    @Override
    public void visit(UnknownType n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("UnknownType Operation not supported");
    }

    @Override
    public void visit(NodeList n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("NodeList Operation not supported");
    }

    @Override
    public void visit(ModuleDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ModuleDeclaration Operation not supported");
    }

    @Override
    public void visit(ModuleRequiresDirective n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ModuleRequiresDirective Operation not supported");
    }

    @Override
    public void visit(ModuleExportsDirective n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ModuleExportsDirective Operation not supported");
    }

    @Override
    public void visit(ModuleProvidesDirective n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ModuleProvidesDirective Operation not supported");
    }

    @Override
    public void visit(ModuleUsesDirective n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ModuleUsesDirective Operation not supported");
    }

    @Override
    public void visit(ModuleOpensDirective n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ModuleOpensDirective Operation not supported");
    }

    @Override
    public void visit(UnparsableStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("UnparsableStmt Operation not supported");
    }

    @Override
    public void visit(ReceiverParameter n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("ReceiverParameter Operation not supported");
    }

    @Override
    public void visit(VarType n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("VarType Operation not supported");
    }

    @Override
    public void visit(Modifier n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("Modifier Operation not supported");
    }

    @Override
    public void visit(SwitchExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("SwitchExpr Operation not supported");
    }

    @Override
    public void visit(TextBlockLiteralExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("TextBlockLiteralExpr Operation not supported");
    }

    @Override
    public void visit(YieldStmt n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("YieldStmt Operation not supported");
    }

    @Override
    public void visit(TypePatternExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("TypePatternExpr Operation not supported");
    }

    @Override
    public void visit(RecordPatternExpr n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("RecordPatternExpr Operation not supported");
    }

    @Override
    public void visit(RecordDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("RecordDeclaration Operation not supported");
    }

    @Override
    public void visit(CompactConstructorDeclaration n, ShiftedColumnAttributes arg) {
        throw new UnsupportedOperationException("CompactConstructorDeclaration Operation not supported");
    }

    // ----------------------------------------------------------------------------------------------------------------

    public static class ShiftedColumnAttributes {
        final MutableInt index;
        final StringBuilder formulaBuilder;
        final Set<ShiftedColumnDefinition> shifted;

        public ShiftedColumnAttributes() {
            this.index = new MutableInt(1);
            this.formulaBuilder = new StringBuilder();
            this.shifted = new HashSet<>();
        }
    }
}
