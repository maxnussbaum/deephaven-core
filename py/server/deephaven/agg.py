#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
"""This module implement various aggregations that can be used in deephaven table's aggregation operations."""

from typing import List, Union, Any, Optional, Sequence

import jpy

from deephaven import DHError
from deephaven.jcompat import to_sequence
from deephaven.filters import Filter, and_

_JAggregation = jpy.get_type("io.deephaven.api.agg.Aggregation")
_JAggSpec = jpy.get_type("io.deephaven.api.agg.spec.AggSpec")
_JPair = jpy.get_type("io.deephaven.api.Pair")
_JUnionObject = jpy.get_type("io.deephaven.api.object.UnionObject")


class Aggregation:
    """An Aggregation object represents an aggregation operation.

    Note: It should not be instantiated directly by user code but rather through the static methods on the class.
    """

    def __init__(self, j_agg_spec: jpy.JType = None, j_aggregation: jpy.JType = None,
                 cols: Union[str, List[str]] = None):
        self._j_agg_spec = j_agg_spec
        self._j_aggregation = j_aggregation
        self._cols = to_sequence(cols)

    @property
    def j_aggregation(self):
        if self._j_aggregation:
            return self._j_aggregation
        else:
            if not self._cols:
                raise DHError(message="No columns specified for the aggregation operation.")
            return self._j_agg_spec.aggregation(*[_JPair.parse(col) for col in self._cols])

    @property
    def j_agg_spec(self):
        if self._j_aggregation:
            raise DHError(message="unsupported aggregation operation.")
        return self._j_agg_spec

    @property
    def is_formula(self):
        if self._j_agg_spec is not None:
            return isinstance(self._j_agg_spec, jpy.get_type("io.deephaven.api.agg.spec.AggSpecFormula"))
        elif self._j_aggregation is not None:
            return isinstance(self._j_aggregation, jpy.get_type("io.deephaven.api.agg.Formula"))
        else:
            raise DHError(message="Aggregation object is not initialized with a valid aggregation specification or "
                                  "aggregation object.")


def sum_(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Sum aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.sum(), cols=cols)


def abs_sum(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates an Absolute-sum aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.absSum(), cols=cols)


def group(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Group aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.group(), cols=cols)


def avg(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates an Average aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.avg(), cols=cols)


def count_(col: str) -> Aggregation:
    """Creates a Count aggregation. This is not supported in 'Table.agg_all_by'.

    Args:
        col (str): the column to hold the counts of each distinct group

    Returns:
        an aggregation
    """
    if not isinstance(col, str):
        raise DHError(message="count_ aggregation requires a string value for the 'col' argument.")
    return Aggregation(j_aggregation=_JAggregation.AggCount(col))

def count_where(col: str, filters: Union[str, Filter, Sequence[str], Sequence[Filter]]) -> Aggregation:
    """Creates a CountWhere aggregation with the supplied output column name, counting values that pass the supplied
    filters.

    Args:
        col (str): the column to hold the counts of rows that pass the filter condition
        filters (Union[str, Filter, Sequence[str], Sequence[Filter]], optional): the filter condition
            expression(s) or Filter object(s)

    Returns:
        an aggregation
    """
    if not isinstance(col, str):
        raise DHError(message="count_where aggregation requires a string value for the 'col' argument.")
    filters = to_sequence(filters)

    return Aggregation(j_aggregation=_JAggregation.AggCountWhere(col, and_(filters).j_filter))


def partition(col: str, include_by_columns: bool = True) -> Aggregation:
    """Creates a Partition aggregation. This is not supported in 'Table.agg_all_by'.

    Args:
        col (str): the column to hold the sub tables
        include_by_columns (bool): whether to include the group by columns in the result, default is True

    Returns:
        an aggregation
    """
    if not isinstance(col, str):
        raise DHError(message="partition aggregation requires a string value for the 'col' argument.")
    return Aggregation(j_aggregation=_JAggregation.AggPartition(col, include_by_columns))


def count_distinct(cols: Union[str, List[str]] = None, count_nulls: bool = False) -> Aggregation:
    """Creates a Count Distinct aggregation which computes the count of distinct values within an aggregation group for
    each of the given columns.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation
        count_nulls (bool): whether null values should be counted, default is False

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.countDistinct(count_nulls), cols=cols)


def first(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a First aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.first(), cols=cols)


def formula(formula: str, formula_param: Optional[str] = None, cols: Optional[Union[str, List[str]]] = None) -> Aggregation:
    """Creates a user defined formula aggregation. This formula can contain a combination of any of the following:
        |  Built-in functions such as `min`, `max`, etc.
        |  Mathematical arithmetic such as `*`, `+`, `/`, etc.
        |  User-defined functions

    There are two variants of this call. The preferred variant requires the formula to provide the output column name
    and specific input column names in the following format:
        |  formula('output_col=(input_col1 + input_col2) * input_col3')
    This form does not accept `formula_param` or `cols` arguments because the input and output columns are explicitly
    set within the formula string.

    The second (deprecated) variant allows the user to apply a formula expression to one input column, producing one
    output column. In this call the `formula_param` is used as a placeholder for the input column name and the `cols`
    argument is used to identify the output column name and the input source column when applying the formula. If
    multiple input/output pairs are specified in the `cols` argument, the formula will be applied to each column in the
    list.

    Args:
        formula (str): the user defined formula to apply
        formula_param (Optional[str]): If provided, supplies the parameter name for the input column's vector within the
            formula. If formula is `max(each)`, then `each` should be the formula_param. This must be set to None (the
            default when omitted) when the `formula`argument specifies the input and output columns.
        cols (Optional[Union[str, List[str]]]): If provided, supplies the column(s) to aggregate, can be renaming
            expressions, i.e. "new_col = col". This must be set to None (the default when omitted) when the `formula`
            argument specifies the input and output columns.

    Returns:
        an aggregation
    """
    if formula_param:
        return Aggregation(j_agg_spec=_JAggSpec.formula(formula, formula_param), cols=cols)
    if cols:
        raise DHError(message="The 'cols' argument is only valid when 'formula_param' is provided.")
    return Aggregation(j_aggregation=_JAggregation.AggFormula(formula))


def last(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates Last aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.last(), cols=cols)


def min_(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Min aggregation.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.min(), cols=cols)


def max_(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Max aggregation to the ComboAggregation object.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.max(), cols=cols)


def median(cols: Union[str, List[str]] = None, average_evenly_divided: bool = True) -> Aggregation:
    """Creates a Median aggregation which computes the median value within an aggregation group for each of the
    given columns.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation
        average_evenly_divided (bool): when the group size is an even number, whether to average the two middle values
            for the output value. When set to True, average the two middle values. When set to False, use the smaller
            value. The default is True. This flag is only valid for numeric types.

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.median(average_evenly_divided), cols=cols)


def pct(percentile: float, cols: Union[str, List[str]] = None, average_evenly_divided: bool = False) -> Aggregation:
    """Creates a Percentile aggregation which computes the percentile value within an aggregation group for each of
    the given columns.

    Args:
        percentile (float): the percentile used for calculation
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation
        average_evenly_divided (bool): when the percentile splits the group into two halves, whether to average the two
            middle values for the output value. When set to True, average the two middle values. When set to False, use
            the smaller value. The default is False. This flag is only valid for numeric types.

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.percentile(percentile, average_evenly_divided), cols=cols)


def sorted_first(order_by: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a SortedFirst aggregation.

    Args:
        order_by (str): the column to sort by
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.sortedFirst(order_by), cols=cols)


def sorted_last(order_by: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a SortedLast aggregation.

    Args:
        order_by (str): the column to sort by
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.sortedLast(order_by), cols=cols)


def std(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Std (sample standard deviation) aggregation.

    Sample standard deviation is computed using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
    which ensures that the sample variance will be an unbiased estimator of population variance.


    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.std(), cols=cols)


def unique(cols: Union[str, List[str]] = None, include_nulls: bool = False, non_unique_sentinel: Any = None) -> Aggregation:
    """Creates a Unique aggregation which computes the single unique value within an aggregation group for each of
    the given columns. If all values in a column are null, or if there is more than one distinct value in a column, the
    result is null or the specified non_unique_sentinel value.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation
        include_nulls (bool): whether null is treated as a value for the purpose of determining if the values in the
            aggregation group are unique, default is False.
        non_unique_sentinel (Any): the non-null sentinel value when no unique value exists, default is None. Must be
            a non-None value when include_nulls is True.

    Returns:
        an aggregation
    """

    if non_unique_sentinel is None:
        if include_nulls:
            raise DHError(message="when include_nulls is True, a non-null sentinel value must be provided.")
        else:
            return Aggregation(j_agg_spec=_JAggSpec.unique(), cols=cols)
    else:
        non_unique_sentinel = _JUnionObject.of(non_unique_sentinel)
        return Aggregation(j_agg_spec=_JAggSpec.unique(include_nulls, non_unique_sentinel), cols=cols)


def var(cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a sample Var aggregation.

    Sample standard deviation is computed using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
    which ensures that the sample variance will be an unbiased estimator of population variance.


    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.var(), cols=cols)


def weighted_avg(wcol: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Weighted-avg aggregation.

    Args:
        wcol (str): the name of the weight column
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.wavg(wcol), cols=cols)


def weighted_sum(wcol: str, cols: Union[str, List[str]] = None) -> Aggregation:
    """Creates a Weighted-sum aggregation.

    Args:
        wcol (str): the name of the weight column
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.wsum(wcol), cols=cols)


def distinct(cols: Union[str, List[str]] = None, include_nulls: bool = False) -> Aggregation:
    """Creates a Distinct aggregation which computes the distinct values within an aggregation group for each of the
    given columns and stores them as vectors.

    Args:
        cols (Union[str, List[str]]): the column(s) to aggregate, can be renaming expressions, i.e. "new_col = col";
            default is None, only valid when used in Table agg_all_by operation
        include_nulls (bool): whether nulls should be included as distinct values, default is False

    Returns:
        an aggregation
    """
    return Aggregation(j_agg_spec=_JAggSpec.distinct(include_nulls), cols=cols)
