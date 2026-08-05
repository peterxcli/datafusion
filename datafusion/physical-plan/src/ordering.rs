// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion_common::{Result, ScalarValue, assert_or_internal_err};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{
    BinaryExpr, CaseExpr, is_not_null, is_null, lit,
};
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};

/// Specifies how the input to an aggregation or window operator is ordered
/// relative to their `GROUP BY` or  `PARTITION BY` expressions.
///
/// For example, if the existing ordering is `[a ASC, b ASC, c ASC]`
///
/// ## Window Functions
/// - A `PARTITION BY b` clause can use `Linear` mode.
/// - A `PARTITION BY a, c` or a `PARTITION BY c, a` can use
///   `PartiallySorted([0])` or `PartiallySorted([1])` modes, respectively.
///   (The vector stores the index of `a` in the respective PARTITION BY expression.)
/// - A `PARTITION BY a, b` or a `PARTITION BY b, a` can use `Sorted` mode.
///
/// ## Aggregations
/// - A `GROUP BY b` clause can use `Linear` mode, as the only one permutation `[b]`
///   cannot satisfy the existing ordering.
/// - A `GROUP BY a, c` or a `GROUP BY c, a` can use
///   `PartiallySorted([0])` or `PartiallySorted([1])` modes, respectively, as
///   the permutation `[a]` satisfies the existing ordering.
///   (The vector stores the index of `a` in the respective PARTITION BY expression.)
/// - A `GROUP BY a, b` or a `GROUP BY b, a` can use `Sorted` mode, as the
///   full permutation `[a, b]` satisfies the existing ordering.
///
/// Note these are the same examples as above, but with `GROUP BY` instead of
/// `PARTITION BY` to make the examples easier to read.
#[derive(Debug, Clone, PartialEq)]
pub enum InputOrderMode {
    /// There is no partial permutation of the expressions satisfying the
    /// existing ordering.
    Linear,
    /// There is a partial permutation of the expressions satisfying the
    /// existing ordering. Indices describing the longest partial permutation
    /// are stored in the vector.
    PartiallySorted(Vec<usize>),
    /// There is a (full) permutation of the expressions satisfying the
    /// existing ordering.
    Sorted,
}

fn float_zero_is_negative(value: &ScalarValue) -> Option<bool> {
    match value {
        ScalarValue::Float16(Some(value)) if value.to_bits() << 1 == 0 => {
            Some(value.is_sign_negative())
        }
        ScalarValue::Float32(Some(value)) if value.to_bits() << 1 == 0 => {
            Some(value.is_sign_negative())
        }
        ScalarValue::Float64(Some(value)) if value.to_bits() << 1 == 0 => {
            Some(value.is_sign_negative())
        }
        _ => None,
    }
}

/// Match a scalar without SQL comparison's signed-zero normalization.
fn exact_match_or_else(
    expr: Arc<dyn PhysicalExpr>,
    value: ScalarValue,
    else_expr: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(CaseExpr::try_new(
        Some(expr),
        vec![(lit(value), lit(true))],
        Some(else_expr),
    )?))
}

/// Build the filter expression with the given thresholds.
/// This is now called outside of any locks to reduce critical section time.
pub(crate) fn build_lexicographic_filter(
    sort_exprs: &[PhysicalSortExpr],
    thresholds: &[ScalarValue],
) -> Result<Arc<dyn PhysicalExpr>> {
    assert_or_internal_err!(!sort_exprs.is_empty(), "Sort expressions must not be empty");
    assert_or_internal_err!(
        sort_exprs.len() == thresholds.len(),
        "Sort expressions and thresholds must have the same length"
    );

    // Create filter expressions for each threshold
    let mut filters: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(thresholds.len());

    let mut prev_sort_expr: Option<Arc<dyn PhysicalExpr>> = None;
    for (sort_expr, value) in sort_exprs.iter().zip(thresholds.iter()) {
        // Create the appropriate operator based on sort order
        let op = if sort_expr.options.descending {
            // For descending sort, we want col > threshold (exclude smaller values)
            Operator::Gt
        } else {
            // For ascending sort, we want col < threshold (exclude larger values)
            Operator::Lt
        };

        let value_null = value.is_null();
        let signed_zero = float_zero_is_negative(value);

        let mut comparison = Arc::new(BinaryExpr::new(
            Arc::clone(&sort_expr.expr),
            op,
            lit(value.clone()),
        )) as Arc<dyn PhysicalExpr>;

        if let Some(value_is_negative) = signed_zero {
            // Total ordering and SQL comparison differ only for ASC/+0 and DESC/-0.
            if sort_expr.options.descending == value_is_negative {
                comparison = exact_match_or_else(
                    Arc::clone(&sort_expr.expr),
                    value.arithmetic_negate()?,
                    comparison,
                )?;
            }
        }

        let comparison_with_null = match (sort_expr.options.nulls_first, value_null) {
            // For nulls first, transform to (threshold.value is not null) and (threshold.expr is null or comparison)
            (true, true) => lit(false),
            (true, false) => Arc::new(BinaryExpr::new(
                is_null(Arc::clone(&sort_expr.expr))?,
                Operator::Or,
                comparison,
            )),
            // For nulls last, transform to (threshold.value is null and threshold.expr is not null)
            // or (threshold.value is not null and comparison)
            (false, true) => is_not_null(Arc::clone(&sort_expr.expr))?,
            (false, false) => comparison,
        };

        let mut eq_expr = Arc::new(BinaryExpr::new(
            Arc::clone(&sort_expr.expr),
            Operator::Eq,
            lit(value.clone()),
        )) as Arc<dyn PhysicalExpr>;

        if signed_zero.is_some() {
            // Simple CASE uses exact float matching, unlike SQL comparisons.
            eq_expr = exact_match_or_else(
                Arc::clone(&sort_expr.expr),
                value.clone(),
                lit(false),
            )?;
        }

        if value_null {
            eq_expr = Arc::new(BinaryExpr::new(
                is_null(Arc::clone(&sort_expr.expr))?,
                Operator::Or,
                eq_expr,
            ));
        }

        // For a query like order by a, b, the filter for column `b` is only applied if
        // the condition a = threshold.value (considering null equality) is met.
        // Therefore, we add equality predicates for all preceding fields to the filter logic of the current field,
        // and include the current field's equality predicate in `prev_sort_expr` for use with subsequent fields.
        match prev_sort_expr.take() {
            None => {
                prev_sort_expr = Some(eq_expr);
                filters.push(comparison_with_null);
            }
            Some(p) => {
                filters.push(Arc::new(BinaryExpr::new(
                    Arc::clone(&p),
                    Operator::And,
                    comparison_with_null,
                )));

                prev_sort_expr =
                    Some(Arc::new(BinaryExpr::new(p, Operator::And, eq_expr)));
            }
        }
    }

    let dynamic_predicate = filters
        .into_iter()
        .reduce(|a, b| Arc::new(BinaryExpr::new(a, Operator::Or, b)))
        .expect("sort expressions are checked non-empty");

    Ok(dynamic_predicate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array};
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::utils::compare_rows;
    use datafusion_physical_expr::expressions::Column;

    #[test]
    fn lexicographic_filter_matches_signed_zero_range_ordering() -> Result<()> {
        let a_values = [-1.0, -0.0, -0.0, 0.0, 0.0, 1.0];
        let b_values = [0.0, 0.0, 20.0, 0.0, 20.0, 0.0];
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(a_values.to_vec())),
                Arc::new(Float64Array::from(b_values.to_vec())),
            ],
        )?;
        let a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let b = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        for descending in [false, true] {
            let sort_options = [
                SortOptions {
                    descending,
                    nulls_first: false,
                },
                SortOptions::default(),
            ];
            let sort_exprs = [
                PhysicalSortExpr::new(Arc::clone(&a), sort_options[0]),
                PhysicalSortExpr::new(Arc::clone(&b), sort_options[1]),
            ];

            for threshold in [-0.0, 0.0] {
                let thresholds = [
                    ScalarValue::Float64(Some(threshold)),
                    ScalarValue::Float64(Some(10.0)),
                ];
                let filter = build_lexicographic_filter(&sort_exprs, &thresholds)?;
                let actual = filter.evaluate(&batch)?.into_array(batch.num_rows())?;
                let actual = actual
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("filter must return BooleanArray");
                let expected = a_values
                    .iter()
                    .zip(b_values.iter())
                    .map(|(a, b)| {
                        compare_rows(
                            &[
                                ScalarValue::Float64(Some(*a)),
                                ScalarValue::Float64(Some(*b)),
                            ],
                            &thresholds,
                            &sort_options,
                        )
                        .map(|ordering| ordering.is_lt())
                    })
                    .collect::<Result<Vec<_>>>()?;

                assert_eq!(
                    actual,
                    &BooleanArray::from(expected),
                    "descending={descending}, threshold={threshold:?}"
                );
            }
        }

        Ok(())
    }
}
