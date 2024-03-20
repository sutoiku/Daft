use arrow2;

use super::DaftProductAggable;

use super::as_arrow::AsArrow;

use crate::array::ops::GroupIndices;
use crate::{array::DataArray, datatypes::*};

use common_error::DaftResult;

use arrow2::array::Array;
macro_rules! impl_daft_product_agg {
    ($T:ident, $AggType: ident) => {
        impl DaftProductAggable for &DataArray<$T> {
            type Output = DaftResult<DataArray<$T>>;

            fn product(&self) -> Self::Output {
                let primitive_arr = self.as_arrow();
                let product_value =
                    primitive_arr
                        .iter()
                        .fold(None, |acc, value| match (acc, value) {
                            (None, None) => None,
                            (acc, None) => acc,
                            (None, Some(value)) => Some(*value),
                            (Some(acc), Some(value)) => Some(acc * value),
                        });
                let arrow_array = Box::new(arrow2::array::PrimitiveArray::from([product_value]));
                DataArray::new(self.field.clone(), arrow_array)
            }

            fn grouped_product(&self, groups: &GroupIndices) -> Self::Output {
                use arrow2::array::PrimitiveArray;
                let arrow_array = self.as_arrow();
                let product_per_group = if arrow_array.null_count() > 0 {
                    Box::new(PrimitiveArray::from_trusted_len_iter(groups.iter().map(
                        |g| {
                            g.iter().fold(None, |acc, index| {
                                let idx = *index as usize;
                                match (acc, arrow_array.is_null(idx)) {
                                    (acc, true) => acc,
                                    (None, false) => Some(arrow_array.value(idx)),
                                    (Some(acc), false) => Some(acc * arrow_array.value(idx)),
                                }
                            })
                        },
                    )))
                } else {
                    Box::new(PrimitiveArray::from_trusted_len_values_iter(
                        groups.iter().map(|g| {
                            g.iter().fold(1 as $AggType, |acc, index| {
                                let idx = *index as usize;
                                acc * unsafe { arrow_array.value_unchecked(idx) }
                            })
                        }),
                    ))
                };

                Ok(DataArray::from((
                    self.field.name.as_ref(),
                    product_per_group,
                )))
            }
        }
    };
}

impl_daft_product_agg!(Int64Type, i64);
impl_daft_product_agg!(UInt64Type, u64);
impl_daft_product_agg!(Float32Type, f32);
impl_daft_product_agg!(Float64Type, f64);
