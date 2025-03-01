use std::ops::Not;

use arrow2::{compute::comparison, scalar::PrimitiveScalar};
use common_error::{DaftError, DaftResult};
use num_traits::{NumCast, ToPrimitive};

use super::{as_arrow::AsArrow, from_arrow::FromArrow, full::FullNull, DaftCompare, DaftLogical};
use crate::{
    array::DataArray,
    datatypes::{
        BinaryArray, BooleanArray, DaftArrowBackedType, DaftPrimitiveType, DataType, Field,
        FixedSizeBinaryArray, NullArray, Utf8Array,
    },
    utils::arrow::arrow_bitmap_and_helper,
};

impl<T> PartialEq for DataArray<T>
where
    T: DaftArrowBackedType + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        arrow2::array::equal(self.data(), other.data())
    }
}

impl<T> DaftCompare<&Self> for DataArray<T>
where
    T: DaftPrimitiveType,
{
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::eq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.equal(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.equal(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn eq_null_safe(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let l_validity = self.as_arrow().validity();
                let r_validity = rhs.as_arrow().validity();

                let mut result_values = comparison::eq(self.as_arrow(), rhs.as_arrow())
                    .values()
                    .clone();

                match (l_validity, r_validity) {
                    (None, None) => {}
                    (None, Some(r_valid)) => {
                        result_values = arrow2::bitmap::and(&result_values, r_valid);
                    }
                    (Some(l_valid), None) => {
                        result_values = arrow2::bitmap::and(&result_values, l_valid);
                    }
                    (Some(l_valid), Some(r_valid)) => {
                        let nulls_match = arrow2::bitmap::bitwise_eq(l_valid, r_valid);
                        result_values = arrow2::bitmap::and(&result_values, &nulls_match);
                    }
                }

                Ok(BooleanArray::from((
                    self.name(),
                    arrow2::array::BooleanArray::new(
                        arrow2::datatypes::DataType::Boolean,
                        result_values,
                        None,
                    ),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.eq_null_safe(value))
                } else {
                    let result_values = match self.as_arrow().validity() {
                        None => arrow2::bitmap::Bitmap::new_zeroed(l_size),
                        Some(validity) => validity.not(),
                    };
                    Ok(BooleanArray::from((
                        self.name(),
                        arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            result_values,
                            None,
                        ),
                    )))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.eq_null_safe(value))
                } else {
                    let result_values = match rhs.as_arrow().validity() {
                        None => arrow2::bitmap::Bitmap::new_zeroed(r_size),
                        Some(validity) => validity.not(),
                    };
                    Ok(BooleanArray::from((
                        self.name(),
                        arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            result_values,
                            None,
                        ),
                    )))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn not_equal(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::neq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.not_equal(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.not_equal(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lt(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::lt(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.lt(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.gt(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lte(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::lt_eq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.lte(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.gte(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gt(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::gt(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.gt(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.lt(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gte(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::gt_eq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.gte(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.lte(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}

impl<T> DataArray<T>
where
    T: DaftPrimitiveType,
{
    fn compare_to_scalar(
        &self,
        rhs: T::Native,
        func: impl Fn(
            &dyn arrow2::array::Array,
            &dyn arrow2::scalar::Scalar,
        ) -> arrow2::array::BooleanArray,
    ) -> BooleanArray {
        let arrow_array = self.as_arrow();

        let scalar = PrimitiveScalar::new(arrow_array.data_type().clone(), Some(rhs));

        let validity = self.as_arrow().validity().cloned();

        let arrow_result = func(self.as_arrow(), &scalar).with_validity(validity);
        DataArray::from((self.name(), arrow_result))
    }
}

impl<T, Scalar> DaftCompare<Scalar> for DataArray<T>
where
    T: DaftPrimitiveType,
    Scalar: ToPrimitive,
{
    type Output = BooleanArray;

    fn equal(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        self.compare_to_scalar(rhs, comparison::eq_scalar)
    }

    fn not_equal(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        self.compare_to_scalar(rhs, comparison::neq_scalar)
    }

    fn lt(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        self.compare_to_scalar(rhs, comparison::lt_scalar)
    }

    fn lte(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        self.compare_to_scalar(rhs, comparison::lt_eq_scalar)
    }

    fn gt(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        self.compare_to_scalar(rhs, comparison::gt_scalar)
    }

    fn gte(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        self.compare_to_scalar(rhs, comparison::gt_eq_scalar)
    }

    fn eq_null_safe(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");

        let arrow_array = self.as_arrow();
        let scalar = PrimitiveScalar::new(arrow_array.data_type().clone(), Some(rhs));

        let result_values = comparison::eq_scalar(arrow_array, &scalar).values().clone();

        let final_values = match arrow_array.validity() {
            None => result_values,
            Some(valid) => arrow2::bitmap::and(&result_values, valid),
        };

        BooleanArray::from((
            self.name(),
            arrow2::array::BooleanArray::new(
                arrow2::datatypes::DataType::Boolean,
                final_values,
                None,
            ),
        ))
    }
}

impl DaftCompare<&Self> for BooleanArray {
    type Output = DaftResult<Self>;

    fn equal(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(Self::from((
                    self.name(),
                    comparison::eq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.equal(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, l_size))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.equal(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, r_size))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn not_equal(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(Self::from((
                    self.name(),
                    comparison::neq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.not_equal(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, l_size))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.not_equal(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, r_size))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lt(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(Self::from((
                    self.name(),
                    comparison::lt(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.lt(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, l_size))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.gt(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, r_size))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lte(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(Self::from((
                    self.name(),
                    comparison::lt_eq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.lte(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, l_size))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.gte(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, r_size))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gt(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(Self::from((
                    self.name(),
                    comparison::gt(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.gt(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, l_size))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.lt(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, r_size))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gte(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(Self::from((
                    self.name(),
                    comparison::gt_eq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.gte(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, l_size))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.lte(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, r_size))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn eq_null_safe(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let l_validity = self.as_arrow().validity();
                let r_validity = rhs.as_arrow().validity();

                let mut result_values = comparison::eq(self.as_arrow(), rhs.as_arrow())
                    .values()
                    .clone();

                match (l_validity, r_validity) {
                    (None, None) => {}
                    (None, Some(r_valid)) => {
                        result_values = arrow2::bitmap::and(&result_values, r_valid);
                    }
                    (Some(l_valid), None) => {
                        result_values = arrow2::bitmap::and(&result_values, l_valid);
                    }
                    (Some(l_valid), Some(r_valid)) => {
                        let nulls_match = arrow2::bitmap::bitwise_eq(l_valid, r_valid);
                        result_values = arrow2::bitmap::and(&result_values, &nulls_match);
                    }
                }

                Ok(Self::from((
                    self.name(),
                    arrow2::array::BooleanArray::new(
                        arrow2::datatypes::DataType::Boolean,
                        result_values,
                        None,
                    ),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.eq_null_safe(value)?)
                } else {
                    let result_values = match self.as_arrow().validity() {
                        None => arrow2::bitmap::Bitmap::new_zeroed(l_size),
                        Some(validity) => validity.not(),
                    };
                    Ok(Self::from((
                        self.name(),
                        arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            result_values,
                            None,
                        ),
                    )))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.eq_null_safe(value)?)
                } else {
                    let result_values = match rhs.as_arrow().validity() {
                        None => arrow2::bitmap::Bitmap::new_zeroed(r_size),
                        Some(validity) => validity.not(),
                    };
                    Ok(Self::from((
                        self.name(),
                        arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            result_values,
                            None,
                        ),
                    )))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}

impl DaftCompare<bool> for BooleanArray {
    type Output = DaftResult<Self>;

    fn equal(&self, rhs: bool) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::boolean::eq_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(Self::from((self.name(), arrow_result)))
    }

    fn not_equal(&self, rhs: bool) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::boolean::neq_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(Self::from((self.name(), arrow_result)))
    }

    fn lt(&self, rhs: bool) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::boolean::lt_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(Self::from((self.name(), arrow_result)))
    }

    fn lte(&self, rhs: bool) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::boolean::lt_eq_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(Self::from((self.name(), arrow_result)))
    }

    fn gt(&self, rhs: bool) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::boolean::gt_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(Self::from((self.name(), arrow_result)))
    }

    fn gte(&self, rhs: bool) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::boolean::gt_eq_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(Self::from((self.name(), arrow_result)))
    }

    fn eq_null_safe(&self, rhs: bool) -> Self::Output {
        let result_values = comparison::boolean::eq_scalar(self.as_arrow(), rhs)
            .values()
            .clone();

        let final_values = match self.as_arrow().validity() {
            None => result_values,
            Some(valid) => arrow2::bitmap::and(&result_values, valid),
        };

        Ok(Self::from((
            self.name(),
            arrow2::array::BooleanArray::new(
                arrow2::datatypes::DataType::Boolean,
                final_values,
                None,
            ),
        )))
    }
}

impl Not for &BooleanArray {
    type Output = DaftResult<BooleanArray>;
    fn not(self) -> Self::Output {
        let new_bitmap = self.as_arrow().values().not();
        let arrow_array = arrow2::array::BooleanArray::new(
            arrow2::datatypes::DataType::Boolean,
            new_bitmap,
            self.as_arrow().validity().cloned(),
        );
        Ok(BooleanArray::from((self.name(), arrow_array)))
    }
}

impl DaftLogical<&Self> for BooleanArray {
    type Output = DaftResult<Self>;
    fn and(&self, rhs: &Self) -> Self::Output {
        // When performing a logical AND with a NULL value:
        // - If the non-null value is false, the result is false (not null)
        // - If the non-null value is true, the result is null
        fn and_with_null(name: &str, arr: &BooleanArray) -> BooleanArray {
            let values = arr.as_arrow().values();

            let new_validity = match arr.as_arrow().validity() {
                None => values.not(),
                Some(validity) => arrow2::bitmap::and(&values.not(), validity),
            };

            BooleanArray::from((
                name,
                arrow2::array::BooleanArray::new(
                    arrow2::datatypes::DataType::Boolean,
                    values.clone(),
                    Some(new_validity),
                ),
            ))
        }

        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let l_values = self.as_arrow().values();
                let r_values = rhs.as_arrow().values();

                // (false & NULL) should be false, compute validity to ensure that
                let validity = match (self.as_arrow().validity(), rhs.as_arrow().validity()) {
                    (None, None) => None,
                    (None, Some(r_valid)) => Some(arrow2::bitmap::or(&l_values.not(), r_valid)),
                    (Some(l_valid), None) => Some(arrow2::bitmap::or(l_valid, &r_values.not())),
                    (Some(l_valid), Some(r_valid)) => Some(arrow2::bitmap::or(
                        &arrow2::bitmap::or(
                            &arrow2::bitmap::and(&l_values.not(), l_valid),
                            &arrow2::bitmap::and(&r_values.not(), r_valid),
                        ),
                        &arrow2::bitmap::and(l_valid, r_valid),
                    )),
                };

                let result_bitmap = arrow2::bitmap::and(l_values, r_values);
                Ok(Self::from((
                    self.name(),
                    arrow2::array::BooleanArray::new(
                        arrow2::datatypes::DataType::Boolean,
                        result_bitmap,
                        validity,
                    ),
                )))
            }
            (_, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.and(value)
                } else {
                    Ok(and_with_null(self.name(), self))
                }
            }
            (1, _) => {
                if let Some(value) = self.get(0) {
                    rhs.and(value)
                } else {
                    Ok(and_with_null(self.name(), rhs))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn or(&self, rhs: &Self) -> Self::Output {
        // When performing a logical OR with a NULL value:
        // - If the non-null value is false, the result is null
        // - If the non-null value is true, the result is true (not null)
        fn or_with_null(name: &str, arr: &BooleanArray) -> BooleanArray {
            let values = arr.as_arrow().values();

            let new_validity = match arr.as_arrow().validity() {
                None => values.clone(),
                Some(validity) => arrow2::bitmap::and(values, validity),
            };

            BooleanArray::from((
                name,
                arrow2::array::BooleanArray::new(
                    arrow2::datatypes::DataType::Boolean,
                    values.clone(),
                    Some(new_validity),
                ),
            ))
        }

        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let l_values = self.as_arrow().values();
                let r_values = rhs.as_arrow().values();

                // (true | NULL) should be true, compute validity to ensure that
                let validity = match (self.as_arrow().validity(), rhs.as_arrow().validity()) {
                    (None, None) => None,
                    (None, Some(r_valid)) => Some(arrow2::bitmap::or(l_values, r_valid)),
                    (Some(l_valid), None) => Some(arrow2::bitmap::or(l_valid, r_values)),
                    (Some(l_valid), Some(r_valid)) => Some(arrow2::bitmap::or(
                        &arrow2::bitmap::or(
                            &arrow2::bitmap::and(l_values, l_valid),
                            &arrow2::bitmap::and(r_values, r_valid),
                        ),
                        &arrow2::bitmap::and(l_valid, r_valid),
                    )),
                };

                let result_bitmap = arrow2::bitmap::or(l_values, r_values);
                Ok(Self::from((
                    self.name(),
                    arrow2::array::BooleanArray::new(
                        arrow2::datatypes::DataType::Boolean,
                        result_bitmap,
                        validity,
                    ),
                )))
            }
            (_, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.or(value)
                } else {
                    Ok(or_with_null(self.name(), self))
                }
            }
            (1, _) => {
                if let Some(value) = self.get(0) {
                    rhs.or(value)
                } else {
                    Ok(or_with_null(self.name(), rhs))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn xor(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());

                let result_bitmap =
                    arrow2::bitmap::xor(self.as_arrow().values(), rhs.as_arrow().values());
                Ok(Self::from((
                    self.name(),
                    arrow2::array::BooleanArray::new(
                        arrow2::datatypes::DataType::Boolean,
                        result_bitmap,
                        validity,
                    ),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.xor(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, l_size))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.xor(value)
                } else {
                    Ok(Self::full_null(self.name(), &DataType::Boolean, r_size))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}

macro_rules! null_array_comparison_method {
    ($func_name:ident) => {
        fn $func_name(&self, rhs: &NullArray) -> Self::Output {
            match (self.len(), rhs.len()) {
                (x, y) if x == y => Ok(BooleanArray::full_null(self.name(), &DataType::Boolean, x)),
                (l_size, 1) => Ok(BooleanArray::full_null(
                    self.name(),
                    &DataType::Boolean,
                    l_size,
                )),
                (1, r_size) => Ok(BooleanArray::full_null(
                    self.name(),
                    &DataType::Boolean,
                    r_size,
                )),
                (l, r) => Err(DaftError::ValueError(format!(
                    "trying to compare different length arrays: {}: {l} vs {}: {r}",
                    self.name(),
                    rhs.name()
                ))),
            }
        }
    };
}

impl DaftCompare<&Self> for NullArray {
    type Output = DaftResult<BooleanArray>;
    null_array_comparison_method!(equal);
    null_array_comparison_method!(not_equal);
    null_array_comparison_method!(lt);
    null_array_comparison_method!(lte);
    null_array_comparison_method!(gt);
    null_array_comparison_method!(gte);
    null_array_comparison_method!(eq_null_safe);
}

impl DaftLogical<bool> for BooleanArray {
    type Output = DaftResult<Self>;
    fn and(&self, rhs: bool) -> Self::Output {
        if rhs {
            Ok(self.clone())
        } else {
            Ok(Self::from((
                self.name(),
                arrow2::array::BooleanArray::new(
                    arrow2::datatypes::DataType::Boolean,
                    arrow2::bitmap::Bitmap::new_zeroed(self.len()),
                    None, // false & x is always valid false for any x
                ),
            )))
        }
    }

    fn or(&self, rhs: bool) -> Self::Output {
        if rhs {
            Ok(Self::from((
                self.name(),
                arrow2::array::BooleanArray::new(
                    arrow2::datatypes::DataType::Boolean,
                    arrow2::bitmap::Bitmap::new_trued(self.len()),
                    None, // true | x is always valid true for any x
                ),
            )))
        } else {
            Ok(self.clone())
        }
    }

    fn xor(&self, rhs: bool) -> Self::Output {
        if rhs {
            self.not()
        } else {
            Ok(self.clone())
        }
    }
}

impl DaftCompare<&Self> for Utf8Array {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::eq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn not_equal(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::neq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.not_equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.not_equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lt(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::lt(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.lt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.gt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lte(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::lt_eq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.lte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.gte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gt(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::gt(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.gt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.lt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gte(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::gt_eq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.gte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.lte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn eq_null_safe(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let l_validity = self.as_arrow().validity();
                let r_validity = rhs.as_arrow().validity();

                let mut result_values = comparison::eq(self.as_arrow(), rhs.as_arrow())
                    .values()
                    .clone();

                match (l_validity, r_validity) {
                    (None, None) => {}
                    (None, Some(r_valid)) => {
                        result_values = arrow2::bitmap::and(&result_values, r_valid);
                    }
                    (Some(l_valid), None) => {
                        result_values = arrow2::bitmap::and(&result_values, l_valid);
                    }
                    (Some(l_valid), Some(r_valid)) => {
                        let nulls_match = arrow2::bitmap::bitwise_eq(l_valid, r_valid);
                        result_values = arrow2::bitmap::and(&result_values, &nulls_match);
                    }
                }

                Ok(BooleanArray::from((
                    self.name(),
                    arrow2::array::BooleanArray::new(
                        arrow2::datatypes::DataType::Boolean,
                        result_values,
                        None,
                    ),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.eq_null_safe(value)?)
                } else {
                    let result_values = match self.as_arrow().validity() {
                        None => arrow2::bitmap::Bitmap::new_zeroed(l_size),
                        Some(validity) => validity.not(),
                    };
                    Ok(BooleanArray::from((
                        self.name(),
                        arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            result_values,
                            None,
                        ),
                    )))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.eq_null_safe(value)?)
                } else {
                    let result_values = match rhs.as_arrow().validity() {
                        None => arrow2::bitmap::Bitmap::new_zeroed(r_size),
                        Some(validity) => validity.not(),
                    };
                    Ok(BooleanArray::from((
                        self.name(),
                        arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            result_values,
                            None,
                        ),
                    )))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}

impl DaftCompare<&str> for Utf8Array {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &str) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::utf8::eq_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn not_equal(&self, rhs: &str) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::utf8::neq_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn lt(&self, rhs: &str) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::utf8::lt_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn lte(&self, rhs: &str) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::utf8::lt_eq_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn gt(&self, rhs: &str) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::utf8::gt_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn gte(&self, rhs: &str) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::utf8::gt_eq_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn eq_null_safe(&self, rhs: &str) -> Self::Output {
        let arrow_array = self.as_arrow();

        let result_values = comparison::utf8::eq_scalar(arrow_array, rhs)
            .values()
            .clone();

        let final_values = match arrow_array.validity() {
            None => result_values,
            Some(valid) => arrow2::bitmap::and(&result_values, valid),
        };

        Ok(BooleanArray::from((
            self.name(),
            arrow2::array::BooleanArray::new(
                arrow2::datatypes::DataType::Boolean,
                final_values,
                None,
            ),
        )))
    }
}

impl DaftCompare<&Self> for BinaryArray {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::eq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn not_equal(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::neq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.not_equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.not_equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lt(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::lt(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.lt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.gt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lte(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::lt_eq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.lte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.gte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gt(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::gt(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.gt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.lt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gte(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.as_arrow().validity(), rhs.as_arrow().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::gt_eq(self.as_arrow(), rhs.as_arrow()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.gte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.lte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn eq_null_safe(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let l_validity = self.as_arrow().validity();
                let r_validity = rhs.as_arrow().validity();

                let mut result_values = comparison::eq(self.as_arrow(), rhs.as_arrow())
                    .values()
                    .clone();

                match (l_validity, r_validity) {
                    (None, None) => {}
                    (None, Some(r_valid)) => {
                        result_values = arrow2::bitmap::and(&result_values, r_valid);
                    }
                    (Some(l_valid), None) => {
                        result_values = arrow2::bitmap::and(&result_values, l_valid);
                    }
                    (Some(l_valid), Some(r_valid)) => {
                        let nulls_match = arrow2::bitmap::bitwise_eq(l_valid, r_valid);
                        result_values = arrow2::bitmap::and(&result_values, &nulls_match);
                    }
                }

                Ok(BooleanArray::from((
                    self.name(),
                    arrow2::array::BooleanArray::new(
                        arrow2::datatypes::DataType::Boolean,
                        result_values,
                        None,
                    ),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.eq_null_safe(value)?)
                } else {
                    let result_values = match self.as_arrow().validity() {
                        None => arrow2::bitmap::Bitmap::new_zeroed(l_size),
                        Some(validity) => validity.not(),
                    };
                    Ok(BooleanArray::from((
                        self.name(),
                        arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            result_values,
                            None,
                        ),
                    )))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.eq_null_safe(value)?)
                } else {
                    let result_values = match rhs.as_arrow().validity() {
                        None => arrow2::bitmap::Bitmap::new_zeroed(r_size),
                        Some(validity) => validity.not(),
                    };
                    Ok(BooleanArray::from((
                        self.name(),
                        arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            result_values,
                            None,
                        ),
                    )))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}

impl DaftCompare<&[u8]> for BinaryArray {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &[u8]) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::binary::eq_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn not_equal(&self, rhs: &[u8]) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::binary::neq_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn lt(&self, rhs: &[u8]) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::binary::lt_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn lte(&self, rhs: &[u8]) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::binary::lt_eq_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn gt(&self, rhs: &[u8]) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::binary::gt_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn gte(&self, rhs: &[u8]) -> Self::Output {
        let validity = self.as_arrow().validity().cloned();
        let arrow_result =
            comparison::binary::gt_eq_scalar(self.as_arrow(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn eq_null_safe(&self, rhs: &[u8]) -> Self::Output {
        let arrow_array = self.as_arrow();

        let result_values = comparison::binary::eq_scalar(arrow_array, rhs)
            .values()
            .clone();

        let final_values = match arrow_array.validity() {
            None => result_values,
            Some(valid) => arrow2::bitmap::and(&result_values, valid),
        };

        Ok(BooleanArray::from((
            self.name(),
            arrow2::array::BooleanArray::new(
                arrow2::datatypes::DataType::Boolean,
                final_values,
                None,
            ),
        )))
    }
}

fn compare_fixed_size_binary<F>(
    lhs: &FixedSizeBinaryArray,
    rhs: &FixedSizeBinaryArray,
    op: F,
) -> DaftResult<BooleanArray>
where
    F: Fn(&[u8], &[u8]) -> bool,
{
    let lhs_arrow = lhs.as_arrow();
    let rhs_arrow = rhs.as_arrow();
    let validity = match (lhs_arrow.validity(), rhs_arrow.validity()) {
        (Some(lhs), None) => Some(lhs.clone()),
        (None, Some(rhs)) => Some(rhs.clone()),
        (None, None) => None,
        (Some(lhs), Some(rhs)) => Some(lhs & rhs),
    };

    let values = lhs_arrow
        .values_iter()
        .zip(rhs_arrow.values_iter())
        .map(|(lhs, rhs)| op(lhs, rhs));
    let values = arrow2::bitmap::Bitmap::from_trusted_len_iter(values);

    BooleanArray::from_arrow(
        Field::new(lhs.name(), DataType::Boolean).into(),
        Box::new(arrow2::array::BooleanArray::new(
            arrow2::datatypes::DataType::Boolean,
            values,
            validity,
        )),
    )
}

fn cmp_fixed_size_binary_scalar<F>(
    lhs: &FixedSizeBinaryArray,
    rhs: &[u8],
    op: F,
) -> DaftResult<BooleanArray>
where
    F: Fn(&[u8], &[u8]) -> bool,
{
    let lhs_arrow = lhs.as_arrow();
    let validity = lhs_arrow.validity().cloned();

    let values = lhs_arrow.values_iter().map(|lhs| op(lhs, rhs));
    let values = arrow2::bitmap::Bitmap::from_trusted_len_iter(values);

    BooleanArray::from_arrow(
        Field::new(lhs.name(), DataType::Boolean).into(),
        Box::new(arrow2::array::BooleanArray::new(
            arrow2::datatypes::DataType::Boolean,
            values,
            validity,
        )),
    )
}

impl DaftCompare<&Self> for FixedSizeBinaryArray {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => compare_fixed_size_binary(self, rhs, |lhs, rhs| lhs == rhs),
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.equal(value).map(|v| v.rename(self.name()))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn not_equal(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => compare_fixed_size_binary(self, rhs, |lhs, rhs| lhs != rhs),
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.not_equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.not_equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lt(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => compare_fixed_size_binary(self, rhs, |lhs, rhs| lhs < rhs),
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.lt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.gt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lte(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => compare_fixed_size_binary(self, rhs, |lhs, rhs| lhs <= rhs),
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.lte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.gte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gt(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => compare_fixed_size_binary(self, rhs, |lhs, rhs| lhs > rhs),
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.gt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.lt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gte(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => compare_fixed_size_binary(self, rhs, |lhs, rhs| lhs >= rhs),
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.gte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.lte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn eq_null_safe(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let l_validity = self.as_arrow().validity();
                let r_validity = rhs.as_arrow().validity();

                let mut result_values = comparison::eq(self.as_arrow(), rhs.as_arrow())
                    .values()
                    .clone();

                match (l_validity, r_validity) {
                    (None, None) => {}
                    (None, Some(r_valid)) => {
                        result_values = arrow2::bitmap::and(&result_values, r_valid);
                    }
                    (Some(l_valid), None) => {
                        result_values = arrow2::bitmap::and(&result_values, l_valid);
                    }
                    (Some(l_valid), Some(r_valid)) => {
                        let nulls_match = arrow2::bitmap::bitwise_eq(l_valid, r_valid);
                        result_values = arrow2::bitmap::and(&result_values, &nulls_match);
                    }
                }

                Ok(BooleanArray::from((
                    self.name(),
                    arrow2::array::BooleanArray::new(
                        arrow2::datatypes::DataType::Boolean,
                        result_values,
                        None,
                    ),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.eq_null_safe(value)?)
                } else {
                    let result_values = match self.as_arrow().validity() {
                        None => arrow2::bitmap::Bitmap::new_zeroed(l_size),
                        Some(validity) => validity.not(),
                    };
                    Ok(BooleanArray::from((
                        self.name(),
                        arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            result_values,
                            None,
                        ),
                    )))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.eq_null_safe(value)?)
                } else {
                    let result_values = match rhs.as_arrow().validity() {
                        None => arrow2::bitmap::Bitmap::new_zeroed(r_size),
                        Some(validity) => validity.not(),
                    };
                    Ok(BooleanArray::from((
                        self.name(),
                        arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            result_values,
                            None,
                        ),
                    )))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}

impl DaftCompare<&[u8]> for FixedSizeBinaryArray {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &[u8]) -> Self::Output {
        cmp_fixed_size_binary_scalar(self, rhs, |lhs, rhs| lhs == rhs)
    }

    fn not_equal(&self, rhs: &[u8]) -> Self::Output {
        cmp_fixed_size_binary_scalar(self, rhs, |lhs, rhs| lhs != rhs)
    }

    fn lt(&self, rhs: &[u8]) -> Self::Output {
        cmp_fixed_size_binary_scalar(self, rhs, |lhs, rhs| lhs < rhs)
    }

    fn lte(&self, rhs: &[u8]) -> Self::Output {
        cmp_fixed_size_binary_scalar(self, rhs, |lhs, rhs| lhs <= rhs)
    }

    fn gt(&self, rhs: &[u8]) -> Self::Output {
        cmp_fixed_size_binary_scalar(self, rhs, |lhs, rhs| lhs > rhs)
    }

    fn gte(&self, rhs: &[u8]) -> Self::Output {
        cmp_fixed_size_binary_scalar(self, rhs, |lhs, rhs| lhs >= rhs)
    }

    fn eq_null_safe(&self, rhs: &[u8]) -> Self::Output {
        let arrow_array = self.as_arrow();

        let result_values = arrow2::bitmap::Bitmap::from_trusted_len_iter(
            arrow_array.values_iter().map(|lhs| lhs == rhs),
        );

        let final_values = match arrow_array.validity() {
            None => result_values,
            Some(valid) => arrow2::bitmap::and(&result_values, valid),
        };

        Ok(BooleanArray::from((
            self.name(),
            arrow2::array::BooleanArray::new(
                arrow2::datatypes::DataType::Boolean,
                final_values,
                None,
            ),
        )))
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use crate::{array::ops::DaftCompare, datatypes::Int64Array};

    #[test]
    fn equal_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.equal(2).into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(false)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.equal(2).into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn not_equal_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.not_equal(2).into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(true)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.not_equal(2).into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn lt_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.lt(2).into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(false)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.lt(2).into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn lte_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.lte(2).into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(true), Some(false)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.lte(2).into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn gt_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.gt(2).into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(false), Some(true)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.gt(2).into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn gte_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.gte(2).into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(true)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.gte(2).into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn equal_int64_array_with_same_array() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(true), Some(true)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn not_equal_int64_array_with_same_array() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.not_equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(false), Some(false)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.not_equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn lt_int64_array_with_array() -> DaftResult<()> {
        let lhs = Int64Array::arange("a", 1, 4, 1)?;
        let rhs = Int64Array::arange("a", 0, 6, 2)?;
        let result: Vec<_> = lhs.lt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(false), Some(true)]);

        let lhs = lhs.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = lhs.lt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);

        let rhs = rhs.with_validity_slice(&[false, true, true])?;
        let result: Vec<_> = lhs.lt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(true)]);
        Ok(())
    }

    #[test]
    fn lte_int64_array_with_array() -> DaftResult<()> {
        let lhs = Int64Array::arange("a", 1, 4, 1)?;
        let rhs = Int64Array::arange("a", 0, 6, 2)?;
        let result: Vec<_> = lhs.lte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(true)]);

        let lhs = lhs.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = lhs.lte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);

        let rhs = rhs.with_validity_slice(&[false, true, true])?;
        let result: Vec<_> = lhs.lte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(true)]);
        Ok(())
    }

    #[test]
    fn gt_int64_array_with_array() -> DaftResult<()> {
        let lhs = Int64Array::arange("a", 1, 4, 1)?;
        let rhs = Int64Array::arange("a", 0, 6, 2)?;
        let result: Vec<_> = lhs.gt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(false)]);

        let lhs = lhs.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = lhs.gt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);

        let rhs = rhs.with_validity_slice(&[false, true, true])?;
        let result: Vec<_> = lhs.gt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(false)]);
        Ok(())
    }

    #[test]
    fn gte_int64_array_with_array() -> DaftResult<()> {
        let lhs = Int64Array::arange("a", 1, 4, 1)?;
        let rhs = Int64Array::arange("a", 0, 6, 2)?;
        let result: Vec<_> = lhs.gte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(true), Some(false)]);

        let lhs = lhs.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = lhs.gte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);

        let rhs = rhs.with_validity_slice(&[false, true, true])?;
        let result: Vec<_> = lhs.gte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(false)]);
        Ok(())
    }
}
