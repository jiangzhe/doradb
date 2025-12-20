pub mod arith;
pub mod cmp;
pub mod error;
pub mod eval;
pub mod logic;

use crate::error::Result;
use doradb_datatype::PreciseType;
use doradb_storage::col::attr::Attr;
use doradb_storage::col::sel::Sel;

/// Evaluation of binary expression.
pub trait BinaryEval {
    fn binary_eval(
        &self,
        res_ty: PreciseType,
        lhs: &Attr,
        rhs: &Attr,
        sel: Option<&Sel>,
    ) -> Result<Attr>;
}

/// Evaluation of unary expression.
pub trait UnaryEval {
    fn unary_eval(&self, res_ty: PreciseType, lhs: &Attr, sel: Option<&Sel>) -> Result<Attr>;
}
