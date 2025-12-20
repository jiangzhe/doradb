pub mod controlflow;
pub mod error;
pub mod expr;
pub mod expr_ext;
pub mod fold;
pub mod func;
pub mod id;
pub mod pred;
pub mod source;
pub mod util;

// re-export column index
pub use doradb_catalog::ColIndex;

pub use crate::expr::*;
pub use crate::expr_ext::*;
pub use crate::func::*;
pub use crate::id::*;
pub use crate::pred::*;
pub use crate::source::*;
pub use crate::util::*;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_size_of_expr() {
        println!("size of Expr {}", std::mem::size_of::<ExprKind>());
        println!("size of Const {}", std::mem::size_of::<Const>());
        println!("size of Col {}", std::mem::size_of::<Col>());
    }
}
