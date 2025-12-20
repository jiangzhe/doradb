#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FuncKind {
    // Uninitialized, should be used only for intermediate placeholder
    // in transformation
    Uninit,
    /// Unary operators: 1 arg
    // Negate
    Neg,
    // Bit inverse
    BitInv,
    /// Binary operators: 2 args
    Add,
    Sub,
    Mul,
    Div,
    IntDiv,
    BitAnd,
    BitOr,
    BitXor,
    BitShl,
    BitShr,
    /// SQL builtin functions: variable args
    // extract: [time_unit, arg]
    Extract,
    // substring: [arg, start, end]
    Substring,
}

impl FuncKind {
    #[inline]
    pub fn to_lower(&self) -> &'static str {
        match self {
            FuncKind::Uninit => "uninit",
            FuncKind::Neg => "neg",
            FuncKind::BitInv => "bitinv",
            FuncKind::Add => "add",
            FuncKind::Sub => "sub",
            FuncKind::Mul => "mul",
            FuncKind::Div => "div",
            FuncKind::IntDiv => "intdiv",
            FuncKind::BitAnd => "bitand",
            FuncKind::BitOr => "bitor",
            FuncKind::BitXor => "bitxor",
            FuncKind::BitShl => "bitshl",
            FuncKind::BitShr => "bitshr",
            FuncKind::Extract => "extract",
            FuncKind::Substring => "substring",
        }
    }

    /// Returns minimum and maximum argument numbers of this function.
    /// if maximum number is None, the function can accept arbitrary number
    /// of arguments.
    #[inline]
    pub fn n_args(&self) -> (usize, Option<usize>) {
        use FuncKind::*;
        let n = match self {
            Uninit => 0,
            Neg | BitInv => 1,
            Add | Sub | Mul | Div | IntDiv | BitAnd | BitOr | BitXor | BitShl | BitShr => 2,
            Extract => 2,
            Substring => 3,
        };
        (n, Some(n))
    }
}
