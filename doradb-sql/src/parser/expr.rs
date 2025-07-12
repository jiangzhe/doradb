/// Expression parsing is backed by pratt parser.
///
/// The core technique is to specify precedence of
/// infix operators and compare left/right precedence
/// to construct syntax tree accordingly.
///
/// For each infix operator, left binding power and
/// right binding power are defined.
/// The nearby operators are compared by its binding powers.
/// e.g.
/// In case of "expr0 op_a expr1 op_b expr2", the right_bp
/// of op_a is compared with left_bp of op_b, if greater than,
/// result is `op_b(op_a(expr0, expr1), expr2)`, otherwise,
/// result is  `op_a(expr0, op_b(expr1, expr2))`.
///
/// See the blog for more details:
/// https://matklad.github.io/2020/04/13/simple-but-powerful-pratt-parsing.html
///
/// MySQL precedence rules:
/// https://dev.mysql.com/doc/refman/8.0/en/operator-precedence.html
#[cfg(test)]
mod tests;

use crate::ast::*;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_till1};
use nom::character::complete::{alphanumeric1, char, hex_digit0, one_of};
use nom::combinator::{cut, map, map_opt, not, opt, peek, recognize, value};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{fold_many0, many0, many1, separated_list1};
use nom::number::complete::recognize_float;
use nom::sequence::{delimited, pair, preceded, terminated};
use nom::IResult;

use crate::parser::query::{query_expr, subquery};
use crate::parser::ParseInput;
use crate::parser::{
    ident, ident_tag, match_builtin_keyword, match_reserved_keyword, next_cut, quoted_ident,
    regular_ident, set_quantifier, spcmt0, spcmt1, BuiltinKeyword, ReservedKeyword,
};

use super::is_reserved_keyword;

/// BindingPower determine the expression should be
/// left associated or right associated.
pub(crate) type BindingPower = u8;

#[allow(dead_code)]
const MIN_BP: BindingPower = 0;

/// MySQL document says CASE WHEN has higher precedence than logical operator, but actual not
/// So put it as lowest.
#[allow(dead_code)]
const PREFIX_BP_CASE: BindingPower = 2; // "CASE", "CASE WHEN"
                                        // define_bp!(INFIX_BP_ASSIGN = 2, 1); // "=", ":="
define_bp!(INFIX_BP_LOGICAL_OR = 3, 4); // "OR"
define_bp!(INFIX_BP_LOGICAL_XOR = 5, 6); // "XOR"
define_bp!(INFIX_BP_LOGICAL_AND = 7, 8); // "AND"
define_bp!(PREFIX_BP_LOGICAL_NOT = 9); // "NOT"
define_bp!(INFIX_BP_BETWEEN = 10, 11); // "BETWEEN"
define_bp!(INFIX_BP_CMP = 13, 14); // "=", "<=>", ">=", ">", "<=", "<", "<>", "!=", "IS", "LIKE", "REGEXP", "IN", "MEMBER OF"
define_bp!(INFIX_BP_BIT_OR = 15, 16); // "|"
define_bp!(INFIX_BP_BIT_AND = 17, 18); // "&"
define_bp!(INFIX_BP_BIT_SHIFT = 19, 20); // "<<", ">>"
define_bp!(INFIX_BP_ADD = 21, 22); // "+", "-"
define_bp!(INFIX_BP_MUL = 23, 24); // "*", "/", "DIV", "%", "MOD"
define_bp!(INFIX_BP_BIT_XOR = 25, 26); // "^"
define_bp!(PREFIX_BP_NEG = 27); // "-"
define_bp!(PREFIX_BP_BIT_INV = 27); // "~"
define_bp!(INFIX_BP_CALL = 28, 29); // "("

#[inline]
pub(crate) fn prefix_binding_power(op: UnaryOp) -> BindingPower {
    match op {
        UnaryOp::Neg => PREFIX_BP_NEG,
        UnaryOp::BitInv => PREFIX_BP_BIT_INV,
        UnaryOp::LogicalNot => PREFIX_BP_LOGICAL_NOT,
    }
}

#[inline]
pub(crate) fn infix_binding_power(op: InfixOp) -> (BindingPower, BindingPower) {
    match op {
        InfixOp::Binary(bin_op) => bin_infix_binding_power(bin_op),
        InfixOp::Is => INFIX_BP_CMP,
        InfixOp::Predicate(pred_op) => pred_infix_binding_power(pred_op),
        InfixOp::Call => INFIX_BP_CALL,
    }
}

#[inline]
pub(crate) fn bin_infix_binding_power(op: BinaryOp) -> (BindingPower, BindingPower) {
    match op {
        BinaryOp::Add | BinaryOp::Sub => INFIX_BP_ADD,
        BinaryOp::Mul | BinaryOp::Div => INFIX_BP_MUL,
        BinaryOp::BitAnd => INFIX_BP_BIT_AND,
        BinaryOp::BitOr => INFIX_BP_BIT_OR,
        BinaryOp::BitXor => INFIX_BP_BIT_XOR,
        BinaryOp::BitShl | BinaryOp::BitShr => INFIX_BP_BIT_SHIFT,
    }
}

#[inline]
pub(crate) fn pred_infix_binding_power(op: PredicateOp) -> (BindingPower, BindingPower) {
    match op {
        PredicateOp::Cmp(_) | PredicateOp::Match(_) | PredicateOp::In | PredicateOp::NotIn => {
            INFIX_BP_CMP
        }
        PredicateOp::Between | PredicateOp::NotBetween => INFIX_BP_BETWEEN,
        PredicateOp::LogicalAnd => INFIX_BP_LOGICAL_AND,
        PredicateOp::LogicalOr => INFIX_BP_LOGICAL_OR,
        PredicateOp::LogicalXor => INFIX_BP_LOGICAL_XOR,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrefixToken {
    Pos,
    Neg,
    BitInv,
    Paren,
}

pub(crate) fn expr_sp0<'a, I: ParseInput<'a>, E: ParseError<I>>(i: I) -> IResult<I, Expr<'a>, E> {
    terminated(|i| pratt_expr(i, MIN_BP), spcmt0)(i)
}

#[inline]
fn is_op(s: &str, not: bool) -> Option<IsOp> {
    if not {
        let res = if s.eq_ignore_ascii_case("null") {
            IsOp::NotNull
        } else if s.eq_ignore_ascii_case("true") {
            IsOp::NotTrue
        } else if s.eq_ignore_ascii_case("false") {
            IsOp::NotFalse
        } else {
            return None;
        };
        Some(res)
    } else {
        let res = if s.eq_ignore_ascii_case("null") {
            IsOp::Null
        } else if s.eq_ignore_ascii_case("true") {
            IsOp::True
        } else if s.eq_ignore_ascii_case("false") {
            IsOp::False
        } else {
            return None;
        };
        Some(res)
    }
}

/// pratt_expr implements the operator precedence ordered parsing.
fn pratt_expr<'a, I: ParseInput<'a>, E: ParseError<I>>(
    i: I,
    min_bp: BindingPower,
) -> IResult<I, Expr<'a>, E> {
    let (mut i, mut lhs) = prefix_expr(i)?;
    i = spcmt0(i)?.0;
    loop {
        if i.input_len() == 0 {
            break;
        }
        match infix_op::<I, E>(i) {
            Ok((ti, op)) => {
                let (l_bp, r_bp) = infix_binding_power(op);
                if l_bp < min_bp {
                    break;
                }
                i = ti; // advance input
                match op {
                    InfixOp::Call => {
                        if let Expr::ColumnRef(f) = lhs {
                            // let (ri, _) = spcmt0(i)?; // remove spaces after the operator
                            // let (ri, rhs) = separated_list0(char_sp0(','), expr_sp0)(ri)?;
                            // let (ri, _) = char_sp0(')')(ri)?; // must terminated with ')'
                            // lhs = Expr::func(f, rhs);

                            let (ri, _) = spcmt0(i)?; // remove spaces after the operator
                                                      // currently we do not support <package>.<function>
                                                      // and UDF is disabled.
                            if f.len() != 1 {
                                return Err(nom::Err::Error(E::from_error_kind(
                                    ri,
                                    ErrorKind::Tag,
                                )));
                            }
                            // deal with different functions
                            let kw = match_builtin_keyword(f[0].s).ok_or_else(|| {
                                nom::Err::Error(E::from_error_kind(ri, ErrorKind::Tag))
                            })?;
                            match kw {
                                BuiltinKeyword::Extract => {
                                    let (ri, (unit, expr)) = cut(builtin_args_extract)(ri)?;
                                    let (ri, _) = char_sp0(')')(ri)?; // must terminated with ')'
                                    lhs = Expr::func(
                                        FuncType::Extract,
                                        vec![Expr::FuncArg(ConstArg::DatetimeUnit(unit)), expr],
                                    );
                                    i = ri;
                                }
                                BuiltinKeyword::Substring => {
                                    let (ri, (expr, (start, end))) =
                                        cut(builtin_args_substring)(ri)?;
                                    let (ri, _) = char_sp0(')')(ri)?; // must terminated with ')'
                                    let end = end.unwrap_or(Expr::FuncArg(ConstArg::None));
                                    lhs = Expr::func(FuncType::Substring, vec![expr, start, end]);
                                    i = ri;
                                }
                                BuiltinKeyword::NoKeyword => unreachable!(),
                            };
                            continue;
                        }
                        return Err(nom::Err::Error(E::from_error_kind(i, ErrorKind::Tag)));
                    }
                    InfixOp::Is => {
                        let (ri, _) = spcmt0(i)?;
                        // is null, is not null, is true, is not true, is false, is not false
                        i = ri;
                        let (ri, id) = regular_ident(ri)?;
                        let (ri, _) = spcmt0(ri)?;
                        if id.eq_ignore_ascii_case("not") {
                            i = ri;
                            let (ri, id) = regular_ident(i)?;
                            let (ri, _) = spcmt0(ri)?;
                            let is_op = if let Some(op) = is_op(&id, true) {
                                op
                            } else {
                                return Err(nom::Err::Error(E::from_error_kind(i, ErrorKind::Tag)));
                            };
                            lhs = Expr::pred_is(is_op, lhs);
                            i = ri;
                        } else {
                            let is_op = if let Some(op) = is_op(&id, false) {
                                op
                            } else {
                                return Err(nom::Err::Error(E::from_error_kind(i, ErrorKind::Tag)));
                            };
                            lhs = Expr::pred_is(is_op, lhs);
                            i = ri;
                        }
                        continue;
                    }
                    InfixOp::Binary(bin_op) => {
                        let (ri, _) = spcmt0(i)?;
                        let (ri, rhs) = pratt_expr(ri, r_bp)?;
                        lhs = Expr::binary(bin_op, lhs, rhs);
                        i = ri; // advance input
                    }
                    InfixOp::Predicate(pred_op) => {
                        match pred_op {
                            PredicateOp::Cmp(cmp_op) => {
                                // check quantified comparison first
                                let (ri, _) = spcmt0(i)?;
                                if let Ok((ri, cq)) = cmp_quantifier::<'_, _, E>(ri) {
                                    // <cmp> ANY ( <subquery> )
                                    let (ri, _) = spcmt0(ri)?;
                                    let (ri, sq) = subquery(ri)?;
                                    lhs = Expr::pred_quant_cmp(cmp_op, cq, lhs, sq);
                                    i = ri; // advance input
                                } else {
                                    let (ri, _) = spcmt0(i)?;
                                    let (ri, rhs) = pratt_expr(ri, r_bp)?;
                                    lhs = Expr::pred_cmp(cmp_op, lhs, rhs);
                                    i = ri; // advance input
                                }
                            }
                            PredicateOp::Match(match_op) => {
                                let (ri, _) = spcmt0(i)?;
                                let (ri, rhs) = pratt_expr(ri, r_bp)?;
                                lhs = Expr::pred_match(match_op, lhs, rhs);
                                i = ri; // advance input
                            }
                            PredicateOp::LogicalAnd => {
                                let (ri, _) = spcmt0(i)?;
                                let (ri, rhs) = pratt_expr(ri, r_bp)?;
                                // construct CNF with flatten form
                                if let Expr::Predicate(boxed) = &mut lhs {
                                    if let Predicate::Conj(conj_exprs) = boxed.as_mut() {
                                        conj_exprs.push(rhs);
                                    } else {
                                        lhs = Expr::logical_and(lhs, rhs);
                                    }
                                } else {
                                    lhs = Expr::logical_and(lhs, rhs);
                                }
                                i = ri; // advance input
                            }
                            PredicateOp::LogicalOr => {
                                let (ri, _) = spcmt0(i)?;
                                let (ri, rhs) = pratt_expr(ri, r_bp)?;
                                // construct DNF with flatten form
                                if let Expr::Predicate(boxed) = &mut lhs {
                                    if let Predicate::Disj(disj_exprs) = boxed.as_mut() {
                                        disj_exprs.push(rhs);
                                    } else {
                                        lhs = Expr::logical_or(lhs, rhs);
                                    }
                                } else {
                                    lhs = Expr::logical_or(lhs, rhs);
                                }
                                i = ri;
                            }
                            PredicateOp::LogicalXor => {
                                let (ri, _) = spcmt0(i)?;
                                let (ri, rhs) = pratt_expr(ri, r_bp)?;
                                // construct DNF with flatten form
                                if let Expr::Predicate(boxed) = &mut lhs {
                                    if let Predicate::LogicalXor(xor_exprs) = boxed.as_mut() {
                                        xor_exprs.push(rhs);
                                    } else {
                                        lhs = Expr::logical_xor(lhs, rhs);
                                    }
                                } else {
                                    lhs = Expr::logical_xor(lhs, rhs);
                                }
                                i = ri;
                            }
                            PredicateOp::Between | PredicateOp::NotBetween => {
                                let (ri, _) = spcmt0(i)?;
                                let (ri, mhs) = pratt_expr(ri, r_bp)?;
                                let (ri, _) = spcmt0(ri)?;
                                // must follow AND keyword
                                let (ri, _) = ident_tag("and")(ri)?;
                                let (ri, _) = spcmt0(ri)?;
                                let (ri, rhs) = pratt_expr(ri, r_bp)?;
                                lhs = if pred_op == PredicateOp::Between {
                                    Expr::pred_btw(lhs, mhs, rhs)
                                } else {
                                    Expr::pred_nbtw(lhs, mhs, rhs)
                                };
                                i = ri;
                            }
                            PredicateOp::In | PredicateOp::NotIn => {
                                // tuple and table subquery expression are handled here
                                let (ri, _) = spcmt0(i)?;
                                let (ri, _) = char_sp0('(')(ri)?;
                                // try subquery first
                                match query_expr(ri) {
                                    Ok((ri, q)) => {
                                        let (ri, _) = sp0_char(')')(ri)?; // consume ending ')'
                                        let (ri, _) = spcmt0(ri)?;
                                        lhs = if pred_op == PredicateOp::In {
                                            Expr::pred_in_subquery(lhs, q)
                                        } else {
                                            Expr::pred_nin_subquery(lhs, q)
                                        };
                                        i = ri;
                                        continue;
                                    }
                                    Err(nom::Err::Failure(fail)) => {
                                        return Err(nom::Err::Failure(fail))
                                    }
                                    _ => (), // other error is ok, try tuple next
                                }
                                // try tuple next
                                let (ri, values) =
                                    separated_list1(char(','), preceded(spcmt0, expr_sp0))(ri)?;
                                let (ri, _) = char_sp0(')')(ri)?; // consume ending ')'
                                lhs = if pred_op == PredicateOp::In {
                                    Expr::pred_in_values(lhs, values)
                                } else {
                                    Expr::pred_nin_values(lhs, values)
                                };
                                i = ri;
                            }
                        }
                    }
                }
            }
            Err(_) => {
                // not a token, just break
                // here we do not update input so the caller will continue with
                // the failed position
                break;
            }
        }
    }
    Ok((i, lhs))
}

parse!(
    /// Parse compare quantifier
    fn cmp_quantifier -> CmpQuantifier = {
        alt((
            value(CmpQuantifier::Any, ident_tag("any")),
            value(CmpQuantifier::Any, ident_tag("some")),
            value(CmpQuantifier::All, ident_tag("all")),
        ))
    }
);

parse!(
    fn prefix_expr -> 'a Expr<'a> = {
        alt((
            map(numeric_lit, Expr::numeric_lit),
            map(charstr_lit, Expr::charstr_lit),
            map(bitstr_lit, Expr::bitstr_lit),
            map(hexstr_lit, Expr::hexstr_lit),
            next_cut(terminated(prefix_token, spcmt0), |_, i, tk| match tk {
                PrefixToken::Neg => {
                    let (i, e) = pratt_expr(i, PREFIX_BP_NEG)?;
                    Ok((i, -e))
                }
                PrefixToken::Pos => pratt_expr(i, PREFIX_BP_NEG), // ignore '+'
                PrefixToken::BitInv => {
                    let (i, e) = pratt_expr(i, PREFIX_BP_BIT_INV)?;
                    Ok((i, Expr::bit_inv(e)))
                }
                PrefixToken::Paren => {
                    let (i, _) = spcmt0(i)?;
                    // try subquery first
                    if let Ok((i, q)) = query_expr::<I, E>(i) {
                        let (i, _) = sp0_char(')')(i)?; // consume ending ')'
                        return Ok((i, Expr::scalar_subquery(q)))
                    }
                    // support tuple expression
                    let (i, mut exprs) = separated_list1(char_sp0(','), expr_sp0)(i)?;
                    let (i, _) = sp0_char(')')(i)?; // consume ending ')'
                    if exprs.len() == 1 {
                        Ok((i, exprs.pop().unwrap()))
                    } else {
                        Ok((i, Expr::Tuple(exprs)))
                    }
                }
            }),
            map(aggr_func, Expr::AggrFunc),
            // preceded with regular identifier
            next_cut(terminated(regular_ident, spcmt0), |pi, i: I, id| {
                // literals starting with keyword
                if let Some(kw) = match_reserved_keyword(&id) {
                    match kw {
                        ReservedKeyword::Not => {
                            let (i, operand) = pratt_expr(i, PREFIX_BP_LOGICAL_NOT)?;
                            return Ok((i, Expr::logical_not(operand)))
                        }
                        ReservedKeyword::True => {
                            return Ok((i, Expr::bool_lit(true)))
                        }
                        ReservedKeyword::False => {
                            return Ok((i, Expr::bool_lit(false)))
                        }
                        ReservedKeyword::Null => {
                            return Ok((i, Expr::null_lit()))
                        }
                        ReservedKeyword::Interval => {
                            return map(interval_lit, |v| Expr::Literal(Literal::Interval(v)))(i)
                        }
                        ReservedKeyword::Exists => { // exists expression
                            let (i, _) = char_sp0('(')(i)?;
                            let (i, q) = query_expr(i)?;
                            let (i, _) = sp0_char(')')(i)?; // consume ending ')'
                            return Ok((i, Expr::exists(q)))
                        }
                        ReservedKeyword::Case => { // case expression
                            if ident_tag::<'_, I, E>("when")(i).is_ok() { // precheck when is ok
                                return map(
                                    pair(many1(case_branch), case_end),
                                    |(branches, fallback)| Expr::case_when(None, branches, fallback.map(Box::new)),
                                )(i)
                            }
                            // switch-like structure
                            let (i, operand) = expr_sp0(i)?;
                            let (i, (branches, fallback)) = pair(many1(case_branch), case_end)(i)?;
                            return Ok((i, Expr::case_when(Some(Box::new(operand)), branches, fallback.map(Box::new))))
                        }
                        _ => return Err(nom::Err::Error(E::from_error_kind(pi, ErrorKind::Tag)))
                    }
                }
                if id.eq_ignore_ascii_case("timestamp") {
                    return map(charstr_part, Expr::timestamp_lit)(i)
                }
                if id.eq_ignore_ascii_case("date") {
                    return map(charstr_part, Expr::date_lit)(i)
                }
                if id.eq_ignore_ascii_case("time") {
                    return map(charstr_part, Expr::time_lit)(i)
                }
                // can only be identifier
                let (i, ids) = fold_many0(preceded(char_sp0('.'), terminated(ident, spcmt0)), || vec![Ident::regular(id)], |mut ids, id| {
                    ids.push(id);
                    ids
                })(i)?;
                Ok((i, Expr::column_ref(ids)))
            }),
            next_cut(idents, |_, i, ids| {
                // check if match reserved keyword
                Ok((i, Expr::column_ref(ids)))
            }),
        ))
    }
);

parse!(
    /// Parse an prefix token.
    fn prefix_token -> PrefixToken = {
        alt((
            value(PrefixToken::Neg, char('-')),
            value(PrefixToken::Pos, char('+')),
            value(PrefixToken::BitInv, char('~')),
            value(PrefixToken::Paren, char('(')),
        ))
    }
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InfixOp {
    Binary(BinaryOp),
    Predicate(PredicateOp),
    Is,
    Call,
}

parse!(
    /// Parse an infix token.
    /// The order in alt list is sufficient.
    fn infix_op -> InfixOp = {
        alt((
            value(InfixOp::Binary(BinaryOp::Add), char('+')),
            value(InfixOp::Binary(BinaryOp::Sub), char('-')),
            value(InfixOp::Binary(BinaryOp::Mul), char('*')),
            value(InfixOp::Binary(BinaryOp::Div), char('/')),
            value(InfixOp::Binary(BinaryOp::BitAnd), char('&')),
            value(InfixOp::Binary(BinaryOp::BitOr), char('|')),
            value(InfixOp::Binary(BinaryOp::BitXor), char('^')),
            value(InfixOp::Binary(BinaryOp::BitShl), tag("<<")),
            value(InfixOp::Binary(BinaryOp::BitShr), tag(">>")),
            value(InfixOp::Predicate(PredicateOp::Cmp(CompareOp::Equal)), char('=')),
            value(InfixOp::Predicate(PredicateOp::Cmp(CompareOp::NotEqual)), tag("!=")),
            value(InfixOp::Predicate(PredicateOp::Cmp(CompareOp::NotEqual)), tag("<>")),
            value(InfixOp::Predicate(PredicateOp::Match(MatchOp::SafeEqual)), tag("<=>")),
            value(InfixOp::Predicate(PredicateOp::Cmp(CompareOp::GreaterEqual)), tag(">=")),
            value(InfixOp::Predicate(PredicateOp::Cmp(CompareOp::Greater)), char('>')),
            value(InfixOp::Predicate(PredicateOp::Cmp(CompareOp::LessEqual)), tag("<=")),
            value(InfixOp::Predicate(PredicateOp::Cmp(CompareOp::Less)), char('<')),
            value(InfixOp::Call, char('(')),
            ident_infix_op,
            preceded(
                terminated(ident_tag("not"), spcmt0),
                map_opt(regular_ident, |id: I| {
                    if let Some(kw) = match_reserved_keyword(&id) {
                        let res = match kw {
                            ReservedKeyword::Like => InfixOp::Predicate(PredicateOp::Match(MatchOp::NotLike)),
                            ReservedKeyword::Regexp => InfixOp::Predicate(PredicateOp::Match(MatchOp::NotRegexp)),
                            ReservedKeyword::In => InfixOp::Predicate(PredicateOp::NotIn),
                            ReservedKeyword::Between => InfixOp::Predicate(PredicateOp::NotBetween),
                            _ => return None
                        };
                        return Some(res)
                    }
                    None
                })
            )
        ))
    }
);

parse!(
    /// Parse textual infix token.
    fn ident_infix_op -> InfixOp = {
        map_opt(regular_ident, |id: I| {
            if let Some(kw) = match_reserved_keyword(&id) {
                let res = match kw {
                    ReservedKeyword::And => InfixOp::Predicate(PredicateOp::LogicalAnd),
                    ReservedKeyword::Or => InfixOp::Predicate(PredicateOp::LogicalOr),
                    ReservedKeyword::Xor => InfixOp::Predicate(PredicateOp::LogicalXor),
                    ReservedKeyword::Is => InfixOp::Is,
                    ReservedKeyword::Like => InfixOp::Predicate(PredicateOp::Match(MatchOp::Like)),
                    ReservedKeyword::Regexp => InfixOp::Predicate(PredicateOp::Match(MatchOp::Regexp)),
                    ReservedKeyword::In => InfixOp::Predicate(PredicateOp::In),
                    ReservedKeyword::Between => InfixOp::Predicate(PredicateOp::Between),
                    _ => return None
                };
                return Some(res)
            }
            None
        })
    }
);

parse!(
    /// Parse a list of identifiers.
    /// ```bnf
    /// <idents> ::= <ident> [ <period>  <ident> ]
    /// ```
    fn idents -> 'a Vec<Ident<'a>> = {
        separated_list1(
            char_sp0('.'),
            terminated(
                alt((
                    map_opt(regular_ident, |id: I| if is_reserved_keyword(&id) { None } else { Some(Ident::regular(id))} ),
                    map(quoted_ident, |id: I| Ident::quoted(id)),
                )),
                spcmt0,
            )
        )
    }
);

parse!(
    /// Parse a set(aggregate) function.
    /// ```bnf
    /// <aggr_func> ::= <aggr_func_type> <left_paren> [ <set_quantifier> ] <value_expr> <right_paren>
    /// ```
    fn aggr_func -> 'a AggrFunc<'a> = {
        alt((
            value(
                AggrFunc::count_asterisk(),
                pair(
                    terminated(ident_tag("count"), spcmt0),
                    delimited(
                        char_sp0('('),
                        char('*'),
                        sp0_char(')'),
                    ),
                )),
            map(
                pair(
                    terminated(aggr_func_kind, spcmt0),
                    cut(delimited(
                        char_sp0('('),
                        pair(opt(terminated(set_quantifier, spcmt1)), expr_sp0),
                        char(')'),
                    )),
                ),
                |(kind, (opt_q, expr))| {
                    let q = opt_q.unwrap_or(SetQuantifier::All);
                    let expr = Box::new(expr);
                    AggrFunc{kind, q, expr}
                },
            ),
        ))
    }
);

parse!(
    /// Parse a set function type.
    /// ```bnf
    /// <aggr_func_type> ::= COUNT | SUM | AVG | MAX | MIN
    /// ```
    fn aggr_func_kind -> AggrFuncKind = {
        alt((
            value(AggrFuncKind::Count, ident_tag("count")),
            value(AggrFuncKind::Sum, ident_tag("sum")),
            value(AggrFuncKind::Avg, ident_tag("avg")),
            value(AggrFuncKind::Min, ident_tag("min")),
            value(AggrFuncKind::Max, ident_tag("max")),
        ))
    }
);

parse!(
    /// Parse EXTRACT function arguments
    fn builtin_args_extract -> 'a (DatetimeUnit, Expr<'a>) = {
        pair(
            datetime_unit,
            preceded(preceded(spcmt1, ident_tag("from")), preceded(spcmt0, expr_sp0)),
        )
    }
);

parse!(
    /// Parse SUBSTRING function arguments
    fn builtin_args_substring -> 'a (Expr<'a>, (Expr<'a>, Option<Expr<'a>>)) = {
        pair(
            expr_sp0,
            alt((
                preceded(
                    char_sp0(','),
                    pair(
                        expr_sp0,
                        opt(preceded(char_sp0(','), expr_sp0)),
                    ),
                ),
                preceded(
                    terminated(ident_tag("from"), spcmt0),
                    pair(
                        expr_sp0,
                        alt((
                            value(None, peek(char(')'))),
                            map(preceded(ident_tag("for"), preceded(spcmt0, expr_sp0)), Some),
                        ))
                    )
                )
            ))
        )
    }
);

parse!(
    /// Parse a numeric literal.
    fn numeric_lit = recognize_float
);

parse!(
    fn charstr_lit -> 'a CharStr<'a> = {
        map(
            pair(
                map(terminated(charstr_part, spcmt0), Into::into),
                many0(map(terminated(charstr_part, spcmt0), Into::into)),
            ),
            |(first, rest)| CharStr{charset: None, first, rest}, // todo: implements charset
        )
    }
);

parse!(
    /// Parse a character string part.
    /// ```bnf
    /// <char_str_part> ::= <quote> [ <char_representation> ... ] <quote>
    /// ```
    fn charstr_part = {
        const DELIMITER: char = '\'';
        const SYMBOL: &str = "''";
        delimited(
            char(DELIMITER),
            recognize(fold_many0(
                alt((take_till1(|c| c == DELIMITER), tag(SYMBOL))), || (), |_, _| ())),
            cut(char(DELIMITER)),
        )
    }
);

parse!(
    /// Parse a initial bit string part.
    /// ```bnf
    /// <bit_str_init_part> ::= B<quote> [ <bit> ... ] <quote>
    /// ```
    fn bitstr_lit = {
        const INIT_DELIMITER: &str = "b'";
        const DELIMITER: char = '\'';
        delimited(
            tag_no_case(INIT_DELIMITER),
            cut(recognize(fold_many0(one_of("01"), || (), |_, _| ()))),
            cut(char(DELIMITER)),
        )
    }
);

parse!(
    /// Parse a interval literal
    fn interval_lit -> 'a Interval<'a> = {
        map(
            pair(
                terminated(charstr_part, spcmt0),
                datetime_unit,
            ),
            |(value, unit): (I, _)| Interval{unit, value: value.into()},
        )
    }
);

parse!(
    fn datetime_unit -> DatetimeUnit = {
        terminated(
            alt((
                value(DatetimeUnit::Year, ident_tag("year")),
                value(DatetimeUnit::Quarter, ident_tag("quarter")),
                value(DatetimeUnit::Month, ident_tag("month")),
                value(DatetimeUnit::Week, ident_tag("week")),
                value(DatetimeUnit::Day, ident_tag("day")),
                value(DatetimeUnit::Hour, ident_tag("hour")),
                value(DatetimeUnit::Minute, ident_tag("minute")),
                value(DatetimeUnit::Second, ident_tag("second")),
                value(DatetimeUnit::Microsecond, ident_tag("microsecond")),
            )),
            peek(not(alphanumeric1))
        )
    }
);

parse!(
    /// Parse an initial hex string part.
    /// ```bnf
    /// <bit_str_init_part> ::= B<quote> [ <bit> ... ] <quote>
    /// ```
    fn hexstr_lit = {
        const INIT_DELIMITER: &str = "x'";
        const DELIMITER: char = '\'';
        delimited(
            tag_no_case(INIT_DELIMITER),
            hex_digit0,
            cut(char(DELIMITER)),
        )
    }
);

parse!(
    /// Parse 'when ... then ...' part of case when
    fn case_branch -> 'a (Expr<'a>, Expr<'a>) = {
        pair(
            preceded(ident_tag("when"), preceded(spcmt0, expr_sp0)),
            preceded(ident_tag("then"), preceded(spcmt0, expr_sp0)),
        )
    }
);

parse!(
    /// Parse 'else ... end' part of case when
    fn case_end -> 'a Option<Expr<'a>> = {
        terminated(
            opt(preceded(ident_tag("else"), preceded(spcmt0, expr_sp0))),
            ident_tag("end"))
    }
);

pub(crate) fn char_sp0<'a, I: ParseInput<'a>, E: ParseError<I>>(
    c: char,
) -> impl Fn(I) -> IResult<I, char, E> + use<I, E> {
    move |input: I| terminated(char(c), spcmt0)(input)
}

pub(crate) fn sp0_char<'a, I: ParseInput<'a>, E: ParseError<I>>(
    c: char,
) -> impl Fn(I) -> IResult<I, char, E> + use<I, E> {
    move |input: I| preceded(spcmt0, char(c))(input)
}
