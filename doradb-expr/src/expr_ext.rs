use crate::expr::{Col, ColKind, ExprKind};
use crate::id::QueryID;
use std::collections::HashSet;
use std::ops::ControlFlow;

pub trait Effect: Default {
    fn merge(&mut self, other: Self);
}

impl Effect for () {
    #[inline]
    fn merge(&mut self, _other: Self) {}
}

pub trait ExprVisitor<'a>: Sized {
    type Cont: Effect;
    type Break;
    /// Returns true if continue
    #[inline]
    fn enter(&mut self, _e: &'a ExprKind) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }

    /// Returns true if continue
    #[inline]
    fn leave(&mut self, _e: &'a ExprKind) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }
}

pub trait ExprMutVisitor {
    type Cont: Effect;
    type Break;
    /// Returns true if continue
    #[inline]
    fn enter(&mut self, _e: &mut ExprKind) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }

    /// Returns true if continue
    #[inline]
    fn leave(&mut self, _e: &mut ExprKind) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }
}

pub struct CollectQryIDs<'a>(pub &'a mut HashSet<QueryID>);

impl ExprVisitor<'_> for CollectQryIDs<'_> {
    type Cont = ();
    type Break = ();
    #[inline]
    fn leave(&mut self, e: &ExprKind) -> ControlFlow<()> {
        if let ExprKind::Col(Col {
            kind: ColKind::Query(qry_id),
            ..
        }) = &e
        {
            self.0.insert(*qry_id);
        }
        ControlFlow::Continue(())
    }
}

/// Extended opeartions on ExprKind.
pub trait ExprExt {
    fn walk<'a, V: ExprVisitor<'a>>(&'a self, visitor: &mut V) -> ControlFlow<V::Break, V::Cont>;

    fn walk_mut<V: ExprMutVisitor>(&mut self, visitor: &mut V) -> ControlFlow<V::Break, V::Cont>;

    fn collect_non_aggr_cols(&self) -> (Vec<Col>, bool);

    fn collect_non_aggr_cols_into(&self, cols: &mut Vec<Col>) -> bool;

    fn contains_aggr_func(&self) -> bool;

    fn contains_non_aggr_cols(&self) -> bool;

    fn collect_qry_ids(&self, hs: &mut HashSet<QueryID>);
}

impl ExprExt for ExprKind {
    fn walk<'a, V: ExprVisitor<'a>>(&'a self, visitor: &mut V) -> ControlFlow<V::Break, V::Cont> {
        let mut eff = visitor.enter(self)?;
        for c in self.args() {
            eff.merge(c.walk(visitor)?)
        }
        eff.merge(visitor.leave(self)?);
        ControlFlow::Continue(eff)
    }

    fn walk_mut<V: ExprMutVisitor>(&mut self, visitor: &mut V) -> ControlFlow<V::Break, V::Cont> {
        let mut eff = visitor.enter(self)?;
        for c in self.args_mut() {
            eff.merge(c.walk_mut(visitor)?)
        }
        eff.merge(visitor.leave(self)?);
        ControlFlow::Continue(eff)
    }

    // collect non-aggr columns and whether any aggr columns found.
    #[inline]
    fn collect_non_aggr_cols(&self) -> (Vec<Col>, bool) {
        let mut cols = vec![];
        let has_aggr = self.collect_non_aggr_cols_into(&mut cols);
        (cols, has_aggr)
    }

    /// Collect non-aggr columns and returns true if aggr function exists
    #[inline]
    fn collect_non_aggr_cols_into(&self, cols: &mut Vec<Col>) -> bool {
        struct Collect<'a> {
            aggr_lvl: usize,
            has_aggr: bool,
            cols: &'a mut Vec<Col>,
        }
        impl ExprVisitor<'_> for Collect<'_> {
            type Cont = ();
            type Break = ();
            #[inline]
            fn enter(&mut self, e: &ExprKind) -> ControlFlow<()> {
                match e {
                    ExprKind::Aggf { .. } => {
                        self.aggr_lvl += 1;
                        self.has_aggr = true
                    }
                    ExprKind::Col(col) => {
                        if self.aggr_lvl == 0 {
                            self.cols.push(col.clone())
                        }
                    }
                    _ => (),
                }
                ControlFlow::Continue(())
            }

            #[inline]
            fn leave(&mut self, e: &ExprKind) -> ControlFlow<()> {
                if let ExprKind::Aggf { .. } = e {
                    self.aggr_lvl -= 1
                }
                ControlFlow::Continue(())
            }
        }
        let mut c = Collect {
            aggr_lvl: 0,
            cols,
            has_aggr: false,
        };
        let _ = self.walk(&mut c);
        c.has_aggr
    }

    #[inline]
    fn contains_aggr_func(&self) -> bool {
        struct Contains(bool);
        impl ExprVisitor<'_> for Contains {
            type Cont = ();
            type Break = ();
            #[inline]
            fn enter(&mut self, e: &ExprKind) -> ControlFlow<()> {
                if let ExprKind::Aggf { .. } = e {
                    self.0 = true;
                    return ControlFlow::Break(());
                }
                ControlFlow::Continue(())
            }
        }
        let mut c = Contains(false);
        let _ = self.walk(&mut c);
        c.0
    }

    #[inline]
    fn contains_non_aggr_cols(&self) -> bool {
        struct Contains {
            aggr_lvl: usize,
            has_non_aggr_cols: bool,
        }

        impl ExprVisitor<'_> for Contains {
            type Cont = ();
            type Break = ();
            #[inline]
            fn enter(&mut self, e: &ExprKind) -> ControlFlow<()> {
                match e {
                    ExprKind::Aggf { .. } => self.aggr_lvl += 1,
                    ExprKind::Col(_) => {
                        if self.aggr_lvl == 0 {
                            self.has_non_aggr_cols = true;
                            return ControlFlow::Break(());
                        }
                    }
                    _ => (),
                }
                ControlFlow::Continue(())
            }

            #[inline]
            fn leave(&mut self, e: &ExprKind) -> ControlFlow<()> {
                if let ExprKind::Aggf { .. } = e {
                    self.aggr_lvl -= 1
                }
                ControlFlow::Continue(())
            }
        }

        let mut c = Contains {
            aggr_lvl: 0,
            has_non_aggr_cols: false,
        };
        let _ = self.walk(&mut c);
        c.has_non_aggr_cols
    }

    #[inline]
    fn collect_qry_ids(&self, hs: &mut HashSet<QueryID>) {
        let mut c = CollectQryIDs(hs);
        let _ = self.walk(&mut c);
    }
}
