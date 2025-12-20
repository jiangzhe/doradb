macro_rules! parse {
    // parse ref
    ($(#[$attr:meta])* fn $f:ident -> $a:tt $ty:ty = $body:expr) => {
        $(#[$attr])*
        pub(crate) fn $f<$a, I: ParseInput<$a>, E: ParseError<I>>(i: I) -> IResult<I, $ty, E> {
            ($body)(i)
        }
    };
    // parse own
    ($(#[$attr:meta])* fn $f:ident -> $ty:ty = $body:expr) => {
        $(#[$attr])*
        pub(crate) fn $f<'a, I: ParseInput<'a>, E: ParseError<I>>(i: I) -> IResult<I, $ty, E> {
            ($body)(i)
        }
    };
    // parse str
    ($(#[$attr:meta])* fn $f:ident = $body:expr) => {
        $(#[$attr])*
        pub(crate) fn $f<'a, I: ParseInput<'a>, E: ParseError<I>>(i: I) -> IResult<I, I, E> {
            ($body)(i)
        }
    }
}

macro_rules! define_bp {
    ($id:ident = $l_bp:literal, $r_bp:literal) => {
        const $id: (BindingPower, BindingPower) = ($l_bp, $r_bp);
    };
    ($id:ident = $bp:literal) => {
        const $id: BindingPower = $bp;
    };
}

macro_rules! reserved_keywords {
    ($(#[$attr:meta])* $($lit:literal => $ident:ident),*) => {
        $(#[$attr])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum ReservedKeyword {
            NoKeyword,
            $($ident),*
        }

        #[static_init::dynamic]
        static RESERVED_KEYWORDS: fnv::FnvHashMap<CastAsciiLowerCase<'static>, ReservedKeyword> = {
            let mut map = fnv::FnvHashMap::default();
            $(
                map.insert(CastAsciiLowerCase($lit), ReservedKeyword::$ident);
            )*
            map
        };
    }
}

macro_rules! builtin_keywords {
    ($(#[$attr:meta])* $($lit:literal => $ident:ident),*) => {
        $(#[$attr])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum BuiltinKeyword {
            NoKeyword,
            $($ident),*
        }

        #[static_init::dynamic]
        static BUILTIN_KEYWORDS: fnv::FnvHashMap<CastAsciiLowerCase<'static>, BuiltinKeyword> = {
            let mut map = fnv::FnvHashMap::default();
            $(
                map.insert(CastAsciiLowerCase($lit), BuiltinKeyword::$ident);
            )*
            map
        };
    }
}

macro_rules! define_dialect {
    ($ty:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $ty<'a>(pub &'a str);

        impl<'a> Deref for $ty<'a> {
            type Target = str;

            // Always delegate string methods to its input part.
            fn deref(&self) -> &'a str {
                self.0
            }
        }

        impl<'a> From<$ty<'a>> for &'a str {
            fn from(src: $ty<'a>) -> Self {
                src.0
            }
        }

        impl AsBytes for $ty<'_> {
            #[inline]
            fn as_bytes(&self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl<'a, 'b> Compare<&'b str> for $ty<'a> {
            #[inline]
            fn compare(&self, t: &'b str) -> CompareResult {
                self.0.compare(t)
            }

            #[inline]
            fn compare_no_case(&self, t: &'b str) -> CompareResult {
                self.0.compare_no_case(t)
            }
        }

        impl<'a, 'b> Compare<&'b [u8]> for $ty<'a> {
            #[inline]
            fn compare(&self, t: &'b [u8]) -> CompareResult {
                self.0.compare(t)
            }

            #[inline]
            fn compare_no_case(&self, t: &'b [u8]) -> CompareResult {
                self.0.compare_no_case(t)
            }
        }

        impl<'a, 'b> Compare<$ty<'b>> for &'a [u8] {
            #[inline]
            fn compare(&self, t: $ty<'b>) -> CompareResult {
                self.compare(t.0)
            }

            #[inline]
            fn compare_no_case(&self, t: $ty<'b>) -> CompareResult {
                self.compare_no_case(t.0)
            }
        }

        impl<'a, 'b> Compare<$ty<'b>> for &'a str {
            #[inline]
            fn compare(&self, t: $ty<'b>) -> CompareResult {
                self.compare(t.0)
            }

            #[inline]
            fn compare_no_case(&self, t: $ty<'b>) -> CompareResult {
                self.compare_no_case(t.0)
            }
        }

        impl<'a, 'b> FindSubstring<&'b str> for $ty<'a> {
            fn find_substring(&self, substr: &'b str) -> Option<usize> {
                self.0.find_substring(substr)
            }
        }

        impl<'a> FindToken<char> for $ty<'a> {
            #[inline]
            fn find_token(&self, token: char) -> bool {
                self.0.find_token(token)
            }
        }

        impl<'a> InputIter for $ty<'a> {
            type Item = char;
            type Iter = CharIndices<'a>;
            type IterElem = Chars<'a>;
            #[inline]
            fn iter_indices(&self) -> Self::Iter {
                self.0.char_indices()
            }

            #[inline]
            fn iter_elements(&self) -> Self::IterElem {
                self.0.iter_elements()
            }

            fn position<P>(&self, predicate: P) -> Option<usize>
            where
                P: Fn(Self::Item) -> bool,
            {
                self.0.position(predicate)
            }

            #[inline]
            fn slice_index(&self, count: usize) -> Result<usize, Needed> {
                self.0.slice_index(count)
            }
        }

        impl<'a> InputLength for $ty<'a> {
            #[inline]
            fn input_len(&self) -> usize {
                self.0.input_len()
            }
        }

        impl<'a> InputTake for $ty<'a> {
            #[inline]
            fn take(&self, count: usize) -> Self {
                $ty(&self.0[..count])
            }

            #[inline]
            fn take_split(&self, count: usize) -> (Self, Self) {
                let (r0, r1) = self.0.take_split(count);
                ($ty(r0), $ty(r1))
            }
        }

        impl<'a> InputTakeAtPosition for $ty<'a> {
            type Item = char;

            fn split_at_position<P, E: ParseError<Self>>(
                &self,
                predicate: P,
            ) -> IResult<Self, Self, E>
            where
                P: Fn(Self::Item) -> bool,
            {
                match self.0.find(predicate) {
                    Some(i) => unsafe {
                        Ok((
                            $ty(self.0.get_unchecked(i..)),
                            $ty(self.0.get_unchecked(..i)),
                        ))
                    },
                    None => Err(Err::Incomplete(Needed::new(1))),
                }
            }

            fn split_at_position1<P, E: ParseError<Self>>(
                &self,
                predicate: P,
                e: nom::error::ErrorKind,
            ) -> IResult<Self, Self, E>
            where
                P: Fn(Self::Item) -> bool,
            {
                match self.0.find(predicate) {
                    Some(0) => Err(Err::Error(E::from_error_kind(*self, e))),
                    Some(i) => unsafe {
                        Ok((
                            $ty(self.0.get_unchecked(i..)),
                            $ty(self.0.get_unchecked(..i)),
                        ))
                    },
                    None => Err(Err::Incomplete(Needed::new(1))),
                }
            }

            fn split_at_position_complete<P, E: ParseError<Self>>(
                &self,
                predicate: P,
            ) -> IResult<Self, Self, E>
            where
                P: Fn(Self::Item) -> bool,
            {
                match self.0.find(predicate) {
                    Some(i) => unsafe {
                        Ok((
                            $ty(self.0.get_unchecked(i..)),
                            $ty(self.0.get_unchecked(..i)),
                        ))
                    },
                    None => unsafe {
                        Ok((
                            $ty(self.0.get_unchecked(self.0.len()..)),
                            $ty(self.0.get_unchecked(..self.0.len())),
                        ))
                    },
                }
            }

            fn split_at_position1_complete<P, E: ParseError<Self>>(
                &self,
                predicate: P,
                e: nom::error::ErrorKind,
            ) -> IResult<Self, Self, E>
            where
                P: Fn(Self::Item) -> bool,
            {
                match self.0.find(predicate) {
                    Some(0) => Err(Err::Error(E::from_error_kind(*self, e))),
                    Some(i) => unsafe {
                        Ok((
                            $ty(self.0.get_unchecked(i..)),
                            $ty(self.0.get_unchecked(..i)),
                        ))
                    },
                    None => {
                        if self.0.is_empty() {
                            Err(Err::Error(E::from_error_kind($ty(self.0), e)))
                        } else {
                            unsafe {
                                Ok((
                                    $ty(self.0.get_unchecked(self.0.len()..)),
                                    $ty(self.0.get_unchecked(..self.0.len())),
                                ))
                            }
                        }
                    }
                }
            }
        }

        impl<'a> Slice<Range<usize>> for $ty<'a> {
            #[inline]
            fn slice(&self, range: Range<usize>) -> Self {
                $ty(&self.0[range])
            }
        }

        impl<'a> Slice<RangeFrom<usize>> for $ty<'a> {
            #[inline]
            fn slice(&self, range: RangeFrom<usize>) -> Self {
                $ty(self.0.slice(range))
            }
        }

        impl<'a> Slice<RangeTo<usize>> for $ty<'a> {
            #[inline]
            fn slice(&self, range: RangeTo<usize>) -> Self {
                $ty(self.0.slice(range))
            }
        }

        impl<'a> Slice<RangeFull> for $ty<'a> {
            #[inline]
            fn slice(&self, range: RangeFull) -> Self {
                $ty(self.0.slice(range))
            }
        }

        impl<'a> Offset for $ty<'a> {
            fn offset(&self, second: &Self) -> usize {
                self.0.offset(second.0)
            }
        }
    };
}

macro_rules! impl_kw_str {
    ($id:ident) => {
        impl crate::ast::KeywordString for $id {
            fn kw_str(&self, upper: bool) -> &'static str {
                if upper {
                    return self.upper_str();
                }
                self.lower_str()
            }
        }
    };
    ($id:ty) => {
        impl crate::ast::KeywordString for $id {
            fn kw_str(&self, upper: bool) -> &'static str {
                if upper {
                    return self.upper_str();
                }
                self.lower_str()
            }
        }
    };
}
