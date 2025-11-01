use crate::mysql::serde::{LenEncInt, LenEncStr, SerdeCtx};
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, transmute};

/// Defines how to serialize self to bytes.
/// The purpose of serialization is to transfer via network.
/// So method signature defines a fixed-sized buffer and
/// a continue flag.
/// The object its self has to maintain splitted bytes if buffer
/// bound is encountered.
/// All MySQL commands and events should implement this trait.
pub trait MySer<'s> {
    /// length of serialized bytes.
    fn my_len(&self, ctx: &SerdeCtx) -> usize;

    /// Serialize object into fix-sized byte slice.
    /// The buffer is guaranteed to be big enough.
    fn my_ser(&mut self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize;

    /// Serialize partial object into fixed-sized byte slice, due to
    /// insufficient space of the buffer.
    ///
    /// Note: this method should only be called if output buffer is smaller
    /// than the total serialization bytes. Otherwise, call [`MySerialize::my_ser`]
    /// method.
    fn my_ser_partial(&mut self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize);

    /// Try to serialize object into fix-sized byte slice.
    #[inline]
    fn try_my_ser(&mut self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> Option<usize> {
        if self.my_len(ctx) + start_idx > out.len() {
            self.my_ser_partial(ctx, out, start_idx);
            return None;
        }
        Some(self.my_ser(ctx, out, start_idx))
    }
}

pub trait NewMySer {
    type Ser<'a>: MySer<'a>
    where
        Self: 'a;

    /// Construct a serializer.
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_>;
}

impl NewMySer for [u8] {
    type Ser<'s>
        = MySerPackets<'s, 1>
    where
        Self: 's;

    #[inline]
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_> {
        MySerPackets::new(ctx, [MySerElem::slice(self)])
    }
}

pub struct MySerPayload<'a, const N: usize> {
    // elements that are serialized into payload
    elems: [MySerElem<'a>; N],
    // index of elements to serialize.
    idx: usize,
}

impl<'a, const N: usize> MySer<'a> for MySerPayload<'a, N> {
    #[inline]
    fn my_len(&self, ctx: &SerdeCtx) -> usize {
        self.elems.iter().map(|e| e.my_len(ctx)).sum()
    }

    #[inline]
    fn my_ser(&mut self, ctx: &SerdeCtx, out: &mut [u8], mut start_idx: usize) -> usize {
        for elem in &mut self.elems[self.idx..] {
            start_idx = elem.my_ser(ctx, out, start_idx);
            self.idx += 1;
        }
        start_idx
    }

    #[inline]
    fn my_ser_partial(&mut self, ctx: &SerdeCtx, out: &mut [u8], mut start_idx: usize) {
        // let mut total_len = 0;
        for elem in &mut self.elems[self.idx..] {
            let len = elem.my_len(ctx);
            if start_idx + len > out.len() {
                elem.my_ser_partial(ctx, out, start_idx);
                return;
            }
            start_idx = elem.my_ser(ctx, out, start_idx);
            self.idx += 1;
        }
    }
}

pub struct MySerPackets<'a, const N: usize> {
    // total bytes of all packets.
    packet_total: usize,
    // remained bytes of current packet, including header.
    packet_rem: usize,
    // payload of packets.
    payload: MySerPayload<'a, N>,
    // whether to add header or not.
    header: Option<MySerElem<'static>>,
    // number of result packets,
    // this field should be only accessed after serialization.
    pub res_pkts: usize,
}

impl<'a, const N: usize> MySerPackets<'a, N> {
    /// Create new MySQL packets.
    /// The payload may be splitted if its length exceeds maximum
    /// packet size.
    #[inline]
    pub fn new(ctx: &SerdeCtx, elems: [MySerElem<'a>; N]) -> Self {
        let payload = MySerPayload { elems, idx: 0 };
        let payload_total = payload.my_len(ctx);
        let packet_total = calc_packet_total(payload_total, ctx.max_payload_size);
        // first payload length
        let (packet_rem, header) = calc_packet_rem_and_header(packet_total, ctx, 0);
        MySerPackets {
            packet_total,
            packet_rem,
            payload,
            header: Some(header),
            res_pkts: 1,
        }
    }

    #[inline]
    fn ser_header_partial(
        &mut self,
        ctx: &SerdeCtx,
        mut header: MySerElem<'static>,
        out: &mut [u8],
        start_idx: usize,
    ) {
        let outlen = out.len() - start_idx;
        header.my_ser_partial(ctx, out, start_idx);
        self.packet_total -= outlen;
        self.packet_rem -= outlen;
        // now we need to save header back, so next time
        // we can finish the deserialization.
        self.header = Some(header);
    }

    #[inline]
    fn ser_header(
        &mut self,
        ctx: &SerdeCtx,
        mut header: MySerElem<'static>,
        out: &mut [u8],
        mut start_idx: usize,
    ) -> usize {
        let header_len = header.my_len(ctx);
        start_idx = header.my_ser(ctx, out, start_idx);
        self.packet_total -= header_len;
        self.packet_rem -= header_len;
        start_idx
    }

    #[inline]
    fn ser_payload_partial(&mut self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) {
        let outlen = out.len() - start_idx;
        self.payload.my_ser_partial(ctx, out, start_idx);
        self.packet_total -= outlen;
        self.packet_rem -= outlen;
    }

    // serialize payload in case max packet size is reached
    // but buffer is still available.
    #[inline]
    fn ser_payload_rem(&mut self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let packet_rem = self.packet_rem;
        self.payload
            .my_ser_partial(ctx, &mut out[..start_idx + packet_rem], start_idx);
        self.packet_total -= self.packet_rem;
        self.packet_rem = 0;
        start_idx + packet_rem
    }

    #[inline]
    fn ser_payload(&mut self, ctx: &SerdeCtx, out: &mut [u8], mut start_idx: usize) -> usize {
        start_idx = self.payload.my_ser(ctx, out, start_idx);
        self.packet_total = 0;
        self.packet_rem = 0;
        start_idx
    }
}

/// Calculate totoal bytes of MySQL packets.
/// For one packet, it includes 4-byte header and payload.
#[inline]
fn calc_packet_total(payload_total: usize, max_payload_size: usize) -> usize {
    let n_pkts = payload_total / max_payload_size;
    // even if last packet has 0-sized payload, we still need to write packet header
    // so that client could be nofitied the splitting is completed.
    let last_payload_size = payload_total % max_payload_size;
    n_pkts * (max_payload_size + 4) + 4 + last_payload_size
}

#[inline]
fn calc_packet_rem_and_header(
    packet_total: usize,
    ctx: &SerdeCtx,
    pkts: usize,
) -> (usize, MySerElem<'static>) {
    debug_assert!(ctx.max_payload_size <= 0xffffff); // maximum value of u24
    let packet_rem = packet_total.min(ctx.max_payload_size + 4);
    let mut bs = ((packet_rem - 4) as u32).to_le_bytes();
    bs[3] = (ctx.pkt_nr as usize + pkts) as u8;
    let header = MySerElem::inline_bytes(&bs);
    (packet_rem, header)
}

impl<'a, const N: usize> MySer<'a> for MySerPackets<'a, N> {
    #[inline]
    fn my_len(&self, _ctx: &SerdeCtx) -> usize {
        self.packet_total
    }

    #[inline]
    fn my_ser(&mut self, ctx: &SerdeCtx, out: &mut [u8], mut start_idx: usize) -> usize {
        if let Some(header) = self.header.take() {
            // write header: 3-byte payload length + 1-byte packet number
            start_idx = self.ser_header(ctx, header, out, start_idx);
        }
        loop {
            // serialize payload
            if self.packet_rem < self.packet_total {
                start_idx = self.ser_payload_rem(ctx, out, start_idx);
                // calculate header and payload of next packet
                let (packet_rem, header) =
                    calc_packet_rem_and_header(self.packet_total, ctx, self.res_pkts);
                self.res_pkts += 1;
                self.packet_rem = packet_rem;
                start_idx = self.ser_header(ctx, header, out, start_idx);
            } else {
                start_idx = self.ser_payload(ctx, out, start_idx);
                break;
            }
        }
        start_idx
    }

    #[inline]
    fn my_ser_partial(&mut self, ctx: &SerdeCtx, out: &mut [u8], mut start_idx: usize) {
        if let Some(header) = self.header.take() {
            let header_len = header.my_len(ctx);
            if start_idx + header_len > out.len() {
                // too small to serialize header
                self.ser_header_partial(ctx, header, out, start_idx);
                return;
            }
            start_idx = self.ser_header(ctx, header, out, start_idx);
        }
        // serialize payload
        loop {
            // check if current packet can fit buffer.
            if start_idx + self.packet_rem > out.len() {
                // buffer is too small
                self.ser_payload_partial(ctx, out, start_idx);
                return;
            }
            // current packet can fit buffer, but payload is not ended.
            start_idx = self.ser_payload_rem(ctx, out, start_idx);
            // now we prepare next packet and write header.
            let (packet_rem, header) =
                calc_packet_rem_and_header(self.packet_total, ctx, self.res_pkts);
            self.res_pkts += 1;
            self.packet_rem = packet_rem;
            let header_len = header.my_len(ctx);
            if start_idx + header_len > out.len() {
                // too small to serialize header
                self.ser_header_partial(ctx, header, out, start_idx);
                return;
            }
            // enough space to serialize header
            start_idx = self.ser_header(ctx, header, out, start_idx);
        }
    }
}

/// Kind of MySerElem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MySerKind {
    /// nothing to serialize
    Empty = 0,
    /// inline bytes to serialize, maximum length is 14.
    /// the layout is:
    /// 1b-code, 1b-offset, 14b-data
    InlineBytes = 1,
    /// byte slice.
    /// the layout is:
    /// 1b-code, 3b-filler, 4b-length, 8b-pointer
    /// note that when serializing, the pointer will move
    /// forward and length will decrease.
    Slice = 2,
    /// length encoded slice(les), 0 ~ 65535 bytes.
    /// if length is no more than 251(0xfa) bytes,
    /// the length int is 1 byte.
    /// Otherwise, the length int is 3 bytes. (0xfc + u16)
    /// the layout is:
    /// 1b-code, 1b-length-offset, 2b-offset, 3b-length, 1b-filler, 8b-pointer
    ///
    /// Note that: if the length is no more than 13 bytes. we
    /// can store it in [`MySerKind::InlineBytes`] mode.
    LenEncSlice3 = 3,
    /// larger length encoded slice(les), length is 65536 ~ 16777215.
    /// length int is 4 bytes. (0xfd + u24)
    /// the layout is:
    /// 1b-code, 1b-length-offset, 2b-filler, 4b-offset, 8b-pointer
    LenEncSlice4 = 4,
    /// larger length encoded slice(les), length is 16777215 ~ 4294967295.
    /// length int is 9 bytes. (0xfe + u64)
    /// the layout is:
    /// 1b-code, 1b-length-offset, 2b-filler, 4b-offset, 8b-pointer
    LenEncSlice9 = 5,
    /// Filler with 0 bytes. There is no meaning of such bytes, just placeholder.
    Filler = 6,
    /// Null ended slice.
    NullEndSlice = 7,
    /// Slice with prefix length 1 byte. So at most 255 bytes.
    Prefix1BSlice = 8,
    /// Owned buffer
    OwnedBuffer = 9,
}

/// MySerElem is the base elements to serialize objects to MySQL packets.
/// Rust's enum type is not space efficient, so replace it with self
/// tuned types.
#[derive(Debug)]
#[repr(C, align(8))]
pub struct MySerElem<'a> {
    kind: MySerKind,
    data: [u8; 15],
    _marker: PhantomData<&'a [u8]>,
}

impl<'a> MySerElem<'a> {
    /// Create an empty element.
    #[inline]
    pub fn empty() -> Self {
        MySerElem {
            kind: MySerKind::Empty,
            data: [0; 15],
            _marker: PhantomData,
        }
    }

    /// Create a one-byte element.
    /// Backed by inline bytes representation.
    #[inline]
    pub fn one_byte(v: u8) -> Self {
        Self::inline_bytes(&[v])
    }

    /// Create a little-endian u16 element.
    #[inline]
    pub fn le_u16(v: u16) -> Self {
        Self::inline_bytes(&v.to_le_bytes())
    }

    /// Create a filler element.
    /// All bytes of the filler will be zeroed.
    #[inline]
    pub fn filler(len: u32) -> Self {
        let f = Filler {
            kind: MySerKind::Filler,
            _filler1: [0u8; 3],
            len,
            _filler2: [0u8; 8],
        };
        unsafe { transmute(f) }
    }

    /// Create an inline bytes serializable element.
    #[inline]
    pub fn inline_bytes(b: &[u8]) -> Self {
        assert!(b.len() <= 14);
        let mut data = [0u8; 14];
        let idx = data.len() - b.len();
        data[idx..].copy_from_slice(b);
        let ib = InlineBytes {
            kind: MySerKind::InlineBytes,
            idx: idx as u8,
            data,
        };
        unsafe { transmute(ib) }
    }

    #[inline]
    pub fn buffer(mut b: Vec<u8>) -> Self {
        assert!(b.len() <= 0xffffff);
        b.shrink_to_fit();
        let b = ManuallyDrop::new(b);
        let ptr = b.as_ptr();
        let len = b.len();
        let ob = OwnedBuffer {
            kind: MySerKind::OwnedBuffer,
            len: [len as u8, (len >> 8) as u8, (len >> 16) as u8],
            start_idx: 0,
            ptr,
        };
        unsafe { transmute(ob) }
    }

    /// Create an inline len-enc-str.
    #[inline]
    fn inline_len_enc_str(b: &[u8]) -> Self {
        assert!(b.len() <= 13);
        let mut data = [0u8; 14];
        let idx = 13 - b.len();
        data[idx] = b.len() as u8;
        data[idx + 1..].copy_from_slice(b);
        let ib = InlineBytes {
            kind: MySerKind::InlineBytes,
            idx: idx as u8,
            data,
        };
        unsafe { transmute(ib) }
    }

    /// Create a len-enc-int serializable element.
    /// Backed by [`MySerKind::InlineBytes`]
    #[inline]
    pub fn len_enc_int(v: LenEncInt) -> Self {
        match v {
            LenEncInt::Null => MySerElem::inline_bytes(&[0xfb]),
            LenEncInt::Err => MySerElem::inline_bytes(&[0xff]),
            LenEncInt::Len1(v) => MySerElem::inline_bytes(&[v]),
            LenEncInt::Len3(v) => MySerElem::inline_bytes(&[0xfc, v as u8, (v >> 8) as u8]),
            LenEncInt::Len4(v) => {
                MySerElem::inline_bytes(&[0xfd, v as u8, (v >> 8) as u8, (v >> 16) as u8])
            }
            LenEncInt::Len9(v) => {
                let mut b = [0u8; 9];
                b[0] = 0xfe;
                b[1..].copy_from_slice(&v.to_le_bytes());
                MySerElem::inline_bytes(&b)
            }
        }
    }

    /// Create a null-end-str serializable element.
    #[inline]
    pub fn null_end_str(s: &'a [u8]) -> Self {
        assert!(s.len() <= 0xfffffffe); // last byte is '\0', so maximum length is max int - 1
        if s.len() <= 13 {
            // use InlineBytes mode
            let mut data = [0u8; 14];
            let datalen = data.len();
            let idx = datalen - 1 - s.len();
            data[idx..datalen - 1].copy_from_slice(s);
            data[datalen - 1] = 0;
            let ib = InlineBytes {
                kind: MySerKind::InlineBytes,
                idx: idx as u8,
                data,
            };
            return unsafe { transmute::<InlineBytes, MySerElem<'_>>(ib) };
        }
        let slice = NullEndSlice {
            kind: MySerKind::NullEndSlice,
            null_to_fill: true,
            _filler: [0; 2],
            end_offset: s.len() as u32,
            ptr: s.as_ptr(),
            _marker: PhantomData,
        };
        unsafe { transmute(slice) }
    }

    /// Create a prefix-1-byte-str element.
    #[inline]
    pub fn prefix_1b_str(s: &'a [u8]) -> Self {
        assert!(s.len() <= 0xff);
        if s.len() <= 13 {
            // use InlineBytes mode
            let mut data = [0u8; 14];
            let idx = data.len() - s.len();
            data[idx..].copy_from_slice(s);
            data[idx - 1] = s.len() as u8;
            let ib = InlineBytes {
                kind: MySerKind::InlineBytes,
                idx: (idx - 1) as u8,
                data,
            };
            return unsafe { transmute::<InlineBytes, MySerElem<'_>>(ib) };
        }
        let p1bs = Prefix1BSlice {
            kind: MySerKind::Prefix1BSlice,
            prefix_to_fill: true,
            end_offset: s.len() as u8,
            _filler: [0u8; 5],
            ptr: s.as_ptr(),
            _marker: PhantomData,
        };
        unsafe { transmute(p1bs) }
    }

    #[inline]
    fn as_inline_bytes(&self) -> &InlineBytes {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_slice(&self) -> &Slice<'_> {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_les3(&self) -> &LenEncSlice3<'_> {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_les4(&self) -> &LenEncSlice4<'_> {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_les9(&self) -> &LenEncSlice9<'_> {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_filler(&self) -> &Filler {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_nes(&self) -> &NullEndSlice<'_> {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_p1bs(&self) -> &Prefix1BSlice<'_> {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_ob(&self) -> &OwnedBuffer {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_inline_bytes_mut(&mut self) -> &mut InlineBytes {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_slice_mut(&mut self) -> &mut Slice<'_> {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_les3_mut(&mut self) -> &mut LenEncSlice3<'_> {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_les4_mut(&mut self) -> &mut LenEncSlice4<'_> {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_les9_mut(&mut self) -> &mut LenEncSlice9<'_> {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_filler_mut(&mut self) -> &mut Filler {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_nes_mut(&mut self) -> &mut NullEndSlice<'_> {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_p1bs_mut(&mut self) -> &mut Prefix1BSlice<'_> {
        unsafe { transmute(self) }
    }

    #[inline]
    fn as_ob_mut(&mut self) -> &mut OwnedBuffer {
        unsafe { transmute(self) }
    }

    #[inline]
    pub fn slice(s: &'a [u8]) -> Self {
        assert!(s.len() <= 0xffffffff);
        if s.len() <= 14 {
            return Self::inline_bytes(s);
        }
        let slice = Slice {
            kind: MySerKind::Slice,
            _filler: [0; 3],
            end_offset: s.len() as u32,
            ptr: s.as_ptr(),
            _marker: PhantomData,
        };
        unsafe { transmute(slice) }
    }

    #[inline]
    pub fn len_enc_str(s: impl Into<LenEncStr<'a>>) -> Self {
        match s.into() {
            LenEncStr::Null => Self::one_byte(0xfb),
            LenEncStr::Err => Self::inline_bytes(&[0xff]),
            LenEncStr::Bytes(s) => {
                if s.len() <= 13 {
                    // 1b prefix + 13b data
                    Self::inline_len_enc_str(s)
                } else if s.len() < 0xfb {
                    // LenEncStr::Len1
                    let les = LenEncSlice3 {
                        kind: MySerKind::LenEncSlice3,
                        len_idx: 2,
                        end_offset: s.len() as u16,
                        len: [0, 0, s.len() as u8],
                        _filler: 0,
                        ptr: s.as_ptr(),
                        _marker: PhantomData,
                    };
                    unsafe { transmute::<LenEncSlice3<'_>, MySerElem<'_>>(les) }
                } else if s.len() <= 0xffff {
                    // LenEncStr::Len3
                    let les = LenEncSlice3 {
                        kind: MySerKind::LenEncSlice3,
                        len_idx: 0,
                        end_offset: s.len() as u16,
                        len: [0xfc, s.len() as u8, (s.len() >> 8) as u8],
                        _filler: 0,
                        ptr: s.as_ptr(),
                        _marker: PhantomData,
                    };
                    unsafe { transmute::<LenEncSlice3<'_>, MySerElem<'_>>(les) }
                } else if s.len() <= 0xffffff {
                    let les = LenEncSlice4 {
                        kind: MySerKind::LenEncSlice4,
                        len_idx: 0,
                        _filler: [0; 2],
                        end_offset: s.len() as u32,
                        ptr: s.as_ptr(),
                        _marker: PhantomData,
                    };
                    unsafe { transmute::<LenEncSlice4<'_>, MySerElem<'_>>(les) }
                } else if s.len() <= 0xffffffff {
                    let les = LenEncSlice9 {
                        kind: MySerKind::LenEncSlice9,
                        len_idx: 0,
                        _filler: [0; 2],
                        end_offset: s.len() as u32,
                        ptr: s.as_ptr(),
                        _marker: PhantomData,
                    };
                    unsafe { transmute::<LenEncSlice9<'_>, MySerElem<'_>>(les) }
                } else {
                    // currently do not support string longer than 4GB.
                    panic!("string to long")
                }
            }
        }
    }
}

impl PartialEq for MySerElem<'_> {
    #[inline]
    fn eq(&self, rhs: &Self) -> bool {
        if self.kind != rhs.kind {
            return false;
        }
        match self.kind {
            MySerKind::Empty => true,
            MySerKind::InlineBytes => {
                let this = self.as_inline_bytes();
                let that = rhs.as_inline_bytes();
                this.data() == that.data()
            }
            MySerKind::Slice => {
                let this = self.as_slice();
                let that = rhs.as_slice();
                this.data() == that.data()
            }
            MySerKind::LenEncSlice3 => {
                let this = self.as_les3();
                let that = self.as_les3();
                this.lendata() == that.lendata() && this.data() == that.data()
            }
            MySerKind::LenEncSlice4 => {
                let this = self.as_les4();
                let that = self.as_les4();
                this.lendata() == that.lendata() && this.data() == that.data()
            }
            MySerKind::LenEncSlice9 => {
                let this = self.as_les9();
                let that = self.as_les9();
                this.lendata() == that.lendata() && this.data() == that.data()
            }
            MySerKind::Filler => {
                let this = self.as_filler();
                let that = rhs.as_filler();
                this.len == that.len
            }
            MySerKind::NullEndSlice => {
                let this = self.as_nes();
                let that = rhs.as_nes();
                this.null_to_fill == that.null_to_fill
                    && this.end_offset == that.end_offset
                    && this.data() == that.data()
            }
            MySerKind::Prefix1BSlice => {
                let this = self.as_p1bs();
                let that = rhs.as_p1bs();
                this.prefix_to_fill == that.prefix_to_fill
                    && this.end_offset == that.end_offset
                    && this.data() == that.data()
            }
            MySerKind::OwnedBuffer => {
                let this = self.as_ob();
                let that = rhs.as_ob();
                this.data() == that.data()
            }
        }
    }
}

impl Drop for MySerElem<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.kind == MySerKind::OwnedBuffer {
            unsafe {
                let ob: &mut OwnedBuffer = transmute(self);
                let len = ob.len();
                let vec = Vec::from_raw_parts(ob.ptr as *mut u8, len, len);
                drop(vec);
            }
        }
    }
}

impl<'a> MySer<'a> for MySerElem<'a> {
    #[inline]
    fn my_len(&self, _ctx: &SerdeCtx) -> usize {
        match self.kind {
            MySerKind::Empty => 0,
            MySerKind::InlineBytes => {
                let ib = self.as_inline_bytes();
                ib.data().len()
            }
            MySerKind::Slice => {
                let s = self.as_slice();
                s.data().len()
            }
            MySerKind::LenEncSlice3 => {
                let les3 = self.as_les3();
                les3.lendata().len() + les3.data().len()
            }
            MySerKind::LenEncSlice4 => {
                let les4 = self.as_les4();
                (4 - les4.len_idx) as usize + les4.data().len()
            }
            MySerKind::LenEncSlice9 => {
                let les9 = self.as_les9();
                (9 - les9.len_idx) as usize + les9.data().len()
            }
            MySerKind::Filler => {
                let filler = self.as_filler();
                filler.len as usize
            }
            MySerKind::NullEndSlice => {
                let nes = self.as_nes();
                usize::from(nes.null_to_fill) + nes.data().len()
            }
            MySerKind::Prefix1BSlice => {
                let p1bs = self.as_p1bs();
                usize::from(p1bs.prefix_to_fill) + p1bs.data().len()
            }
            MySerKind::OwnedBuffer => {
                let ob = self.as_ob();
                ob.len() - ob.start_idx as usize
            }
        }
    }

    #[inline]
    fn my_ser(&mut self, _ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        match self.kind {
            MySerKind::Empty => start_idx,
            MySerKind::InlineBytes => {
                let ib = self.as_inline_bytes_mut();
                let data = ib.data();
                let datalen = data.len();
                out[start_idx..start_idx + datalen].copy_from_slice(data);
                ib.advance(datalen);
                start_idx + datalen
            }
            MySerKind::Slice => {
                let s = self.as_slice_mut();
                let data = s.data();
                let datalen = data.len();
                out[start_idx..start_idx + datalen].copy_from_slice(data);
                s.advance(datalen);
                start_idx + datalen
            }
            MySerKind::LenEncSlice3 => {
                let les = self.as_les3_mut();
                // append len int
                let lendata = les.lendata();
                let lendatalen = lendata.len();
                out[start_idx..start_idx + lendatalen].copy_from_slice(lendata);
                // append data
                let data = les.data();
                let datalen = data.len();
                out[start_idx + lendatalen..start_idx + lendatalen + datalen].copy_from_slice(data);
                les.advance_len(lendatalen);
                les.advance(datalen);
                start_idx + lendatalen + datalen
            }
            MySerKind::LenEncSlice4 => {
                let les = self.as_les4_mut();
                // append len int
                let (lendata, lendataidx) = les.lendata();
                let lendatalen = lendata.len() - lendataidx;
                out[start_idx..start_idx + lendatalen].copy_from_slice(&lendata[lendataidx..]);
                // append data
                let data = les.data();
                let datalen = data.len();
                out[start_idx + lendatalen..start_idx + lendatalen + datalen].copy_from_slice(data);
                les.advance_len(lendatalen);
                les.advance(datalen);
                start_idx + lendatalen + datalen
            }
            MySerKind::LenEncSlice9 => {
                let les = self.as_les9_mut();
                // append len int
                let (lendata, lendataidx) = les.lendata();
                let lendatalen = lendata.len() - lendataidx;
                out[start_idx..start_idx + lendatalen].copy_from_slice(&lendata[lendataidx..]);
                // append data
                let data = les.data();
                let datalen = data.len();
                out[start_idx + lendatalen..start_idx + lendatalen + datalen].copy_from_slice(data);
                les.advance_len(lendatalen);
                les.advance(datalen);
                start_idx + lendatalen + datalen
            }
            MySerKind::Filler => {
                let filler = self.as_filler_mut();
                let flen = filler.len as usize;
                out[start_idx..start_idx + flen].fill(0);
                filler.len = 0;
                start_idx + flen
            }
            MySerKind::NullEndSlice => {
                let nes = self.as_nes_mut();
                let data = nes.data();
                let datalen = data.len();
                out[start_idx..start_idx + datalen].copy_from_slice(data);
                out[start_idx + datalen] = 0;
                nes.advance(datalen);
                nes.advance_null();
                start_idx + datalen + 1
            }
            MySerKind::Prefix1BSlice => {
                let p1bs = self.as_p1bs_mut();
                if p1bs.prefix_to_fill {
                    if out.is_empty() {
                        return start_idx;
                    }
                    out[start_idx] = p1bs.end_offset;
                    let data = p1bs.data();
                    let datalen = data.len();
                    out[start_idx + 1..start_idx + datalen + 1].copy_from_slice(data);
                    p1bs.advance_prefix();
                    p1bs.advance(datalen);
                    return start_idx + datalen + 1;
                }
                // prefix already filled
                let data = p1bs.data();
                let datalen = data.len();
                out[start_idx..start_idx + datalen].copy_from_slice(data);
                p1bs.advance(datalen);
                start_idx + datalen
            }
            MySerKind::OwnedBuffer => {
                let ob = self.as_ob_mut();
                let data = ob.data();
                let datalen = data.len();
                out[start_idx..start_idx + datalen].copy_from_slice(data);
                ob.advance(datalen);
                start_idx + datalen
            }
        }
    }

    #[inline]
    fn my_ser_partial(&mut self, _ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) {
        match self.kind {
            MySerKind::Empty => (),
            MySerKind::InlineBytes => {
                let ib = self.as_inline_bytes_mut();
                let data = ib.data();
                let outlen = out.len() - start_idx;
                out[start_idx..].copy_from_slice(&data[..outlen]);
                ib.advance(outlen);
            }
            MySerKind::Slice => {
                let s = self.as_slice_mut();
                let data = s.data();
                let outlen = out.len() - start_idx;
                out[start_idx..].copy_from_slice(&data[..outlen]);
                s.advance(outlen);
            }
            MySerKind::LenEncSlice3 => {
                let les = self.as_les3_mut();
                let outlen = out.len() - start_idx;
                let lendata = les.lendata();
                let lendatalen = lendata.len();
                if lendatalen >= outlen {
                    out[start_idx..].copy_from_slice(&lendata[..outlen]);
                    les.advance_len(outlen);
                    return;
                }
                out[start_idx..start_idx + lendatalen].copy_from_slice(lendata);
                les.advance_len(lendatalen);
                let data = les.data();
                out[start_idx + lendatalen..].copy_from_slice(&data[..outlen - lendatalen]);
                les.advance(outlen - lendatalen);
            }
            MySerKind::LenEncSlice4 => {
                let les = self.as_les4_mut();
                let outlen = out.len() - start_idx;
                let (lendata, lendataidx) = les.lendata();
                let lendatalen = lendata.len() - lendataidx;
                if lendatalen >= outlen {
                    out[start_idx..].copy_from_slice(&lendata[lendataidx..lendataidx + outlen]);
                    les.advance_len(outlen);
                    return;
                }
                out[start_idx..start_idx + lendatalen].copy_from_slice(&lendata[lendataidx..]);
                les.advance_len(lendatalen);
                let data = les.data();
                out[start_idx + lendatalen..].copy_from_slice(&data[..outlen - lendatalen]);
                les.advance(outlen);
            }
            MySerKind::LenEncSlice9 => {
                let les = self.as_les9_mut();
                let outlen = out.len() - start_idx;
                let (lendata, lendataidx) = les.lendata();
                let lendatalen = lendata.len() - lendataidx;
                if lendatalen >= outlen {
                    out[start_idx + lendatalen..]
                        .copy_from_slice(&lendata[lendataidx..lendataidx + outlen]);
                    les.advance_len(outlen);
                    return;
                }
                out[start_idx..start_idx + lendatalen].copy_from_slice(&lendata[lendataidx..]);
                les.advance_len(lendatalen);
                let data = les.data();
                out[start_idx + lendatalen..].copy_from_slice(&data[..outlen - lendatalen]);
                les.advance(outlen);
            }
            MySerKind::Filler => {
                let filler = self.as_filler_mut();
                let outlen = out.len() - start_idx;
                out[start_idx..].fill(0);
                filler.len -= outlen as u32;
            }
            MySerKind::NullEndSlice => {
                let nes = self.as_nes_mut();
                let data = nes.data();
                let datalen = data.len();
                let outlen = out.len() - start_idx;
                if datalen >= outlen {
                    out[start_idx..].copy_from_slice(&data[..outlen]);
                    nes.advance(outlen);
                    return;
                }
                out[start_idx..start_idx + datalen].copy_from_slice(data);
                nes.advance(datalen);
                out[start_idx + datalen] = 0;
                nes.advance_null();
            }
            MySerKind::Prefix1BSlice => {
                let p1bs = self.as_p1bs_mut();
                let outlen = out.len() - start_idx;
                if p1bs.prefix_to_fill {
                    if outlen == 0 {
                        return;
                    }
                    out[start_idx] = p1bs.end_offset;
                    p1bs.advance_prefix();
                    let data = p1bs.data();
                    out[start_idx + 1..].copy_from_slice(&data[..outlen - 1]);
                    p1bs.advance(outlen - 1);
                    return;
                }
                // prefix already filled
                let data = p1bs.data();
                out[start_idx..].copy_from_slice(&data[..outlen]);
                p1bs.advance(outlen);
            }
            MySerKind::OwnedBuffer => {
                let ob = self.as_ob_mut();
                let data = ob.data();
                let outlen = out.len() - start_idx;
                out[start_idx..].copy_from_slice(&data[..outlen]);
                ob.advance(outlen);
            }
        }
    }
}

/// Utility to write primitives and mysql codec.
/// This trait is designed to serialize objects to
/// growable container that won't fail, so no error
/// will return.
pub trait MySerExt {
    fn ser_u8(&mut self, n: u8) -> &mut Self;

    fn ser_le_u16(&mut self, n: u16) -> &mut Self;

    fn ser_le_u24(&mut self, n: u32) -> &mut Self;

    fn ser_le_u32(&mut self, n: u32) -> &mut Self;

    fn ser_le_u48(&mut self, n: u64) -> &mut Self;

    fn ser_le_u64(&mut self, n: u64) -> &mut Self;

    fn ser_le_u128(&mut self, n: u128) -> &mut Self;

    fn ser_le_f32(&mut self, n: f32) -> &mut Self;

    fn ser_le_f64(&mut self, n: f64) -> &mut Self;

    fn ser_len_enc_int(&mut self, n: LenEncInt) -> &mut Self;

    fn ser_len_enc_str(&mut self, s: LenEncStr) -> &mut Self;

    fn ser_null_end_str(&mut self, s: &[u8]) -> &mut Self;

    fn ser_bytes(&mut self, bs: &[u8]) -> &mut Self;
}

impl MySerExt for [u8] {
    #[inline]
    fn ser_u8(&mut self, n: u8) -> &mut Self {
        self[0] = n;
        &mut self[1..]
    }

    #[inline]
    fn ser_le_u16(&mut self, n: u16) -> &mut Self {
        self[..2].copy_from_slice(&n.to_le_bytes());
        &mut self[2..]
    }

    #[inline]
    fn ser_le_u24(&mut self, n: u32) -> &mut Self {
        self[..3].copy_from_slice(&n.to_le_bytes()[..3]);
        &mut self[3..]
    }

    #[inline]
    fn ser_le_u32(&mut self, n: u32) -> &mut Self {
        self[..4].copy_from_slice(&n.to_le_bytes());
        &mut self[4..]
    }

    #[inline]
    fn ser_le_u48(&mut self, n: u64) -> &mut Self {
        self[..6].copy_from_slice(&n.to_le_bytes()[..6]);
        &mut self[6..]
    }

    #[inline]
    fn ser_le_u64(&mut self, n: u64) -> &mut Self {
        self[..8].copy_from_slice(&n.to_le_bytes());
        &mut self[8..]
    }

    #[inline]
    fn ser_le_u128(&mut self, n: u128) -> &mut Self {
        self[..16].copy_from_slice(&n.to_le_bytes());
        &mut self[16..]
    }

    #[inline]
    fn ser_le_f32(&mut self, n: f32) -> &mut Self {
        self[..4].copy_from_slice(&n.to_le_bytes());
        &mut self[4..]
    }

    #[inline]
    fn ser_le_f64(&mut self, n: f64) -> &mut Self {
        self[..8].copy_from_slice(&n.to_le_bytes());
        &mut self[8..]
    }

    #[inline]
    fn ser_len_enc_int(&mut self, n: LenEncInt) -> &mut Self {
        match n {
            LenEncInt::Null => {
                self[0] = 0xfb;
                &mut self[1..]
            }
            LenEncInt::Err => {
                self[0] = 0xff;
                &mut self[1..]
            }
            LenEncInt::Len1(v) => {
                self[0] = v;
                &mut self[1..]
            }
            LenEncInt::Len3(v) => {
                self[0] = 0xfc;
                self[1..].ser_le_u16(v);
                &mut self[3..]
            }
            LenEncInt::Len4(v) => {
                self[0] = 0xfd;
                self[1..].ser_le_u24(v);
                &mut self[4..]
            }
            LenEncInt::Len9(v) => {
                self[0] = 0xfe;
                self[1..].ser_le_u64(v);
                &mut self[9..]
            }
        }
    }

    #[inline]
    fn ser_len_enc_str(&mut self, s: LenEncStr) -> &mut Self {
        match s {
            LenEncStr::Null => {
                self[0] = 0xfb;
                &mut self[1..]
            }
            LenEncStr::Err => {
                self[0] = 0xff;
                &mut self[1..]
            }
            LenEncStr::Bytes(s) => {
                let lei = LenEncInt::from(s.len() as u64);
                let next = self.ser_len_enc_int(lei);
                next.ser_bytes(s)
            }
        }
    }

    #[inline]
    fn ser_null_end_str(&mut self, s: &[u8]) -> &mut Self {
        let next = self.ser_bytes(s);
        next[0] = 0;
        &mut next[1..]
    }

    #[inline]
    fn ser_bytes(&mut self, bs: &[u8]) -> &mut Self {
        self[..bs.len()].copy_from_slice(bs);
        &mut self[bs.len()..]
    }
}

trait SerSlice {
    fn data(&self) -> &[u8];

    fn advance(&mut self, len: usize);
}

#[repr(C, align(8))]
struct InlineBytes {
    kind: MySerKind,
    idx: u8,
    data: [u8; 14],
}

impl SerSlice for InlineBytes {
    #[inline]
    fn data(&self) -> &[u8] {
        &self.data[self.idx as usize..]
    }

    #[inline]
    fn advance(&mut self, len: usize) {
        self.idx += len as u8;
    }
}

#[repr(C, align(8))]
struct Slice<'a> {
    kind: MySerKind,
    _filler: [u8; 3],
    end_offset: u32,
    ptr: *const u8,
    _marker: PhantomData<&'a [u8]>,
}

impl SerSlice for Slice<'_> {
    #[inline]
    fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.end_offset as usize) }
    }

    #[inline]
    fn advance(&mut self, len: usize) {
        self.end_offset -= len as u32;
        unsafe {
            self.ptr = self.ptr.add(len);
        }
    }
}

/// Buffer is owned bytes.
/// maximum length is u24::MAX.
#[repr(C, align(8))]
struct OwnedBuffer {
    kind: MySerKind,
    len: [u8; 3],
    start_idx: u32,
    ptr: *const u8,
}

impl OwnedBuffer {
    #[inline]
    fn len(&self) -> usize {
        self.len[0] as usize | ((self.len[1] as usize) << 8) | ((self.len[2] as usize) << 16)
    }
}

impl SerSlice for OwnedBuffer {
    #[inline]
    fn data(&self) -> &[u8] {
        let len = self.len();
        let len = len - self.start_idx as usize;
        unsafe { std::slice::from_raw_parts(self.ptr.add(self.start_idx as usize), len) }
    }

    #[inline]
    fn advance(&mut self, len: usize) {
        self.start_idx += len as u32;
    }
}

#[repr(C, align(8))]
struct LenEncSlice3<'a> {
    kind: MySerKind,
    len_idx: u8,
    end_offset: u16,
    len: [u8; 3],
    _filler: u8,
    ptr: *const u8,
    _marker: PhantomData<&'a [u8]>,
}

impl SerSlice for LenEncSlice3<'_> {
    #[inline]
    fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.end_offset as usize) }
    }

    #[inline]
    fn advance(&mut self, len: usize) {
        self.end_offset -= len as u16;
        unsafe {
            self.ptr = self.ptr.add(len);
        }
    }
}

impl LenEncSlice3<'_> {
    #[inline]
    fn lendata(&self) -> &[u8] {
        &self.len[self.len_idx as usize..]
    }

    #[inline]
    fn advance_len(&mut self, len: usize) {
        self.len_idx += len as u8;
    }
}

#[repr(C, align(8))]
struct LenEncSlice4<'a> {
    kind: MySerKind,
    len_idx: u8,
    _filler: [u8; 2],
    end_offset: u32,
    ptr: *const u8,
    _marker: PhantomData<&'a [u8]>,
}

impl SerSlice for LenEncSlice4<'_> {
    #[inline]
    fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.end_offset as usize) }
    }

    #[inline]
    fn advance(&mut self, len: usize) {
        self.end_offset -= len as u32;
        unsafe {
            self.ptr = self.ptr.add(len);
        }
    }
}

impl LenEncSlice4<'_> {
    #[inline]
    fn lendata(&self) -> ([u8; 4], usize) {
        (
            (0xfd | (self.end_offset << 8)).to_le_bytes(),
            self.len_idx as usize,
        )
    }

    #[inline]
    fn advance_len(&mut self, len: usize) {
        self.len_idx += len as u8;
    }
}

#[repr(C, align(8))]
struct LenEncSlice9<'a> {
    kind: MySerKind,
    len_idx: u8,
    _filler: [u8; 2],
    end_offset: u32,
    ptr: *const u8,
    _marker: PhantomData<&'a [u8]>,
}

impl SerSlice for LenEncSlice9<'_> {
    #[inline]
    fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.end_offset as usize) }
    }

    #[inline]
    fn advance(&mut self, len: usize) {
        self.end_offset -= len as u32;
        unsafe {
            self.ptr = self.ptr.add(len);
        }
    }
}

impl LenEncSlice9<'_> {
    #[inline]
    fn lendata(&self) -> ([u8; 9], usize) {
        let mut ld = [0u8; 9];
        ld[0] = 0xfe;
        ld[1..].copy_from_slice(&(self.end_offset as u64).to_le_bytes());
        (ld, self.len_idx as usize)
    }

    #[inline]
    fn advance_len(&mut self, len: usize) {
        self.len_idx += len as u8;
    }
}

#[repr(C, align(8))]
struct Filler {
    kind: MySerKind,
    _filler1: [u8; 3],
    len: u32,
    _filler2: [u8; 8],
}

#[repr(C, align(8))]
struct NullEndSlice<'a> {
    kind: MySerKind,
    null_to_fill: bool,
    _filler: [u8; 2],
    end_offset: u32,
    ptr: *const u8,
    _marker: PhantomData<&'a [u8]>,
}

impl SerSlice for NullEndSlice<'_> {
    #[inline]
    fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.end_offset as usize) }
    }

    #[inline]
    fn advance(&mut self, len: usize) {
        self.end_offset -= len as u32;
        unsafe {
            self.ptr = self.ptr.add(len);
        }
    }
}

impl NullEndSlice<'_> {
    #[inline]
    fn advance_null(&mut self) {
        self.null_to_fill = false;
    }
}

#[repr(C, align(8))]
struct Prefix1BSlice<'a> {
    kind: MySerKind,
    prefix_to_fill: bool,
    end_offset: u8,
    _filler: [u8; 5],
    ptr: *const u8,
    _marker: PhantomData<&'a [u8]>,
}

impl SerSlice for Prefix1BSlice<'_> {
    #[inline]
    fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.end_offset as usize) }
    }

    #[inline]
    fn advance(&mut self, len: usize) {
        self.end_offset -= len as u8;
        unsafe {
            self.ptr = self.ptr.add(len);
        }
    }
}

impl Prefix1BSlice<'_> {
    #[inline]
    fn advance_prefix(&mut self) {
        self.prefix_to_fill = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::serde::MyDeserExt;

    #[test]
    fn test_len_enc_int() {
        for (orig, expected) in vec![
            (vec![0x0a_u8], LenEncInt::Len1(0x0a)),
            (vec![0xfc_u8, 0xfd, 0x00], LenEncInt::Len3(0xfd_u16)),
            (vec![0xfc_u8, 0x1d, 0x05], LenEncInt::Len3(0x051d_u16)),
            (
                vec![0xfd_u8, 0xc2, 0xb2, 0xa2],
                LenEncInt::Len4(0xa2b2c2_u32),
            ),
            (
                vec![0xfe, 0x0d, 0x0c, 0x0b, 0x0a, 0x04, 0x03, 0x02, 0x01],
                LenEncInt::Len9(0x010203040a0b0c0d_u64),
            ),
            (vec![0xff_u8], LenEncInt::Err),
            (vec![0xfb_u8], LenEncInt::Null),
        ] {
            let bs = &mut &orig[..];
            let lei = bs.deser_len_enc_int();
            assert_eq!(expected, lei);
            // write
            let mut encoded = vec![0u8; 32];
            let rest = encoded.ser_len_enc_int(lei);
            let new_len = rest.len();
            assert_eq!(orig, encoded[..encoded.len() - new_len]);
        }
    }

    #[test]
    fn test_len_enc_str() {
        // read
        let orig = b"\x05hello";
        let bs = &mut &orig[..];
        let les = bs.deser_len_enc_str();
        assert_eq!(LenEncStr::Bytes(b"hello"), les);
        // write
        let mut encoded = vec![0u8; 32];
        encoded.ser_len_enc_str(les);
        assert_eq!(orig, &encoded[..6]);
    }

    #[test]
    fn test_bytes_len_enc_str_invalid() {
        let fail = std::panic::catch_unwind(|| {
            let orig = b"\x05hell";
            let bs = &mut &orig[..];
            bs.deser_len_enc_str();
        });
        assert!(fail.is_err());
    }

    #[test]
    fn test_ser_elem_size_and_align() {
        use std::mem::{align_of, size_of};
        let size = size_of::<MySerElem>();
        let align = align_of::<MySerElem>();
        println!("MySerElem: size={}, align={}", size, align);
        assert_eq!(size, 16);
        assert_eq!(align, 8);
        let size = size_of::<InlineBytes>();
        let align = align_of::<InlineBytes>();
        println!("InlineBytes: size={}, align={}", size, align);
        assert_eq!(size, 16);
        assert_eq!(align, 8);
        let size = size_of::<Slice>();
        let align = align_of::<Slice>();
        println!("Slice: size={}, align={}", size, align);
        assert_eq!(size, 16);
        assert_eq!(align, 8);
        let size = size_of::<LenEncSlice3>();
        let align = align_of::<LenEncSlice3>();
        println!("LenEncSlice3: size={}, align={}", size, align);
        assert_eq!(size, 16);
        assert_eq!(align, 8);
        let size = size_of::<LenEncSlice4>();
        let align = align_of::<LenEncSlice4>();
        println!("LenEncSlice4: size={}, align={}", size, align);
        assert_eq!(size, 16);
        assert_eq!(align, 8);
        let size = size_of::<LenEncSlice9>();
        let align = align_of::<LenEncSlice9>();
        println!("LenEncSlice9: size={}, align={}", size, align);
        assert_eq!(size, 16);
        assert_eq!(align, 8);
        let size = size_of::<NullEndSlice>();
        let align = align_of::<NullEndSlice>();
        println!("NullEndSlice: size={}, align={}", size, align);
        assert_eq!(size, 16);
        assert_eq!(align, 8);
        let size = size_of::<Filler>();
        let align = align_of::<Filler>();
        println!("Filler: size={}, align={}", size, align);
        assert_eq!(size, 16);
        assert_eq!(align, 8);
    }

    #[test]
    fn test_ser_elems() {
        let mut ctx = SerdeCtx::default();
        let mut out = vec![0u8; 32];
        let mut idx = 0usize;
        let mut e1 = MySerElem::inline_bytes(&123u32.to_le_bytes());
        assert_eq!(e1.my_len(&ctx), 4);
        idx = e1.my_ser(&mut ctx, &mut out, idx);
        assert_eq!(idx, 4);
        assert_eq!(&[123, 0, 0, 0], &out[..4]);

        let mut e2 = MySerElem::slice(b"hello");
        assert_eq!(e2.my_len(&ctx), 5);
        idx = e2.my_ser(&mut ctx, &mut out, idx);
        assert_eq!(idx, 4 + 5);
        assert_eq!(b"hello", &out[4..9]);

        let mut e3 = MySerElem::slice(b"hello");
        let mut out = vec![0u8; 32];
        e3.my_ser_partial(&mut ctx, &mut out[..4], 0);
        assert_eq!(b"hell\0", &out[..5]);
        assert_eq!(e3, MySerElem::inline_bytes(b"o"));
        idx = e3.my_ser(&mut ctx, &mut out, 4);
        assert_eq!(idx, 5);
        assert_eq!(b"hello", &out[..5]);
    }

    #[test]
    fn test_ser_elem_len_enc_int() {
        let mut ctx = SerdeCtx::default();
        let mut buf = vec![0u8; 255];
        let ser_buf = &mut buf[..];
        let mut idx = 0usize;
        idx = MySerElem::len_enc_int(LenEncInt::Null).my_ser(&mut ctx, ser_buf, idx);
        idx = MySerElem::len_enc_int(LenEncInt::Err).my_ser(&mut ctx, ser_buf, idx);
        idx = MySerElem::len_enc_int(LenEncInt::from(1024u16)).my_ser(&mut ctx, ser_buf, idx); // u16
        idx =
            MySerElem::len_enc_int(LenEncInt::from(1024u32 * 1024)).my_ser(&mut ctx, ser_buf, idx); // u24
        let _ = MySerElem::len_enc_int(LenEncInt::from(1024u64 * 1024 * 1024))
            .my_ser(&mut ctx, ser_buf, idx); // u64
        let de_buf = &mut &buf[..];
        let v = de_buf.try_deser_len_enc_int().unwrap();
        assert_eq!(v, LenEncInt::Null);
        let v = de_buf.try_deser_len_enc_int().unwrap();
        assert_eq!(v, LenEncInt::Err);
        let v = de_buf.try_deser_len_enc_int().unwrap();
        assert_eq!(v, LenEncInt::from(1024u16));
        let v = de_buf.try_deser_len_enc_int().unwrap();
        assert_eq!(v, LenEncInt::from(1024u32 * 1024));
        let v = de_buf.try_deser_len_enc_int().unwrap();
        assert_eq!(v, LenEncInt::from(1024u64 * 1024 * 1024));
    }

    #[test]
    fn test_ser_elem_len_enc_str() {
        let mut ctx = SerdeCtx::default();
        let data = vec![1u8; 1024 * 1024 * 17];
        let mut buf = vec![0u8; 1024 * 1024 * 18];
        let ser_buf = &mut buf[..];
        let mut idx = 0usize;
        idx = MySerElem::len_enc_str(LenEncStr::Null).my_ser(&mut ctx, ser_buf, idx);
        idx = MySerElem::len_enc_str(LenEncStr::Err).my_ser(&mut ctx, ser_buf, idx);
        idx = MySerElem::len_enc_str(LenEncStr::from(&data[..1024])).my_ser(&mut ctx, ser_buf, idx); // u16
        idx = MySerElem::len_enc_str(LenEncStr::from(&data[..1024 * 128]))
            .my_ser(&mut ctx, ser_buf, idx); // u24
        let _ = MySerElem::len_enc_str(LenEncStr::from(&data[..1024 * 1024 * 17]))
            .my_ser(&mut ctx, ser_buf, idx); // u64
        let de_buf = &mut &buf[..];
        let v = de_buf.try_deser_len_enc_str().unwrap();
        assert_eq!(v, LenEncStr::Null);
        let v = de_buf.try_deser_len_enc_str().unwrap();
        assert_eq!(v, LenEncStr::Err);
        let v = de_buf.try_deser_len_enc_str().unwrap();
        assert_eq!(v, LenEncStr::from(&data[..1024]));
        let v = de_buf.try_deser_len_enc_str().unwrap();
        assert_eq!(v, LenEncStr::from(&data[..1024 * 128]));
        let v = de_buf.try_deser_len_enc_str().unwrap();
        assert_eq!(v, LenEncStr::from(&data[..1024 * 1024 * 17]));
    }

    #[test]
    fn test_ser_elem_prefix_1b_str() {
        let mut ctx = SerdeCtx::default();
        let mut buf = vec![0u8; 1024];
        let ser_buf = &mut buf[..];
        let s = b"this is greeting from j";
        let _ = MySerElem::prefix_1b_str(s).my_ser(&mut ctx, ser_buf, 0);
        let de_buf = &mut &buf[..];
        let v = de_buf.try_deser_prefix_1b_str().unwrap();
        assert_eq!(v, s);
    }

    #[test]
    fn test_ser_pkt() {
        let mut ctx = SerdeCtx::default();
        ctx.max_payload_size = 4;
        let mut buf = vec![0u8; 32];

        // case 1: single packet
        ctx.pkt_nr = 0;
        let mut pkts = MySerPackets::new(&ctx, [MySerElem::inline_bytes(&[1u8])]);
        assert_eq!(pkts.my_len(&ctx), 5);
        let _ = pkts.my_ser(&mut ctx, &mut buf[..], 0);
        assert_eq!(&buf[..5], &[1, 0, 0, 0, 1]);

        // case 2: split packets
        ctx.pkt_nr = 0;
        let mut pkts = MySerPackets::new(&ctx, [MySerElem::slice(b"hello")]);
        assert_eq!(pkts.my_len(&ctx), 13);
        let next = pkts.my_ser(&mut ctx, &mut buf[..], 0);
        assert_eq!(next, 13);
        assert_eq!(
            &buf[..13],
            &[4, 0, 0, 0, b'h', b'e', b'l', b'l', 1, 0, 0, 1, b'o']
        );
        assert_eq!(pkts.res_pkts, 2);

        // case 3: partial packet
        ctx.pkt_nr = 0;
        let mut pkts = MySerPackets::new(&ctx, [MySerElem::slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9])]);
        assert_eq!(pkts.my_len(&ctx), 21);
        pkts.my_ser_partial(&mut ctx, &mut buf[..10], 0);
        assert_eq!(&buf[..10], &[4, 0, 0, 0, 1, 2, 3, 4, 4, 0]);
        pkts.my_ser_partial(&mut ctx, &mut buf[..20], 10);
        assert_eq!(&buf[10..20], &[0, 1, 5, 6, 7, 8, 1, 0, 0, 2]);
        pkts.my_ser(&mut ctx, &mut buf, 20);
        assert_eq!(
            &buf[..21],
            &[
                4, 0, 0, 0, 1, 2, 3, 4, 4, 0, 0, 1, 5, 6, 7, 8, 1, 0, 0, 2, 9
            ]
        );
    }
}
