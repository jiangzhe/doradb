use crate::cancel::Cancellation;
use doradb_protocol::mysql::error::{Error, Result};
use doradb_storage::col::chunk::Chunk;
use flume::{Receiver, Sender};
use futures_lite::Stream;

pub type ExecChannel = (InputChannel, OutputChannel);

/// Channel of input data blocks backed by [`flume::Receiver`].
#[derive(Clone)]
pub struct InputChannel {
    rx: Receiver<Chunk>,
}

impl InputChannel {
    #[inline]
    pub fn to_stream(&self, cancel: &Cancellation) -> impl Stream<Item = Result<Chunk>> + use<> {
        let rx = self.rx.clone();
        cancel.select_stream(rx.into_stream())
    }
}

/// Channel of output data blocks backed by [`flume::Sender`].
#[derive(Clone)]
pub struct OutputChannel {
    pub expected_len: usize,
    tx: Sender<Chunk>,
}

impl OutputChannel {
    /// Send a block to output channel.
    /// The failure only occurs when the receiver side closes
    /// the channel, as somewhere cancellation is triggered.
    #[inline]
    pub async fn send(&self, block: Chunk) -> Result<()> {
        self.tx
            .send_async(block)
            .await
            .map_err(|_| Error::Cancelled())
    }
}

#[inline]
pub fn bounded(buf_size: usize, block_len: usize) -> (InputChannel, OutputChannel) {
    let (tx, rx) = flume::bounded(buf_size);
    (
        InputChannel { rx },
        OutputChannel {
            expected_len: block_len,
            tx,
        },
    )
}

#[inline]
pub fn unbounded(block_len: usize) -> (InputChannel, OutputChannel) {
    let (tx, rx) = flume::unbounded();
    (
        InputChannel { rx },
        OutputChannel {
            expected_len: block_len,
            tx,
        },
    )
}
