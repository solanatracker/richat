use {
    super::{bytes_encode, bytes_encoded_len},
    prost::{
        DecodeError, Message,
        bytes::{Buf, BufMut},
        encoding::{self, DecodeContext, WireType},
    },
    solana_clock::{BankId, Slot},
    solana_entry::block_component::VersionedBlockFooter,
};

/// Block footer fields shared between `prost` and `raw` encoders
#[derive(Debug)]
pub struct BlockFooterView<'a> {
    pub version: u32,
    pub bank_hash: [u8; 32],
    pub block_producer_time_nanos: u64,
    pub block_user_agent: &'a [u8],
    /// wincode-serialized `VersionedBlockFooter`
    pub footer: Vec<u8>,
}

impl<'a> BlockFooterView<'a> {
    pub fn new(block_footer: &'a VersionedBlockFooter) -> Self {
        // Footers are deserialized by agave with the same schema, so this can not fail
        // in practice (`block_user_agent` is capped at 255 bytes); never panic in a callback.
        let footer = wincode::serialize(block_footer).unwrap_or_else(|error| {
            log::error!("failed to serialize block footer: {error:?}");
            Vec::new()
        });
        match block_footer {
            VersionedBlockFooter::V1(v1) => Self {
                version: 1,
                bank_hash: v1.bank_hash.to_bytes(),
                block_producer_time_nanos: v1.block_producer_time_nanos,
                block_user_agent: &v1.block_user_agent,
                footer,
            },
        }
    }
}

#[derive(Debug)]
pub struct BlockFooter<'a> {
    slot: Slot,
    bank_id: BankId,
    view: BlockFooterView<'a>,
}

impl<'a> BlockFooter<'a> {
    pub fn new(slot: Slot, bank_id: BankId, block_footer: &'a VersionedBlockFooter) -> Self {
        Self {
            slot,
            bank_id,
            view: BlockFooterView::new(block_footer),
        }
    }
}

impl Message for BlockFooter<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if self.slot != 0 {
            encoding::uint64::encode(1, &self.slot, buf);
        }
        if self.bank_id != 0 {
            encoding::uint64::encode(2, &self.bank_id, buf);
        }
        if self.view.version != 0 {
            encoding::uint32::encode(3, &self.view.version, buf);
        }
        bytes_encode(4, &self.view.bank_hash, buf);
        if self.view.block_producer_time_nanos != 0 {
            encoding::uint64::encode(5, &self.view.block_producer_time_nanos, buf);
        }
        if !self.view.block_user_agent.is_empty() {
            bytes_encode(6, self.view.block_user_agent, buf);
        }
        if !self.view.footer.is_empty() {
            bytes_encode(7, &self.view.footer, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        (if self.slot != 0 {
            encoding::uint64::encoded_len(1, &self.slot)
        } else {
            0
        }) + if self.bank_id != 0 {
            encoding::uint64::encoded_len(2, &self.bank_id)
        } else {
            0
        } + if self.view.version != 0 {
            encoding::uint32::encoded_len(3, &self.view.version)
        } else {
            0
        } + bytes_encoded_len(4, &self.view.bank_hash)
            + if self.view.block_producer_time_nanos != 0 {
                encoding::uint64::encoded_len(5, &self.view.block_producer_time_nanos)
            } else {
                0
            }
            + if self.view.block_user_agent.is_empty() {
                0
            } else {
                bytes_encoded_len(6, self.view.block_user_agent)
            }
            + if self.view.footer.is_empty() {
                0
            } else {
                bytes_encoded_len(7, &self.view.footer)
            }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}
