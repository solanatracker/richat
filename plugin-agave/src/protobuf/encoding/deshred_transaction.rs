use {
    super::{
        bytes_encode, bytes_encoded_len,
        transaction::{VersionedTransactionWrapper, pubkeys_encode, pubkeys_encoded_len},
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaDeshredTransactionInfoV2,
    prost::{
        DecodeError, Message,
        bytes::{Buf, BufMut},
        encoding::{self, DecodeContext, WireType},
    },
    solana_clock::Slot,
};

#[derive(Debug)]
pub struct DeshredTransaction<'a> {
    slot: Slot,
    transaction: &'a ReplicaDeshredTransactionInfoV2<'a>,
}

impl<'a> DeshredTransaction<'a> {
    pub const fn new(slot: Slot, transaction: &'a ReplicaDeshredTransactionInfoV2<'a>) -> Self {
        Self { slot, transaction }
    }
}

impl Message for DeshredTransaction<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        let tx = ReplicaWrapper(self.transaction);
        encoding::message::encode(1, &tx, buf);
        if self.slot != 0 {
            encoding::uint64::encode(2, &self.slot, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let tx = ReplicaWrapper(self.transaction);
        encoding::message::encoded_len(1, &tx)
            + if self.slot != 0 {
                encoding::uint64::encoded_len(2, &self.slot)
            } else {
                0
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

#[derive(Debug)]
struct ReplicaWrapper<'a>(&'a ReplicaDeshredTransactionInfoV2<'a>);

impl Message for ReplicaWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        let tx = self.0;

        bytes_encode(1, tx.signature.as_ref(), buf);
        if tx.is_vote {
            encoding::bool::encode(2, &tx.is_vote, buf)
        }
        encoding::message::encode(3, &VersionedTransactionWrapper(tx.transaction), buf);
        if let Some(loaded_addresses) = tx.loaded_addresses {
            pubkeys_encode(4, &loaded_addresses.writable, buf);
            pubkeys_encode(5, &loaded_addresses.readonly, buf);
        }
        if tx.completed_data_set_starting_shred_index != 0 {
            encoding::uint32::encode(6, &tx.completed_data_set_starting_shred_index, buf);
        }
        if tx.completed_data_set_ending_shred_index_exclusive != 0 {
            encoding::uint32::encode(7, &tx.completed_data_set_ending_shred_index_exclusive, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let tx = self.0;

        bytes_encoded_len(1, tx.signature.as_ref())
            + if tx.is_vote {
                encoding::bool::encoded_len(2, &tx.is_vote)
            } else {
                0
            }
            + encoding::message::encoded_len(3, &VersionedTransactionWrapper(tx.transaction))
            + tx.loaded_addresses.map_or(0, |loaded_addresses| {
                pubkeys_encoded_len(4, &loaded_addresses.writable)
                    + pubkeys_encoded_len(5, &loaded_addresses.readonly)
            })
            + if tx.completed_data_set_starting_shred_index != 0 {
                encoding::uint32::encoded_len(6, &tx.completed_data_set_starting_shred_index)
            } else {
                0
            }
            + if tx.completed_data_set_ending_shred_index_exclusive != 0 {
                encoding::uint32::encoded_len(
                    7,
                    &tx.completed_data_set_ending_shred_index_exclusive,
                )
            } else {
                0
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
