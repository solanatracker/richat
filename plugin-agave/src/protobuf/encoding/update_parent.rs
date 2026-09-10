use {
    super::{bytes_encode, bytes_encoded_len},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaDeshredUpdateParentInfo, ReplicaEntryUpdateParentInfo,
    },
    prost::{
        DecodeError, Message,
        bytes::{Buf, BufMut},
        encoding::{self, DecodeContext, WireType},
    },
};

#[derive(Debug)]
pub struct EntryUpdateParent<'a> {
    info: &'a ReplicaEntryUpdateParentInfo<'a>,
}

impl<'a> EntryUpdateParent<'a> {
    pub const fn new(info: &'a ReplicaEntryUpdateParentInfo<'a>) -> Self {
        Self { info }
    }
}

impl Message for EntryUpdateParent<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if self.info.slot != 0 {
            encoding::uint64::encode(1, &self.info.slot, buf);
        }
        if self.info.cleared_bank_id != 0 {
            encoding::uint64::encode(2, &self.info.cleared_bank_id, buf);
        }
        if self.info.parent_slot != 0 {
            encoding::uint64::encode(3, &self.info.parent_slot, buf);
        }
        bytes_encode(4, self.info.parent_block_id.as_ref(), buf);
    }

    fn encoded_len(&self) -> usize {
        (if self.info.slot != 0 {
            encoding::uint64::encoded_len(1, &self.info.slot)
        } else {
            0
        }) + if self.info.cleared_bank_id != 0 {
            encoding::uint64::encoded_len(2, &self.info.cleared_bank_id)
        } else {
            0
        } + if self.info.parent_slot != 0 {
            encoding::uint64::encoded_len(3, &self.info.parent_slot)
        } else {
            0
        } + bytes_encoded_len(4, self.info.parent_block_id.as_ref())
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
pub struct DeshredUpdateParent<'a> {
    info: &'a ReplicaDeshredUpdateParentInfo<'a>,
}

impl<'a> DeshredUpdateParent<'a> {
    pub const fn new(info: &'a ReplicaDeshredUpdateParentInfo<'a>) -> Self {
        Self { info }
    }
}

impl Message for DeshredUpdateParent<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if self.info.slot != 0 {
            encoding::uint64::encode(1, &self.info.slot, buf);
        }
        if self.info.update_parent_fec_set_index != 0 {
            encoding::uint32::encode(2, &self.info.update_parent_fec_set_index, buf);
        }
        if self.info.parent_slot != 0 {
            encoding::uint64::encode(3, &self.info.parent_slot, buf);
        }
        bytes_encode(4, self.info.parent_block_id.as_ref(), buf);
    }

    fn encoded_len(&self) -> usize {
        (if self.info.slot != 0 {
            encoding::uint64::encoded_len(1, &self.info.slot)
        } else {
            0
        }) + if self.info.update_parent_fec_set_index != 0 {
            encoding::uint32::encoded_len(2, &self.info.update_parent_fec_set_index)
        } else {
            0
        } + if self.info.parent_slot != 0 {
            encoding::uint64::encoded_len(3, &self.info.parent_slot)
        } else {
            0
        } + bytes_encoded_len(4, self.info.parent_block_id.as_ref())
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
