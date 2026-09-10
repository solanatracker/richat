use {
    super::{bytes_encode, bytes_encoded_len},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaContactInfoV0_0_1,
    prost::{
        DecodeError, Message,
        bytes::{Buf, BufMut},
        encoding::{self, DecodeContext, WireType},
    },
    std::{fmt, net::SocketAddr},
};

/// Max length of `SocketAddr` display: `[ipv6 with 39 chars%scope_id with 10 chars]:65535`
///
/// Agave builds sockets with `SocketAddr::new(ip, port)` so the scope id is always zero,
/// but the buffer covers `SocketAddrV6::scope_id` anyway to never panic in a plugin callback.
const SOCKET_ADDR_MAX_LEN: usize = 58;

/// Stack buffer for formatting `SocketAddr` without allocation
struct SocketAddrBuffer {
    buffer: [u8; SOCKET_ADDR_MAX_LEN],
    len: usize,
}

impl SocketAddrBuffer {
    fn format(addr: &SocketAddr) -> Self {
        let mut buffer = Self {
            buffer: [0; SOCKET_ADDR_MAX_LEN],
            len: 0,
        };
        fmt::write(&mut buffer, format_args!("{addr}")).expect("socket addr fits in buffer");
        buffer
    }

    fn as_bytes(&self) -> &[u8] {
        &self.buffer[..self.len]
    }
}

impl fmt::Write for SocketAddrBuffer {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let bytes = s.as_bytes();
        let end = self.len + bytes.len();
        if end > SOCKET_ADDR_MAX_LEN {
            return Err(fmt::Error);
        }
        self.buffer[self.len..end].copy_from_slice(bytes);
        self.len = end;
        Ok(())
    }
}

fn socket_addr_encode(tag: u32, addr: Option<&SocketAddr>, buf: &mut impl BufMut) {
    if let Some(addr) = addr {
        bytes_encode(tag, SocketAddrBuffer::format(addr).as_bytes(), buf);
    }
}

fn socket_addr_encoded_len(tag: u32, addr: Option<&SocketAddr>) -> usize {
    addr.map_or(0, |addr| {
        bytes_encoded_len(tag, SocketAddrBuffer::format(addr).as_bytes())
    })
}

fn u16_encode(tag: u32, value: u16, buf: &mut impl BufMut) {
    if value != 0 {
        encoding::uint32::encode(tag, &(value as u32), buf);
    }
}

fn u16_encoded_len(tag: u32, value: u16) -> usize {
    if value != 0 {
        encoding::uint32::encoded_len(tag, &(value as u32))
    } else {
        0
    }
}

fn u32_encode(tag: u32, value: u32, buf: &mut impl BufMut) {
    if value != 0 {
        encoding::uint32::encode(tag, &value, buf);
    }
}

fn u32_encoded_len(tag: u32, value: u32) -> usize {
    if value != 0 {
        encoding::uint32::encoded_len(tag, &value)
    } else {
        0
    }
}

fn u64_encode(tag: u32, value: u64, buf: &mut impl BufMut) {
    if value != 0 {
        encoding::uint64::encode(tag, &value, buf);
    }
}

fn u64_encoded_len(tag: u32, value: u64) -> usize {
    if value != 0 {
        encoding::uint64::encoded_len(tag, &value)
    } else {
        0
    }
}

#[derive(Debug)]
pub struct ContactInfo<'a> {
    info: &'a ReplicaContactInfoV0_0_1<'a>,
}

impl<'a> ContactInfo<'a> {
    pub const fn new(info: &'a ReplicaContactInfoV0_0_1<'a>) -> Self {
        Self { info }
    }
}

impl Message for ContactInfo<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        let info = self.info;

        if !info.pubkey.is_empty() {
            bytes_encode(1, info.pubkey, buf);
        }
        u64_encode(2, info.wallclock, buf);
        u64_encode(3, info.outset, buf);
        u16_encode(4, info.shred_version, buf);
        u16_encode(5, info.version_major, buf);
        u16_encode(6, info.version_minor, buf);
        u16_encode(7, info.version_patch, buf);
        u32_encode(8, info.version_commit, buf);
        u32_encode(9, info.version_feature_set, buf);
        u16_encode(10, info.version_client_id, buf);
        socket_addr_encode(11, info.gossip.as_ref(), buf);
        socket_addr_encode(12, info.tpu_quic.as_ref(), buf);
        socket_addr_encode(13, info.tpu_forwards_quic.as_ref(), buf);
        socket_addr_encode(14, info.tpu_vote_udp.as_ref(), buf);
        socket_addr_encode(15, info.tpu_vote_quic.as_ref(), buf);
        socket_addr_encode(16, info.tvu_udp.as_ref(), buf);
        socket_addr_encode(17, info.tvu_quic.as_ref(), buf);
        socket_addr_encode(18, info.serve_repair_udp.as_ref(), buf);
        socket_addr_encode(19, info.serve_repair_quic.as_ref(), buf);
        socket_addr_encode(20, info.rpc.as_ref(), buf);
        socket_addr_encode(21, info.rpc_pubsub.as_ref(), buf);
        socket_addr_encode(22, info.alpenglow.as_ref(), buf);
    }

    fn encoded_len(&self) -> usize {
        let info = self.info;

        (if info.pubkey.is_empty() {
            0
        } else {
            bytes_encoded_len(1, info.pubkey)
        }) + u64_encoded_len(2, info.wallclock)
            + u64_encoded_len(3, info.outset)
            + u16_encoded_len(4, info.shred_version)
            + u16_encoded_len(5, info.version_major)
            + u16_encoded_len(6, info.version_minor)
            + u16_encoded_len(7, info.version_patch)
            + u32_encoded_len(8, info.version_commit)
            + u32_encoded_len(9, info.version_feature_set)
            + u16_encoded_len(10, info.version_client_id)
            + socket_addr_encoded_len(11, info.gossip.as_ref())
            + socket_addr_encoded_len(12, info.tpu_quic.as_ref())
            + socket_addr_encoded_len(13, info.tpu_forwards_quic.as_ref())
            + socket_addr_encoded_len(14, info.tpu_vote_udp.as_ref())
            + socket_addr_encoded_len(15, info.tpu_vote_quic.as_ref())
            + socket_addr_encoded_len(16, info.tvu_udp.as_ref())
            + socket_addr_encoded_len(17, info.tvu_quic.as_ref())
            + socket_addr_encoded_len(18, info.serve_repair_udp.as_ref())
            + socket_addr_encoded_len(19, info.serve_repair_quic.as_ref())
            + socket_addr_encoded_len(20, info.rpc.as_ref())
            + socket_addr_encoded_len(21, info.rpc_pubsub.as_ref())
            + socket_addr_encoded_len(22, info.alpenglow.as_ref())
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
pub struct ContactInfoRemoved<'a> {
    pubkey: &'a [u8],
}

impl<'a> ContactInfoRemoved<'a> {
    pub const fn new(pubkey: &'a [u8]) -> Self {
        Self { pubkey }
    }
}

impl Message for ContactInfoRemoved<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if !self.pubkey.is_empty() {
            bytes_encode(1, self.pubkey, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        if self.pubkey.is_empty() {
            0
        } else {
            bytes_encoded_len(1, self.pubkey)
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
