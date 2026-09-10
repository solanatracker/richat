#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaContactInfoV0_0_1,
    arbitrary::Arbitrary,
    richat_plugin_agave::protobuf::ProtobufMessage,
    std::{
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        time::SystemTime,
    },
};

#[derive(Debug, Arbitrary)]
enum FuzzSocketAddr {
    V4([u8; 4], u16),
    V6([u16; 8], u16),
}

impl From<FuzzSocketAddr> for SocketAddr {
    fn from(fuzz: FuzzSocketAddr) -> Self {
        match fuzz {
            FuzzSocketAddr::V4(ip, port) => SocketAddr::new(IpAddr::V4(Ipv4Addr::from(ip)), port),
            FuzzSocketAddr::V6(ip, port) => SocketAddr::new(IpAddr::V6(Ipv6Addr::from(ip)), port),
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzContactInfo {
    pubkey: Vec<u8>,
    wallclock: u64,
    outset: u64,
    shred_version: u16,
    version_major: u16,
    version_minor: u16,
    version_patch: u16,
    version_commit: u32,
    version_feature_set: u32,
    version_client_id: u16,
    gossip: Option<FuzzSocketAddr>,
    tpu_quic: Option<FuzzSocketAddr>,
    tpu_forwards_quic: Option<FuzzSocketAddr>,
    tpu_vote_udp: Option<FuzzSocketAddr>,
    tpu_vote_quic: Option<FuzzSocketAddr>,
    tvu_udp: Option<FuzzSocketAddr>,
    tvu_quic: Option<FuzzSocketAddr>,
    serve_repair_udp: Option<FuzzSocketAddr>,
    serve_repair_quic: Option<FuzzSocketAddr>,
    rpc: Option<FuzzSocketAddr>,
    rpc_pubsub: Option<FuzzSocketAddr>,
    alpenglow: Option<FuzzSocketAddr>,
}

libfuzzer_sys::fuzz_target!(|fuzz: FuzzContactInfo| {
    let created_at = SystemTime::now();

    let replica = ReplicaContactInfoV0_0_1 {
        pubkey: &fuzz.pubkey,
        wallclock: fuzz.wallclock,
        outset: fuzz.outset,
        shred_version: fuzz.shred_version,
        version_major: fuzz.version_major,
        version_minor: fuzz.version_minor,
        version_patch: fuzz.version_patch,
        version_commit: fuzz.version_commit,
        version_feature_set: fuzz.version_feature_set,
        version_client_id: fuzz.version_client_id,
        gossip: fuzz.gossip.map(Into::into),
        tpu_quic: fuzz.tpu_quic.map(Into::into),
        tpu_forwards_quic: fuzz.tpu_forwards_quic.map(Into::into),
        tpu_vote_udp: fuzz.tpu_vote_udp.map(Into::into),
        tpu_vote_quic: fuzz.tpu_vote_quic.map(Into::into),
        tvu_udp: fuzz.tvu_udp.map(Into::into),
        tvu_quic: fuzz.tvu_quic.map(Into::into),
        serve_repair_udp: fuzz.serve_repair_udp.map(Into::into),
        serve_repair_quic: fuzz.serve_repair_quic.map(Into::into),
        rpc: fuzz.rpc.map(Into::into),
        rpc_pubsub: fuzz.rpc_pubsub.map(Into::into),
        alpenglow: fuzz.alpenglow.map(Into::into),
    };

    for message in [
        ProtobufMessage::ContactInfo { info: &replica },
        ProtobufMessage::ContactInfoRemoved {
            pubkey: &fuzz.pubkey,
        },
    ] {
        let vec_prost = message.encode_prost(created_at);
        let vec_raw = message.encode_raw(created_at);

        assert_eq!(
            vec_prost,
            vec_raw,
            "prost hex: {}",
            const_hex::encode(&vec_prost)
        );
    }
});
