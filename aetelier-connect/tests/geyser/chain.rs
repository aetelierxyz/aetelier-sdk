use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aetelier_connect::config::workers::common::ReconnectSection;
use aetelier_connect::config::workers::market_worker_config::GrpcSection;
use aetelier_connect::framework::grpc::{
    AttachSpec, BLOCKS_META_FILTER, GrpcAttach, TRANSACTIONS_FILTER, XToken,
};
use aetelier_connect::framework::onchain::WatchedAddress;
use aetelier_connect::framework::{DomainEvent, RateWindow, SourceMetrics};
use aetelier_types::onchain::{ChainAddress, Commitment, MatchRole};
use tokio::time::timeout;
use yellowstone_grpc_proto::prelude::{
    BlockHeight, Message, MessageHeader, SubscribeRequest, SubscribeUpdate,
    SubscribeUpdateBlockMeta, SubscribeUpdatePing, SubscribeUpdatePong,
    SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo, Transaction,
    TransactionStatusMeta, UnixTimestamp, subscribe_update::UpdateOneof,
};

use crate::server::ENDPOINT;

const BASE_SLOT: u64 = 100;
const HEIGHT_OFFSET: u64 = 1_000;
const BATCH_WAIT: Duration = Duration::from_secs(1);

pub struct Chain {
    t0_secs: i64,
}

impl Chain {
    pub fn recent() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_secs();
        Self {
            t0_secs: now as i64 - 60,
        }
    }

    pub fn height(slot: u64) -> u64 {
        slot + HEIGHT_OFFSET
    }

    pub fn meta(&self, slot: u64) -> SubscribeUpdate {
        self.meta_of(slot, slot - 1, Self::height(slot))
    }

    pub fn meta_of(&self, slot: u64, parent_slot: u64, height: u64) -> SubscribeUpdate {
        update(
            BLOCKS_META_FILTER,
            UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot,
                blockhash: format!("blockhash{slot}"),
                block_time: Some(UnixTimestamp {
                    timestamp: self.t0_secs + slot as i64 - BASE_SLOT as i64,
                }),
                block_height: Some(BlockHeight {
                    block_height: height,
                }),
                parent_slot,
                ..Default::default()
            }),
        )
    }

    pub fn partial_meta(&self, slot: u64) -> SubscribeUpdate {
        let mut partial = self.meta(slot);
        if let Some(UpdateOneof::BlockMeta(meta)) = &mut partial.update_oneof {
            meta.block_time = None;
        }
        partial
    }

    pub fn tx(
        &self,
        slot: u64,
        index: u64,
        keys: &[ChainAddress],
        signers: u32,
    ) -> SubscribeUpdate {
        let mut signature = vec![7u8; 64];
        signature[..8].copy_from_slice(&slot.to_le_bytes());
        signature[8..16].copy_from_slice(&index.to_le_bytes());
        update(
            TRANSACTIONS_FILTER,
            UpdateOneof::Transaction(SubscribeUpdateTransaction {
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: signature.clone(),
                    is_vote: false,
                    transaction: Some(Transaction {
                        signatures: vec![signature],
                        message: Some(Message {
                            header: Some(MessageHeader {
                                num_required_signatures: signers,
                                num_readonly_signed_accounts: 0,
                                num_readonly_unsigned_accounts: 0,
                            }),
                            account_keys: keys
                                .iter()
                                .map(|key| key.pubkey().to_vec())
                                .collect(),
                            recent_blockhash: vec![9u8; 32],
                            instructions: Vec::new(),
                            versioned: false,
                            address_table_lookups: Vec::new(),
                            config: None,
                        }),
                    }),
                    meta: Some(TransactionStatusMeta {
                        fee: 5_000,
                        log_messages_none: true,
                        ..Default::default()
                    }),
                    index,
                }),
                slot,
                bank_id: 0,
            }),
        )
    }

    pub fn vote(&self, slot: u64, index: u64, keys: &[ChainAddress]) -> SubscribeUpdate {
        let mut vote = self.tx(slot, index, keys, 1);
        if let Some(UpdateOneof::Transaction(transaction)) = &mut vote.update_oneof
            && let Some(info) = &mut transaction.transaction
        {
            info.is_vote = true;
        }
        vote
    }

    pub fn bank(
        &self,
        slot: u64,
        rows: &[(u64, &[ChainAddress])],
    ) -> Vec<SubscribeUpdate> {
        rows.iter()
            .map(|&(index, keys)| self.tx(slot, index, keys, 1))
            .chain([self.meta(slot)])
            .collect()
    }

    pub fn oversized(bytes: usize) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec!["x".repeat(bytes)],
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            created_at: None,
        }
    }
}

pub fn pong(id: i32) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: Vec::new(),
        update_oneof: Some(UpdateOneof::Pong(SubscribeUpdatePong { id })),
        created_at: None,
    }
}

pub fn server_ping() -> SubscribeUpdate {
    SubscribeUpdate {
        filters: Vec::new(),
        update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
        created_at: None,
    }
}

pub fn ping_id(request: &SubscribeRequest) -> Option<i32> {
    request.ping.as_ref().map(|ping| ping.id)
}

pub fn account_include(request: &SubscribeRequest) -> Vec<String> {
    request
        .transactions
        .get(TRANSACTIONS_FILTER)
        .map(|filter| filter.account_include.clone())
        .unwrap_or_default()
}

pub fn address(seed: u8) -> ChainAddress {
    let mut pubkey = [0u8; 32];
    pubkey[0] = seed;
    pubkey[31] = seed;
    ChainAddress::new(aetelier_types::onchain::Chain::Solana, pubkey)
}

pub fn watching(seed: u8) -> WatchedAddress {
    WatchedAddress {
        address: address(seed),
        required_role: MatchRole::AccountKey,
        include_failed: true,
        keep_logs: false,
    }
}

pub fn section() -> GrpcSection {
    GrpcSection {
        endpoint: ENDPOINT.to_string(),
        x_token_env: Some("GRPC_X_TOKEN".to_string()),
        commitment: Commitment::Finalized,
        server_filter: true,
        accept_zstd: true,
        max_decoding_message_size_mib: 64,
        ping_interval_secs: 15,
        max_pending_slots: 32,
        max_addresses_per_connection: 50,
        account_include_max: None,
        account_include_reject: Vec::new(),
    }
}

pub fn spec(
    section: GrpcSection,
    watch: WatchedAddress,
    metrics: SourceMetrics,
    queue_banks: usize,
) -> AttachSpec {
    AttachSpec {
        venue: "solana",
        section,
        x_token: Some(XToken::new("t".to_string())),
        watch,
        reconnect: Some(ReconnectSection {
            initial_delay_ms: Some(1_000),
            max_delay_ms: Some(30_000),
            max_attempts: None,
            jitter_factor: Some(0.0),
        }),
        connect_rate: RateWindow {
            max: 1_000,
            per: Duration::from_secs(60),
        },
        queue_banks,
        metrics,
    }
}

pub async fn next_batch(attach: &mut GrpcAttach) -> Vec<DomainEvent> {
    timeout(BATCH_WAIT, attach.recv())
        .await
        .expect("a batch arrives within a second")
        .expect("the attach is open")
}

pub fn kinds(batch: &[DomainEvent]) -> Vec<String> {
    batch
        .iter()
        .map(|event| match event {
            DomainEvent::ConnectionLost { .. } => "lost".to_string(),
            DomainEvent::ConnectionResumed => "resumed".to_string(),
            DomainEvent::SlotGap(gap) => format!(
                "gap {}->{} lost {}",
                gap.prev_slot,
                gap.next_slot,
                gap.blocks_lost()
            ),
            DomainEvent::Transaction(row) => format!("row {}:{}", row.slot, row.tx_index),
            DomainEvent::SlotTick(tick) => format!("tick {}", tick.slot),
            other => format!("{other:?}"),
        })
        .collect()
}

fn update(filter: &str, update_oneof: UpdateOneof) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec![filter.to_string()],
        update_oneof: Some(update_oneof),
        created_at: None,
    }
}
