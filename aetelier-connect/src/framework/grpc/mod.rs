pub mod assemble;
pub mod attach;
pub mod connection;
pub mod decode;
pub mod pool;
pub mod replay;
pub mod request;
pub mod session;
pub mod status;

use aetelier_types::config::markets::market_config::TransactionsConfig;
use aetelier_types::exchanges::Exchange;

use crate::config::workers::common::ReconnectSection;
use crate::config::workers::market_worker_config::GrpcSection;
use crate::framework::adapters::solana::SolanaAdapter;
use crate::framework::registry::ExchangeAdapter;

pub use attach::{GrpcAttach, drive_grpc};
pub use connection::ATTACH_QUEUE_BANKS;
pub use decode::{
    GrpcUpdate, MalformedTransaction, TransactionUpdate, block_meta, decode_transaction,
    decode_update, execution_error,
};
pub use pool::{AttachError, AttachId, AttachSpec, ConnectionKey, GrpcConnections};
pub use replay::replay_frames;
pub use request::{
    BLOCKS_META_FILTER, StreamFilter, TRANSACTIONS_FILTER, commitment_level, ping_request,
};
pub use session::{
    GeyserChannel, GeyserDial, GrpcSession, GrpcSettings, NetworkDial, STALE_AFTER,
    SessionEvent, XToken,
};
pub use status::{
    ExitClass, GrpcExitReason, PendingRequest, RATE_LIMIT_COOL_DOWN,
    RATE_LIMIT_COOL_DOWN_MAX, ServerVersion, StatusClass, classify_status, rejection,
};

pub const PROTOCOL_REVISION: &str = "yellowstone-grpc-proto@14.0.1";

pub fn build_grpc_adapter(
    exchange: Exchange,
    section: &GrpcSection,
    transactions: &TransactionsConfig,
    reconnect: Option<&ReconnectSection>,
    connections: GrpcConnections,
) -> Option<Box<dyn ExchangeAdapter>> {
    match exchange {
        Exchange::Solana => Some(Box::new(SolanaAdapter::live(
            section.clone(),
            transactions.clone(),
            reconnect.cloned(),
            connections,
        ))),
        _ => None,
    }
}
