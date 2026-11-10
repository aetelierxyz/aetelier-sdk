use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use aetelier_types::onchain::{ChainAddress, Commitment};
use tokio::sync::{mpsc, oneshot};

use super::attach::GrpcAttach;
use super::connection::{Command, ConnectionContext, NewAttach, SharedGrpcConnection};
use super::session::{GeyserDial, GrpcSettings, NetworkDial, XToken};
use crate::config::workers::common::ReconnectSection;
use crate::config::workers::market_worker_config::GrpcSection;
use crate::framework::budget::{RateWindow, SourceMetrics};
use crate::framework::onchain::WatchedAddress;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionKey {
    pub endpoint: String,
    pub commitment: Commitment,
    pub token_env: Option<String>,
    pub shard: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AttachId(pub(crate) u64);

#[derive(Debug, Clone)]
pub struct AttachSpec {
    pub venue: &'static str,
    pub section: GrpcSection,
    pub x_token: Option<XToken>,
    pub watch: WatchedAddress,
    pub reconnect: Option<ReconnectSection>,
    pub connect_rate: RateWindow,
    pub queue_banks: usize,
    pub metrics: SourceMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AttachError {
    #[error("{reason}")]
    InvalidSection { reason: String },
    #[error(
        "[collect.grpc] for {endpoint} differs from the section already connected under \
         the same endpoint, commitment and token variable"
    )]
    SectionMismatch { endpoint: String },
    #[error("symbol = \"{address}\" is in [collect.grpc] account_include_reject")]
    RejectListed { address: ChainAddress },
    #[error("an attach needs a queue of at least one bank")]
    NoQueue,
}

#[derive(Clone)]
pub struct GrpcConnections {
    inner: Arc<PoolInner>,
}

impl GrpcConnections {
    pub fn new(dial: Arc<dyn GeyserDial>) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                dial,
                state: Mutex::new(PoolState::default()),
                next_id: AtomicU64::new(0),
            }),
        }
    }

    pub fn process() -> Self {
        static PROCESS: OnceLock<GrpcConnections> = OnceLock::new();
        PROCESS
            .get_or_init(|| Self::new(Arc::new(NetworkDial)))
            .clone()
    }

    pub fn attach(&self, spec: AttachSpec) -> Result<GrpcAttach, AttachError> {
        let AttachSpec {
            venue,
            section,
            x_token,
            watch,
            reconnect,
            connect_rate,
            queue_banks,
            metrics,
        } = spec;
        section
            .validate()
            .map_err(|reason| AttachError::InvalidSection { reason })?;
        let mut state = self.inner.lock();
        let group_key = group_key(&section);
        if state
            .groups
            .get(&group_key)
            .is_some_and(|group| group.section != section)
        {
            return Err(AttachError::SectionMismatch {
                endpoint: GrpcSettings::new(&section, None).endpoint_host(),
            });
        }
        if section.account_include_reject.contains(&watch.address) {
            return Err(AttachError::RejectListed {
                address: watch.address,
            });
        }
        if queue_banks == 0 {
            return Err(AttachError::NoQueue);
        }
        let group = state.groups.entry(group_key).or_insert_with(|| ShardGroup {
            section: section.clone(),
            shard_size: section.shard_size(),
            shards: Vec::new(),
            placement: HashMap::new(),
        });
        let (shard, placed) = group.place(watch.address);
        let id = AttachId(self.inner.next_id.fetch_add(1, Ordering::Relaxed) + 1);
        let key = ConnectionKey {
            endpoint: section.endpoint.clone(),
            commitment: section.commitment,
            token_env: section.x_token_env.clone(),
            shard,
        };
        let (events_tx, events) = mpsc::channel(queue_banks);
        let (exit_tx, exit) = oneshot::channel();
        let attach = NewAttach {
            id,
            watch,
            metrics,
            events: events_tx,
            exit: exit_tx,
        };
        let context = ConnectionContext {
            venue,
            section,
            x_token,
            reconnect,
            connect_rate,
        };
        let spawn = |attach: NewAttach| {
            SharedGrpcConnection::spawn(
                key.clone(),
                context,
                self.inner.dial.clone(),
                attach,
            )
        };
        let slot = match &mut group.shards[shard] {
            Some(slot) => {
                let refused = if slot.connection.is_closed() {
                    Err(Command::Attach(attach))
                } else {
                    slot.connection.send(Command::Attach(attach))
                };
                if let Err(Command::Attach(attach)) = refused {
                    slot.connection = spawn(attach);
                }
                slot
            }
            empty => empty.insert(ShardSlot {
                connection: spawn(attach),
                attaches: 0,
                addresses: 0,
            }),
        };
        slot.attaches += 1;
        if placed {
            slot.addresses += 1;
        }
        drop(state);
        Ok(GrpcAttach::from_parts(
            id,
            key.clone(),
            events,
            exit,
            Some(DetachGuard {
                inner: self.inner.clone(),
                key,
                id,
                address: watch.address,
            }),
        ))
    }

    pub fn connections(&self) -> Vec<(ConnectionKey, usize)> {
        let state = self.inner.lock();
        let mut connections: Vec<(ConnectionKey, usize)> = state
            .groups
            .iter()
            .flat_map(|((endpoint, commitment, token_env), group)| {
                group
                    .shards
                    .iter()
                    .enumerate()
                    .filter_map(move |(shard, slot)| {
                        slot.as_ref().map(|slot| {
                            let key = ConnectionKey {
                                endpoint: endpoint.clone(),
                                commitment: *commitment,
                                token_env: token_env.clone(),
                                shard,
                            };
                            (key, slot.attaches)
                        })
                    })
            })
            .collect();
        connections.sort_by(|(left, _), (right, _)| order(left).cmp(&order(right)));
        connections
    }
}

struct PoolInner {
    dial: Arc<dyn GeyserDial>,
    state: Mutex<PoolState>,
    next_id: AtomicU64,
}

impl PoolInner {
    fn lock(&self) -> MutexGuard<'_, PoolState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Default)]
struct PoolState {
    groups: HashMap<(String, Commitment, Option<String>), ShardGroup>,
}

struct ShardGroup {
    section: GrpcSection,
    shard_size: usize,
    shards: Vec<Option<ShardSlot>>,
    placement: HashMap<ChainAddress, (usize, usize)>,
}

impl ShardGroup {
    fn place(&mut self, address: ChainAddress) -> (usize, bool) {
        if let Some((shard, attaches)) = self.placement.get_mut(&address) {
            *attaches += 1;
            return (*shard, false);
        }
        let fits = self.shards.iter().position(|slot| {
            slot.as_ref().map_or(0, |slot| slot.addresses) < self.shard_size
        });
        let shard = fits.unwrap_or_else(|| {
            self.shards.push(None);
            self.shards.len() - 1
        });
        self.placement.insert(address, (shard, 1));
        (shard, true)
    }
}

struct ShardSlot {
    connection: SharedGrpcConnection,
    attaches: usize,
    addresses: usize,
}

pub(crate) struct DetachGuard {
    inner: Arc<PoolInner>,
    key: ConnectionKey,
    id: AttachId,
    address: ChainAddress,
}

impl Drop for DetachGuard {
    fn drop(&mut self) {
        let mut state = self.inner.lock();
        let group_key = (
            self.key.endpoint.clone(),
            self.key.commitment,
            self.key.token_env.clone(),
        );
        let Some(group) = state.groups.get_mut(&group_key) else {
            return;
        };
        let freed = match group.placement.get_mut(&self.address) {
            Some((_, attaches)) if *attaches > 1 => {
                *attaches -= 1;
                false
            }
            Some(_) => group.placement.remove(&self.address).is_some(),
            None => false,
        };
        if let Some(entry) = group.shards.get_mut(self.key.shard)
            && let Some(slot) = entry
        {
            let _ = slot.connection.send(Command::Detach(self.id));
            slot.attaches = slot.attaches.saturating_sub(1);
            if freed {
                slot.addresses = slot.addresses.saturating_sub(1);
            }
            if slot.attaches == 0 {
                *entry = None;
            }
        }
        if group.shards.iter().all(Option::is_none) {
            state.groups.remove(&group_key);
        }
    }
}

fn group_key(section: &GrpcSection) -> (String, Commitment, Option<String>) {
    (
        section.endpoint.clone(),
        section.commitment,
        section.x_token_env.clone(),
    )
}

fn order(key: &ConnectionKey) -> (&str, &str, Option<&str>, usize) {
    (
        &key.endpoint,
        key.commitment.as_str(),
        key.token_env.as_deref(),
        key.shard,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use super::*;
    use crate::clients::disconnect::DisconnectReason;
    use crate::framework::grpc::{GeyserChannel, GrpcExitReason};
    use crate::framework::onchain::decoded::fixtures::key;
    use crate::framework::registry::TaskExit;
    use aetelier_types::onchain::MatchRole;

    #[derive(Default)]
    struct ScriptedDial {
        failures: Mutex<VecDeque<GrpcExitReason>>,
        dials: AtomicUsize,
    }

    impl ScriptedDial {
        fn failing_once(failure: GrpcExitReason) -> Self {
            Self {
                failures: Mutex::new(VecDeque::from([failure])),
                dials: AtomicUsize::new(0),
            }
        }

        fn dials(&self) -> usize {
            self.dials.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl GeyserDial for ScriptedDial {
        async fn dial(
            &self,
            _settings: &GrpcSettings,
        ) -> Result<GeyserChannel, GrpcExitReason> {
            self.dials.fetch_add(1, Ordering::SeqCst);
            let failure = self.failures.lock().expect("failures").pop_front();
            match failure {
                Some(failure) => Err(failure),
                None => std::future::pending().await,
            }
        }
    }

    fn section() -> GrpcSection {
        GrpcSection {
            endpoint: "http://mock.geyser".to_string(),
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

    fn capped(addresses: usize) -> GrpcSection {
        GrpcSection {
            max_addresses_per_connection: addresses,
            ..section()
        }
    }

    fn spec(section: GrpcSection, seed: u8) -> AttachSpec {
        AttachSpec {
            venue: "solana",
            section,
            x_token: Some(XToken::new("t".to_string())),
            watch: WatchedAddress {
                address: key(seed),
                required_role: MatchRole::AccountKey,
                include_failed: true,
                keep_logs: false,
            },
            reconnect: None,
            connect_rate: RateWindow {
                max: 10,
                per: Duration::from_secs(60),
            },
            queue_banks: 8,
            metrics: SourceMetrics::default(),
        }
    }

    fn pool(dial: &Arc<ScriptedDial>) -> GrpcConnections {
        let dial: Arc<dyn GeyserDial> = dial.clone();
        GrpcConnections::new(dial)
    }

    fn shard_key(section: &GrpcSection, shard: usize) -> ConnectionKey {
        ConnectionKey {
            endpoint: section.endpoint.clone(),
            commitment: section.commitment,
            token_env: section.x_token_env.clone(),
            shard,
        }
    }

    async fn settle() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn attaches_share_one_connection_per_endpoint_commitment_token_and_shard() {
        let dial = Arc::new(ScriptedDial::default());
        let pool = pool(&dial);
        let a = pool.attach(spec(section(), 1)).expect("attach");
        let b = pool.attach(spec(section(), 2)).expect("attach");
        let again = pool.attach(spec(section(), 1)).expect("attach");
        assert_eq!(a.key(), &shard_key(&section(), 0));
        assert_eq!((b.key(), again.key()), (a.key(), a.key()));
        assert!(a.id() < b.id() && b.id() < again.id());
        let confirmed_section = GrpcSection {
            commitment: Commitment::Confirmed,
            ..section()
        };
        let token_section = GrpcSection {
            x_token_env: Some("OTHER_X_TOKEN".to_string()),
            ..section()
        };
        let endpoint_section = GrpcSection {
            endpoint: "http://other.geyser".to_string(),
            ..section()
        };
        let confirmed = pool
            .attach(spec(confirmed_section.clone(), 1))
            .expect("attach");
        let token = pool.attach(spec(token_section.clone(), 1)).expect("attach");
        let endpoint = pool
            .attach(spec(endpoint_section.clone(), 1))
            .expect("attach");
        for other in [&confirmed, &token, &endpoint] {
            assert_ne!(other.key(), a.key());
            assert_eq!(other.key().shard, 0);
        }
        assert_eq!(
            pool.connections(),
            vec![
                (shard_key(&confirmed_section, 0), 1),
                (shard_key(&section(), 0), 3),
                (shard_key(&token_section, 0), 1),
                (shard_key(&endpoint_section, 0), 1),
            ]
        );
        settle().await;
        assert_eq!(dial.dials(), 4);
        assert!(!format!("{:?}", spec(section(), 1)).contains("\"t\""));
    }

    #[tokio::test(start_paused = true)]
    async fn an_address_beyond_the_shard_size_opens_the_next_shard_and_never_moves() {
        let dial = Arc::new(ScriptedDial::default());
        let pool = pool(&dial);
        let two = capped(2);
        let a = pool.attach(spec(two.clone(), 1)).expect("attach");
        let b = pool.attach(spec(two.clone(), 2)).expect("attach");
        let c = pool.attach(spec(two.clone(), 3)).expect("attach");
        let again = pool.attach(spec(two.clone(), 1)).expect("attach");
        let d = pool.attach(spec(two.clone(), 4)).expect("attach");
        let shards: Vec<usize> = [&a, &b, &c, &again, &d]
            .iter()
            .map(|attach| attach.key().shard)
            .collect();
        assert_eq!(shards, vec![0, 0, 1, 0, 1]);
        assert_eq!(
            pool.connections(),
            vec![(shard_key(&two, 0), 3), (shard_key(&two, 1), 2)]
        );
        drop(b);
        let e = pool.attach(spec(two.clone(), 5)).expect("attach");
        let c_again = pool.attach(spec(two.clone(), 3)).expect("attach");
        assert_eq!((e.key().shard, c_again.key().shard), (0, 1));
        let f = pool.attach(spec(two.clone(), 6)).expect("attach");
        assert_eq!(f.key().shard, 2);
        settle().await;
        assert_eq!(dial.dials(), 3);

        let narrow = Arc::new(ScriptedDial::default());
        let narrow_pool = self::pool(&narrow);
        let one = GrpcSection {
            account_include_max: Some(1),
            ..capped(2)
        };
        let first = narrow_pool.attach(spec(one.clone(), 1)).expect("attach");
        let second = narrow_pool.attach(spec(one, 2)).expect("attach");
        assert_eq!((first.key().shard, second.key().shard), (0, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn without_server_filtering_every_address_shares_one_connection() {
        let dial = Arc::new(ScriptedDial::default());
        let pool = pool(&dial);
        let unfiltered = GrpcSection {
            server_filter: false,
            account_include_max: Some(1),
            ..capped(2)
        };
        let attaches: Vec<GrpcAttach> = (1..=5)
            .map(|seed| pool.attach(spec(unfiltered.clone(), seed)).expect("attach"))
            .collect();
        assert!(
            attaches
                .iter()
                .all(|attach| attach.key() == &shard_key(&unfiltered, 0))
        );
        assert_eq!(pool.connections(), vec![(shard_key(&unfiltered, 0), 5)]);
        settle().await;
        assert_eq!(dial.dials(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_invalid_section_is_refused_before_any_dial() {
        let dial = Arc::new(ScriptedDial::default());
        let pool = pool(&dial);
        for (section, reason) in [
            (
                GrpcSection {
                    ping_interval_secs: 0,
                    ..section()
                },
                "[collect.grpc] ping_interval_secs = 0 admits nothing: set it to 1 or more",
            ),
            (
                GrpcSection {
                    ping_interval_secs: u64::MAX,
                    ..section()
                },
                "[collect.grpc] ping_interval_secs = 18446744073709551615 is above the \
                 limit of 3600: set it to 3600 or less",
            ),
            (
                GrpcSection {
                    max_pending_slots: 10_001,
                    ..section()
                },
                "[collect.grpc] max_pending_slots = 10001 is above the limit of 10000: set \
                 it to 10000 or less",
            ),
        ] {
            let refused = pool.attach(spec(section, 1)).err();
            assert_eq!(
                refused,
                Some(AttachError::InvalidSection {
                    reason: reason.to_string(),
                })
            );
            assert_eq!(refused.expect("refused").to_string(), reason);
        }
        settle().await;
        assert_eq!(dial.dials(), 0);
        assert!(pool.connections().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_detach_frees_its_slot_and_the_last_one_drops_the_shard() {
        let dial = Arc::new(ScriptedDial::default());
        let pool = pool(&dial);
        let two = capped(2);
        let a = pool.attach(spec(two.clone(), 1)).expect("attach");
        let b = pool.attach(spec(two.clone(), 2)).expect("attach");
        let c = pool.attach(spec(two.clone(), 3)).expect("attach");
        assert_eq!(c.key().shard, 1);
        drop(c);
        assert_eq!(pool.connections(), vec![(shard_key(&two, 0), 2)]);
        drop(a);
        assert_eq!(pool.connections(), vec![(shard_key(&two, 0), 1)]);
        let d = pool.attach(spec(two.clone(), 4)).expect("attach");
        assert_eq!(d.key().shard, 0);
        drop(b);
        drop(d);
        assert!(pool.connections().is_empty());
        let wider = pool
            .attach(spec(capped(3), 7))
            .expect("a new section after the last detach");
        assert_eq!(wider.key(), &shard_key(&capped(3), 0));
    }

    #[tokio::test(start_paused = true)]
    async fn a_second_section_under_the_same_key_is_refused() {
        let dial = Arc::new(ScriptedDial::default());
        let pool = pool(&dial);
        let _a = pool.attach(spec(section(), 1)).expect("attach");
        let longer = GrpcSection {
            max_pending_slots: 64,
            ..section()
        };
        let refused = pool.attach(spec(longer, 2)).err();
        assert_eq!(
            refused,
            Some(AttachError::SectionMismatch {
                endpoint: "http://mock.geyser".to_string(),
            })
        );
        assert_eq!(
            refused.expect("refused").to_string(),
            "[collect.grpc] for http://mock.geyser differs from the section already \
             connected under the same endpoint, commitment and token variable"
        );
        let secret = GrpcSection {
            endpoint: "http://mock.geyser:10000/s3cr3t".to_string(),
            ..section()
        };
        let _b = pool.attach(spec(secret.clone(), 1)).expect("attach");
        let mismatch = pool
            .attach(spec(
                GrpcSection {
                    accept_zstd: false,
                    ..secret
                },
                2,
            ))
            .err()
            .expect("refused");
        assert!(!mismatch.to_string().contains("s3cr3t"), "{mismatch}");
        assert!(mismatch.to_string().contains("http://mock.geyser:10000"));
        assert_eq!(pool.connections().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_reject_listed_address_is_refused_before_any_request() {
        let dial = Arc::new(ScriptedDial::default());
        let pool = pool(&dial);
        let rejecting = GrpcSection {
            account_include_reject: vec![key(1)],
            ..section()
        };
        let refused = pool.attach(spec(rejecting.clone(), 1)).err();
        assert_eq!(refused, Some(AttachError::RejectListed { address: key(1) }));
        let message = refused.expect("refused").to_string();
        assert!(message.contains(&key(1).to_base58()), "{message}");
        assert!(message.contains("account_include_reject"), "{message}");
        let unqueued = AttachSpec {
            queue_banks: 0,
            ..spec(rejecting.clone(), 2)
        };
        assert_eq!(pool.attach(unqueued).err(), Some(AttachError::NoQueue));
        settle().await;
        assert_eq!(dial.dials(), 0);
        assert!(pool.connections().is_empty());
        let _admitted = pool.attach(spec(rejecting, 2)).expect("attach");
        settle().await;
        assert_eq!(dial.dials(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_ended_connection_is_replaced_by_the_next_attach() {
        let dial = Arc::new(ScriptedDial::failing_once(GrpcExitReason::Setup(
            "x-token is not a header value".to_string(),
        )));
        let pool = pool(&dial);
        let mut a = pool.attach(spec(section(), 1)).expect("attach");
        assert!(a.recv().await.is_none());
        let b = pool.attach(spec(section(), 2)).expect("attach");
        assert_eq!(b.key(), a.key());
        assert_eq!(pool.connections(), vec![(shard_key(&section(), 0), 2)]);
        settle().await;
        assert_eq!(dial.dials(), 2);
        match a.finish().await {
            TaskExit::Failed(DisconnectReason::ProtocolRejection { code: 0, reason }) => {
                assert_eq!(reason, "x-token is not a header value");
            }
            other => panic!("expected a code 0 rejection, got {other:?}"),
        }
        assert_eq!(pool.connections(), vec![(shard_key(&section(), 0), 1)]);
        drop(b);
        assert!(pool.connections().is_empty());
    }
}
