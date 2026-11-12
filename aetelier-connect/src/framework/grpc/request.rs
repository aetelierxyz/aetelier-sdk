use std::collections::{BTreeSet, HashMap};

use aetelier_types::onchain::{ChainAddress, Commitment};
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterTransactions, SubscribeRequestPing,
};

use crate::config::workers::market_worker_config::GrpcSection;
use crate::framework::onchain::WatchedAddress;

pub const TRANSACTIONS_FILTER: &str = "transactions";
pub const BLOCKS_META_FILTER: &str = "blocks_meta";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamFilter {
    pub addresses: BTreeSet<ChainAddress>,
    pub include_failed: bool,
    pub commitment: Commitment,
    pub server_filter: bool,
}

impl StreamFilter {
    pub fn new<'a>(
        watched: impl IntoIterator<Item = &'a WatchedAddress>,
        section: &GrpcSection,
    ) -> Self {
        let mut addresses = BTreeSet::new();
        let mut include_failed = false;
        for watch in watched {
            addresses.insert(watch.address);
            include_failed |= watch.include_failed;
        }
        Self {
            addresses,
            include_failed,
            commitment: section.commitment,
            server_filter: section.server_filter,
        }
    }

    pub fn admit(&self, section: &GrpcSection) -> Result<(), String> {
        if self.addresses.is_empty() {
            return Err(
                "[collect.grpc] the subscription watches no address: an empty \
                 account_include streams every transaction"
                    .to_string(),
            );
        }
        if !self.server_filter {
            return Ok(());
        }
        let count = self.addresses.len();
        if let Some(max) = section.account_include_max
            && count > max
        {
            return Err(format!(
                "[collect.grpc] the subscription watches {count} addresses, over \
                 account_include_max = {max}: raise account_include_max or watch fewer \
                 addresses"
            ));
        }
        match self
            .addresses
            .iter()
            .find(|address| section.account_include_reject.contains(address))
        {
            Some(address) => Err(format!(
                "[collect.grpc] address {address} is in account_include_reject: the \
                 provider refuses to filter on it, so choose another address or provider"
            )),
            None => Ok(()),
        }
    }

    pub fn request(&self) -> SubscribeRequest {
        let account_include = if self.server_filter {
            self.addresses.iter().map(ChainAddress::to_base58).collect()
        } else {
            Vec::new()
        };
        SubscribeRequest {
            transactions: HashMap::from([(
                TRANSACTIONS_FILTER.to_string(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: (!self.include_failed).then_some(false),
                    account_include,
                    ..Default::default()
                },
            )]),
            blocks_meta: HashMap::from([(
                BLOCKS_META_FILTER.to_string(),
                SubscribeRequestFilterBlocksMeta {},
            )]),
            commitment: Some(commitment_level(self.commitment) as i32),
            ..Default::default()
        }
    }
}

pub const fn commitment_level(commitment: Commitment) -> CommitmentLevel {
    match commitment {
        Commitment::Confirmed => CommitmentLevel::Confirmed,
        Commitment::Finalized => CommitmentLevel::Finalized,
    }
}

pub fn ping_request(id: i32) -> SubscribeRequest {
    SubscribeRequest {
        ping: Some(SubscribeRequestPing { id }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::onchain::decoded::fixtures::key;
    use aetelier_types::onchain::MatchRole;

    fn section() -> GrpcSection {
        GrpcSection {
            endpoint: "https://solana-yellowstone-grpc.publicnode.com:443".to_string(),
            x_token_env: None,
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

    fn watching(address: ChainAddress, include_failed: bool) -> WatchedAddress {
        WatchedAddress {
            address,
            required_role: MatchRole::AccountKey,
            include_failed,
            keep_logs: false,
        }
    }

    fn transactions(request: &SubscribeRequest) -> &SubscribeRequestFilterTransactions {
        &request.transactions[TRANSACTIONS_FILTER]
    }

    #[test]
    fn the_request_streams_every_watched_address_with_votes_excluded_and_no_replay() {
        let watched = [
            watching(key(3), true),
            watching(key(1), true),
            watching(key(2), true),
        ];
        let request = StreamFilter::new(&watched, &section()).request();
        let expected = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions: HashMap::from([(
                "transactions".to_string(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: None,
                    signature: None,
                    account_include: vec![
                        key(1).to_base58(),
                        key(2).to_base58(),
                        key(3).to_base58(),
                    ],
                    account_exclude: Vec::new(),
                    account_required: Vec::new(),
                    cuckoo_account_include: None,
                    token_accounts: None,
                },
            )]),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::from([(
                "blocks_meta".to_string(),
                SubscribeRequestFilterBlocksMeta {},
            )]),
            entry: HashMap::new(),
            commitment: Some(2),
            accounts_data_slice: Vec::new(),
            ping: None,
            from_slot: None,
            block_footer: HashMap::new(),
        };
        assert_eq!(request, expected);
    }

    #[test]
    fn failed_transactions_are_dropped_by_the_server_only_when_no_watch_keeps_them() {
        let failed = |keeps: [bool; 2]| {
            let watched = [watching(key(1), keeps[0]), watching(key(2), keeps[1])];
            transactions(&StreamFilter::new(&watched, &section()).request()).failed
        };
        assert_eq!(failed([false, true]), None);
        assert_eq!(failed([true, false]), None);
        assert_eq!(failed([true, true]), None);
        assert_eq!(failed([false, false]), Some(false));
        for keeps in [[false, false], [false, true], [true, false], [true, true]] {
            assert_ne!(failed(keeps), Some(true), "{keeps:?}");
        }
    }

    #[test]
    fn the_commitment_is_always_explicit() {
        let watched = [watching(key(1), true)];
        for (commitment, level, wire) in [
            (Commitment::Confirmed, CommitmentLevel::Confirmed, 1),
            (Commitment::Finalized, CommitmentLevel::Finalized, 2),
        ] {
            assert_eq!(commitment_level(commitment), level);
            let section = GrpcSection {
                commitment,
                ..section()
            };
            let filter = StreamFilter::new(&watched, &section);
            assert_eq!(filter.commitment, commitment);
            assert_eq!(filter.request().commitment, Some(wire), "{commitment}");
        }
    }

    #[test]
    fn without_server_filtering_the_request_names_no_address() {
        let section = GrpcSection {
            server_filter: false,
            ..section()
        };
        let watched = [watching(key(1), false), watching(key(2), false)];
        let filter = StreamFilter::new(&watched, &section);
        assert_eq!(filter.addresses, BTreeSet::from([key(1), key(2)]));
        let request = filter.request();
        let only = transactions(&request);
        assert!(only.account_include.is_empty());
        assert_eq!((only.vote, only.failed), (Some(false), Some(false)));
        assert_eq!(request.transactions.len(), 1);
        assert_eq!(request.blocks_meta.len(), 1);
        assert_eq!(request.commitment, Some(2));
        assert_eq!((request.from_slot, request.ping), (None, None));
    }

    #[test]
    fn a_ping_request_carries_the_ping_and_nothing_else() {
        let ping = ping_request(7);
        assert_eq!(
            ping,
            SubscribeRequest {
                ping: Some(SubscribeRequestPing { id: 7 }),
                ..Default::default()
            }
        );
        assert!(ping.transactions.is_empty() && ping.blocks_meta.is_empty());
        assert!(ping.accounts.is_empty() && ping.slots.is_empty());
        assert_eq!((ping.commitment, ping.from_slot), (None, None));
    }

    #[test]
    fn admission_refuses_an_empty_set_a_set_over_the_cap_and_a_reject_listed_address() {
        let empty = StreamFilter::new(&[], &section()).admit(&section());
        assert!(
            empty
                .unwrap_err()
                .contains("an empty account_include streams every transaction")
        );

        let watched = [
            watching(key(1), true),
            watching(key(2), true),
            watching(key(3), true),
        ];
        let filter = StreamFilter::new(&watched, &section());
        assert_eq!(filter.admit(&section()), Ok(()));
        let capped = GrpcSection {
            account_include_max: Some(2),
            ..section()
        };
        let over = filter.admit(&capped).unwrap_err();
        assert!(over.contains("account_include_max = 2"), "{over}");
        assert!(over.contains("watches 3 addresses"), "{over}");
        let at_cap = GrpcSection {
            account_include_max: Some(3),
            ..section()
        };
        assert_eq!(filter.admit(&at_cap), Ok(()));

        let rejecting = GrpcSection {
            account_include_reject: vec![key(9), key(2)],
            ..section()
        };
        let listed = filter.admit(&rejecting).unwrap_err();
        assert!(listed.contains(&key(2).to_base58()), "{listed}");
        assert!(listed.contains("account_include_reject"), "{listed}");

        let unfiltered = StreamFilter {
            server_filter: false,
            ..filter
        };
        assert_eq!(unfiltered.admit(&capped), Ok(()));
        assert_eq!(unfiltered.admit(&rejecting), Ok(()));
        let none = StreamFilter {
            addresses: BTreeSet::new(),
            ..unfiltered
        };
        assert!(none.admit(&section()).is_err());
    }

    #[test]
    fn a_duplicate_watch_names_its_address_once() {
        let watched = [
            watching(key(1), false),
            WatchedAddress {
                required_role: MatchRole::Signer,
                ..watching(key(1), true)
            },
        ];
        let filter = StreamFilter::new(&watched, &section());
        assert_eq!(filter.addresses, BTreeSet::from([key(1)]));
        assert!(filter.include_failed);
        assert_eq!(
            transactions(&filter.request()).account_include,
            vec![key(1).to_base58()]
        );
    }
}
