use aetelier_types::config::markets::market_config::TransactionsConfig;
use aetelier_types::onchain::{ChainAddress, MatchRole};

use super::decoded::DecodedTransaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchedAddress {
    pub address: ChainAddress,
    pub required_role: MatchRole,
    pub include_failed: bool,
    pub keep_logs: bool,
}

impl WatchedAddress {
    pub fn new(address: ChainAddress, config: &TransactionsConfig) -> Self {
        Self {
            address,
            required_role: config.match_role,
            include_failed: config.include_failed,
            keep_logs: config.logs,
        }
    }

    pub fn matches(&self, tx: &DecodedTransaction) -> Option<MatchRole> {
        if tx.is_vote || (!self.include_failed && !tx.succeeded()) {
            return None;
        }
        tx.role_of(&self.address)
            .filter(|role| role.satisfies(self.required_role))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::onchain::decoded::fixtures::{decoded, key};
    use crate::framework::onchain::decoded::{DecodedError, ExecutionError};

    fn watching(address: ChainAddress, required_role: MatchRole) -> WatchedAddress {
        WatchedAddress {
            address,
            required_role,
            include_failed: true,
            keep_logs: false,
        }
    }

    #[test]
    fn the_watch_follows_the_configured_match_role_and_failure_toggle() {
        let config = TransactionsConfig::default();
        let account_key = WatchedAddress::new(key(3), &config);
        assert_eq!(account_key.required_role, MatchRole::AccountKey);
        assert!(account_key.include_failed && !account_key.keep_logs);
        let tx = decoded(0);
        assert_eq!(account_key.matches(&tx), Some(MatchRole::AccountKey));
        assert_eq!(
            watching(key(1), MatchRole::Signer).matches(&tx),
            Some(MatchRole::FeePayer)
        );
        assert_eq!(watching(key(3), MatchRole::Signer).matches(&tx), None);
        assert_eq!(watching(key(2), MatchRole::FeePayer).matches(&tx), None);
        assert_eq!(watching(key(9), MatchRole::AccountKey).matches(&tx), None);
        let mut failed = decoded(1);
        failed.error = Some(ExecutionError {
            raw: vec![1, 0, 0, 0],
            decoded: Some(DecodedError {
                error: "AccountInUse".to_string(),
                instruction: None,
            }),
        });
        assert_eq!(account_key.matches(&failed), Some(MatchRole::AccountKey));
        let successful_only = WatchedAddress {
            include_failed: false,
            ..account_key
        };
        assert_eq!(successful_only.matches(&failed), None);
        assert_eq!(successful_only.matches(&tx), Some(MatchRole::AccountKey));
    }

    #[test]
    fn a_loaded_key_matches_only_the_account_key_role() {
        let mut tx = decoded(0);
        tx.loaded_writable = vec![key(5)];
        assert_eq!(
            watching(key(5), MatchRole::AccountKey).matches(&tx),
            Some(MatchRole::AccountKey)
        );
        assert_eq!(watching(key(5), MatchRole::Signer).matches(&tx), None);
    }

    #[test]
    fn the_vote_flag_excludes_but_a_vote_program_call_without_it_is_kept() {
        let vote_program =
            ChainAddress::solana("Vote111111111111111111111111111111111111111").unwrap();
        let mut management = decoded(0);
        management.static_keys[3] = vote_program;
        let watch = watching(key(1), MatchRole::AccountKey);
        assert_eq!(watch.matches(&management), Some(MatchRole::FeePayer));
        management.is_vote = true;
        assert_eq!(watch.matches(&management), None);
    }
}
