use std::collections::HashSet;

use aetelier_types::funding::{FundingRate, FundingSettlement};
use aetelier_types::liquidations::Liquidation;
use aetelier_types::onchain::{ChainAddress, Transaction};
use aetelier_types::open_interest::OpenInterest;
use aetelier_types::orderbooks::Orderbook;
use aetelier_types::snapshots::MarketSnapshot;
use aetelier_types::trades::Trade;

#[derive(Default)]
pub struct DecomposedSnapshots {
    pub orderbooks: Vec<Orderbook>,
    pub trades: Vec<Trade>,
    pub liquidations: Vec<Liquidation>,
    pub funding_rates: Vec<FundingRate>,
    pub open_interests: Vec<OpenInterest>,
    pub funding_settlements: Vec<FundingSettlement>,
    pub transactions: Vec<Transaction>,
}

pub fn decompose_snapshots(snapshots: &[MarketSnapshot]) -> DecomposedSnapshots {
    let mut out = DecomposedSnapshots::default();
    let mut seen_fr: HashSet<(String, String, u64, u64)> = HashSet::new();
    let mut seen_oi: HashSet<(String, String, u64, u64)> = HashSet::new();
    let mut seen_fs: HashSet<(String, String, u64)> = HashSet::new();
    let mut seen_tx: HashSet<(ChainAddress, String)> = HashSet::new();

    for snap in snapshots {
        if let Some(ob) = &snap.orderbook {
            let unsourced =
                ob.source_orderbook_ts_us == 0 && ob.local_orderbook_ts_us == 0;
            out.orderbooks.push(Orderbook {
                orderbook_ts_us: snap.ts_us.max(ob.orderbook_ts_us),
                source_orderbook_ts_us: if unsourced {
                    ob.orderbook_ts_us
                } else {
                    ob.source_orderbook_ts_us
                },
                ..ob.clone()
            });
        }
        out.trades.extend(snap.trades.iter().cloned());
        out.liquidations.extend(snap.liquidations.iter().cloned());
        for fr in &snap.funding_rate {
            let provenance_free = fr.recv_seq == 0;
            if provenance_free
                || seen_fr.insert((
                    fr.exchange.clone(),
                    fr.pair.to_canonical(),
                    fr.conn_epoch_us,
                    fr.recv_seq,
                ))
            {
                out.funding_rates.push(fr.clone());
            }
        }
        for oi in &snap.open_interest {
            let provenance_free = oi.recv_seq == 0;
            if provenance_free
                || seen_oi.insert((
                    oi.exchange.clone(),
                    oi.pair.to_canonical(),
                    oi.conn_epoch_us,
                    oi.recv_seq,
                ))
            {
                out.open_interests.push(oi.clone());
            }
        }
        for fs in &snap.funding_settlements {
            if seen_fs.insert((
                fs.exchange.clone(),
                fs.pair.to_canonical(),
                fs.funding_time_us,
            )) {
                out.funding_settlements.push(fs.clone());
            }
        }
        for tx in &snap.transactions {
            if seen_tx.insert((tx.address, tx.signature.clone())) {
                out.transactions.push(tx.clone());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use aetelier_types::trading_pair::TradingPair;
    use rust_decimal::Decimal;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn fr_epoch(recv_seq: u64, conn_epoch_us: u64) -> FundingRate {
        FundingRate {
            conn_epoch_us,
            ..fr(recv_seq)
        }
    }

    fn fr(recv_seq: u64) -> FundingRate {
        FundingRate {
            funding_rate_ts_us: 0,
            local_funding_ts_us: 1_700_000_000_000_000,
            recv_seq,
            conn_epoch_us: 1,
            pair: TradingPair::new("BTC", "USDC"),
            funding_rate: d("0.0000125"),
            premium: None,
            interval_hours: 1,
            next_funding_ts_us: 0,
            exchange: "hyperliquid".to_string(),
        }
    }

    fn settlement(funding_time_us: u64) -> FundingSettlement {
        FundingSettlement {
            funding_time_us,
            local_ts_us: funding_time_us + 50_000,
            rtt_us: 1_000,
            pair: TradingPair::new("BTC", "USDC"),
            funding_rate: d("0.0000125"),
            premium: None,
            exchange: "hyperliquid".to_string(),
        }
    }

    fn tx(address: &str, signature: &str) -> Transaction {
        use aetelier_types::onchain::{Commitment, MatchRole, TransactionVersion};
        Transaction {
            exchange: "solana".to_string(),
            address: ChainAddress::solana(address).unwrap(),
            match_role: MatchRole::AccountKey,
            source_tx_ts_us: 1_791_321_067_000_000,
            local_tx_ts_us: 0,
            source_tx_rtt_us: 0,
            recv_seq: 0,
            conn_epoch_us: 0,
            commitment: Commitment::Finalized,
            slot: 380_000_000,
            block_height: 358_000_000,
            blockhash: "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn".to_string(),
            tx_index: 0,
            signature: signature.to_string(),
            version: TransactionVersion::Legacy,
            success: true,
            fee_lamports: 5_000,
            compute_units_consumed: None,
            compute_unit_limit: None,
            compute_unit_price_micro_lamports: None,
            priority_fee_lamports: None,
            cost_units: None,
            signers: Vec::new(),
            account_keys: Vec::new(),
            log_messages: None,
            failure: None,
        }
    }

    #[test]
    fn transactions_dedup_by_address_and_signature_only() {
        let jupiter = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
        let whirlpool = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
        let mut snap_a = MarketSnapshot::empty(1_000_000);
        snap_a.transactions = vec![tx(jupiter, "s1"), tx(jupiter, "s2")];
        let mut snap_b = MarketSnapshot::empty(2_000_000);
        snap_b.transactions = vec![tx(jupiter, "s2"), tx(whirlpool, "s2")];

        let out = decompose_snapshots(&[snap_a, snap_b]);
        let keys: Vec<(String, String)> = out
            .transactions
            .iter()
            .map(|t| (t.address.to_base58(), t.signature.clone()))
            .collect();
        assert_eq!(
            keys,
            vec![
                (jupiter.to_string(), "s1".to_string()),
                (jupiter.to_string(), "s2".to_string()),
                (whirlpool.to_string(), "s2".to_string()),
            ]
        );
    }

    #[test]
    fn dedups_carried_forward_samples_by_provenance_key() {
        let mut snap_a = MarketSnapshot::empty(1_000_000);
        snap_a.funding_rate = vec![fr(1), fr(2)];
        let mut snap_b = MarketSnapshot::empty(2_000_000);
        snap_b.funding_rate = vec![fr(2), fr(3)];

        let out = decompose_snapshots(&[snap_a, snap_b]);
        let seqs: Vec<u64> = out.funding_rates.iter().map(|f| f.recv_seq).collect();
        assert_eq!(seqs, vec![1, 2, 3], "wire truth: one row per push");
    }

    #[test]
    fn reconnect_rows_reusing_a_recv_seq_survive_on_their_epoch() {
        let mut before = MarketSnapshot::empty(1_000_000);
        before.funding_rate = vec![fr_epoch(1, 1_788_375_005_000_000)];
        let mut after = MarketSnapshot::empty(2_000_000);
        after.funding_rate = vec![fr_epoch(1, 1_788_375_198_000_000)];

        let out = decompose_snapshots(&[before, after]);
        let epochs: Vec<u64> =
            out.funding_rates.iter().map(|f| f.conn_epoch_us).collect();
        assert_eq!(
            epochs,
            vec![1_788_375_005_000_000, 1_788_375_198_000_000],
            "recv_seq restarts at 1 on reconnect; the epoch keeps both rows"
        );
    }

    #[test]
    fn one_epoch_still_collapses_a_repeated_recv_seq() {
        let mut snap_a = MarketSnapshot::empty(1_000_000);
        snap_a.funding_rate = vec![fr_epoch(2, 1_788_375_005_000_000)];
        let mut snap_b = MarketSnapshot::empty(2_000_000);
        snap_b.funding_rate = vec![fr_epoch(2, 1_788_375_005_000_000)];

        let out = decompose_snapshots(&[snap_a, snap_b]);
        assert_eq!(out.funding_rates.len(), 1);
    }

    #[test]
    fn provenance_free_legacy_rows_are_never_collapsed() {
        let mut snap = MarketSnapshot::empty(1_000_000);
        snap.funding_rate = vec![fr(0), fr(0)];
        let out = decompose_snapshots(&[snap]);
        assert_eq!(out.funding_rates.len(), 2);
    }

    #[test]
    fn dedups_settlements_by_venue_settlement_time() {
        let mut snap_a = MarketSnapshot::empty(1_000_000);
        snap_a.funding_settlements = vec![settlement(3_600_000_000)];
        let mut snap_b = MarketSnapshot::empty(2_000_000);
        snap_b.funding_settlements =
            vec![settlement(3_600_000_000), settlement(7_200_000_000)];

        let out = decompose_snapshots(&[snap_a, snap_b]);
        let times: Vec<u64> = out
            .funding_settlements
            .iter()
            .map(|f| f.funding_time_us)
            .collect();
        assert_eq!(times, vec![3_600_000_000, 7_200_000_000]);
    }

    #[test]
    fn carried_forward_books_take_their_grid_tick_and_keep_venue_time() {
        let venue_ts = 1_791_321_096_929_000;
        let mut book = Orderbook::new(
            0,
            venue_ts,
            TradingPair::new("NVDAB", "USDT"),
            "binance".to_string(),
            std::collections::BTreeMap::new(),
            std::collections::BTreeMap::new(),
        );
        book.source_orderbook_ts_us = venue_ts;
        let mut snap_a = MarketSnapshot::empty(1_791_321_097_000_000);
        snap_a.orderbook = Some(book.clone());
        let mut snap_b = MarketSnapshot::empty(1_791_321_097_100_000);
        snap_b.orderbook = Some(book);

        let out = decompose_snapshots(&[snap_a, snap_b]);
        let stamps: Vec<(u64, u64)> = out
            .orderbooks
            .iter()
            .map(|o| (o.orderbook_ts_us, o.source_orderbook_ts_us))
            .collect();
        assert_eq!(
            stamps,
            vec![
                (1_791_321_097_000_000, venue_ts),
                (1_791_321_097_100_000, venue_ts)
            ]
        );
    }

    #[test]
    fn a_book_newer_than_its_tick_keeps_its_own_time() {
        let venue_ts = 1_791_321_097_142_000;
        let mut book = Orderbook::new(
            0,
            venue_ts,
            TradingPair::new("NVDAB", "USDT"),
            "binance".to_string(),
            std::collections::BTreeMap::new(),
            std::collections::BTreeMap::new(),
        );
        book.source_orderbook_ts_us = venue_ts;
        let mut terminal = MarketSnapshot::empty(1_791_321_097_100_000);
        terminal.orderbook = Some(book);

        let out = decompose_snapshots(&[terminal]);
        assert_eq!(out.orderbooks[0].orderbook_ts_us, venue_ts);
    }

    #[test]
    fn rest_seeded_books_keep_their_receipt_time() {
        let mut book = Orderbook::new(
            0,
            1_791_321_096_990_000,
            TradingPair::new("BTC", "USDT"),
            "binance".to_string(),
            std::collections::BTreeMap::new(),
            std::collections::BTreeMap::new(),
        );
        book.local_orderbook_ts_us = 1_791_321_096_950_000;
        let mut snap = MarketSnapshot::empty(1_791_321_097_000_000);
        snap.orderbook = Some(book);

        let row = &decompose_snapshots(&[snap]).orderbooks[0];
        assert_eq!(
            (
                row.orderbook_ts_us,
                row.source_orderbook_ts_us,
                row.local_orderbook_ts_us
            ),
            (1_791_321_097_000_000, 0, 1_791_321_096_950_000)
        );
        assert_eq!(row.effective_ts_us(), 1_791_321_096_950_000);
    }

    #[test]
    fn books_without_provenance_keep_their_venue_time_as_source() {
        let venue_ts = 1_791_321_096_929_000;
        let book = Orderbook::new(
            0,
            venue_ts,
            TradingPair::new("BTC", "USDT"),
            "bybit".to_string(),
            std::collections::BTreeMap::new(),
            std::collections::BTreeMap::new(),
        );
        let mut snap = MarketSnapshot::empty(1_791_321_097_000_000);
        snap.orderbook = Some(book);

        let out = decompose_snapshots(&[snap]);
        assert_eq!(
            (
                out.orderbooks[0].orderbook_ts_us,
                out.orderbooks[0].source_orderbook_ts_us
            ),
            (1_791_321_097_000_000, venue_ts)
        );
    }
}
