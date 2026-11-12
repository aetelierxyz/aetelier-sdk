//! Venue-agnostic symbol codec: encodes and decodes trading pairs to and
//! from each venue's wire spelling, selected per venue by the registry.

use aetelier_types::Instrument;
use aetelier_types::onchain::{Chain, ChainAddress};
use aetelier_types::trading_pair::TradingPair;

/// How a venue spells a trading pair on the wire.
#[derive(Debug, Clone)]
pub enum SymbolCodec {
    /// `BTCUSDT` (Binance/Bybit/Bitget); `upper` controls case (HTX = false).
    Concat {
        upper: bool,
    },
    /// `BTC-USDT` (Coinbase/OKX/KuCoin), base first.
    Hyphen,
    /// `BTC/USDT` (Kraken).
    Slash,
    /// `BTC_USDT` (Gate.io/Poloniex/Bitso); `upper` controls case.
    Underscore {
        upper: bool,
    },
    /// `KRW-BTC` (Upbit), quote first.
    QuoteFirst {
        sep: char,
    },
    BareCoin {
        quote: &'static str,
    },
    ChainAddress {
        chain: Chain,
    },
}

impl SymbolCodec {
    /// Render a canonical pair to this venue's wire symbol.
    pub fn encode(&self, pair: &TradingPair) -> String {
        let (b, q) = (pair.base(), pair.quote());
        match self {
            SymbolCodec::Concat { upper } => Self::case(format!("{b}{q}"), *upper),
            SymbolCodec::Hyphen => format!("{b}-{q}"),
            SymbolCodec::Slash => format!("{b}/{q}"),
            SymbolCodec::Underscore { upper } => Self::case(format!("{b}_{q}"), *upper),
            SymbolCodec::QuoteFirst { sep } => format!("{q}{sep}{b}"),
            SymbolCodec::BareCoin { .. } => b.to_string(),
            SymbolCodec::ChainAddress { .. } => pair.to_canonical(),
        }
    }

    /// Parse a venue wire symbol back to a canonical pair.
    ///
    /// `Concat` (no separator) splits against the shared known-quote suffix
    /// table in `aetelier-types`; the other codecs split on their separator.
    pub fn decode(&self, raw: &str) -> Option<TradingPair> {
        let up = raw.to_uppercase();
        let (base, quote) = match self {
            SymbolCodec::Hyphen => up.split_once('-')?,
            SymbolCodec::Slash => up.split_once('/')?,
            SymbolCodec::Underscore { .. } => up.split_once('_')?,
            SymbolCodec::QuoteFirst { sep } => {
                let (q, b) = up.split_once(*sep)?;
                (b, q)
            }
            // No separator → suffix-match the known-quote table.
            SymbolCodec::Concat { .. } => return TradingPair::from_concatenated(&up),
            SymbolCodec::BareCoin { quote } => {
                let bare = raw.trim();
                if let Some((dex, ticker)) = bare.split_once(':') {
                    if dex.is_empty()
                        || ticker.is_empty()
                        || !dex.chars().all(|c| c.is_ascii_alphanumeric())
                        || !ticker.chars().all(|c| c.is_ascii_alphanumeric())
                    {
                        return None;
                    }
                    return Some(TradingPair::new(bare, *quote));
                }
                if up.is_empty() || up.contains(['@', '/', '-', '_']) {
                    return None;
                }
                (up.as_str(), *quote)
            }
            SymbolCodec::ChainAddress { .. } => return None,
        };
        Some(TradingPair::new(base, quote))
    }

    pub fn decode_instrument(&self, raw: &str) -> Option<Instrument> {
        match self {
            SymbolCodec::ChainAddress { chain } => ChainAddress::parse(*chain, raw)
                .ok()
                .map(Instrument::Address),
            SymbolCodec::Concat { .. }
            | SymbolCodec::Hyphen
            | SymbolCodec::Slash
            | SymbolCodec::Underscore { .. }
            | SymbolCodec::QuoteFirst { .. }
            | SymbolCodec::BareCoin { .. } => self.decode(raw).map(Instrument::Pair),
        }
    }

    fn case(s: String, upper: bool) -> String {
        if upper {
            s.to_uppercase()
        } else {
            s.to_lowercase()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codecs_round_trip() {
        let pair = TradingPair::new("BTC", "USDT");
        assert_eq!(SymbolCodec::Concat { upper: true }.encode(&pair), "BTCUSDT");
        assert_eq!(
            SymbolCodec::Concat { upper: false }.encode(&pair),
            "btcusdt"
        );
        assert_eq!(SymbolCodec::Hyphen.encode(&pair), "BTC-USDT");
        assert_eq!(
            SymbolCodec::Underscore { upper: false }.encode(&pair),
            "btc_usdt"
        );
        assert_eq!(
            SymbolCodec::QuoteFirst { sep: '-' }.encode(&pair),
            "USDT-BTC"
        );

        assert_eq!(SymbolCodec::Hyphen.decode("BTC-USDT"), Some(pair.clone()));
        assert_eq!(
            SymbolCodec::Slash.decode("BTC/USD"),
            Some(TradingPair::new("BTC", "USD"))
        );
        assert_eq!(
            SymbolCodec::Underscore { upper: false }.decode("BTC_USDT"),
            Some(pair.clone())
        );
        assert_eq!(
            SymbolCodec::QuoteFirst { sep: '-' }.decode("KRW-BTC"),
            Some(TradingPair::new("BTC", "KRW"))
        );
        // Concat is total: it suffix-matches the known-quote table.
        assert_eq!(
            SymbolCodec::Concat { upper: true }.decode("BTCUSDT"),
            Some(pair.clone())
        );
        assert_eq!(
            SymbolCodec::Concat { upper: false }.decode("btcusdt"),
            Some(pair)
        );
    }

    #[test]
    fn bare_coin_maps_to_fixed_quote_and_rejects_spot_forms() {
        let codec = SymbolCodec::BareCoin { quote: "USDC" };
        assert_eq!(codec.decode("BTC"), Some(TradingPair::new("BTC", "USDC")));
        assert_eq!(codec.encode(&TradingPair::new("BTC", "USDC")), "BTC");
        for raw in [
            "@107",
            "PURR/USDC",
            "BTC-USDC",
            "BTC_USDC",
            "xyz:",
            ":TSLA",
            "xyz:TS:LA",
            "xyz:@107",
            "",
        ] {
            assert_eq!(codec.decode(raw), None, "{raw:?} must be rejected");
        }
    }

    #[test]
    fn bare_coin_admits_dex_prefixed_hip3_markets_wire_exact() {
        let codec = SymbolCodec::BareCoin { quote: "USDC" };
        let pair = codec.decode("xyz:TSLA").unwrap();
        assert_eq!(pair.base(), "xyz:TSLA");
        assert_eq!(pair.quote(), "USDC");
        assert_eq!(codec.encode(&pair), "xyz:TSLA");

        let shouty = codec.decode("XYZ:tsla").unwrap();
        assert_eq!(shouty.base(), "xyz:TSLA");

        for raw in ["para:STX", "mkts:US500", "hyna:1000PEPE", "km:UNITREE"] {
            let p = codec.decode(raw).expect(raw);
            assert_eq!(codec.encode(&p), raw);
        }
    }

    const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

    #[test]
    fn chain_address_codec_is_case_exact_and_never_yields_a_pair() {
        let codec = SymbolCodec::ChainAddress {
            chain: Chain::Solana,
        };
        let instrument = codec.decode_instrument(JUPITER_V6).unwrap();
        assert!(matches!(instrument, Instrument::Address(_)));
        assert_eq!(instrument.to_string(), JUPITER_V6);
        assert_eq!(codec.decode(JUPITER_V6), None);
        for raw in [
            JUPITER_V6.to_lowercase(),
            format!(" {JUPITER_V6}"),
            "SOL/USDC".to_string(),
        ] {
            assert_eq!(codec.decode_instrument(&raw), None, "{raw:?}");
            assert_eq!(codec.decode(&raw), None, "{raw:?}");
        }
    }

    #[test]
    fn decode_instrument_equals_decode_for_every_pair_codec() {
        let registry = crate::framework::registry::registry();
        let entrepot = SymbolCodec::BareCoin { quote: "USDC" };
        let venues = [
            ("binance", "BTCUSDT"),
            ("bybit", "BTCUSDT"),
            ("bitget", "BTCUSDT"),
            ("htx", "btcusdt"),
            ("coinbase", "BTC-USD"),
            ("okx", "BTC-USDT"),
            ("kucoin", "BTC-USDT"),
            ("kraken", "BTC/USD"),
            ("gateio", "BTC_USDT"),
            ("poloniex", "BTC_USDT"),
            ("bitso", "btc_mxn"),
            ("upbit", "KRW-BTC"),
            ("hyperliquid", "BTC"),
        ];
        let cases = venues
            .into_iter()
            .map(|(venue, raw)| (venue, &registry[venue].profile().symbol_codec, raw))
            .chain([("entrepot", &entrepot, "ETH")]);
        for (venue, codec, raw) in cases {
            let pair = codec.decode(raw);
            assert!(pair.is_some(), "{venue} must decode {raw:?}");
            assert_eq!(
                codec.decode_instrument(raw),
                pair.map(Instrument::Pair),
                "{venue}"
            );
        }
    }
}
