use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::exchanges::Exchange;
use crate::onchain::{Chain, ChainAddress};
use crate::trading_pair::TradingPair;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Instrument {
    Pair(TradingPair),
    Address(ChainAddress),
}

impl Instrument {
    pub fn as_pair(&self) -> Option<&TradingPair> {
        match self {
            Instrument::Pair(pair) => Some(pair),
            Instrument::Address(_) => None,
        }
    }

    pub fn as_address(&self) -> Option<&ChainAddress> {
        match self {
            Instrument::Pair(_) => None,
            Instrument::Address(address) => Some(address),
        }
    }

    pub fn canonical(&self) -> String {
        match self {
            Instrument::Pair(pair) => pair.to_canonical(),
            Instrument::Address(address) => address.to_base58(),
        }
    }

    pub fn from_exchange_symbol(raw: &str, exchange: Exchange) -> Option<Instrument> {
        match exchange.chain() {
            Some(chain) => ChainAddress::parse(chain, raw)
                .ok()
                .map(Instrument::Address),
            None => {
                TradingPair::from_exchange_symbol(raw, exchange).map(Instrument::Pair)
            }
        }
    }
}

impl fmt::Display for Instrument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical())
    }
}

impl FromStr for Instrument {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains('/') {
            TradingPair::from_canonical(s)
                .map(Instrument::Pair)
                .ok_or_else(|| format!("invalid trading pair: {s}"))
        } else {
            ChainAddress::parse(Chain::Solana, s)
                .map(Instrument::Address)
                .map_err(|e| e.to_string())
        }
    }
}

impl Serialize for Instrument {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.canonical())
    }
}

impl<'de> Deserialize<'de> for Instrument {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl From<TradingPair> for Instrument {
    fn from(pair: TradingPair) -> Self {
        Instrument::Pair(pair)
    }
}

impl From<ChainAddress> for Instrument {
    fn from(address: ChainAddress) -> Self {
        Instrument::Address(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

    #[test]
    fn instrument_canonical_keeps_each_identity_form() {
        let pair = Instrument::from(TradingPair::new("sol", "usdc"));
        assert_eq!(pair.canonical(), "SOL/USDC");
        assert!(pair.as_address().is_none());
        let address = Instrument::from(ChainAddress::solana(JUPITER_V6).unwrap());
        assert_eq!(address.to_string(), JUPITER_V6);
        assert_eq!(
            address.as_address().map(ChainAddress::chain),
            Some(crate::onchain::Chain::Solana)
        );
        assert!(address.as_pair().is_none());
    }

    #[test]
    fn pair_string_form_is_byte_identical_to_trading_pair() {
        for (base, quote) in [("btc", "usdt"), ("xyz:tsla", "usdc")] {
            let pair = TradingPair::new(base, quote);
            let instrument = Instrument::from(pair.clone());
            assert_eq!(
                serde_json::to_string(&instrument).unwrap(),
                serde_json::to_string(&pair).unwrap()
            );
            assert_eq!(instrument.to_string(), pair.to_canonical());
        }
        let expected = Instrument::Pair(TradingPair::new("BTC", "USDT"));
        let back: Instrument = serde_json::from_str("\"btc/usdt\"").unwrap();
        assert_eq!(back, expected);
        assert_eq!("btc/usdt".parse::<Instrument>(), Ok(expected));
    }

    #[test]
    fn address_string_form_is_case_exact_base58() {
        let address = Instrument::from(ChainAddress::solana(JUPITER_V6).unwrap());
        let json = serde_json::to_string(&address).unwrap();
        assert_eq!(json, format!("\"{JUPITER_V6}\""));
        let back: Instrument = serde_json::from_str(&json).unwrap();
        assert_eq!(back, address);
        assert_eq!(JUPITER_V6.parse::<Instrument>(), Ok(address));
        let folded = JUPITER_V6.to_lowercase();
        let err = folded.parse::<Instrument>().unwrap_err();
        assert!(err.contains("is not base58"), "{err}");
        assert!(serde_json::from_str::<Instrument>(&format!("\"{folded}\"")).is_err());
    }

    #[test]
    fn slashless_strings_never_become_pairs() {
        for raw in ["BTCUSDT", "SOLUSDC"] {
            let err = raw.parse::<Instrument>().unwrap_err();
            assert!(err.starts_with("solana address"), "{raw}: {err}");
            assert!(serde_json::from_str::<Instrument>(&format!("\"{raw}\"")).is_err());
        }
        assert_eq!(
            "BTC/".parse::<Instrument>(),
            Err("invalid trading pair: BTC/".to_string())
        );
        assert!("".parse::<Instrument>().is_err());
    }

    #[test]
    fn from_exchange_symbol_routes_by_venue() {
        assert_eq!(
            Instrument::from_exchange_symbol(JUPITER_V6, Exchange::Solana),
            Some(Instrument::Address(
                ChainAddress::solana(JUPITER_V6).unwrap()
            ))
        );
        assert_eq!(
            Instrument::from_exchange_symbol("BTCUSDT", Exchange::Bybit),
            Some(Instrument::Pair(TradingPair::new("BTC", "USDT")))
        );
        assert_eq!(
            Instrument::from_exchange_symbol("BTC", Exchange::Hyperliquid),
            Some(Instrument::Pair(TradingPair::new("BTC", "USDC")))
        );
        assert_eq!(
            Instrument::from_exchange_symbol("BTCUSDT", Exchange::Solana),
            None
        );
    }
}
