use std::fmt;

use crate::onchain::ChainAddress;
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
}

impl fmt::Display for Instrument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical())
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
}
