use std::fmt;

use serde::{Deserialize, Serialize};

pub const PUBKEY_LEN: usize = 32;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Chain {
    Solana,
}

impl Chain {
    pub const fn id(self) -> &'static str {
        match self {
            Chain::Solana => "solana",
        }
    }
}

impl fmt::Display for Chain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.id())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AddressError {
    #[error("{chain} address is empty")]
    Empty { chain: Chain },
    #[error("{chain} address '{input}' is not base58")]
    NotBase58 { chain: Chain, input: String },
    #[error(
        "{chain} address '{input}' decodes to {len} bytes, expected {}",
        PUBKEY_LEN
    )]
    WrongLength {
        chain: Chain,
        input: String,
        len: usize,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChainAddress {
    chain: Chain,
    pubkey: [u8; PUBKEY_LEN],
}

impl ChainAddress {
    pub const fn new(chain: Chain, pubkey: [u8; PUBKEY_LEN]) -> Self {
        Self { chain, pubkey }
    }

    pub fn parse(chain: Chain, input: &str) -> Result<Self, AddressError> {
        if input.is_empty() {
            return Err(AddressError::Empty { chain });
        }
        let bytes = crate::onchain::base58::decode(input).ok_or_else(|| {
            AddressError::NotBase58 {
                chain,
                input: input.to_string(),
            }
        })?;
        let pubkey: [u8; PUBKEY_LEN] =
            bytes
                .as_slice()
                .try_into()
                .map_err(|_| AddressError::WrongLength {
                    chain,
                    input: input.to_string(),
                    len: bytes.len(),
                })?;
        Ok(Self { chain, pubkey })
    }

    pub fn solana(input: &str) -> Result<Self, AddressError> {
        Self::parse(Chain::Solana, input)
    }

    pub const fn chain(&self) -> Chain {
        self.chain
    }

    pub const fn pubkey(&self) -> &[u8; PUBKEY_LEN] {
        &self.pubkey
    }

    pub fn to_base58(&self) -> String {
        crate::onchain::base58::encode(&self.pubkey)
    }
}

impl fmt::Display for ChainAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_base58())
    }
}

impl fmt::Debug for ChainAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChainAddress({}:{})", self.chain, self.to_base58())
    }
}

impl Serialize for ChainAddress {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_base58())
    }
}

impl<'de> Deserialize<'de> for ChainAddress {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        ChainAddress::solana(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
    const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

    #[test]
    fn base58_round_trips_byte_exact_and_case_preserving() {
        for input in [JUPITER_V6, SYSTEM_PROGRAM] {
            let address = ChainAddress::solana(input).unwrap();
            assert_eq!(address.to_base58(), input);
            assert_eq!(address.to_string(), input);
            assert_eq!(address.chain(), Chain::Solana);
        }
        assert_eq!(
            ChainAddress::solana(SYSTEM_PROGRAM).unwrap().pubkey(),
            &[0u8; PUBKEY_LEN]
        );
    }

    #[test]
    fn case_folding_changes_the_address() {
        let original = ChainAddress::solana(JUPITER_V6).unwrap();
        let folded = ChainAddress::solana(&JUPITER_V6.to_lowercase());
        assert_ne!(folded.ok(), Some(original));
    }

    #[test]
    fn rejects_empty_non_base58_and_wrong_length_input() {
        assert_eq!(
            ChainAddress::solana(""),
            Err(AddressError::Empty {
                chain: Chain::Solana
            })
        );
        for bad in ["0OIl", " JUP6", "JUP6/USDC", "jup6-usdc"] {
            assert!(
                matches!(
                    ChainAddress::solana(bad),
                    Err(AddressError::NotBase58 { .. })
                ),
                "{bad}"
            );
        }
        assert!(matches!(
            ChainAddress::solana("JUP6"),
            Err(AddressError::WrongLength { len: 3, .. })
        ));
        assert!(matches!(
            ChainAddress::solana(&format!("1{SYSTEM_PROGRAM}")),
            Err(AddressError::WrongLength { len: 33, .. })
        ));
    }

    #[test]
    fn serde_is_the_bare_base58_string() {
        let address = ChainAddress::solana(JUPITER_V6).unwrap();
        let json = serde_json::to_string(&address).unwrap();
        assert_eq!(json, format!("\"{JUPITER_V6}\""));
        let back: ChainAddress = serde_json::from_str(&json).unwrap();
        assert_eq!(back, address);
        assert!(serde_json::from_str::<ChainAddress>("\"not-base58\"").is_err());
    }

    #[test]
    fn chain_serde_and_display_use_the_lowercase_id() {
        assert_eq!(Chain::Solana.to_string(), "solana");
        assert_eq!(serde_json::to_string(&Chain::Solana).unwrap(), "\"solana\"");
    }
}
