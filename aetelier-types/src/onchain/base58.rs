const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

fn digit(symbol: u8) -> Option<u32> {
    ALPHABET
        .iter()
        .position(|&candidate| candidate == symbol)
        .map(|value| value as u32)
}

pub fn encode(bytes: &[u8]) -> String {
    let zeros = bytes.iter().take_while(|&&byte| byte == 0).count();
    let mut digits: Vec<u8> = Vec::with_capacity(bytes.len() * 138 / 100 + 1);
    for &byte in &bytes[zeros..] {
        let mut carry = u32::from(byte);
        for value in digits.iter_mut() {
            carry += u32::from(*value) << 8;
            *value = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut out = String::with_capacity(zeros + digits.len());
    out.extend(std::iter::repeat_n('1', zeros));
    out.extend(
        digits
            .iter()
            .rev()
            .map(|&value| char::from(ALPHABET[usize::from(value)])),
    );
    out
}

pub(crate) fn decode(input: &str) -> Option<Vec<u8>> {
    let symbols = input.as_bytes();
    let zeros = symbols.iter().take_while(|&&symbol| symbol == b'1').count();
    let mut little_endian: Vec<u8> = Vec::with_capacity(symbols.len());
    for &symbol in &symbols[zeros..] {
        let mut carry = digit(symbol)?;
        for byte in little_endian.iter_mut() {
            carry += u32::from(*byte) * 58;
            *byte = (carry & 0xff) as u8;
            carry >>= 8;
        }
        while carry > 0 {
            little_endian.push((carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    let mut out = vec![0u8; zeros];
    out.extend(little_endian.iter().rev());
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    #[test]
    fn matches_the_reference_codec_on_random_inputs() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0x05ee_db58);
        for _ in 0..2_000 {
            let len = rng.random_range(0..=64);
            let zeros = rng.random_range(0..=len.min(4));
            let mut bytes = vec![0u8; len];
            rng.fill(&mut bytes[zeros..]);
            let reference = bs58::encode(&bytes).into_string();
            assert_eq!(encode(&bytes), reference, "{bytes:?}");
            assert_eq!(decode(&reference).as_deref(), Some(bytes.as_slice()));
        }
    }

    #[test]
    fn known_vectors() {
        assert_eq!(encode(&[]), "");
        assert_eq!(decode(""), Some(Vec::new()));
        assert_eq!(encode(&[0, 0, 1]), "112");
        assert_eq!(decode("112"), Some(vec![0, 0, 1]));
        assert_eq!(encode(b"hello world"), "StV1DL6CwTryKyV");
        assert_eq!(decode("StV1DL6CwTryKyV"), Some(b"hello world".to_vec()));
    }

    #[test]
    fn the_public_encoder_renders_a_signature_like_the_reference_codec() {
        let signature = [0x5au8; 64];
        assert_eq!(
            crate::onchain::encode_base58(&signature),
            bs58::encode(signature).into_string()
        );
    }

    #[test]
    fn rejects_symbols_outside_the_alphabet() {
        for bad in ["0", "O", "I", "l", "+", "/", " ", "é", "JUP6-"] {
            assert_eq!(decode(bad), None, "{bad}");
        }
    }
}
