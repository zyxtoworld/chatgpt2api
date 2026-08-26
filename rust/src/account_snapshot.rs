use std::sync::LazyLock;

use serde::Deserialize;

pub(super) const MAX_BYTES: u64 = 16 * 1024 * 1024;
pub(super) const MAX_RECORDS: usize = 10_000;
pub(super) const MAX_DEPTH: usize = 16;
pub(super) const MAX_NODES: usize = 262_144;
pub(super) const MAX_OBJECT_FIELDS: usize = 128;
pub(super) const MAX_KEY_BYTES: usize = 256;
pub(super) const MAX_STRING_BYTES: usize = 16 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Contract {
    max_bytes: u64,
    max_records: usize,
    max_depth: usize,
    max_nodes: usize,
    max_object_fields: usize,
    max_key_bytes: usize,
    max_string_bytes: usize,
}

static CONTRACT_VALID: LazyLock<bool> = LazyLock::new(|| {
    let Ok(contract) =
        serde_json::from_str::<Contract>(include_str!("../../account_snapshot_contract.json"))
    else {
        return false;
    };
    contract.max_bytes == MAX_BYTES
        && contract.max_records == MAX_RECORDS
        && contract.max_depth == MAX_DEPTH
        && contract.max_nodes == MAX_NODES
        && contract.max_object_fields == MAX_OBJECT_FIELDS
        && contract.max_key_bytes == MAX_KEY_BYTES
        && contract.max_string_bytes == MAX_STRING_BYTES
});

pub(super) fn validate_bytes(payload: &[u8]) -> Result<(), ()> {
    if !*CONTRACT_VALID || payload.len() as u64 > MAX_BYTES {
        return Err(());
    }
    Scanner {
        payload,
        index: 0,
        nodes: 0,
    }
    .scan()
}

struct Scanner<'a> {
    payload: &'a [u8],
    index: usize,
    nodes: usize,
}

impl Scanner<'_> {
    fn scan(mut self) -> Result<(), ()> {
        self.skip_whitespace();
        self.scan_value(1)?;
        self.skip_whitespace();
        (self.index == self.payload.len()).then_some(()).ok_or(())
    }

    fn skip_whitespace(&mut self) {
        while self
            .payload
            .get(self.index)
            .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
        {
            self.index += 1;
        }
    }

    fn consume(&mut self, expected: u8) -> Result<(), ()> {
        if self.payload.get(self.index).copied() != Some(expected) {
            return Err(());
        }
        self.index += 1;
        Ok(())
    }

    fn count_node(&mut self) -> Result<(), ()> {
        self.nodes = self.nodes.saturating_add(1);
        (self.nodes <= MAX_NODES).then_some(()).ok_or(())
    }

    fn scan_value(&mut self, depth: usize) -> Result<(), ()> {
        if depth > MAX_DEPTH {
            return Err(());
        }
        self.count_node()?;
        match self.payload.get(self.index).copied().ok_or(())? {
            b'{' => self.scan_object(depth),
            b'[' => self.scan_array(depth),
            b'"' => self.scan_string(MAX_STRING_BYTES),
            b't' => self.scan_literal(b"true"),
            b'f' => self.scan_literal(b"false"),
            b'n' => self.scan_literal(b"null"),
            b'-' | b'0'..=b'9' => self.scan_number(),
            _ => Err(()),
        }
    }

    fn scan_object(&mut self, depth: usize) -> Result<(), ()> {
        self.index += 1;
        self.skip_whitespace();
        if self.payload.get(self.index).copied() == Some(b'}') {
            self.index += 1;
            return Ok(());
        }
        let mut fields = 0usize;
        loop {
            fields = fields.saturating_add(1);
            if fields > MAX_OBJECT_FIELDS {
                return Err(());
            }
            self.scan_string(MAX_KEY_BYTES)?;
            self.skip_whitespace();
            self.consume(b':')?;
            self.skip_whitespace();
            self.scan_value(depth + 1)?;
            self.skip_whitespace();
            match self.payload.get(self.index).copied().ok_or(())? {
                b'}' => {
                    self.index += 1;
                    return Ok(());
                }
                b',' => {
                    self.index += 1;
                    self.skip_whitespace();
                }
                _ => return Err(()),
            }
        }
    }

    fn scan_array(&mut self, depth: usize) -> Result<(), ()> {
        self.index += 1;
        self.skip_whitespace();
        if self.payload.get(self.index).copied() == Some(b']') {
            self.index += 1;
            return Ok(());
        }
        let mut items = 0usize;
        loop {
            items = items.saturating_add(1);
            if items > MAX_RECORDS {
                return Err(());
            }
            self.scan_value(depth + 1)?;
            self.skip_whitespace();
            match self.payload.get(self.index).copied().ok_or(())? {
                b']' => {
                    self.index += 1;
                    return Ok(());
                }
                b',' => {
                    self.index += 1;
                    self.skip_whitespace();
                }
                _ => return Err(()),
            }
        }
    }

    fn scan_string(&mut self, max_bytes: usize) -> Result<(), ()> {
        self.consume(b'"')?;
        let mut decoded_bytes = 0usize;
        while let Some(byte) = self.payload.get(self.index).copied() {
            self.index += 1;
            match byte {
                b'"' => return Ok(()),
                0x00..=0x1f => return Err(()),
                b'\\' => {
                    let escape = self.payload.get(self.index).copied().ok_or(())?;
                    self.index += 1;
                    match escape {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {
                            decoded_bytes = decoded_bytes.saturating_add(1);
                        }
                        b'u' => {
                            let codepoint = self.scan_u_escape()?;
                            if (0xd800..=0xdbff).contains(&codepoint) {
                                if self.payload.get(self.index..self.index + 2) != Some(b"\\u") {
                                    return Err(());
                                }
                                self.index += 2;
                                let low = self.scan_u_escape()?;
                                if !(0xdc00..=0xdfff).contains(&low) {
                                    return Err(());
                                }
                                decoded_bytes = decoded_bytes.saturating_add(4);
                            } else if (0xdc00..=0xdfff).contains(&codepoint) {
                                return Err(());
                            } else {
                                decoded_bytes = decoded_bytes.saturating_add(match codepoint {
                                    0x0000..=0x007f => 1,
                                    0x0080..=0x07ff => 2,
                                    _ => 3,
                                });
                            }
                        }
                        _ => return Err(()),
                    }
                }
                0x20..=0x7f => decoded_bytes = decoded_bytes.saturating_add(1),
                lead => {
                    let width = utf8_width(lead).ok_or(())?;
                    let start = self.index - 1;
                    let end = start.checked_add(width).ok_or(())?;
                    let sequence = self.payload.get(start..end).ok_or(())?;
                    std::str::from_utf8(sequence).map_err(|_| ())?;
                    self.index = end;
                    decoded_bytes = decoded_bytes.saturating_add(width);
                }
            }
            if decoded_bytes > max_bytes {
                return Err(());
            }
        }
        Err(())
    }

    fn scan_u_escape(&mut self) -> Result<u16, ()> {
        let bytes = self.payload.get(self.index..self.index + 4).ok_or(())?;
        let mut value = 0u16;
        for byte in bytes {
            value = (value << 4) | u16::from(hex_value(*byte).ok_or(())?);
        }
        self.index += 4;
        Ok(value)
    }

    fn scan_literal(&mut self, literal: &[u8]) -> Result<(), ()> {
        if self.payload.get(self.index..self.index + literal.len()) != Some(literal) {
            return Err(());
        }
        self.index += literal.len();
        Ok(())
    }

    fn scan_number(&mut self) -> Result<(), ()> {
        if self.payload.get(self.index).copied() == Some(b'-') {
            self.index += 1;
        }
        match self.payload.get(self.index).copied().ok_or(())? {
            b'0' => self.index += 1,
            b'1'..=b'9' => {
                self.index += 1;
                while self.payload.get(self.index).is_some_and(u8::is_ascii_digit) {
                    self.index += 1;
                }
            }
            _ => return Err(()),
        }
        if self.payload.get(self.index).copied() == Some(b'.') {
            self.index += 1;
            let start = self.index;
            while self.payload.get(self.index).is_some_and(u8::is_ascii_digit) {
                self.index += 1;
            }
            if self.index == start {
                return Err(());
            }
        }
        if self
            .payload
            .get(self.index)
            .is_some_and(|byte| matches!(byte, b'e' | b'E'))
        {
            self.index += 1;
            if self
                .payload
                .get(self.index)
                .is_some_and(|byte| matches!(byte, b'+' | b'-'))
            {
                self.index += 1;
            }
            let start = self.index;
            while self.payload.get(self.index).is_some_and(u8::is_ascii_digit) {
                self.index += 1;
            }
            if self.index == start {
                return Err(());
            }
        }
        Ok(())
    }
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn utf8_width(lead: u8) -> Option<usize> {
    match lead {
        0xc2..=0xdf => Some(2),
        0xe0..=0xef => Some(3),
        0xf0..=0xf4 => Some(4),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn array_with_items(count: usize) -> Vec<u8> {
        let mut payload = Vec::with_capacity(count.saturating_mul(2).saturating_add(2));
        payload.push(b'[');
        for index in 0..count {
            if index > 0 {
                payload.push(b',');
            }
            payload.push(b'0');
        }
        payload.push(b']');
        payload
    }

    fn object_with_fields(count: usize) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(b'{');
        for index in 0..count {
            if index > 0 {
                payload.push(b',');
            }
            payload.extend_from_slice(format!(r#""k{index}":0"#).as_bytes());
        }
        payload.push(b'}');
        payload
    }

    fn nested_array_at_depth(depth: usize) -> Vec<u8> {
        let containers = depth.saturating_sub(1);
        let mut payload = vec![b'['; containers];
        payload.push(b'0');
        payload.extend(std::iter::repeat_n(b']', containers));
        payload
    }

    #[test]
    fn executable_contract_enforces_depth_array_and_object_boundaries() {
        assert!(validate_bytes(&nested_array_at_depth(MAX_DEPTH)).is_ok());
        assert!(validate_bytes(&nested_array_at_depth(MAX_DEPTH + 1)).is_err());
        assert!(validate_bytes(&array_with_items(MAX_RECORDS)).is_ok());
        assert!(validate_bytes(&array_with_items(MAX_RECORDS + 1)).is_err());
        assert!(validate_bytes(&object_with_fields(MAX_OBJECT_FIELDS)).is_ok());
        assert!(validate_bytes(&object_with_fields(MAX_OBJECT_FIELDS + 1)).is_err());
    }

    #[test]
    fn executable_contract_enforces_key_string_and_node_boundaries() {
        let key_at_limit = format!(r#"{{"{}":0}}"#, "k".repeat(MAX_KEY_BYTES));
        let key_over_limit = format!(r#"{{"{}":0}}"#, "k".repeat(MAX_KEY_BYTES + 1));
        assert!(validate_bytes(key_at_limit.as_bytes()).is_ok());
        assert!(validate_bytes(key_over_limit.as_bytes()).is_err());

        let string_at_limit = format!(r#""{}""#, "s".repeat(MAX_STRING_BYTES));
        let string_over_limit = format!(r#""{}""#, "s".repeat(MAX_STRING_BYTES + 1));
        assert!(validate_bytes(string_at_limit.as_bytes()).is_ok());
        assert!(validate_bytes(string_over_limit.as_bytes()).is_err());

        let mut node_bomb = Vec::new();
        node_bomb.push(b'[');
        for group in 0..MAX_RECORDS {
            if group > 0 {
                node_bomb.push(b',');
            }
            node_bomb.extend_from_slice(&array_with_items(26));
        }
        node_bomb.push(b']');
        assert!(node_bomb.len() as u64 <= MAX_BYTES);
        assert!(validate_bytes(&node_bomb).is_err());
    }
}
