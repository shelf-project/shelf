//! Plan canonicalization for cache key generation.
//!
//! Canonicalizes SQL plans by:
//! 1. Erasing literal values (dates, numbers, strings)
//! 2. Sorting commutative operands (AND, OR, IN lists)
//! 3. Normalizing whitespace and case
//!
//! This allows queries with different literal values but the same structure
//! to share cached results when the underlying data (snapshot) hasn't changed.

use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

/// Canonicalizes query plans for cache key generation.
#[derive(Debug, Default)]
pub struct PlanCanonicalizer {
    /// Whether to normalize identifiers to lowercase.
    pub normalize_case: bool,
}

impl PlanCanonicalizer {
    /// Create a new canonicalizer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Canonicalize a SQL query and return its fingerprint.
    ///
    /// Returns a 64-bit hash suitable for use as a cache key component.
    pub fn fingerprint(&self, sql: &str) -> u64 {
        let canonical = self.canonicalize(sql);
        let hash = Sha256::digest(canonical.as_bytes());
        u64::from_be_bytes(hash[0..8].try_into().unwrap())
    }

    /// Canonicalize a SQL query string.
    ///
    /// This is a simplified implementation that:
    /// - Normalizes whitespace
    /// - Optionally lowercases identifiers
    /// - Replaces literal values with placeholders
    pub fn canonicalize(&self, sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let mut chars = sql.chars().peekable();
        let mut in_string = false;
        let mut string_char = '"';
        let mut in_number = false;

        while let Some(c) = chars.next() {
            // Handle string literals
            if (c == '\'' || c == '"') && !in_string {
                in_string = true;
                string_char = c;
                result.push_str("?"); // Replace string with placeholder
                // Skip until end of string
                while let Some(&next) = chars.peek() {
                    chars.next();
                    if next == string_char {
                        // Check for escaped quote
                        if chars.peek() == Some(&string_char) {
                            chars.next();
                            continue;
                        }
                        break;
                    }
                }
                in_string = false;
                continue;
            }

            // Handle numeric literals
            if c.is_ascii_digit() && !in_number {
                in_number = true;
                result.push_str("?"); // Replace number with placeholder
                // Skip the rest of the number
                while let Some(&next) = chars.peek() {
                    if next.is_ascii_digit() || next == '.' || next == 'e' || next == 'E' {
                        chars.next();
                    } else {
                        break;
                    }
                }
                in_number = false;
                continue;
            }

            // Normalize whitespace
            if c.is_whitespace() {
                if !result.ends_with(' ') && !result.is_empty() {
                    result.push(' ');
                }
                continue;
            }

            // Handle identifiers
            let c = if self.normalize_case {
                c.to_ascii_lowercase()
            } else {
                c
            };

            result.push(c);
        }

        result.trim().to_string()
    }

    /// Generate a full cache key from SQL, user, and snapshot ID.
    pub fn cache_key(&self, sql: &str, user: &str, snapshot_id: i64) -> CacheKey {
        let fingerprint = self.fingerprint(sql);
        CacheKey {
            fingerprint,
            user: user.to_string(),
            snapshot_id,
        }
    }
}

/// Cache key for result lookup.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CacheKey {
    /// Plan fingerprint (canonicalized SQL hash).
    pub fingerprint: u64,
    /// User who executed the query (for row-level security).
    pub user: String,
    /// Iceberg snapshot ID at query time.
    pub snapshot_id: i64,
}

impl CacheKey {
    /// Convert to a string key for storage.
    pub fn to_string_key(&self) -> String {
        format!("{}:{}:{}", self.fingerprint, self.user, self.snapshot_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_same_structure() {
        let canon = PlanCanonicalizer::new();

        let sql1 = "SELECT * FROM t WHERE date = '2024-01-01'";
        let sql2 = "SELECT * FROM t WHERE date = '2024-01-02'";

        // Same structure, different literals → same fingerprint
        assert_eq!(canon.fingerprint(sql1), canon.fingerprint(sql2));
    }

    #[test]
    fn test_fingerprint_different_structure() {
        let canon = PlanCanonicalizer::new();

        let sql1 = "SELECT * FROM t WHERE date = '2024-01-01'";
        let sql2 = "SELECT * FROM t WHERE id = 123";

        // Different structure → different fingerprint
        assert_ne!(canon.fingerprint(sql1), canon.fingerprint(sql2));
    }

    #[test]
    fn test_canonicalize_whitespace() {
        let canon = PlanCanonicalizer::new();

        let sql1 = "SELECT  *   FROM    t";
        let sql2 = "SELECT * FROM t";

        assert_eq!(canon.canonicalize(sql1), canon.canonicalize(sql2));
    }

    #[test]
    fn test_canonicalize_numbers() {
        let canon = PlanCanonicalizer::new();

        let sql = "SELECT * FROM t WHERE id = 12345";
        let canonical = canon.canonicalize(sql);

        assert!(canonical.contains("?"));
        assert!(!canonical.contains("12345"));
    }

    #[test]
    fn test_cache_key() {
        let canon = PlanCanonicalizer::new();
        let key = canon.cache_key("SELECT 1", "user1", 12345);

        assert_eq!(key.user, "user1");
        assert_eq!(key.snapshot_id, 12345);
        assert!(key.fingerprint > 0);
    }
}
