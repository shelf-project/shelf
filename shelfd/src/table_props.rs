//! SHELF G3 — Iceberg sort/Z-order awareness.
//!
//! Reads Iceberg table properties from a metadata JSON file and
//! distils them into a small control-plane tag used by the
//! prefetch listener. This module is deliberately minimal: the
//! expensive part (walking manifests to pick exact files) happens
//! downstream; G3 only answers the question *"is column C
//! selectivity-friendly for this table?"*.
//!
//! # Signals we care about
//!
//! - `write.distribution-mode = hash`  — cluster by hash of keys.
//! - `sort-order` spec (from `default-sort-order-id` + the
//!   `sort-orders` array in the metadata JSON). Any ASC/DESC on a
//!   column means min/max pruning is effective on that column.
//! - `write.metadata.z-order.columns` — explicit Z-order directive.
//!
//! A column that appears in any of the above is `Clustered`; a
//! table with no such columns is `Unclustered` and the prefetch
//! listener falls back to the G2 side-bloom path.
//!
//! # What this module does *not* do
//!
//! - It does not schedule prefetches. That's the listener's job.
//! - It does not read manifests. The cheap win in G3 is "use the
//!   manifest min/max the engine already sees"; we just tell the
//!   listener *which columns* are worth asking min/max about.
//! - It does not network. Callers pass in the metadata-JSON bytes
//!   (already in the metadata pool after D1), keeping this unit
//!   trivially testable.

use std::collections::BTreeSet;

use serde::Deserialize;

/// Outcome of distilling an Iceberg metadata JSON. `Unclustered`
/// is represented by an empty `clustered_columns` set; callers
/// should treat it as "no G3 hint, fall back to G2".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableTag {
    /// Columns where a point/range predicate is likely to prune
    /// files via manifest min/max.
    pub clustered_columns: BTreeSet<String>,
    /// `true` iff `write.metadata.z-order.columns` was set; the
    /// listener prefers Z-order tables when choosing between
    /// competing prefetch strategies.
    pub has_z_order: bool,
    /// `true` iff `write.distribution-mode = hash`.
    pub hash_distributed: bool,
}

impl TableTag {
    /// Parse an Iceberg metadata JSON (the small top-level file
    /// ending in `-<uuid>.metadata.json`) and extract the G3 hint.
    ///
    /// Unknown / missing fields are silently treated as absent.
    /// The Iceberg spec is append-only at the field level so a
    /// metadata file written by a newer writer will still parse
    /// the fields we need.
    pub fn from_metadata_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let raw: RawMetadata = serde_json::from_slice(bytes)?;
        Ok(raw.distil())
    }
}

/// Shape we pick out of the Iceberg metadata JSON. Everything is
/// optional because older metadata files don't carry every field.
#[derive(Debug, Deserialize, Default)]
struct RawMetadata {
    #[serde(default)]
    properties: std::collections::HashMap<String, String>,
    #[serde(default, rename = "default-sort-order-id")]
    default_sort_order_id: Option<i32>,
    #[serde(default, rename = "sort-orders")]
    sort_orders: Vec<RawSortOrder>,
}

#[derive(Debug, Deserialize, Default)]
struct RawSortOrder {
    #[serde(rename = "order-id")]
    order_id: i32,
    #[serde(default)]
    fields: Vec<RawSortField>,
}

#[derive(Debug, Deserialize, Default)]
struct RawSortField {
    /// Iceberg encodes the target by `source-id` (an integer
    /// pointing at the schema), not by name. For G3 we only need
    /// to know *that a field is sorted*; the caller knows the
    /// column name separately via its own schema probe. To keep
    /// this module self-contained we also accept a `name` hint if
    /// the writer included it in `transform`-synthesised fields
    /// (rare but not forbidden by the spec).
    #[serde(default, rename = "source-id")]
    source_id: Option<i32>,
    #[serde(default)]
    name: Option<String>,
}

impl RawMetadata {
    fn distil(self) -> TableTag {
        let mut tag = TableTag::default();

        if matches!(
            self.properties
                .get("write.distribution-mode")
                .map(String::as_str),
            Some("hash")
        ) {
            tag.hash_distributed = true;
        }

        if let Some(cols) = self.properties.get("write.metadata.z-order.columns") {
            tag.has_z_order = true;
            for col in cols.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                tag.clustered_columns.insert(col.to_string());
            }
        }

        if let Some(target_id) = self.default_sort_order_id {
            if let Some(order) = self.sort_orders.iter().find(|o| o.order_id == target_id) {
                for field in &order.fields {
                    if let Some(name) = field.name.as_ref() {
                        tag.clustered_columns.insert(name.clone());
                    } else if let Some(id) = field.source_id {
                        // No name available — emit the schema id
                        // so the caller can resolve it later when
                        // it walks the schema. Prefixed with `#`
                        // to make the ambiguity explicit.
                        tag.clustered_columns.insert(format!("#{id}"));
                    }
                }
            }
        }

        tag
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_metadata_is_unclustered() {
        let tag = TableTag::from_metadata_json(b"{}").unwrap();
        assert!(tag.clustered_columns.is_empty());
        assert!(!tag.has_z_order);
        assert!(!tag.hash_distributed);
    }

    #[test]
    fn z_order_columns_are_parsed() {
        let json = br#"{
            "properties": {
                "write.metadata.z-order.columns": "user_id, event_ts"
            }
        }"#;
        let tag = TableTag::from_metadata_json(json).unwrap();
        assert!(tag.has_z_order);
        assert!(tag.clustered_columns.contains("user_id"));
        assert!(tag.clustered_columns.contains("event_ts"));
    }

    #[test]
    fn sort_order_names_come_through() {
        let json = br#"{
            "default-sort-order-id": 2,
            "sort-orders": [
                {"order-id": 1, "fields": []},
                {"order-id": 2, "fields": [
                    {"source-id": 3, "name": "region"},
                    {"source-id": 4}
                ]}
            ]
        }"#;
        let tag = TableTag::from_metadata_json(json).unwrap();
        assert!(tag.clustered_columns.contains("region"));
        assert!(tag.clustered_columns.contains("#4"));
    }

    #[test]
    fn hash_distribution_sets_flag() {
        let json = br#"{"properties": {"write.distribution-mode": "hash"}}"#;
        let tag = TableTag::from_metadata_json(json).unwrap();
        assert!(tag.hash_distributed);
    }
}
