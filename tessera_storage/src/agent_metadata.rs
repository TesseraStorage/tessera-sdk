//! Agent metadata stored inside an object's encrypted metadata. See
//! [`AgentMetadata`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thiserror::Error;

use crate::Object;

/// The top-level key in an object's metadata JSON object that holds
/// [`AgentMetadata`].
pub const AGENT_METADATA_KEY: &str = "agent";

/// Records which agent wrote an object and why.
///
/// It is stored under [`AGENT_METADATA_KEY`] in [`Object::metadata`], which
/// is encrypted with the rest of the metadata when the object is sealed. The
/// indexer never sees it in plaintext.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMetadata {
    /// Identifies the agent that wrote the object.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Identifies the agent run or session that wrote the object.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run: String,
    /// Describes what the agent was doing when it wrote the object.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub task: String,
    /// Free-form key/value pairs set by the agent.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

/// Errors from [`Object::agent_metadata`] and [`Object::set_agent_metadata`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AgentMetadataError {
    /// The object's existing metadata is not a JSON object, so agent
    /// metadata cannot be added without replacing it.
    #[error("object metadata is not a JSON object")]
    NotObject,
    /// The metadata could not be encoded or decoded as JSON.
    #[error("invalid metadata JSON: {0}")]
    Json(#[from] serde_json::Error),
}

type Fields = BTreeMap<String, Box<RawValue>>;

impl Object {
    /// Decodes the metadata as a JSON object. Empty or `null` metadata
    /// decodes to an empty map.
    fn metadata_fields(&self) -> Result<Fields, AgentMetadataError> {
        let trimmed = self.metadata.trim_ascii();
        if trimmed.is_empty() || trimmed == b"null" {
            return Ok(Fields::new());
        }
        if trimmed[0] != b'{' {
            return Err(AgentMetadataError::NotObject);
        }
        Ok(serde_json::from_slice(trimmed)?)
    }

    /// Returns the [`AgentMetadata`] stored in the object's metadata, or
    /// `None` when none is stored.
    pub fn agent_metadata(&self) -> Result<Option<AgentMetadata>, AgentMetadataError> {
        let fields = self.metadata_fields()?;
        match fields.get(AGENT_METADATA_KEY) {
            Some(raw) => Ok(Some(serde_json::from_str(raw.get())?)),
            None => Ok(None),
        }
    }

    /// Stores `meta` under [`AGENT_METADATA_KEY`] in the object's metadata.
    /// Every other top-level key is kept with its value unchanged. Empty
    /// metadata becomes a JSON object holding only the agent metadata.
    /// Metadata that is not a JSON object is left unchanged and
    /// [`AgentMetadataError::NotObject`] is returned.
    ///
    /// The change is local. Pin the object with
    /// [`Sdk::pin_object`](crate::Sdk::pin_object) or
    /// [`Sdk::update_object_metadata`](crate::Sdk::update_object_metadata)
    /// to save it on the indexer.
    pub fn set_agent_metadata(&mut self, meta: &AgentMetadata) -> Result<(), AgentMetadataError> {
        let mut fields = self.metadata_fields()?;
        fields.insert(
            AGENT_METADATA_KEY.to_string(),
            serde_json::value::to_raw_value(meta)?,
        );
        self.metadata = serde_json::to_vec(&fields)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agent_metadata_empty() {
        let mut obj = Object::default();
        assert!(obj.agent_metadata().unwrap().is_none());

        let meta = AgentMetadata {
            name: "indexer-bot".into(),
            run: "run-42".into(),
            task: "nightly export".into(),
            labels: [("env".to_string(), "test".to_string())].into(),
        };
        obj.set_agent_metadata(&meta).unwrap();
        assert_eq!(obj.agent_metadata().unwrap(), Some(meta));
    }

    #[test]
    fn test_agent_metadata_keeps_other_keys() {
        let mut obj = Object::new(Some(
            br#"{"name":"report.pdf","v":1,"tessera":{"sha256":"ab","size":3}}"#.to_vec(),
        ));
        obj.set_agent_metadata(&AgentMetadata {
            name: "a".into(),
            ..Default::default()
        })
        .unwrap();
        let fields: Fields = serde_json::from_slice(&obj.metadata).unwrap();
        assert_eq!(fields["name"].get(), r#""report.pdf""#);
        assert_eq!(fields["v"].get(), "1");
        assert_eq!(fields["tessera"].get(), r#"{"sha256":"ab","size":3}"#);
        assert_eq!(fields["agent"].get(), r#"{"name":"a"}"#);
    }

    #[test]
    fn test_agent_metadata_not_object() {
        let mut obj = Object::new(Some(b"metadata".to_vec()));
        assert!(matches!(
            obj.set_agent_metadata(&AgentMetadata::default()),
            Err(AgentMetadataError::NotObject)
        ));
        assert_eq!(obj.metadata, b"metadata".to_vec());
        assert!(matches!(
            obj.agent_metadata(),
            Err(AgentMetadataError::NotObject)
        ));
    }
}
