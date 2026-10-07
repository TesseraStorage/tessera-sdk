use std::cell::RefCell;
use std::time::Duration;

use sia_storage::{
    ApprovedState, Builder as StorageBuilder, DisconnectedState, RequestingApprovalState, Url,
};
use wasm_bindgen::prelude::*;

use crate::app_key::AppKey;
use crate::helpers::{make_app_metadata, to_js_err};
use crate::run_local;
use crate::sdk::Sdk;

enum BuilderState {
    Disconnected(StorageBuilder<DisconnectedState>),
    RequestingApproval(StorageBuilder<RequestingApprovalState>),
    Approved(StorageBuilder<ApprovedState>),
    Finalized,
}

/// Applies a consuming egress-config setter to the wrapped storage builder in
/// whichever connection state it currently holds, keeping the wasm `Builder`'s
/// state slot populated.
///
/// Configuration is only meaningful before `connected()`/`requestConnection()`
/// /`register()`; a finalized builder returns a JS error.
macro_rules! configure {
    ($self:expr, $builder:ident => $apply:expr) => {{
        let state = $self.state.borrow_mut().take();
        let next = match state {
            Some(BuilderState::Disconnected($builder)) => Some(BuilderState::Disconnected($apply)),
            Some(BuilderState::RequestingApproval($builder)) => {
                Some(BuilderState::RequestingApproval($apply))
            }
            Some(BuilderState::Approved($builder)) => Some(BuilderState::Approved($apply)),
            Some(BuilderState::Finalized) => {
                *$self.state.borrow_mut() = Some(BuilderState::Finalized);
                return Err(JsError::new("builder has already been finalized"));
            }
            None => return Err(JsError::new("builder has already been finalized")),
        };
        *$self.state.borrow_mut() = next;
        Ok(())
    }};
}

/// SDK Builder — handles the connection and registration flow with an indexer.
#[wasm_bindgen]
pub struct Builder {
    state: RefCell<Option<BuilderState>>,
}

#[wasm_bindgen]
impl Builder {
    #[wasm_bindgen(constructor)]
    pub fn new(
        indexer_url: &str,
        #[wasm_bindgen(unchecked_param_type = "AppMetadata")] app: JsValue,
    ) -> Result<Builder, JsError> {
        let meta = make_app_metadata(&app)?;
        let builder = StorageBuilder::new(indexer_url, meta).map_err(to_js_err)?;
        Ok(Builder {
            state: RefCell::new(Some(BuilderState::Disconnected(builder))),
        })
    }

    /// Attempts to connect using an existing AppKey.
    /// Returns a Sdk if the key is valid, or undefined if not registered.
    pub async fn connected(&self, app_key: &AppKey) -> Result<Option<Sdk>, JsError> {
        let state = self.state.borrow_mut().take();
        let ak = app_key.0.clone();

        let (next_state, result) = run_local(async move {
            match state {
                Some(BuilderState::Disconnected(builder)) => match builder.connected(&ak).await {
                    Ok(Some(sdk)) => (Some(BuilderState::Finalized), Ok(Some(Sdk::new(sdk)))),
                    Ok(None) => (Some(BuilderState::Disconnected(builder)), Ok(None)),
                    Err(e) => (Some(BuilderState::Disconnected(builder)), Err(to_js_err(e))),
                },
                other => (other, Err(JsError::new("must be in disconnected state"))),
            }
        })
        .await;

        *self.state.borrow_mut() = next_state;
        result
    }

    /// Requests connection approval from the indexer.
    #[wasm_bindgen(js_name = "requestConnection")]
    pub async fn request_connection(&self) -> Result<(), JsError> {
        let state = self.state.borrow_mut().take();

        let (next_state, result) = run_local(async move {
            match state {
                Some(BuilderState::Disconnected(builder)) => {
                    match builder.request_connection().await {
                        Ok(pending) => (Some(BuilderState::RequestingApproval(pending)), Ok(())),
                        Err(e) => (None, Err(to_js_err(e))),
                    }
                }
                other => (other, Err(JsError::new("must be in disconnected state"))),
            }
        })
        .await;

        *self.state.borrow_mut() = next_state;
        result
    }

    /// Returns the approval URL the user must visit.
    #[wasm_bindgen(js_name = "responseUrl")]
    pub fn response_url(&self) -> Result<String, JsError> {
        let state = self.state.borrow();
        match state.as_ref() {
            Some(BuilderState::RequestingApproval(builder)) => {
                Ok(builder.response_url().to_owned())
            }
            _ => Err(JsError::new("must be in requesting_approval state")),
        }
    }

    /// Waits for the user to approve the connection request.
    #[wasm_bindgen(js_name = "waitForApproval")]
    pub async fn wait_for_approval(&self) -> Result<(), JsError> {
        let state = self.state.borrow_mut().take();

        let (next_state, result) = run_local(async move {
            match state {
                Some(BuilderState::RequestingApproval(builder)) => {
                    match builder.wait_for_approval().await {
                        Ok(approved) => (Some(BuilderState::Approved(approved)), Ok(())),
                        Err(e) => (None, Err(to_js_err(e))),
                    }
                }
                other => (
                    other,
                    Err(JsError::new("must be in requesting_approval state")),
                ),
            }
        })
        .await;

        *self.state.borrow_mut() = next_state;
        result
    }

    /// Completes registration and returns a Sdk instance.
    pub async fn register(&self, mnemonic: &str) -> Result<Sdk, JsError> {
        let state = self.state.borrow_mut().take();
        let mnemonic = mnemonic.to_string();

        let (next_state, result) = run_local(async move {
            match state {
                Some(BuilderState::Approved(builder)) => match builder.register(&mnemonic).await {
                    Ok(sdk) => (Some(BuilderState::Finalized), Ok(Sdk::new(sdk))),
                    Err(e) => (None, Err(to_js_err(e))),
                },
                other => (other, Err(JsError::new("must be in approved state"))),
            }
        })
        .await;

        *self.state.borrow_mut() = next_state;
        result
    }

    /// Enables signed egress reporting (P3b) and sets the URL served-shard
    /// batches are POSTed to. Reporting stays disabled until this is set.
    ///
    /// The wasm reporter is memory-only (no filesystem spool); see
    /// `egressIntervalMs` and `egressBatchMax` for batching.
    #[wasm_bindgen(js_name = "egressUrl")]
    pub fn egress_url(&self, url: &str) -> Result<(), JsError> {
        let url = Url::parse(url).map_err(to_js_err)?;
        configure!(self, builder => builder.egress_url(url))
    }

    /// Sets how often queued egress events are flushed, in milliseconds.
    /// Defaults to 30000 (30 seconds); the batch threshold flushes sooner.
    #[wasm_bindgen(js_name = "egressIntervalMs")]
    pub fn egress_interval_ms(&self, ms: u32) -> Result<(), JsError> {
        configure!(self, builder => builder.egress_interval(Duration::from_millis(ms as u64)))
    }

    /// Sets the maximum number of events per reported egress batch.
    /// Defaults to 500; values below 1 are treated as 1.
    #[wasm_bindgen(js_name = "egressBatchMax")]
    pub fn egress_batch_max(&self, batch_max: u32) -> Result<(), JsError> {
        configure!(self, builder => builder.egress_batch_max(batch_max as usize))
    }
}
