//! The wire shape of the control API: newline-delimited JSON, five frames.
//!
//! Payloads are `serde_json::Value` here on purpose. The transport should not
//! know what a method means — that keeps this module stable while methods come
//! and go, and it means a UDS transport and the in-memory one share every line
//! of framing code.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Control API version, independent of the protocol's `WIRE_VERSION`.
pub const API_VERSION: u32 = 1;

/// Largest single frame we will read, in bytes.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// One line on the control socket.
///
/// Requests flow in *both* directions: the daemon asks connected UIs for
/// consent (`Ask`), because the protocol's [`OfferDecider`] is synchronous and
/// must never block on a human.
///
/// [`OfferDecider`]: iroh_drop::policy::OfferDecider
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "lowercase")]
pub enum Frame {
    /// Client → daemon: invoke a method.
    Req {
        /// Client-scoped correlation id.
        id: u64,
        /// Method name, `noun.verb`.
        m: String,
        /// Parameters; always a JSON object.
        #[serde(default)]
        p: Value,
    },

    /// A successful reply, in either direction.
    Res {
        /// The id of the `Req` or `Ask` being answered.
        id: u64,
        /// Result; always a JSON object.
        #[serde(default)]
        p: Value,
    },

    /// A failed reply, in either direction.
    Err {
        /// The id being answered.
        id: u64,
        /// Stable machine-readable code.
        code: String,
        /// Human-readable detail. Never parse this.
        msg: String,
    },

    /// Daemon → client: a question that needs a `Res`.
    Ask {
        /// Daemon-scoped correlation id (a separate space from `Req` ids).
        id: u64,
        /// Question name, e.g. `offer.accept`.
        q: String,
        /// Context for the decision.
        p: Value,
    },

    /// Daemon → every client: something happened.
    Ev {
        /// Monotonic sequence number, for replay after a reconnect.
        seq: u64,
        /// Event name.
        e: String,
        /// Event payload.
        p: Value,
    },
}

impl Frame {
    /// Encode as one JSONL line, without the trailing newline.
    pub fn to_line(&self) -> String {
        // Frames are built from owned data and always serialize.
        serde_json::to_string(self).expect("frame serializes")
    }

    /// Decode one JSONL line.
    pub fn from_line(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line)
    }
}

/// What a client tells the daemon on connect.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hello {
    /// Free-form client identification, for logs only.
    pub client: String,
    /// Control API version the client speaks.
    pub api: u32,
    /// What the client is willing to do.
    #[serde(default)]
    pub roles: Vec<Role>,
}

impl Hello {
    /// A client that only watches.
    pub fn observer(client: impl Into<String>) -> Self {
        Self {
            client: client.into(),
            api: API_VERSION,
            roles: vec![Role::Observer],
        }
    }

    /// A client that drives transfers and answers consent questions.
    pub fn ui(client: impl Into<String>) -> Self {
        Self {
            client: client.into(),
            api: API_VERSION,
            roles: vec![Role::Ui, Role::Control],
        }
    }
}

/// A client's declared capabilities.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Receives events. Implied by every other role.
    Observer,
    /// May be sent `Ask` frames.
    Ui,
    /// May publish, fetch, and leave drops.
    Control,
}

/// An event as recorded in the replay ring.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    /// Monotonic sequence number.
    pub seq: u64,
    /// Event name.
    pub e: String,
    /// Event payload.
    pub p: Value,
}

impl Envelope {
    /// Turn a recorded event back into a frame.
    pub fn to_frame(&self) -> Frame {
        Frame::Ev {
            seq: self.seq,
            e: self.e.clone(),
            p: self.p.clone(),
        }
    }
}
