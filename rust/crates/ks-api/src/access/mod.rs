//! Who may ask: the dashboard's access layer, answer for answer with
//! `web/routes/auth.py` and `core/permissions.py`.
//!
//! A request is routed the way Starlette routes it (`routing`), against the
//! routes `rust/contract/access.json` lists in the app's own order. The route
//! names its dependencies in the order FastAPI runs them; the gates among
//! them run here (`gate`), each refusing in the words the Python one uses,
//! and the first dependency that is not a gate is the endpoint's business —
//! the pilot stops comparing there. A gate that needs the caller resolves the
//! session once per request (`session`): the cookie checked as itsdangerous
//! checks it (`signer`), then the user list, the bot's list and the role
//! matrix in Postgres (`store`), narrowed by the person's tabs
//! (`permissions`). Limits are slowapi's (`ratelimit`).
//!
//! Nothing here issues a session — the Telegram sign-in stays in Python —
//! and nothing here writes: where Python seeds the role matrix on its first
//! read, this computes what the seed would have stored.

pub mod contract;
pub mod cookie;
pub mod gate;
pub mod paths;
pub mod permissions;
pub mod pybase64;
pub mod pyjson;
pub mod ratelimit;
pub mod routing;
pub mod session;
pub mod signer;
pub mod store;
