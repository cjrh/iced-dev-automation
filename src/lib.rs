//! Debug-only Unix-socket automation and rendered screenshots for Iced apps.
//!
//! See the repository `README.md` for the protocol and integration guide.

extern crate self as iced_dev_automation;

pub mod automation;
pub mod screenshot;

pub use automation::{AutomationRequest, AutomationSocket, DevOptions};
pub use dev_automation_derive::DevAutomation;
pub use screenshot::{CapturedScreenshot, ScreenshotRequest, ScreenshotSocket};

#[doc(hidden)]
pub use serde;
#[doc(hidden)]
pub use serde_json;

/// Converts opt-in tagged JSON requests into application messages.
///
/// Implement this trait with `#[derive(DevAutomation)]` on an enum. Annotate
/// each exposed variant with `#[automation]`.
pub trait DevAutomation: Sized {
    /// Decodes one tagged JSON value into an allowed enum variant.
    fn from_automation_value(value: serde_json::Value) -> Result<Self, serde_json::Error>;

    /// Describes the enum variants available in this running binary.
    fn automation_schema() -> serde_json::Value;
}
