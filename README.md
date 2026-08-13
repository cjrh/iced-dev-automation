# iced-dev-automation

Debug-only local automation for native [Iced](https://iced.rs) applications.

It provides two private Unix sockets:

- **Screenshot**: returns the Iced-rendered viewport as PNG.
- **Automation**: dispatches selected application message variants.

This is for local development and coding-agent workflows. It does not inject OS
mouse or keyboard events. It works independently of X11, Wayland, and the
window manager.

## Security model

- Compile the integration only with `#[cfg(debug_assertions)]`.
- Start sockets only when their CLI paths are supplied.
- Socket files use mode `0600`.
- Use paths under `$XDG_RUNTIME_DIR`, not a shared directory.
- Do not annotate destructive or trusted completion messages with
  `#[automation]`.

Any process running as the same user can connect to a socket. Do not enable this
for production builds or untrusted local users.

## Add the dependency

```toml
[dependencies]
iced-dev-automation = { path = "../iced-dev-automation" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

Use a released crate version instead of `path` when one is available.

## Define automatable messages

Derive `DevAutomation` on the Iced message enum in debug builds. Mark only
allowed variants in debug builds.

```rust
#[cfg(debug_assertions)]
use iced_dev_automation::DevAutomation;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
struct WorktreeKey {
    root_id: String,
    path: PathBuf,
}

#[derive(Debug, Clone)]
#[cfg_attr(debug_assertions, derive(DevAutomation))]
enum Message {
    #[cfg_attr(debug_assertions, automation)]
    Refresh,

    #[cfg_attr(debug_assertions, automation)]
    SelectRepository(String),

    #[cfg_attr(debug_assertions, automation)]
    SelectWorktree(WorktreeKey),

    // Not exposed: task results, dialog callbacks, destructive actions, etc.
    RepositoryRefreshed(Result<(), String>),
}
```

The derive generates a JSON decoder and a live schema from the annotated
variants. There is no separate dispatcher to maintain. Use `cfg_attr` on both
the derive and each `automation` attribute so release builds omit the decoder.

Every field in an annotated variant must implement `serde::Deserialize`.

## Start the sockets

Parse the supplied debug options before starting Iced:

```rust
#[cfg(debug_assertions)]
use iced_dev_automation::{AutomationSocket, DevOptions, ScreenshotSocket};

#[cfg(debug_assertions)]
let options = DevOptions::from_cli().expect("valid debug options");

#[cfg(debug_assertions)]
let screenshot_socket = options
    .screenshot_socket
    .map(ScreenshotSocket::bind)
    .transpose()
    .expect("screenshot socket starts");

#[cfg(debug_assertions)]
let automation_socket = options
    .automation_socket
    .map(AutomationSocket::bind)
    .transpose()
    .expect("automation socket starts");
```

Start an app with private socket paths:

```sh
cargo run -- \
  --screenshot-socket "$XDG_RUNTIME_DIR/my-app-screenshot.sock" \
  --automation-socket "$XDG_RUNTIME_DIR/my-app-automation.sock"
```

`DevOptions` accepts these options:

```text
--screenshot-socket PATH
--automation-socket PATH
```

Normal release builds omit code guarded with `debug_assertions`. A custom
release profile that enables Rust debug assertions also enables the integration.

## Connect the sockets to Iced

Store the optional sockets in application state. Merge their subscriptions with
your normal subscription. Add the socket request messages to the application
message enum, but do **not** annotate these internal request variants.

```rust
#[cfg(debug_assertions)]
use iced_dev_automation::{
    AutomationRequest, AutomationSocket, CapturedScreenshot, ScreenshotRequest, ScreenshotSocket,
};

struct State {
    #[cfg(debug_assertions)]
    screenshot_socket: Option<ScreenshotSocket>,
    #[cfg(debug_assertions)]
    automation_socket: Option<AutomationSocket>,
}

#[derive(Debug, Clone)]
#[cfg_attr(debug_assertions, derive(DevAutomation))]
enum Message {
    #[cfg_attr(debug_assertions, automation)]
    Refresh,

    #[cfg(debug_assertions)]
    AutomationRequested(AutomationRequest),
    #[cfg(debug_assertions)]
    ScreenshotRequested(ScreenshotRequest),
    #[cfg(debug_assertions)]
    ScreenshotCaptured {
        request: ScreenshotRequest,
        screenshot: CapturedScreenshot,
    },
    #[cfg(debug_assertions)]
    ScreenshotFailed {
        request: ScreenshotRequest,
        error: String,
    },
}

fn subscription(state: &State) -> iced::Subscription<Message> {
    let normal = iced::Subscription::none();

    #[cfg(debug_assertions)]
    {
        let screenshots = state.screenshot_socket.as_ref().map_or_else(
            iced::Subscription::none,
            |socket| socket.subscription().map(Message::ScreenshotRequested),
        );
        let automation = state.automation_socket.as_ref().map_or_else(
            iced::Subscription::none,
            |socket| socket.subscription().map(Message::AutomationRequested),
        );
        iced::Subscription::batch([normal, screenshots, automation])
    }

    #[cfg(not(debug_assertions))]
    normal
}
```

In `update`, dispatch the request through the normal application update path:

```rust
#[cfg(debug_assertions)]
Message::AutomationRequested(request) => {
    if !request.begin_dispatch() {
        return iced::Task::none();
    }

    if request.is_describe() {
        request.respond_ok(Message::automation_schema());
        iced::Task::none()
    } else {
        let value = request.dispatch_value().expect("dispatch request").clone();
        match Message::from_automation_value(value) {
            Ok(message) => {
                let task = update(state, message);
                request.respond_ok(serde_json::json!({ "accepted": true }));
                task
            }
            Err(error) => {
                request.respond_error("invalid_message", error.to_string());
                iced::Task::none()
            }
        }
    }
}

#[cfg(debug_assertions)]
Message::ScreenshotRequested(request) => iced::window::oldest().then(move |id| {
    let request = request.clone();
    match id {
        Some(id) => iced::window::screenshot(id).map(move |screenshot| {
            Message::ScreenshotCaptured {
                request: request.clone(),
                screenshot: screenshot.into(),
            }
        }),
        None => iced::Task::done(Message::ScreenshotFailed {
            request,
            error: "no application window is open".to_owned(),
        }),
    }
}),

#[cfg(debug_assertions)]
Message::ScreenshotCaptured { request, screenshot } => {
    request.respond(Ok(screenshot));
    iced::Task::none()
}

#[cfg(debug_assertions)]
Message::ScreenshotFailed { request, error } => {
    request.respond(Err(error));
    iced::Task::none()
}
```

## Automation protocol

The automation socket accepts one JSON object followed by `\n`. It returns one
JSON object followed by `\n`, then closes the connection.

Ask the running app for its exact allowed variants:

```json
{"op":"describe"}
```

Each allowed request mirrors the annotated enum variant:

```json
{"variant":"Refresh"}
{"variant":"SelectRepository","value":"my-repo"}
{"variant":"SelectWorktree","value":{"root_id":"my-repo","path":"/repos/my-repo"}}
```

Rules:

- Unit variants omit `value`.
- A one-field tuple variant uses that field as `value`.
- Multi-field tuple variants use an array as `value`.
- Named-field variants use an object as `value`.

Successful dispatch:

```json
{"status":"ok","result":{"accepted":true}}
```

`accepted` means the normal `update` function processed the message. It does
not mean later Git, network, filesystem, or other `Task` work completed.
Capture a screenshot after the response to inspect the UI.

Errors have this shape:

```json
{"status":"error","code":"invalid_message","message":"..."}
```

The maximum request size is 8192 bytes. The server allows four queued requests
and four active clients. A timeout before dispatch cancels the request. If
dispatch starts at the timeout boundary, the response is successful with
`"in_progress": true`.

### Python helper

```sh
python3 - "$XDG_RUNTIME_DIR/my-app-automation.sock" \
  '{"variant":"Refresh"}' <<'PY'
import socket
import sys

with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
    client.connect(sys.argv[1])
    client.sendall(sys.argv[2].encode() + b"\n")
    print(client.makefile("r", encoding="utf-8").read(), end="")
PY
```

## Screenshot protocol

The screenshot socket accepts this exact line:

```text
screenshot
```

It returns a PNG byte stream and closes the connection. The PNG is the Iced
rendered viewport in physical pixels. It excludes OS decorations, other
windows, and compositor overlays.

```sh
python3 - "$XDG_RUNTIME_DIR/my-app-screenshot.sock" ./app.png <<'PY'
from pathlib import Path
import socket
import sys

with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
    client.connect(sys.argv[1])
    client.sendall(b"screenshot\n")
    response = bytearray()
    while chunk := client.recv(64 * 1024):
        response.extend(chunk)

if not response.startswith(b"\x89PNG\r\n\x1a\n"):
    raise SystemExit(response.decode("utf-8", errors="replace"))

Path(sys.argv[2]).write_bytes(response)
PY
```

On failure, the screenshot socket returns:

```text
ERR reason
```
