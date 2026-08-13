//! Debug-only in-app automation over a private Unix socket.
//!
//! Each client sends one JSON value followed by a newline. The app accepts only
//! `Msg` variants annotated with `#[automation]`. It returns one JSON response
//! and closes the connection.

use std::{
    env, fs,
    hash::{Hash, Hasher},
    io::{self, BufRead, BufReader, Read, Write},
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, SyncSender},
    },
    thread,
    time::Duration,
};

use iced::{
    Subscription,
    futures::{SinkExt, StreamExt, channel::mpsc as futures_mpsc, lock::Mutex},
};
use serde_json::{Value, json};

const MAX_REQUEST_BYTES: u64 = 8 * 1024;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(20);
const MAX_CLIENTS: usize = 4;
const MAX_PENDING_REQUESTS: usize = 4;
const REQUEST_QUEUED: u8 = 0;
const REQUEST_DISPATCHING: u8 = 1;
const REQUEST_CANCELLED: u8 = 2;

static NEXT_SOCKET_ID: AtomicU64 = AtomicU64::new(1);

/// CLI paths for the optional debug sockets.
#[derive(Debug, Default)]
pub struct DevOptions {
    /// Path for the rendered-viewport screenshot socket.
    pub screenshot_socket: Option<PathBuf>,
    /// Path for the tagged-message automation socket.
    pub automation_socket: Option<PathBuf>,
}

impl DevOptions {
    /// Parses `--screenshot-socket PATH` and `--automation-socket PATH`.
    pub fn from_cli() -> Result<Self, String> {
        let mut options = Self::default();
        let mut args = env::args_os().skip(1);

        while let Some(argument) = args.next() {
            let target = match argument.to_str() {
                Some("--screenshot-socket") => &mut options.screenshot_socket,
                Some("--automation-socket") => &mut options.automation_socket,
                Some("--help" | "-h") => {
                    println!(
                        "Usage: application [--screenshot-socket PATH] [--automation-socket PATH]\n\n\
                         --screenshot-socket PATH  Start the debug screenshot socket.\n\
                         --automation-socket PATH  Start the debug automation socket."
                    );
                    std::process::exit(0);
                }
                _ => return Err(format!("unknown argument: {}", argument.to_string_lossy())),
            };

            let value = args.next().ok_or_else(|| {
                format!("{} requires a Unix-socket path", argument.to_string_lossy())
            })?;
            if target.replace(PathBuf::from(value)).is_some() {
                return Err(format!(
                    "{} can be specified only once",
                    argument.to_string_lossy()
                ));
            }
        }

        if options.screenshot_socket.is_some()
            && options.screenshot_socket.as_ref() == options.automation_socket.as_ref()
        {
            return Err(
                "--screenshot-socket and --automation-socket must use different paths".to_owned(),
            );
        }

        Ok(options)
    }
}

/// A parsed automation request received from the Unix socket.
#[derive(Debug, Clone)]
pub struct AutomationRequest {
    kind: AutomationRequestKind,
    lifecycle: Arc<AtomicU8>,
    response: SyncSender<Value>,
}

#[derive(Debug, Clone)]
enum AutomationRequestKind {
    Describe,
    Dispatch(Value),
}

impl AutomationRequest {
    /// Claims this request for dispatch. Returns false if its client timed out.
    pub fn begin_dispatch(&self) -> bool {
        self.lifecycle
            .compare_exchange(
                REQUEST_QUEUED,
                REQUEST_DISPATCHING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Returns true for the reserved schema-discovery request.
    pub fn is_describe(&self) -> bool {
        matches!(self.kind, AutomationRequestKind::Describe)
    }

    /// Returns the raw tagged-enum value for a dispatch request.
    pub fn dispatch_value(&self) -> Option<&Value> {
        match &self.kind {
            AutomationRequestKind::Dispatch(value) => Some(value),
            AutomationRequestKind::Describe => None,
        }
    }

    /// Replies with a successful JSON value.
    pub fn respond_ok(self, result: Value) {
        let _ = self
            .response
            .send(json!({ "status": "ok", "result": result }));
    }

    /// Replies with a structured error.
    pub fn respond_error(self, code: &str, message: impl Into<String>) {
        let _ = self.response.send(json!({
            "status": "error",
            "code": code,
            "message": message.into(),
        }));
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct SocketIdentity {
    device: u64,
    inode: u64,
}

struct SocketState {
    id: u64,
    path: PathBuf,
    identity: SocketIdentity,
    shutdown: AtomicBool,
}

/// Owns a private automation socket and supplies its requests as an Iced subscription.
#[derive(Clone)]
pub struct AutomationSocket {
    state: Arc<SocketState>,
    receiver: Arc<Mutex<futures_mpsc::Receiver<AutomationRequest>>>,
}

impl AutomationSocket {
    /// Starts the server at `path`.
    pub fn bind(path: PathBuf) -> Result<Self, String> {
        prepare_socket_path(&path)?;

        let listener = UnixListener::bind(&path)
            .map_err(|error| format!("could not bind {}: {error}", path.display()))?;
        let identity = socket_identity(&path)?;

        if let Err(error) = fs::set_permissions(&path, fs::Permissions::from_mode(0o600)) {
            remove_owned_socket(&path, identity);
            return Err(format!(
                "could not set permissions on {}: {error}",
                path.display()
            ));
        }

        if let Err(error) = listener.set_nonblocking(true) {
            drop(listener);
            remove_owned_socket(&path, identity);
            return Err(format!("could not configure {}: {error}", path.display()));
        }

        let state = Arc::new(SocketState {
            id: NEXT_SOCKET_ID.fetch_add(1, Ordering::Relaxed),
            path: path.clone(),
            identity,
            shutdown: AtomicBool::new(false),
        });
        let (sender, receiver) = futures_mpsc::channel(MAX_PENDING_REQUESTS);
        let server_state = Arc::downgrade(&state);
        let identity = state.identity;

        if let Err(error) = thread::Builder::new()
            .name("wtui-automation-socket".to_owned())
            .spawn(move || serve(listener, path, identity, server_state, sender))
        {
            remove_owned_socket(&state.path, state.identity);
            return Err(format!("could not start automation socket server: {error}"));
        }

        Ok(Self {
            state,
            receiver: Arc::new(Mutex::new(receiver)),
        })
    }

    /// Converts socket requests into Iced messages.
    pub fn subscription(&self) -> Subscription<AutomationRequest> {
        Subscription::run_with(self.clone(), stream_requests)
    }
}

impl Hash for AutomationSocket {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.state.id.hash(hasher);
    }
}

impl Drop for AutomationSocket {
    fn drop(&mut self) {
        if Arc::strong_count(&self.state) == 1 {
            self.state.shutdown.store(true, Ordering::Relaxed);
        }
    }
}

fn stream_requests(
    socket: &AutomationSocket,
) -> Pin<Box<dyn iced::futures::Stream<Item = AutomationRequest> + Send + 'static>> {
    let receiver = Arc::clone(&socket.receiver);

    iced::stream::channel(
        1,
        move |mut output: futures_mpsc::Sender<AutomationRequest>| async move {
            loop {
                let request = {
                    let mut receiver = receiver.lock().await;
                    receiver.next().await
                };

                let Some(request) = request else {
                    break;
                };

                if output.send(request).await.is_err() {
                    break;
                }
            }
        },
    )
    .boxed()
}

fn prepare_socket_path(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            let identity = socket_identity(path)?;

            match UnixStream::connect(path) {
                Ok(_) => Err(format!("socket {} is already in use", path.display())),
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                    if remove_owned_socket_checked(path, identity)? {
                        Ok(())
                    } else {
                        Err(format!(
                            "socket {} changed while checking for staleness",
                            path.display()
                        ))
                    }
                }
                Err(error) => Err(format!(
                    "could not verify existing socket {}: {error}",
                    path.display()
                )),
            }
        }
        Ok(_) => Err(format!(
            "refusing to replace non-socket path {}",
            path.display()
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("could not inspect {}: {error}", path.display())),
    }
}

fn socket_identity(path: &Path) -> Result<SocketIdentity, String> {
    use std::os::unix::fs::MetadataExt;

    let metadata = fs::metadata(path)
        .map_err(|error| format!("could not inspect {}: {error}", path.display()))?;
    Ok(SocketIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn remove_owned_socket(path: &Path, identity: SocketIdentity) {
    let _ = remove_owned_socket_checked(path, identity);
}

fn remove_owned_socket_checked(path: &Path, identity: SocketIdentity) -> Result<bool, String> {
    use std::os::unix::fs::MetadataExt;

    let current = match fs::metadata(path) {
        Ok(metadata) => SocketIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("could not inspect {}: {error}", path.display())),
    };

    if current != identity {
        return Ok(false);
    }

    fs::remove_file(path)
        .map(|_| true)
        .map_err(|error| format!("could not remove {}: {error}", path.display()))
}

fn serve(
    listener: UnixListener,
    path: PathBuf,
    identity: SocketIdentity,
    state: Weak<SocketState>,
    sender: futures_mpsc::Sender<AutomationRequest>,
) {
    let clients = Arc::new(AtomicUsize::new(0));

    while is_running(&state) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                if clients.fetch_add(1, Ordering::Relaxed) >= MAX_CLIENTS {
                    clients.fetch_sub(1, Ordering::Relaxed);
                    write_response(
                        &mut stream,
                        &json!({
                            "status": "error",
                            "code": "busy",
                            "message": "too many concurrent automation requests",
                        }),
                    );
                    continue;
                }

                let sender = sender.clone();
                let worker_clients = Arc::clone(&clients);
                if let Err(error) = thread::Builder::new()
                    .name("wtui-automation-client".to_owned())
                    .spawn(move || {
                        handle_client(stream, sender);
                        worker_clients.fetch_sub(1, Ordering::Relaxed);
                    })
                {
                    clients.fetch_sub(1, Ordering::Relaxed);
                    eprintln!("automation socket could not handle client: {error}");
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_INTERVAL);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                eprintln!("automation socket accept error: {error}");
                break;
            }
        }
    }

    remove_owned_socket(&path, identity);
}

fn is_running(state: &Weak<SocketState>) -> bool {
    state
        .upgrade()
        .is_some_and(|state| !state.shutdown.load(Ordering::Relaxed))
}

fn cancel_request(lifecycle: &AtomicU8) -> bool {
    lifecycle
        .compare_exchange(
            REQUEST_QUEUED,
            REQUEST_CANCELLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_ok()
}

fn handle_client(mut stream: UnixStream, mut sender: futures_mpsc::Sender<AutomationRequest>) {
    if let Err(error) = stream.set_read_timeout(Some(CLIENT_TIMEOUT)) {
        eprintln!("automation socket could not set read timeout: {error}");
        return;
    }
    if let Err(error) = stream.set_write_timeout(Some(CLIENT_TIMEOUT)) {
        eprintln!("automation socket could not set write timeout: {error}");
        return;
    }

    let value = match read_json(&stream) {
        Ok(value) => value,
        Err(error) => {
            write_response(
                &mut stream,
                &json!({ "status": "error", "code": "invalid_request", "message": error }),
            );
            return;
        }
    };

    let kind = if value
        .as_object()
        .is_some_and(|object| object.len() == 1 && object.get("op") == Some(&json!("describe")))
    {
        AutomationRequestKind::Describe
    } else {
        AutomationRequestKind::Dispatch(value)
    };

    let lifecycle = Arc::new(AtomicU8::new(REQUEST_QUEUED));
    let (response, receiver) = mpsc::sync_channel(1);
    let request = AutomationRequest {
        kind,
        lifecycle: Arc::clone(&lifecycle),
        response,
    };
    if let Err(error) = sender.try_send(request) {
        let (code, message) = if error.is_full() {
            ("busy", "automation request queue is full")
        } else {
            (
                "unavailable",
                "application is not accepting automation requests",
            )
        };
        write_response(
            &mut stream,
            &json!({ "status": "error", "code": code, "message": message }),
        );
        return;
    }

    match receiver.recv_timeout(CLIENT_TIMEOUT) {
        Ok(response) => write_response(&mut stream, &response),
        Err(mpsc::RecvTimeoutError::Timeout) if cancel_request(&lifecycle) => write_response(
            &mut stream,
            &json!({
                "status": "error",
                "code": "timeout",
                "message": "automation request timed out before dispatch",
            }),
        ),
        Err(mpsc::RecvTimeoutError::Timeout) => write_response(
            &mut stream,
            &json!({
                "status": "ok",
                "result": { "accepted": true, "in_progress": true },
            }),
        ),
        Err(mpsc::RecvTimeoutError::Disconnected) => write_response(
            &mut stream,
            &json!({
                "status": "error",
                "code": "unavailable",
                "message": "application stopped before handling the request",
            }),
        ),
    }
}

fn read_json(stream: &UnixStream) -> Result<Value, String> {
    let reader = stream
        .try_clone()
        .map_err(|error| format!("could not read request: {error}"))?;
    let mut reader = BufReader::new(reader);
    let mut request = Vec::new();
    let bytes_read = reader
        .by_ref()
        .take(MAX_REQUEST_BYTES + 1)
        .read_until(b'\n', &mut request)
        .map_err(|error| format!("could not read request: {error}"))?;

    if bytes_read == 0 {
        return Err("empty request".to_owned());
    }
    if request.len() > MAX_REQUEST_BYTES as usize || !request.ends_with(b"\n") {
        return Err("request must be one JSON line no longer than 8192 bytes".to_owned());
    }

    serde_json::from_slice(&request[..request.len() - 1])
        .map_err(|error| format!("invalid JSON: {error}"))
}

fn write_response(stream: &mut UnixStream, response: &Value) {
    if serde_json::to_writer(&mut *stream, response).is_ok() {
        let _ = stream.write_all(b"\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static NEXT_TEST_PATH_ID: AtomicU64 = AtomicU64::new(1);

    fn test_socket_path() -> PathBuf {
        let id = NEXT_TEST_PATH_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "wtui-automation-test-{}-{id}.sock",
            std::process::id()
        ))
    }

    fn read_json_from_client(request: &[u8]) -> Result<Value, String> {
        let path = test_socket_path();
        let listener = UnixListener::bind(&path).expect("test listener must bind");
        let request = request.to_vec();
        let client = thread::spawn({
            let path = path.clone();
            move || {
                let mut stream = UnixStream::connect(path).expect("client must connect");
                stream.write_all(&request).expect("client must write");
            }
        });

        let (stream, _) = listener.accept().expect("listener must accept");
        let result = read_json(&stream);
        client.join().expect("client must not panic");
        fs::remove_file(path).expect("test socket must be removed");
        result
    }

    #[test]
    fn parses_a_json_line() {
        assert_eq!(
            read_json_from_client(
                br#"{"variant":"RefreshAll"}
"#
            ),
            Ok(json!({ "variant": "RefreshAll" }))
        );
    }

    #[test]
    fn rejects_an_unterminated_request() {
        assert!(read_json_from_client(br#"{"variant":"RefreshAll"}"#).is_err());
    }

    #[test]
    fn recognizes_a_schema_request_only_when_it_has_no_extra_fields() {
        let describe = json!({ "op": "describe" });
        let extra = json!({ "op": "describe", "other": true });

        assert!(describe.as_object().is_some_and(
            |object| object.len() == 1 && object.get("op") == Some(&json!("describe"))
        ));
        assert!(!extra.as_object().is_some_and(
            |object| object.len() == 1 && object.get("op") == Some(&json!("describe"))
        ));
    }

    #[test]
    fn uses_private_permissions_and_removes_the_socket_on_drop() {
        let path = test_socket_path();
        let socket = AutomationSocket::bind(path.clone()).expect("test socket must start");
        let mode = fs::metadata(&path)
            .expect("socket metadata must be available")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);

        drop(socket);
        for _ in 0..50 {
            if !path.exists() {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }

        let _ = fs::remove_file(&path);
        panic!("socket was not removed after shutdown");
    }

    #[test]
    fn does_not_replace_a_live_socket() {
        let path = test_socket_path();
        let socket = AutomationSocket::bind(path.clone()).expect("first socket must start");
        assert!(AutomationSocket::bind(path.clone()).is_err());
        drop(socket);
        for _ in 0..50 {
            if !path.exists() {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        let _ = fs::remove_file(&path);
        panic!("socket was not removed after shutdown");
    }

    #[test]
    fn a_cancelled_request_cannot_begin_dispatch() {
        let lifecycle = AtomicU8::new(REQUEST_QUEUED);
        assert!(cancel_request(&lifecycle));
        assert!(
            lifecycle
                .compare_exchange(
                    REQUEST_QUEUED,
                    REQUEST_DISPATCHING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        );
    }

    #[test]
    fn refuses_to_replace_a_regular_file() {
        let path = test_socket_path();
        fs::write(&path, "do not replace").expect("test file must be created");
        assert!(prepare_socket_path(&path).is_err());
        fs::remove_file(path).expect("test file must be removed");
    }
}
