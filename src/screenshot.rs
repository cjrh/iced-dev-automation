//! Debug-only Unix-socket screenshots for local UI automation.
//!
//! A client sends `screenshot\n`. The server replies with one PNG image and
//! closes the connection. On failure, it replies with `ERR <message>\n`.

use std::{
    fs,
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
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, SyncSender},
    },
    thread,
    time::Duration,
};

use iced::{
    Subscription,
    futures::{SinkExt, StreamExt, channel::mpsc as futures_mpsc, lock::Mutex},
};

const COMMAND: &[u8] = b"screenshot";
const MAX_COMMAND_BYTES: u64 = 64;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(20);
const MAX_CLIENTS: usize = 4;

static NEXT_SOCKET_ID: AtomicU64 = AtomicU64::new(1);

/// A screenshot request received from the Unix socket.
#[derive(Debug, Clone)]
pub struct ScreenshotRequest {
    response: SyncSender<Result<CapturedScreenshot, String>>,
}

impl ScreenshotRequest {
    /// Sends the screenshot result back to the waiting socket client.
    pub fn respond(self, result: Result<CapturedScreenshot, String>) {
        let _ = self.response.send(result);
    }
}

/// A rendered Iced viewport in physical RGBA8 pixels.
#[derive(Debug, Clone)]
pub struct CapturedScreenshot {
    rgba: Vec<u8>,
    width: u32,
    height: u32,
}

impl From<iced::window::Screenshot> for CapturedScreenshot {
    fn from(screenshot: iced::window::Screenshot) -> Self {
        Self {
            rgba: screenshot.rgba.to_vec(),
            width: screenshot.size.width,
            height: screenshot.size.height,
        }
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

/// Owns a local screenshot server and supplies its requests as an Iced subscription.
#[derive(Clone)]
pub struct ScreenshotSocket {
    state: Arc<SocketState>,
    receiver: Arc<Mutex<futures_mpsc::UnboundedReceiver<ScreenshotRequest>>>,
}

impl ScreenshotSocket {
    /// Starts a screenshot server at `path`.
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
        let (sender, receiver) = futures_mpsc::unbounded();
        let server_state = Arc::downgrade(&state);
        let identity = state.identity;

        if let Err(error) = thread::Builder::new()
            .name("wtui-screenshot-socket".to_owned())
            .spawn(move || serve(listener, path, identity, server_state, sender))
        {
            remove_owned_socket(&state.path, state.identity);
            return Err(format!("could not start screenshot socket server: {error}"));
        }

        Ok(Self {
            state,
            receiver: Arc::new(Mutex::new(receiver)),
        })
    }

    /// Converts requests from the socket server into Iced messages.
    pub fn subscription(&self) -> Subscription<ScreenshotRequest> {
        Subscription::run_with(self.clone(), stream_requests)
    }
}

impl Hash for ScreenshotSocket {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.state.id.hash(hasher);
    }
}

impl Drop for ScreenshotSocket {
    fn drop(&mut self) {
        if Arc::strong_count(&self.state) == 1 {
            self.state.shutdown.store(true, Ordering::Relaxed);
        }
    }
}

fn stream_requests(
    socket: &ScreenshotSocket,
) -> Pin<Box<dyn iced::futures::Stream<Item = ScreenshotRequest> + Send + 'static>> {
    let receiver = Arc::clone(&socket.receiver);

    iced::stream::channel(
        1,
        move |mut output: futures_mpsc::Sender<ScreenshotRequest>| async move {
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
    sender: futures_mpsc::UnboundedSender<ScreenshotRequest>,
) {
    let clients = Arc::new(AtomicUsize::new(0));

    while is_running(&state) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                if clients.fetch_add(1, Ordering::Relaxed) >= MAX_CLIENTS {
                    clients.fetch_sub(1, Ordering::Relaxed);
                    write_error(&mut stream, "too many concurrent screenshot requests");
                    continue;
                }

                let sender = sender.clone();
                let worker_clients = Arc::clone(&clients);
                if let Err(error) = thread::Builder::new()
                    .name("wtui-screenshot-client".to_owned())
                    .spawn(move || {
                        handle_client(stream, &sender);
                        worker_clients.fetch_sub(1, Ordering::Relaxed);
                    })
                {
                    clients.fetch_sub(1, Ordering::Relaxed);
                    eprintln!("screenshot socket could not handle client: {error}");
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_INTERVAL);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                eprintln!("screenshot socket accept error: {error}");
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

fn handle_client(
    mut stream: UnixStream,
    sender: &futures_mpsc::UnboundedSender<ScreenshotRequest>,
) {
    if let Err(error) = stream.set_read_timeout(Some(CLIENT_TIMEOUT)) {
        eprintln!("screenshot socket could not set read timeout: {error}");
        return;
    }
    if let Err(error) = stream.set_write_timeout(Some(CLIENT_TIMEOUT)) {
        eprintln!("screenshot socket could not set write timeout: {error}");
        return;
    }

    let command = read_command(&stream);
    if command.as_deref() != Ok(COMMAND) {
        write_error(
            &mut stream,
            command
                .err()
                .unwrap_or_else(|| "expected `screenshot` command".to_owned())
                .as_str(),
        );
        return;
    }

    let (response, receiver) = mpsc::sync_channel(1);
    if sender
        .unbounded_send(ScreenshotRequest { response })
        .is_err()
    {
        write_error(
            &mut stream,
            "application is not accepting screenshot requests",
        );
        return;
    }

    let result = match receiver.recv_timeout(CLIENT_TIMEOUT) {
        Ok(Ok(screenshot)) => encode_png(&screenshot),
        Ok(Err(error)) => Err(error),
        Err(mpsc::RecvTimeoutError::Timeout) => Err("screenshot request timed out".to_owned()),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err("application stopped before the screenshot was ready".to_owned())
        }
    };

    match result {
        Ok(png) => {
            if let Err(error) = stream.write_all(&png) {
                eprintln!("screenshot socket write error: {error}");
            }
        }
        Err(error) => write_error(&mut stream, &error),
    }
}

fn read_command(stream: &UnixStream) -> Result<Vec<u8>, String> {
    let reader = stream
        .try_clone()
        .map_err(|error| format!("could not read request: {error}"))?;
    let mut reader = BufReader::new(reader);
    let mut command = Vec::new();
    let bytes_read = reader
        .by_ref()
        .take(MAX_COMMAND_BYTES + 1)
        .read_until(b'\n', &mut command)
        .map_err(|error| format!("could not read request: {error}"))?;

    if bytes_read == 0 {
        return Err("empty request".to_owned());
    }
    if command.len() > MAX_COMMAND_BYTES as usize || !command.ends_with(b"\n") {
        return Err("request must be a line no longer than 64 bytes".to_owned());
    }

    command.pop();
    if command.ends_with(b"\r") {
        command.pop();
    }
    Ok(command)
}

fn encode_png(screenshot: &CapturedScreenshot) -> Result<Vec<u8>, String> {
    let expected_length = screenshot.width as usize * screenshot.height as usize * 4;
    if screenshot.width == 0 || screenshot.height == 0 || screenshot.rgba.len() != expected_length {
        return Err("screenshot has invalid RGBA dimensions".to_owned());
    }

    let mut png = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut png, screenshot.width, screenshot.height);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);

        let mut writer = encoder
            .write_header()
            .map_err(|error| format!("could not encode PNG header: {error}"))?;
        writer
            .write_image_data(&screenshot.rgba)
            .map_err(|error| format!("could not encode PNG data: {error}"))?;
    }

    Ok(png)
}

fn write_error(stream: &mut UnixStream, error: &str) {
    let _ = writeln!(stream, "ERR {error}");
}

#[cfg(test)]
mod tests {
    use super::*;

    static NEXT_TEST_PATH_ID: AtomicU64 = AtomicU64::new(1);

    fn test_socket_path() -> PathBuf {
        let id = NEXT_TEST_PATH_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "wtui-screenshot-test-{}-{id}.sock",
            std::process::id()
        ))
    }

    fn read_command_from_client(command: &[u8]) -> Result<Vec<u8>, String> {
        let path = test_socket_path();
        let listener = UnixListener::bind(&path).expect("test listener must bind");
        let command = command.to_vec();
        let client = thread::spawn({
            let path = path.clone();
            move || {
                let mut stream = UnixStream::connect(path).expect("client must connect");
                stream.write_all(&command).expect("client must write");
            }
        });

        let (stream, _) = listener.accept().expect("listener must accept");
        let result = read_command(&stream);
        client.join().expect("client must not panic");
        fs::remove_file(path).expect("test socket must be removed");
        result
    }

    #[test]
    fn accepts_the_screenshot_command() {
        assert_eq!(
            read_command_from_client(b"screenshot\n"),
            Ok(b"screenshot".to_vec())
        );
    }

    #[test]
    fn rejects_an_unterminated_command() {
        assert!(read_command_from_client(b"screenshot").is_err());
    }

    #[test]
    fn returns_an_error_response_for_an_unknown_command() {
        let path = test_socket_path();
        let listener = UnixListener::bind(&path).expect("test listener must bind");
        let (sender, _receiver) = futures_mpsc::unbounded();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("listener must accept");
            handle_client(stream, &sender);
        });

        let mut client = UnixStream::connect(&path).expect("client must connect");
        client
            .write_all(b"unknown\n")
            .expect("client must write command");
        let mut response = String::new();
        client
            .read_to_string(&mut response)
            .expect("client must read response");
        server.join().expect("server must not panic");
        fs::remove_file(path).expect("test socket must be removed");

        assert_eq!(response, "ERR expected `screenshot` command\n");
    }

    #[test]
    fn does_not_replace_a_live_socket() {
        let path = test_socket_path();
        let socket = ScreenshotSocket::bind(path.clone()).expect("first socket must start");
        assert!(ScreenshotSocket::bind(path.clone()).is_err());
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
    fn refuses_to_replace_a_regular_file() {
        let path = test_socket_path();
        fs::write(&path, "do not replace").expect("test file must be created");
        assert!(prepare_socket_path(&path).is_err());
        fs::remove_file(path).expect("test file must be removed");
    }

    #[test]
    fn uses_private_permissions_and_removes_the_socket_on_drop() {
        let path = test_socket_path();
        let socket = ScreenshotSocket::bind(path.clone()).expect("test socket must start");
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
    fn encodes_rgba_as_png() {
        let screenshot = CapturedScreenshot {
            rgba: vec![255, 0, 0, 255],
            width: 1,
            height: 1,
        };

        let png = encode_png(&screenshot).expect("PNG encoding must succeed");
        assert_eq!(&png[..8], b"\x89PNG\r\n\x1a\n");
    }

    #[test]
    fn rejects_invalid_rgba_dimensions() {
        let screenshot = CapturedScreenshot {
            rgba: vec![0; 3],
            width: 1,
            height: 1,
        };

        assert!(encode_png(&screenshot).is_err());
    }
}
