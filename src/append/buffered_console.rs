use std::{
    io::Write,
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
};

use derivative::Derivative;
use log::{Level, Record};

#[cfg(feature = "config_parsing")]
use crate::config::{Deserialize, Deserializers};
#[cfg(feature = "config_parsing")]
use crate::encode::EncoderConfig;
use crate::{
    append::{console::Target, Append},
    encode::{
        pattern::PatternEncoder,
        writer::{console::ConsoleWriter, simple::SimpleWriter},
        Color, Encode, Style, Write as _,
    },
    priv_io::StdWriter,
};

const DEFAULT_BUFFER_SIZE: usize = 32 * 1024; // 32KB buffer
const FLUSH_INTERVAL_MS: u64 = 1000; // 1 second flush interval

enum BufferMessage {
    Log(Vec<u8>, Style),
    Flush,
    Shutdown,
}

enum Writer {
    Console(ConsoleWriter),
    Std(StdWriter),
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Writer::Console(w) => w.write(buf),
            Writer::Std(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Writer::Console(w) => w.flush(),
            Writer::Std(w) => w.flush(),
        }
    }
}

impl Writer {
    fn set_style(&mut self, style: &Style) {
        if let Writer::Console(w) = self {
            let _ = w.set_style(style);
        }
    }
}

/// A buffered console appender.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct BufferedConsoleAppender {
    #[derivative(Debug = "ignore")]
    sender: Sender<BufferMessage>,
    #[derivative(Debug = "ignore")]
    worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    encoder: Box<dyn Encode>,
}

impl Drop for BufferedConsoleAppender {
    fn drop(&mut self) {
        let _ = self.sender.send(BufferMessage::Shutdown);

        if let Some(worker) = self.worker.lock().unwrap().take() {
            let _ = worker.join();
        }
    }
}

fn level_to_style(level: Level) -> Style {
    let mut style = Style::new();
    match level {
        Level::Error => {
            style.text(Color::Red);
            style.intense(true);
        }
        Level::Warn => {
            style.text(Color::Yellow);
        }
        Level::Info => {
            style.text(Color::Green);
        }
        Level::Debug => {
            style.text(Color::Blue);
        }
        Level::Trace => {
            style.text(Color::White);
        }
    }
    style
}

impl Append for BufferedConsoleAppender {
    fn append(&self, record: &Record) -> anyhow::Result<()> {
        let mut buffer = Vec::new();
        let mut writer = SimpleWriter(&mut buffer);
        self.encoder.encode(&mut writer, record)?;
        let style = level_to_style(record.level());
        self.sender.send(BufferMessage::Log(buffer, style))?;
        Ok(())
    }

    fn flush(&self) {
        let _ = self.sender.send(BufferMessage::Flush);
    }
}

/// Builder for the `BufferedConsoleAppender`.
pub struct BufferedConsoleAppenderBuilder {
    encoder: Option<Box<dyn Encode>>,
    target: Target,
    buffer_size: Option<usize>,
    flush_interval: Option<u64>,
}

impl BufferedConsoleAppenderBuilder {
    /// Sets the encoder for the `BufferedConsoleAppender`.
    pub fn encoder(mut self, encoder: Box<dyn Encode>) -> Self {
        self.encoder = Some(encoder);
        self
    }

    /// Sets the target for the `BufferedConsoleAppender`.
    pub fn target(mut self, target: Target) -> Self {
        self.target = target;
        self
    }

    /// Sets the buffer size for the `BufferedConsoleAppender`.
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = Some(size);
        self
    }

    /// Sets the flush interval for the `BufferedConsoleAppender`.
    pub fn flush_interval(mut self, interval_ms: u64) -> Self {
        self.flush_interval = Some(interval_ms);
        self
    }

    /// Builds the `BufferedConsoleAppender`.
    pub fn build(self) -> BufferedConsoleAppender {
        let buffer_size = self.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE);
        let flush_interval = self.flush_interval.unwrap_or(FLUSH_INTERVAL_MS);

        let (sender, receiver) = mpsc::channel();
        let worker = Arc::new(Mutex::new(None));
        let worker_clone = Arc::clone(&worker);

        let target = self.target;
        let handle = thread::Builder::new()
            .name("buffered-console-appender".into())
            .spawn(move || {
                let mut buffer = Vec::with_capacity(buffer_size);
                let mut last_flush = std::time::Instant::now();
                let mut writer = match target {
                    Target::Stderr => match ConsoleWriter::stderr() {
                        Some(writer) => Writer::Console(writer),
                        None => Writer::Std(StdWriter::stderr()),
                    },
                    Target::Stdout => match ConsoleWriter::stdout() {
                        Some(writer) => Writer::Console(writer),
                        None => Writer::Std(StdWriter::stdout()),
                    },
                };

                loop {
                    match receiver.recv_timeout(std::time::Duration::from_millis(flush_interval)) {
                        Ok(BufferMessage::Log(mut data, style)) => {
                            writer.set_style(&style);
                            buffer.append(&mut data);
                            if buffer.len() >= buffer_size {
                                let _ = writer.write_all(&buffer);
                                let _ = writer.flush();
                                buffer.clear();
                                last_flush = std::time::Instant::now();
                            }
                            writer.set_style(&Style::new());
                        }
                        Ok(BufferMessage::Flush) | Err(mpsc::RecvTimeoutError::Timeout) => {
                            if !buffer.is_empty()
                                && last_flush.elapsed().as_millis() >= flush_interval as u128
                            {
                                let _ = writer.write_all(&buffer);
                                let _ = writer.flush();
                                buffer.clear();
                                last_flush = std::time::Instant::now();
                            }
                        }
                        Ok(BufferMessage::Shutdown) => {
                            if !buffer.is_empty() {
                                let _ = writer.write_all(&buffer);
                                let _ = writer.flush();
                            }
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            if !buffer.is_empty() {
                                let _ = writer.write_all(&buffer);
                                let _ = writer.flush();
                            }
                            break;
                        }
                    }
                }
            })
            .expect("Failed to spawn buffered console appender thread");

        *worker_clone.lock().unwrap() = Some(handle);

        BufferedConsoleAppender {
            sender,
            worker: worker_clone,
            encoder: self
                .encoder
                .unwrap_or_else(|| Box::<PatternEncoder>::default()),
        }
    }
}

impl BufferedConsoleAppender {
    /// Creates a new `BufferedConsoleAppenderBuilder`.
    pub fn builder() -> BufferedConsoleAppenderBuilder {
        BufferedConsoleAppenderBuilder {
            encoder: None,
            target: Target::Stdout,
            buffer_size: None,
            flush_interval: None,
        }
    }
}

/// 配置
#[cfg(feature = "config_parsing")]
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferedConsoleAppenderConfig {
    target: Option<crate::append::console::ConfigTarget>,
    encoder: Option<EncoderConfig>,
    buffer_size: Option<usize>,
    flush_interval: Option<u64>,
}

/// A deserializer for the `BufferedConsoleAppender`.
#[cfg(feature = "config_parsing")]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct BufferedConsoleAppenderDeserializer;

#[cfg(feature = "config_parsing")]
impl Deserialize for BufferedConsoleAppenderDeserializer {
    type Trait = dyn Append;
    type Config = BufferedConsoleAppenderConfig;

    fn deserialize(
        &self,
        config: BufferedConsoleAppenderConfig,
        deserializers: &Deserializers,
    ) -> anyhow::Result<Box<dyn Append>> {
        let mut appender = BufferedConsoleAppender::builder();

        if let Some(target) = config.target {
            use crate::append::console::ConfigTarget;
            let target = match target {
                ConfigTarget::Stdout => Target::Stdout,
                ConfigTarget::Stderr => Target::Stderr,
            };
            appender = appender.target(target);
        }

        if let Some(buffer_size) = config.buffer_size {
            appender = appender.buffer_size(buffer_size);
        }

        if let Some(flush_interval) = config.flush_interval {
            appender = appender.flush_interval(flush_interval);
        }

        if let Some(encoder) = config.encoder {
            appender = appender.encoder(deserializers.deserialize(&encoder.kind, encoder.config)?);
        }

        Ok(Box::new(appender.build()))
    }
}
