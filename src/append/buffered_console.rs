use std::{
    io::Write,
    sync::mpsc::{self, Sender},
    thread,
};

use derivative::Derivative;
use log::Record;

#[cfg(feature = "config_parsing")]
use crate::config::{Deserialize, Deserializers};
#[cfg(feature = "config_parsing")]
use crate::encode::EncoderConfig;
use crate::{
    append::{console::Target, Append},
    encode::{pattern::PatternEncoder, writer::simple::SimpleWriter, Encode},
    priv_io::StdWriter,
};

const DEFAULT_BUFFER_SIZE: usize = 32 * 1024; // 32KB buffer
const FLUSH_INTERVAL_MS: u64 = 1000; // 1 second flush interval

enum BufferMessage {
    Log(Vec<u8>),
    Flush,
    Shutdown,
}

/// A buffered console appender.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct BufferedConsoleAppender {
    #[derivative(Debug = "ignore")]
    sender: Sender<BufferMessage>,
    encoder: Box<dyn Encode>,
}

impl Drop for BufferedConsoleAppender {
    fn drop(&mut self) {
        let _ = self.sender.send(BufferMessage::Shutdown);
    }
}

impl Append for BufferedConsoleAppender {
    fn append(&self, record: &Record) -> anyhow::Result<()> {
        let mut buffer = Vec::new();
        let mut writer = SimpleWriter(&mut buffer);
        self.encoder.encode(&mut writer, record)?;
        self.sender.send(BufferMessage::Log(buffer))?;
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

        let target = self.target;
        thread::Builder::new()
            .name("buffered-console-appender".into())
            .spawn(move || {
                let mut buffer = Vec::with_capacity(buffer_size);
                let mut last_flush = std::time::Instant::now();
                let mut writer = match target {
                    Target::Stderr => match crate::encode::writer::console::ConsoleWriter::stderr()
                    {
                        Some(writer) => Box::new(writer) as Box<dyn Write + Send>,
                        None => Box::new(StdWriter::stderr()) as Box<dyn Write + Send>,
                    },
                    Target::Stdout => match crate::encode::writer::console::ConsoleWriter::stdout()
                    {
                        Some(writer) => Box::new(writer) as Box<dyn Write + Send>,
                        None => Box::new(StdWriter::stdout()) as Box<dyn Write + Send>,
                    },
                };

                loop {
                    match receiver.recv_timeout(std::time::Duration::from_millis(flush_interval)) {
                        Ok(BufferMessage::Log(mut data)) => {
                            buffer.append(&mut data);
                            if buffer.len() >= buffer_size {
                                let _ = writer.write_all(&buffer);
                                let _ = writer.flush();
                                buffer.clear();
                                last_flush = std::time::Instant::now();
                            }
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
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
            })
            .expect("Failed to spawn buffered console appender thread");

        BufferedConsoleAppender {
            sender,
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
