use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::consumer::{Consumer, ConsumerKind};
use crate::error::Error;
use crate::identifier::Identifier;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct PollMessages {
    #[serde(flatten)]
    pub consumer: Consumer,
    #[serde(skip)]
    pub stream_id: Identifier,
    #[serde(skip)]
    pub topic_id: Identifier,
    #[serde(default = "default_partition_id")]
    pub partition_id: Option<u32>,
    #[serde(default = "default_strategy", flatten)]
    pub strategy: PollingStrategy,
    #[serde(default = "default_count")]
    pub count: u32,
    #[serde(default)]
    pub auto_commit: bool,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Copy, Clone)]
pub struct PollingStrategy {
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_kind")]
    pub kind: PollingKind,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_value")]
    pub value: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PollingKind {
    #[default]
    Offset,
    Timestamp,
    First,
    Last,
    Next,
}

impl Default for PollMessages {
    fn default() -> Self {
        Self {
            consumer: Consumer::default(),
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(1).unwrap(),
            partition_id: default_partition_id(),
            strategy: default_strategy(),
            count: default_count(),
            auto_commit: false,
        }
    }
}

impl Default for PollingStrategy {
    fn default() -> Self {
        Self {
            kind: PollingKind::Offset,
            value: 0,
        }
    }
}

impl CommandPayload for PollMessages {}

fn default_partition_id() -> Option<u32> {
    Some(1)
}

fn default_kind() -> PollingKind {
    PollingKind::Offset
}

fn default_value() -> u64 {
    0
}

fn default_strategy() -> PollingStrategy {
    PollingStrategy::default()
}

fn default_count() -> u32 {
    10
}

impl Validatable for PollMessages {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl PollingStrategy {
    pub fn offset(value: u64) -> Self {
        Self {
            kind: PollingKind::Offset,
            value,
        }
    }

    pub fn timestamp(value: u64) -> Self {
        Self {
            kind: PollingKind::Timestamp,
            value,
        }
    }

    pub fn first() -> Self {
        Self {
            kind: PollingKind::First,
            value: 0,
        }
    }

    pub fn last() -> Self {
        Self {
            kind: PollingKind::Last,
            value: 0,
        }
    }

    pub fn next() -> Self {
        Self {
            kind: PollingKind::Next,
            value: 0,
        }
    }
}

impl PollingKind {
    pub fn as_code(&self) -> u8 {
        match self {
            PollingKind::Offset => 1,
            PollingKind::Timestamp => 2,
            PollingKind::First => 3,
            PollingKind::Last => 4,
            PollingKind::Next => 5,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, Error> {
        match code {
            1 => Ok(PollingKind::Offset),
            2 => Ok(PollingKind::Timestamp),
            3 => Ok(PollingKind::First),
            4 => Ok(PollingKind::Last),
            5 => Ok(PollingKind::Next),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl FromStr for PollingKind {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "o" | "offset" => Ok(PollingKind::Offset),
            "t" | "timestamp" => Ok(PollingKind::Timestamp),
            "f" | "first" => Ok(PollingKind::First),
            "l" | "last" => Ok(PollingKind::Last),
            "n" | "next" => Ok(PollingKind::Next),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Display for PollingKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollingKind::Offset => write!(f, "offset"),
            PollingKind::Timestamp => write!(f, "timestamp"),
            PollingKind::First => write!(f, "first"),
            PollingKind::Last => write!(f, "last"),
            PollingKind::Next => write!(f, "next"),
        }
    }
}

impl FromStr for PollMessages {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() < 8 {
            return Err(Error::InvalidCommand);
        }

        let consumer_kind = ConsumerKind::from_str(parts[0])?;
        let consumer_id = parts[1].parse::<u32>()?;
        let consumer = Consumer {
            kind: consumer_kind,
            id: consumer_id,
        };
        let stream_id = parts[2].parse::<Identifier>()?;
        let topic_id = parts[3].parse::<Identifier>()?;
        let partition_id = parts[4].parse::<u32>()?;
        let polling_kind = PollingKind::from_str(parts[5])?;
        let value = parts[6].parse::<u64>()?;
        let strategy = PollingStrategy {
            kind: polling_kind,
            value,
        };
        let count = parts[7].parse::<u32>()?;
        let auto_commit = match parts.get(8) {
            Some(auto_commit) => match *auto_commit {
                "a" | "auto_commit" => true,
                "n" | "no_commit" => false,
                _ => return Err(Error::InvalidCommand),
            },
            None => false,
        };

        let command = PollMessages {
            consumer,
            stream_id,
            topic_id,
            partition_id: Some(partition_id),
            strategy,
            count,
            auto_commit,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for PollMessages {
    fn as_bytes(&self) -> Vec<u8> {
        let consumer_bytes = self.consumer.as_bytes();
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let strategy_bytes = self.strategy.as_bytes();
        let mut bytes = Vec::with_capacity(
            9 + consumer_bytes.len()
                + stream_id_bytes.len()
                + topic_id_bytes.len()
                + strategy_bytes.len(),
        );
        bytes.extend(consumer_bytes);
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        if let Some(partition_id) = self.partition_id {
            bytes.put_u32_le(partition_id);
        } else {
            bytes.put_u32_le(0);
        }
        bytes.extend(strategy_bytes);
        bytes.put_u32_le(self.count);
        if self.auto_commit {
            bytes.put_u8(1);
        } else {
            bytes.put_u8(0);
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 29 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let consumer_kind = ConsumerKind::from_code(bytes[0])?;
        let consumer_id = u32::from_le_bytes(bytes[1..5].try_into()?);
        let consumer = Consumer {
            kind: consumer_kind,
            id: consumer_id,
        };
        position += 5;
        let stream_id = Identifier::from_bytes(&bytes[position..])?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        position += topic_id.get_size_bytes() as usize;
        let partition_id = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let partition_id = match partition_id {
            0 => None,
            partition_id => Some(partition_id),
        };
        let polling_kind = PollingKind::from_code(bytes[position + 4])?;
        position += 5;
        let value = u64::from_le_bytes(bytes[position..position + 8].try_into()?);
        let strategy = PollingStrategy {
            kind: polling_kind,
            value,
        };
        let count = u32::from_le_bytes(bytes[position + 8..position + 12].try_into()?);
        let auto_commit = bytes[position + 12];
        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };
        let command = PollMessages {
            consumer,
            stream_id,
            topic_id,
            partition_id,
            strategy,
            count,
            auto_commit,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for PollMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|{}",
            self.consumer,
            self.stream_id,
            self.topic_id,
            self.partition_id.unwrap_or(0),
            self.strategy,
            self.count,
            auto_commit_to_string(self.auto_commit)
        )
    }
}

impl Display for PollingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.kind, self.value)
    }
}

fn auto_commit_to_string(auto_commit: bool) -> &'static str {
    if auto_commit {
        "a"
    } else {
        "n"
    }
}

impl BytesSerializable for PollingStrategy {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(9);
        bytes.put_u8(self.kind.as_code());
        bytes.put_u64_le(self.value);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != 9 {
            return Err(Error::InvalidCommand);
        }

        let kind = PollingKind::from_code(bytes[0])?;
        let value = u64::from_le_bytes(bytes[1..9].try_into()?);
        let strategy = PollingStrategy { kind, value };
        Ok(strategy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = PollMessages {
            consumer: Consumer::new(1),
            stream_id: Identifier::numeric(2).unwrap(),
            topic_id: Identifier::numeric(3).unwrap(),
            partition_id: Some(4),
            strategy: PollingStrategy::offset(2),
            count: 3,
            auto_commit: true,
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let consumer_kind = ConsumerKind::from_code(bytes[0]).unwrap();
        let consumer_id = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
        let consumer = Consumer {
            kind: consumer_kind,
            id: consumer_id,
        };
        position += 5;
        let stream_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += topic_id.get_size_bytes() as usize;
        let partition_id = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let polling_kind = PollingKind::from_code(bytes[position + 4]).unwrap();
        position += 5;
        let value = u64::from_le_bytes(bytes[position..position + 8].try_into().unwrap());
        let strategy = PollingStrategy {
            kind: polling_kind,
            value,
        };
        let count = u32::from_le_bytes(bytes[position + 8..position + 12].try_into().unwrap());
        let auto_commit = bytes[position + 12];
        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };

        assert!(!bytes.is_empty());
        assert_eq!(consumer, command.consumer);
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(Some(partition_id), command.partition_id);
        assert_eq!(strategy, command.strategy);
        assert_eq!(count, command.count);
        assert_eq!(auto_commit, command.auto_commit);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let consumer = Consumer::new(1);
        let stream_id = Identifier::numeric(2).unwrap();
        let topic_id = Identifier::numeric(3).unwrap();
        let partition_id = 4u32;
        let strategy = PollingStrategy::offset(2);
        let count = 3u32;
        let auto_commit = 1u8;

        let consumer_bytes = consumer.as_bytes();
        let stream_id_bytes = stream_id.as_bytes();
        let topic_id_bytes = topic_id.as_bytes();
        let strategy_bytes = strategy.as_bytes();
        let mut bytes = Vec::with_capacity(
            9 + consumer_bytes.len()
                + stream_id_bytes.len()
                + topic_id_bytes.len()
                + strategy_bytes.len(),
        );
        bytes.extend(consumer_bytes);
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.put_u32_le(partition_id);
        bytes.extend(strategy_bytes);
        bytes.put_u32_le(count);
        bytes.put_u8(auto_commit);

        let command = PollMessages::from_bytes(&bytes);
        assert!(command.is_ok());

        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };

        let command = command.unwrap();
        assert_eq!(command.consumer, consumer);
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partition_id, Some(partition_id));
        assert_eq!(command.strategy, strategy);
        assert_eq!(command.count, count);
        assert_eq!(command.auto_commit, auto_commit);
    }

    #[test]
    fn should_be_read_from_string() {
        let consumer = Consumer::new(1);
        let stream_id = Identifier::numeric(2).unwrap();
        let topic_id = Identifier::numeric(3).unwrap();
        let partition_id = 4u32;
        let strategy = PollingStrategy::timestamp(2);
        let count = 3u32;
        let auto_commit = 1u8;
        let auto_commit_str = "auto_commit";

        let input = format!(
            "{}|{}|{}|{}|{}|{}|{}",
            consumer, stream_id, topic_id, partition_id, strategy, count, auto_commit_str
        );
        let command = PollMessages::from_str(&input);
        assert!(command.is_ok());

        let auto_commit = match auto_commit {
            0 => false,
            1 => true,
            _ => false,
        };

        let command = command.unwrap();
        assert_eq!(command.consumer, consumer);
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partition_id, Some(partition_id));
        assert_eq!(command.strategy, strategy);
        assert_eq!(command.count, count);
        assert_eq!(command.auto_commit, auto_commit);
    }
}
