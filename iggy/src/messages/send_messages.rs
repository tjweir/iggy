use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::models::header;
use crate::models::header::{HeaderKey, HeaderValue};
use crate::validatable::Validatable;
use bytes::{BufMut, Bytes};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

const MAX_HEADERS_SIZE: u32 = 100 * 1024;
const MAX_PAYLOAD_SIZE: u32 = 10 * 1024 * 1024;

const EMPTY_KEY_VALUE: Vec<u8> = vec![];

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SendMessages {
    #[serde(skip)]
    pub stream_id: Identifier,
    #[serde(skip)]
    pub topic_id: Identifier,
    pub partitioning: Partitioning,
    pub messages: Vec<Message>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Partitioning {
    pub kind: PartitioningKind,
    #[serde(skip)]
    pub length: u8,
    #[serde_as(as = "Base64")]
    pub value: Vec<u8>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Message {
    #[serde(default = "default_message_id")]
    pub id: u128,
    #[serde(skip)]
    pub length: u32,
    #[serde_as(as = "Base64")]
    pub payload: Bytes,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PartitioningKind {
    #[default]
    Balanced,
    PartitionId,
    MessagesKey,
}

fn default_message_id() -> u128 {
    0
}

impl Default for SendMessages {
    fn default() -> Self {
        SendMessages {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partitioning: Partitioning::default(),
            messages: vec![Message::default()],
        }
    }
}

impl Default for Partitioning {
    fn default() -> Self {
        Partitioning::balanced()
    }
}

impl Partitioning {
    pub fn balanced() -> Self {
        Partitioning {
            kind: PartitioningKind::Balanced,
            length: 0,
            value: EMPTY_KEY_VALUE,
        }
    }

    pub fn partition_id(partition_id: u32) -> Self {
        Partitioning {
            kind: PartitioningKind::PartitionId,
            length: 4,
            value: partition_id.to_le_bytes().to_vec(),
        }
    }

    pub fn messages_key(value: &[u8]) -> Result<Self, Error> {
        let length = value.len();
        if length == 0 || length > 255 {
            return Err(Error::InvalidCommand);
        }

        Ok(Partitioning {
            kind: PartitioningKind::MessagesKey,
            length: length as u8,
            value: value.to_vec(),
        })
    }

    pub fn messages_key_str(value: &str) -> Result<Self, Error> {
        Self::messages_key(value.as_bytes())
    }

    pub fn messages_key_u32(value: u32) -> Self {
        Partitioning {
            kind: PartitioningKind::MessagesKey,
            length: 4,
            value: value.to_le_bytes().to_vec(),
        }
    }

    pub fn messages_key_u64(value: u64) -> Self {
        Partitioning {
            kind: PartitioningKind::MessagesKey,
            length: 8,
            value: value.to_le_bytes().to_vec(),
        }
    }

    pub fn messages_key_u128(value: u128) -> Self {
        Partitioning {
            kind: PartitioningKind::MessagesKey,
            length: 16,
            value: value.to_le_bytes().to_vec(),
        }
    }

    pub fn from_partitioning(partitioning: &Partitioning) -> Self {
        Partitioning {
            kind: partitioning.kind,
            length: partitioning.length,
            value: partitioning.value.clone(),
        }
    }

    pub fn get_size_bytes(&self) -> u32 {
        2 + self.length as u32
    }
}

impl CommandPayload for SendMessages {}

impl Validatable for SendMessages {
    fn validate(&self) -> Result<(), Error> {
        if self.messages.is_empty() {
            return Err(Error::InvalidMessagesCount);
        }

        let key_value_length = self.partitioning.value.len();
        if key_value_length > 255
            || (self.partitioning.kind != PartitioningKind::Balanced && key_value_length == 0)
        {
            return Err(Error::InvalidKeyValueLength);
        }

        let mut headers_size = 0;
        let mut payload_size = 0;
        for message in &self.messages {
            if let Some(headers) = &message.headers {
                for value in headers.values() {
                    headers_size += value.value.len() as u32;
                    if headers_size > MAX_HEADERS_SIZE {
                        return Err(Error::TooBigHeadersPayload);
                    }
                }
            }
            payload_size += message.payload.len() as u32;
            if payload_size > MAX_PAYLOAD_SIZE {
                return Err(Error::TooBigMessagePayload);
            }
        }

        if payload_size == 0 {
            return Err(Error::EmptyMessagePayload);
        }

        Ok(())
    }
}

impl PartitioningKind {
    pub fn as_code(&self) -> u8 {
        match self {
            PartitioningKind::Balanced => 1,
            PartitioningKind::PartitionId => 2,
            PartitioningKind::MessagesKey => 3,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, Error> {
        match code {
            1 => Ok(PartitioningKind::Balanced),
            2 => Ok(PartitioningKind::PartitionId),
            3 => Ok(PartitioningKind::MessagesKey),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl FromStr for PartitioningKind {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "b" | "balanced" => Ok(PartitioningKind::Balanced),
            "p" | "partition_id" => Ok(PartitioningKind::PartitionId),
            "k" | "messages_key" => Ok(PartitioningKind::MessagesKey),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Message {
    pub fn new(
        id: Option<u128>,
        payload: Bytes,
        headers: Option<HashMap<HeaderKey, HeaderValue>>,
    ) -> Self {
        Message {
            id: id.unwrap_or(0),
            length: payload.len() as u32,
            payload,
            headers,
        }
    }

    pub fn get_size_bytes(&self) -> u32 {
        // ID + Length + Payload + Headers
        16 + 4 + self.payload.len() as u32 + header::get_headers_size_bytes(&self.headers)
    }
}

impl Default for Message {
    fn default() -> Self {
        let payload = Bytes::from("hello world");
        Message {
            id: 0,
            length: payload.len() as u32,
            payload,
            headers: None,
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.id, String::from_utf8_lossy(&self.payload))
    }
}

impl BytesSerializable for Partitioning {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(2 + self.length as usize);
        bytes.put_u8(self.kind.as_code());
        bytes.put_u8(self.length);
        bytes.extend(&self.value);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error>
    where
        Self: Sized,
    {
        if bytes.len() < 3 {
            return Err(Error::InvalidCommand);
        }

        let kind = PartitioningKind::from_code(bytes[0])?;
        let length = bytes[1];
        let value = bytes[2..2 + length as usize].to_vec();
        if value.len() != length as usize {
            return Err(Error::InvalidCommand);
        }

        Ok(Partitioning {
            kind,
            length,
            value,
        })
    }
}

impl BytesSerializable for Message {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.get_size_bytes() as usize);
        bytes.put_u128_le(self.id);
        if let Some(headers) = &self.headers {
            let headers_bytes = headers.as_bytes();
            bytes.put_u32_le(headers_bytes.len() as u32);
            bytes.extend(&headers_bytes);
        } else {
            bytes.put_u32_le(0);
        }
        bytes.put_u32_le(self.length);
        bytes.extend(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 24 {
            return Err(Error::InvalidCommand);
        }

        let id = u128::from_le_bytes(bytes[..16].try_into()?);
        let headers_length = u32::from_le_bytes(bytes[16..20].try_into()?);
        let headers = if headers_length > 0 {
            Some(HashMap::from_bytes(
                &bytes[20..20 + headers_length as usize],
            )?)
        } else {
            None
        };

        let payload_length = u32::from_le_bytes(
            bytes[20 + headers_length as usize..24 + headers_length as usize].try_into()?,
        );
        if payload_length == 0 {
            return Err(Error::EmptyMessagePayload);
        }

        let payload = Bytes::from(
            bytes[24 + headers_length as usize
                ..24 + headers_length as usize + payload_length as usize]
                .to_vec(),
        );
        if payload.len() != payload_length as usize {
            return Err(Error::InvalidMessagePayloadLength);
        }

        Ok(Message {
            id,
            length: payload_length,
            payload,
            headers,
        })
    }
}

impl FromStr for Message {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        let (id, payload) = match parts.len() {
            1 => (0, Bytes::from(parts[0].as_bytes().to_vec())),
            2 => (
                parts[0].parse::<u128>()?,
                Bytes::from(parts[1].as_bytes().to_vec()),
            ),
            _ => return Err(Error::InvalidCommand),
        };
        let length = payload.len() as u32;
        if length == 0 {
            return Err(Error::EmptyMessagePayload);
        }

        Ok(Message {
            id,
            length,
            payload,
            headers: None,
        })
    }
}

impl FromStr for SendMessages {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 6 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let topic_id = parts[1].parse::<Identifier>()?;
        let key_kind = parts[2];
        let key_kind = PartitioningKind::from_str(key_kind)?;
        let (key_value, key_length) = match key_kind {
            PartitioningKind::Balanced => (EMPTY_KEY_VALUE, 0),
            PartitioningKind::PartitionId => (parts[3].parse::<u32>()?.to_le_bytes().to_vec(), 4),
            PartitioningKind::MessagesKey => {
                let key_value = parts[3].as_bytes().to_vec();
                let key_length = parts[3].len() as u8;
                (key_value, key_length)
            }
        };
        let message_id = parts[4].parse::<u128>()?;
        let payload = Bytes::from(parts[5].as_bytes().to_vec());

        // For now, we only support a single payload.
        let message = Message {
            id: message_id,
            length: payload.len() as u32,
            payload,
            headers: None,
        };

        let command = SendMessages {
            stream_id,
            topic_id,
            partitioning: Partitioning {
                kind: key_kind,
                length: key_length,
                value: key_value,
            },
            messages: vec![message],
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for SendMessages {
    fn as_bytes(&self) -> Vec<u8> {
        let messages_size = self
            .messages
            .iter()
            .map(|message| message.get_size_bytes())
            .sum::<u32>();

        let key_bytes = self.partitioning.as_bytes();
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let mut bytes = Vec::with_capacity(
            stream_id_bytes.len() + topic_id_bytes.len() + key_bytes.len() + messages_size as usize,
        );
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.extend(key_bytes);
        for message in &self.messages {
            bytes.extend(message.as_bytes());
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<SendMessages, Error> {
        if bytes.len() < 11 {
            return Err(Error::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes)?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        position += topic_id.get_size_bytes() as usize;
        let key = Partitioning::from_bytes(&bytes[position..])?;
        position += key.get_size_bytes() as usize;
        let messages_payloads = &bytes[position..];
        position = 0;
        let mut messages = Vec::new();
        while position < messages_payloads.len() {
            let message = Message::from_bytes(&messages_payloads[position..])?;
            position += message.get_size_bytes() as usize;
            messages.push(message);
        }

        let command = SendMessages {
            stream_id,
            topic_id,
            partitioning: key,
            messages,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for SendMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.stream_id,
            self.topic_id,
            self.partitioning,
            self.messages
                .iter()
                .map(|message| message.to_string())
                .collect::<Vec<String>>()
                .join("|")
        )
    }
}

impl Display for Partitioning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            PartitioningKind::Balanced => write!(f, "{}|0", self.kind),
            PartitioningKind::PartitionId => write!(
                f,
                "{}|{}",
                self.kind,
                u32::from_le_bytes(self.value[..4].try_into().unwrap())
            ),
            PartitioningKind::MessagesKey => {
                write!(f, "{}|{}", self.kind, String::from_utf8_lossy(&self.value))
            }
        }
    }
}

impl Display for PartitioningKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitioningKind::Balanced => write!(f, "balanced"),
            PartitioningKind::PartitionId => write!(f, "partition_id"),
            PartitioningKind::MessagesKey => write!(f, "messages_key"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let message_1 = Message::from_str("hello 1").unwrap();
        let message_2 = Message::from_str("2|hello 2").unwrap();
        let message_3 = Message::from_str("3|hello 3").unwrap();
        let messages = vec![message_1, message_2, message_3];
        let command = SendMessages {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            partitioning: Partitioning::partition_id(4),
            messages,
        };

        let bytes = command.as_bytes();

        let mut position = 0;
        let stream_id = Identifier::from_bytes(&bytes).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += topic_id.get_size_bytes() as usize;
        let key = Partitioning::from_bytes(&bytes[position..]).unwrap();
        position += key.get_size_bytes() as usize;
        let messages = &bytes[position..];
        let command_messages = &command
            .messages
            .iter()
            .map(|message| message.as_bytes())
            .collect::<Vec<Vec<u8>>>()
            .concat();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(key, command.partitioning);
        assert_eq!(messages, command_messages);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let key = Partitioning::partition_id(4);

        let message_1 = Message::from_str("hello 1").unwrap();
        let message_2 = Message::from_str("2|hello 2").unwrap();
        let message_3 = Message::from_str("3|hello 3").unwrap();
        let messages = [
            message_1.as_bytes(),
            message_2.as_bytes(),
            message_3.as_bytes(),
        ]
        .concat();

        let key_bytes = key.as_bytes();
        let stream_id_bytes = stream_id.as_bytes();
        let topic_id_bytes = topic_id.as_bytes();
        let current_position = stream_id_bytes.len() + topic_id_bytes.len() + key_bytes.len();
        let mut bytes = Vec::with_capacity(current_position);
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.extend(key_bytes);
        bytes.extend(messages);

        let command = SendMessages::from_bytes(&bytes);
        assert!(command.is_ok());

        let messages_payloads = &bytes[current_position..];
        let mut position = 0;
        let mut messages = Vec::new();
        while position < messages_payloads.len() {
            let message = Message::from_bytes(&messages_payloads[position..]).unwrap();
            position += message.get_size_bytes() as usize;
            messages.push(message);
        }

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partitioning, key);
        for (index, message) in command.messages.iter().enumerate() {
            let command_message = &command.messages[index];
            assert_eq!(command_message.id, message.id);
            assert_eq!(command_message.length, message.length);
            assert_eq!(command_message.payload, message.payload);
        }
    }

    // For now, we only support a single payload.
    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let key = Partitioning::partition_id(4);
        let message_id = 1u128;
        let payload = "hello";
        let input = format!(
            "{}|{}|{}|{}|{}",
            stream_id, topic_id, key, message_id, payload
        );

        let command = SendMessages::from_str(&input);

        assert!(command.is_ok());
        let command = command.unwrap();
        let message = &command.messages[0];
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partitioning, key);
        assert_eq!(message.id, message_id);
        assert_eq!(message.length, payload.len() as u32);
        assert_eq!(message.payload, payload.as_bytes());
    }

    #[test]
    fn key_of_type_balanced_should_have_empty_value() {
        let key = Partitioning::balanced();
        assert_eq!(key.kind, PartitioningKind::Balanced);
        assert_eq!(key.length, 0);
        assert_eq!(key.value, EMPTY_KEY_VALUE);
        assert_eq!(
            PartitioningKind::from_code(1).unwrap(),
            PartitioningKind::Balanced
        );
    }

    #[test]
    fn key_of_type_partition_should_have_value_of_const_length_4() {
        let partition_id = 1234u32;
        let key = Partitioning::partition_id(partition_id);
        assert_eq!(key.kind, PartitioningKind::PartitionId);
        assert_eq!(key.length, 4);
        assert_eq!(key.value, partition_id.to_le_bytes());
        assert_eq!(
            PartitioningKind::from_code(2).unwrap(),
            PartitioningKind::PartitionId
        );
    }

    #[test]
    fn key_of_type_messages_key_should_have_value_of_dynamic_length() {
        let messages_key = "hello world";
        let key = Partitioning::messages_key_str(messages_key).unwrap();
        assert_eq!(key.kind, PartitioningKind::MessagesKey);
        assert_eq!(key.length, messages_key.len() as u8);
        assert_eq!(key.value, messages_key.as_bytes());
        assert_eq!(
            PartitioningKind::from_code(3).unwrap(),
            PartitioningKind::MessagesKey
        );
    }

    #[test]
    fn key_of_type_messages_key_that_has_length_0_should_fail() {
        let messages_key = "";
        let key = Partitioning::messages_key_str(messages_key);
        assert!(key.is_err());
    }

    #[test]
    fn key_of_type_messages_key_that_has_length_greater_than_255_should_fail() {
        let messages_key = "a".repeat(256);
        let key = Partitioning::messages_key_str(&messages_key);
        assert!(key.is_err());
    }
}
