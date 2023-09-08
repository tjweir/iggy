use crate::client::{
    Client, ConsumerGroupClient, ConsumerOffsetClient, MessageClient, PartitionClient,
    StreamClient, SystemClient, TopicClient, UserClient,
};
use crate::consumer::Consumer;
use crate::consumer_groups::create_consumer_group::CreateConsumerGroup;
use crate::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use crate::consumer_groups::get_consumer_group::GetConsumerGroup;
use crate::consumer_groups::get_consumer_groups::GetConsumerGroups;
use crate::consumer_groups::join_consumer_group::JoinConsumerGroup;
use crate::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use crate::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use crate::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::messages::poll_messages::{PollMessages, PollingKind};
use crate::messages::send_messages::{Partitioning, PartitioningKind, SendMessages};
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::consumer_offset_info::ConsumerOffsetInfo;
use crate::models::messages::{Message, PolledMessages};
use crate::models::stats::Stats;
use crate::models::stream::{Stream, StreamDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::partitioner::Partitioner;
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::streams::update_stream::UpdateStream;
use crate::system::get_client::GetClient;
use crate::system::get_clients::GetClients;
use crate::system::get_me::GetMe;
use crate::system::get_stats::GetStats;
use crate::system::ping::Ping;
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use crate::topics::update_topic::UpdateTopic;
use crate::users::login_user::LoginUser;
use crate::utils::crypto::Encryptor;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, info};

#[derive(Debug)]
pub struct IggyClient {
    client: Arc<RwLock<Box<dyn Client>>>,
    config: IggyClientConfig,
    send_messages_batch: Arc<Mutex<SendMessagesBatch>>,
    partitioner: Option<Box<dyn Partitioner>>,
    encryptor: Option<Box<dyn Encryptor>>,
}

#[derive(Debug)]
struct SendMessagesBatch {
    pub commands: VecDeque<SendMessages>,
}

#[derive(Debug, Default)]
pub struct IggyClientConfig {
    pub send_messages: SendMessagesConfig,
    pub poll_messages: PollMessagesConfig,
}

#[derive(Debug)]
pub struct SendMessagesConfig {
    pub enabled: bool,
    pub interval: u64,
    pub max_messages: u32,
}

#[derive(Debug, Copy, Clone)]
pub struct PollMessagesConfig {
    pub interval: u64,
    pub store_offset_kind: StoreOffsetKind,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum StoreOffsetKind {
    Never,
    WhenMessagesAreReceived,
    WhenMessagesAreProcessed,
    AfterProcessingEachMessage,
}

impl Default for SendMessagesConfig {
    fn default() -> Self {
        SendMessagesConfig {
            enabled: false,
            interval: 100,
            max_messages: 1000,
        }
    }
}

impl Default for PollMessagesConfig {
    fn default() -> Self {
        PollMessagesConfig {
            interval: 100,
            store_offset_kind: StoreOffsetKind::WhenMessagesAreProcessed,
        }
    }
}

impl IggyClient {
    pub fn new(
        client: Box<dyn Client>,
        config: IggyClientConfig,
        partitioner: Option<Box<dyn Partitioner>>,
        encryptor: Option<Box<dyn Encryptor>>,
    ) -> Self {
        if partitioner.is_some() {
            info!("Partitioner is enabled.");
        }
        if encryptor.is_some() {
            info!("Client-side encryption is enabled.");
        }

        let client = Arc::new(RwLock::new(client));
        let send_messages_batch = Arc::new(Mutex::new(SendMessagesBatch {
            commands: VecDeque::new(),
        }));
        if config.send_messages.enabled && config.send_messages.interval > 0 {
            info!("Messages will be sent in background.");
            Self::send_messages_in_background(
                config.send_messages.interval,
                config.send_messages.max_messages,
                client.clone(),
                send_messages_batch.clone(),
            );
        }

        IggyClient {
            client,
            config,
            send_messages_batch,
            partitioner,
            encryptor,
        }
    }

    pub fn start_polling_messages<F>(
        &self,
        mut poll_messages: PollMessages,
        on_message: F,
        config_override: Option<PollMessagesConfig>,
    ) -> JoinHandle<()>
    where
        F: Fn(Message) + Send + Sync + 'static,
    {
        let client = self.client.clone();
        let config = config_override.unwrap_or(self.config.poll_messages);
        let interval = Duration::from_millis(config.interval);
        let mut store_offset_after_processing_each_message = false;
        let mut store_offset_when_messages_are_processed = false;
        match config.store_offset_kind {
            StoreOffsetKind::Never => {
                poll_messages.auto_commit = false;
            }
            StoreOffsetKind::WhenMessagesAreReceived => {
                poll_messages.auto_commit = true;
            }
            StoreOffsetKind::WhenMessagesAreProcessed => {
                poll_messages.auto_commit = false;
                store_offset_when_messages_are_processed = true;
            }
            StoreOffsetKind::AfterProcessingEachMessage => {
                poll_messages.auto_commit = false;
                store_offset_after_processing_each_message = true;
            }
        }

        tokio::spawn(async move {
            loop {
                sleep(interval).await;
                let client = client.read().await;
                let polled_messages = client.poll_messages(&poll_messages).await;
                if let Err(error) = polled_messages {
                    error!("There was an error while polling messages: {:?}", error);
                    continue;
                }

                let messages = polled_messages.unwrap().messages;
                if messages.is_empty() {
                    continue;
                }

                let mut current_offset = 0;
                for message in messages {
                    current_offset = message.offset;
                    on_message(message);
                    if store_offset_after_processing_each_message {
                        Self::store_offset(client.as_ref(), &poll_messages, current_offset).await;
                    }
                }

                if store_offset_when_messages_are_processed {
                    Self::store_offset(client.as_ref(), &poll_messages, current_offset).await;
                }

                if poll_messages.strategy.kind == PollingKind::Offset {
                    poll_messages.strategy.value = current_offset + 1;
                }
            }
        })
    }

    pub async fn send_messages_using_partitioner(
        &self,
        command: &mut SendMessages,
        partitioner: &dyn Partitioner,
    ) -> Result<(), Error> {
        let partition_id = partitioner.calculate_partition_id(
            &command.stream_id,
            &command.topic_id,
            &command.partitioning,
            &command.messages,
        )?;
        command.partitioning = Partitioning::partition_id(partition_id);
        self.send_messages(command).await
    }

    async fn store_offset(client: &dyn Client, poll_messages: &PollMessages, offset: u64) {
        let result = client
            .store_consumer_offset(&StoreConsumerOffset {
                consumer: Consumer::from_consumer(&poll_messages.consumer),
                stream_id: Identifier::from_identifier(&poll_messages.stream_id),
                topic_id: Identifier::from_identifier(&poll_messages.topic_id),
                partition_id: poll_messages.partition_id,
                offset,
            })
            .await;
        if let Err(error) = result {
            error!("There was an error while storing offset: {:?}", error);
        }
    }

    fn send_messages_in_background(
        interval: u64,
        max_messages: u32,
        client: Arc<RwLock<Box<dyn Client>>>,
        send_messages_batch: Arc<Mutex<SendMessagesBatch>>,
    ) {
        tokio::spawn(async move {
            let max_messages = max_messages as usize;
            let interval = Duration::from_millis(interval);
            loop {
                sleep(interval).await;
                let mut send_messages_batch = send_messages_batch.lock().await;
                if send_messages_batch.commands.is_empty() {
                    continue;
                }

                let mut initialized = false;
                let mut stream_id = Identifier::numeric(1).unwrap();
                let mut topic_id = Identifier::numeric(1).unwrap();
                let mut key = Partitioning::partition_id(1);
                let mut batch_messages = true;

                for send_messages in send_messages_batch.commands.iter() {
                    if !initialized {
                        if send_messages.partitioning.kind != PartitioningKind::PartitionId {
                            batch_messages = false;
                            break;
                        }

                        stream_id = Identifier::from_identifier(&send_messages.stream_id);
                        topic_id = Identifier::from_identifier(&send_messages.topic_id);
                        key.value = send_messages.partitioning.value.clone();
                        initialized = true;
                    }

                    // Batching the messages is only possible for the same stream, topic and partition.
                    if send_messages.stream_id != stream_id
                        || send_messages.topic_id != topic_id
                        || send_messages.partitioning.kind != PartitioningKind::PartitionId
                        || send_messages.partitioning.value != key.value
                    {
                        batch_messages = false;
                        break;
                    }
                }

                if !batch_messages {
                    for send_messages in send_messages_batch.commands.iter_mut() {
                        if let Err(error) = client.read().await.send_messages(send_messages).await {
                            error!("There was an error when sending the messages: {:?}", error);
                        }
                    }
                    send_messages_batch.commands.clear();
                    continue;
                }

                let mut batches = VecDeque::new();
                let mut messages = Vec::new();
                while let Some(send_messages) = send_messages_batch.commands.pop_front() {
                    messages.extend(send_messages.messages);
                    if messages.len() >= max_messages {
                        batches.push_back(messages);
                        messages = Vec::new();
                    }
                }

                if !messages.is_empty() {
                    batches.push_back(messages);
                }

                while let Some(messages) = batches.pop_front() {
                    let mut send_messages = SendMessages {
                        stream_id: Identifier::from_identifier(&stream_id),
                        topic_id: Identifier::from_identifier(&topic_id),
                        partitioning: Partitioning {
                            kind: PartitioningKind::PartitionId,
                            length: 4,
                            value: key.value.clone(),
                        },
                        messages,
                    };

                    if let Err(error) = client.read().await.send_messages(&mut send_messages).await
                    {
                        error!(
                            "There was an error when sending the messages batch: {:?}",
                            error
                        );

                        if !send_messages.messages.is_empty() {
                            batches.push_back(send_messages.messages);
                        }
                    }
                }

                send_messages_batch.commands.clear();
            }
        });
    }
}

#[async_trait]
impl UserClient for IggyClient {
    async fn login_user(&self, command: &LoginUser) -> Result<(), Error> {
        self.client.read().await.login_user(command).await
    }
}

#[async_trait]
impl Client for IggyClient {
    async fn connect(&mut self) -> Result<(), Error> {
        self.client.write().await.connect().await
    }

    async fn disconnect(&mut self) -> Result<(), Error> {
        self.client.write().await.disconnect().await
    }
}

#[async_trait]
impl SystemClient for IggyClient {
    async fn get_stats(&self, command: &GetStats) -> Result<Stats, Error> {
        self.client.read().await.get_stats(command).await
    }

    async fn get_me(&self, command: &GetMe) -> Result<ClientInfoDetails, Error> {
        self.client.read().await.get_me(command).await
    }

    async fn get_client(&self, command: &GetClient) -> Result<ClientInfoDetails, Error> {
        self.client.read().await.get_client(command).await
    }

    async fn get_clients(&self, command: &GetClients) -> Result<Vec<ClientInfo>, Error> {
        self.client.read().await.get_clients(command).await
    }

    async fn ping(&self, command: &Ping) -> Result<(), Error> {
        self.client.read().await.ping(command).await
    }
}

#[async_trait]
impl StreamClient for IggyClient {
    async fn get_stream(&self, command: &GetStream) -> Result<StreamDetails, Error> {
        self.client.read().await.get_stream(command).await
    }

    async fn get_streams(&self, command: &GetStreams) -> Result<Vec<Stream>, Error> {
        self.client.read().await.get_streams(command).await
    }

    async fn create_stream(&self, command: &CreateStream) -> Result<(), Error> {
        self.client.read().await.create_stream(command).await
    }

    async fn update_stream(&self, command: &UpdateStream) -> Result<(), Error> {
        self.client.read().await.update_stream(command).await
    }

    async fn delete_stream(&self, command: &DeleteStream) -> Result<(), Error> {
        self.client.read().await.delete_stream(command).await
    }
}

#[async_trait]
impl TopicClient for IggyClient {
    async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, Error> {
        self.client.read().await.get_topic(command).await
    }

    async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, Error> {
        self.client.read().await.get_topics(command).await
    }

    async fn create_topic(&self, command: &CreateTopic) -> Result<(), Error> {
        self.client.read().await.create_topic(command).await
    }

    async fn update_topic(&self, command: &UpdateTopic) -> Result<(), Error> {
        self.client.read().await.update_topic(command).await
    }

    async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), Error> {
        self.client.read().await.delete_topic(command).await
    }
}

#[async_trait]
impl PartitionClient for IggyClient {
    async fn create_partitions(&self, command: &CreatePartitions) -> Result<(), Error> {
        self.client.read().await.create_partitions(command).await
    }

    async fn delete_partitions(&self, command: &DeletePartitions) -> Result<(), Error> {
        self.client.read().await.delete_partitions(command).await
    }
}

#[async_trait]
impl MessageClient for IggyClient {
    async fn poll_messages(&self, command: &PollMessages) -> Result<PolledMessages, Error> {
        let mut polled_messages = self.client.read().await.poll_messages(command).await?;
        if let Some(ref encryptor) = self.encryptor {
            for message in polled_messages.messages.iter_mut() {
                let payload = encryptor.decrypt(&message.payload)?;
                message.payload = Bytes::from(payload);
            }
        }
        Ok(polled_messages)
    }

    async fn send_messages(&self, command: &mut SendMessages) -> Result<(), Error> {
        if command.messages.is_empty() {
            return Ok(());
        }

        if let Some(partitioner) = &self.partitioner {
            let partition_id = partitioner.calculate_partition_id(
                &command.stream_id,
                &command.topic_id,
                &command.partitioning,
                &command.messages,
            )?;
            command.partitioning = Partitioning::partition_id(partition_id);
        }

        if let Some(encryptor) = &self.encryptor {
            for message in &mut command.messages {
                message.payload = Bytes::from(encryptor.encrypt(&message.payload)?);
                message.length = message.payload.len() as u32;
            }
        }

        if !self.config.send_messages.enabled || self.config.send_messages.interval == 0 {
            return self.client.read().await.send_messages(command).await;
        }

        let mut messages = Vec::with_capacity(command.messages.len());
        for message in &command.messages {
            let message = crate::messages::send_messages::Message {
                id: message.id,
                length: message.length,
                payload: message.payload.clone(),
                headers: message.headers.clone(),
            };
            messages.push(message);
        }
        let send_messages = SendMessages {
            stream_id: Identifier::from_identifier(&command.stream_id),
            topic_id: Identifier::from_identifier(&command.topic_id),
            partitioning: Partitioning::from_partitioning(&command.partitioning),
            messages,
        };

        let mut batch = self.send_messages_batch.lock().await;
        batch.commands.push_back(send_messages);
        Ok(())
    }
}

#[async_trait]
impl ConsumerOffsetClient for IggyClient {
    async fn store_consumer_offset(&self, command: &StoreConsumerOffset) -> Result<(), Error> {
        self.client
            .read()
            .await
            .store_consumer_offset(command)
            .await
    }

    async fn get_consumer_offset(
        &self,
        command: &GetConsumerOffset,
    ) -> Result<ConsumerOffsetInfo, Error> {
        self.client.read().await.get_consumer_offset(command).await
    }
}

#[async_trait]
impl ConsumerGroupClient for IggyClient {
    async fn get_consumer_group(
        &self,
        command: &GetConsumerGroup,
    ) -> Result<ConsumerGroupDetails, Error> {
        self.client.read().await.get_consumer_group(command).await
    }

    async fn get_consumer_groups(
        &self,
        command: &GetConsumerGroups,
    ) -> Result<Vec<ConsumerGroup>, Error> {
        self.client.read().await.get_consumer_groups(command).await
    }

    async fn create_consumer_group(&self, command: &CreateConsumerGroup) -> Result<(), Error> {
        self.client
            .read()
            .await
            .create_consumer_group(command)
            .await
    }

    async fn delete_consumer_group(&self, command: &DeleteConsumerGroup) -> Result<(), Error> {
        self.client
            .read()
            .await
            .delete_consumer_group(command)
            .await
    }

    async fn join_consumer_group(&self, command: &JoinConsumerGroup) -> Result<(), Error> {
        self.client.read().await.join_consumer_group(command).await
    }

    async fn leave_consumer_group(&self, command: &LeaveConsumerGroup) -> Result<(), Error> {
        self.client.read().await.leave_consumer_group(command).await
    }
}
