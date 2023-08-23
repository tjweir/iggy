mod messages_generator;

use crate::messages_generator::MessagesGenerator;
use anyhow::Result;
use clap::Parser;
use iggy::client::{Client, MessageClient};
use iggy::client_provider;
use iggy::client_provider::ClientProviderConfig;
use iggy::clients::client::{IggyClient, IggyClientConfig};
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use samples::shared::args::Args;
use samples::shared::messages::SerializableMessage;
use samples::shared::system;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    
    tracing_subscriber::fmt::init();

    info!(
        "Advanced producer has started, selected transport: {}",
        args.transport
    );

    let client_provider_config: Arc<ClientProviderConfig> =
        Arc::new(ClientProviderConfig::from_args(args.to_sdk_args())?);
    let client: Box<dyn Client> = client_provider::get_client(client_provider_config).await?;
    let client: IggyClient = IggyClient::new(client, IggyClientConfig::default(), None, None);
    system::init_by_producer(&args, &client).await?;
    produce_messages(&args, &client).await
}

async fn produce_messages(args: &Args, client: &IggyClient) -> Result<(), Box<dyn Error>> {
    info!(
        "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {} ms.",
        args.stream_id, args.topic_id, args.partition_id, args.interval
    );

    let mut interval: tokio::time::Interval =
        tokio::time::interval(std::time::Duration::from_millis(args.interval));

    let mut message_generator: MessagesGenerator = MessagesGenerator::new();

    loop {
        let mut messages: Vec<Message> = Vec::new();

        let mut serializable_messages: Vec<Box<dyn SerializableMessage>> = Vec::new();

        for _ in 0..args.messages_per_batch {
            let serializable_message: Box<dyn SerializableMessage> = message_generator.generate();

            // You can send the different message types to the same partition, or stick to the single type.
            let json_envelope: String = serializable_message.to_json_envelope();

            let message: Message = Message::from_str(&json_envelope)?;
            messages.push(message);

            // This is used for the logging purposes only.
            serializable_messages.push(serializable_message);
        }

        client
            .send_messages(&mut SendMessages {
                stream_id: Identifier::numeric(args.stream_id)?,
                topic_id: Identifier::numeric(args.topic_id)?,
                partitioning: Partitioning::partition_id(args.partition_id),
                messages,
            })
            .await?;

        info!("Sent messages: {:#?}", serializable_messages);

        interval.tick().await;
    }
}
