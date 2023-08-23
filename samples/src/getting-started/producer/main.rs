use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning, SendMessages};
use iggy::streams::create_stream::CreateStream;
use iggy::tcp::client::TcpClient;
use iggy::topics::create_topic::CreateTopic;
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

const STREAM_ID: u32 = 1;
const TOPIC_ID: u32 = 1;
const PARTITION_ID: u32 = 1;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let mut client = TcpClient::new("127.0.0.1:8090")?;
    client.connect().await?;
    init_system(&client).await;
    produce_messages(&client).await
}

async fn init_system(client: &dyn Client) {
    match client
        .create_stream(&CreateStream {
            stream_id: STREAM_ID,
            name: "sample-stream".to_string(),
        })
        .await
    {
        Ok(_) => info!("Stream was created."),
        Err(_) => warn!("Stream already exists and will not be created again."),
    }

    match client
        .create_topic(&CreateTopic {
            stream_id: Identifier::numeric(STREAM_ID).unwrap(),
            topic_id: TOPIC_ID,
            partitions_count: 1,
            name: "sample-topic".to_string(),
        })
        .await
    {
        Ok(_) => info!("Topic was created."),
        Err(_) => warn!("Topic already exists and will not be created again."),
    }
}

async fn produce_messages(client: &dyn Client) -> Result<(), Box<dyn Error>> {
    let mut total_sent = 0;
    let interval = Duration::from_millis(10000);
    info!(
        "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {} ms.",
        STREAM_ID,
        TOPIC_ID,
        PARTITION_ID,
        interval.as_millis()
    );

    let mut current_id = 0;
    let messages_per_batch = 1;
    loop {
        let mut messages = Vec::new();
        for _ in 0..messages_per_batch {
            current_id += 1;
            let payload = format!("message-{current_id}");
            let message = Message::from_str(&payload)?;
            messages.push(message);
        }
        client
            .send_messages(&mut SendMessages {
                stream_id: Identifier::numeric(STREAM_ID)?,
                topic_id: Identifier::numeric(TOPIC_ID)?,
                partitioning: Partitioning::partition_id(PARTITION_ID),
                messages,
            })
            .await?;

        total_sent += 1;
        info!("Sent {messages_per_batch} message(s) total: {total_sent}.");
        sleep(interval).await;
    }
}
