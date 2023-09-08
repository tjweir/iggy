mod common;

use crate::common::TestSetup;
use iggy::identifier::Identifier;
use streaming::systems::system::System;
use tokio::fs;

#[tokio::test]
async fn should_initialize_system_and_base_directories() {
    let setup = TestSetup::init().await;
    let mut system = System::new(setup.config.clone(), Some(setup.db.clone()));

    system.init().await.unwrap();

    let mut dir_entries = fs::read_dir(&setup.config.path).await.unwrap();
    let mut names = Vec::new();
    while let Some(entry) = dir_entries.next_entry().await.unwrap() {
        let metadata = entry.metadata().await.unwrap();
        assert!(metadata.is_dir());
        names.push(entry.file_name().into_string().unwrap());
    }

    assert_eq!(names.len(), 2);
    assert!(names.contains(&setup.config.stream.path));
    assert!(names.contains(&setup.config.database.path));
}

#[tokio::test]
async fn should_create_and_persist_stream() {
    let setup = TestSetup::init().await;
    let mut system = System::new(setup.config.clone(), Some(setup.db.clone()));
    let stream_id = 1;
    let stream_name = "test";
    system.init().await.unwrap();

    system.create_stream(stream_id, stream_name).await.unwrap();

    assert_persisted_stream(&system.streams_path, stream_id).await;
}

#[tokio::test]
async fn should_delete_persisted_stream() {
    let setup = TestSetup::init().await;
    let mut system = System::new(setup.config.clone(), Some(setup.db.clone()));
    let stream_id = 1;
    let stream_name = "test";
    system.init().await.unwrap();
    system.create_stream(stream_id, stream_name).await.unwrap();
    assert_persisted_stream(&system.streams_path, stream_id).await;
    let stream_path = system
        .get_stream(&Identifier::numeric(stream_id).unwrap())
        .unwrap()
        .path
        .clone();

    system
        .delete_stream(&Identifier::numeric(1).unwrap())
        .await
        .unwrap();

    assert!(fs::metadata(stream_path).await.is_err());
}

async fn assert_persisted_stream(streams_path: &str, stream_id: u32) {
    let streams_metadata = fs::metadata(streams_path).await.unwrap();
    assert!(streams_metadata.is_dir());
    let stream_path = format!("{}/{}", streams_path, stream_id);
    let stream_metadata = fs::metadata(stream_path).await.unwrap();
    assert!(stream_metadata.is_dir());
}
