use crate::partitions::partition::Partition;
use shared::error::Error;

impl Partition {
    pub async fn load(&mut self) -> Result<(), Error> {
        let storage = self.storage.clone();
        storage.partition.load(self).await
    }

    pub async fn persist(&self) -> Result<(), Error> {
        self.storage.partition.save(self).await
    }

    pub async fn delete(&self) -> Result<(), Error> {
        self.storage.partition.delete(self).await
    }
}
