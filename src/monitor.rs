use chrono::Duration;

use crate::inquisidor::Inquisidor;
use crate::instances::{select_instances, InstanceStatus};

impl Inquisidor {
    pub async fn monitor(&self) {
        // Load all instances and run a qc check on all Active or Sync instnaces
        // Check that Active mita instances have been updated within 3 minutes
        // Check that Sync mita instances have been updated within 10 minutes
        // Check that Active mita instances have all markets up to date
        // Any violations are added to alert table
        let instances = select_instances(&self.ig_pool)
            .await
            .expect("Failed to select instances.");
        for instance in instances.iter() {
            match instance.instance_status {
                InstanceStatus::New | InstanceStatus::Paused | InstanceStatus::Terminated => {
                    continue
                }
                InstanceStatus::Active => {
                    if instance.time_since_last_update() > Duration::minutes(3) {
                        // Add to alerts
                    };
                    if !instance.inactive_markets(&self.ig_pool).await.is_empty() {
                        // Add to alerts
                    };
                }
                InstanceStatus::Sync => {}
                InstanceStatus::Restart => {}
            }
        }
    }
}
