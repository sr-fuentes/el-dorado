use chrono::Duration;

use crate::alerts::Alert;
use crate::candles::TimeFrame;
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
            let mut alert_message = String::new();
            match instance.instance_status {
                InstanceStatus::New | InstanceStatus::Paused | InstanceStatus::Terminated => {
                    continue
                }
                InstanceStatus::Active => {
                    if instance.time_since_last_update() > Duration::minutes(3) {
                        // Add to alerts
                        println!(
                            "Greater than 3 minutes since last update for {:?} {:?} {:?} in Active status.",
                            instance.droplet, instance.exchange_name, instance.instance_type
                        );
                        // If there has been at least 1 hour since last message sent or no last
                        // message, add to instance message to be send
                        match instance.time_since_last_message() {
                            Some(d) => {
                                if d > Duration::hours(1) {
                                    let new_message = "Inactive Mita. ";
                                    alert_message.push_str(new_message);
                                }
                            }
                            None => {
                                let new_message = "Inactive Mita. ";
                                alert_message.push_str(new_message);
                            }
                        }
                    };
                    let instance_markets_fails = instance
                        .inactive_markets(&self.ig_pool, TimeFrame::T15.as_dur())
                        .await;
                    if !instance_markets_fails.is_empty() {
                        // Add to alerts
                        println!(
                            "Bad markets for instance {:?} {:?} {:?}: {:?}",
                            instance.droplet,
                            instance.exchange_name,
                            instance.instance_type,
                            instance_markets_fails
                        );
                        // Build message alert if greater than  1 hour since last message
                        match instance.time_since_last_message() {
                            Some(d) => {
                                if d > Duration::hours(1) {
                                    for inf in instance_markets_fails.iter() {
                                        let new_message =
                                            format!("{:?} {:?}. ", inf.0.market_name, inf.1);
                                        alert_message.push_str(&new_message);
                                    }
                                }
                            }
                            None => {
                                for inf in instance_markets_fails.iter() {
                                    let new_message =
                                        format!("{:?} {:?}. ", inf.0.market_name, inf.1);
                                    alert_message.push_str(&new_message);
                                }
                            }
                        }
                    };
                }
                InstanceStatus::Sync => {
                    if instance.time_since_last_update() > Duration::minutes(15) {
                        // Add to alerts
                        println!(
                            "Greater than 15 minutes since last update for {:?} {:?} {:?} in Sync status.",
                            instance.droplet, instance.exchange_name, instance.instance_type
                        );
                        // If there has been at least 1 hour since last message sent or no last
                        // message, add to instance message to be send
                        match instance.time_since_last_message() {
                            Some(d) => {
                                if d > Duration::hours(1) {
                                    let new_message = "Delayed Mita Sync. ";
                                    alert_message.push_str(new_message);
                                }
                            }
                            None => {
                                let new_message = "Delayed Mita Sync. ";
                                alert_message.push_str(new_message);
                            }
                        }
                    };
                }
                InstanceStatus::Restart => {
                    if instance.time_since_last_update() > Duration::minutes(1) {
                        // Add to alerts
                        println!(
                            "Greater than 1 minutes since last update for {:?} {:?} {:?} in Restart status.",
                            instance.droplet, instance.exchange_name, instance.instance_type
                        );
                        // If there has been at least 1 hour since last message sent or no last
                        // message, add to instance message to be send
                        match instance.time_since_last_message() {
                            Some(d) => {
                                if d > Duration::hours(1) {
                                    let new_message = "Delayed Mita Restart. ";
                                    alert_message.push_str(new_message);
                                }
                            }
                            None => {
                                let new_message = "Delayed Mita Restart. ";
                                alert_message.push_str(new_message);
                            }
                        }
                    };
                }
            }
            // If there is an alert message -> send message and update instance last sent message
            if !alert_message.is_empty() {
                let alert = Alert::new(instance, &alert_message);
                alert
                    .insert(&self.ig_pool)
                    .await
                    .expect("Failed to insert message.");
                alert.send(&self.twilio).await;
                instance
                    .update_last_message_ts(&self.ig_pool)
                    .await
                    .expect("Failed to update last message ts.");
            }
        }
    }
}
