use crate::exchanges::ExchangeName;
use crate::instances::{Instance, InstanceType};
use crate::utilities::Twilio;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Alert {
    pub alert_id: Uuid,
    pub instance_type: InstanceType,
    pub droplet: String,
    pub exchange_name: Option<ExchangeName>,
    pub timestamp: DateTime<Utc>,
    pub message: String,
}

impl Alert {
    pub fn new(instance: &Instance, message: &str) -> Self {
        Self {
            alert_id: Uuid::new_v4(),
            instance_type: instance.instance_type,
            droplet: instance.droplet.clone(),
            exchange_name: instance.exchange_name,
            timestamp: Utc::now(),
            message: message.to_string(),
        }
    }

    pub async fn send(&self, twilio: &Twilio) {
        twilio.send_sms(&self.message).await;
    }

    pub async fn insert(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let sql = r#"
            INSERT INTO alerts (
                alert_id, instance_type, droplet, exchange_name, timestamp, message)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#;
        sqlx::query(sql)
            .bind(&self.alert_id)
            .bind(&self.instance_type.as_str())
            .bind(&self.droplet)
            .bind(&self.exchange_name.map(|e| e.as_str()))
            .bind(&self.timestamp)
            .bind(&self.message)
            .execute(pool)
            .await?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use crate::inquisidor::Inquisidor;
    use crate::instances::select_instances;
    use crate::alerts::Alert;

    
    #[tokio::test]
    pub async fn insert_alert_works() {
        // Create ig to get db pool
        let ig = Inquisidor::new().await;
        // Get instances from db
        let instances = select_instances(&ig.ig_pool).await.expect("Failed to select instances.");
        for instance in instances.iter() {
            let message = "test alerts insert";
            let alert = Alert::new(instance, &message);
            alert.insert(&ig.ig_pool)
                .await
                .expect("Failed to insert message.");
        }
    }

    #[tokio::test]
    pub async fn send_alert_sends_sms() {
        // Create ig to get db pool
        let ig = Inquisidor::new().await;
        // Get instances from db
        let instances = select_instances(&ig.ig_pool).await.expect("Failed to select instances.");
        for instance in instances.iter() {
            let message = "test alerts send";
            let alert = Alert::new(instance, &message);
            alert.send(&ig.twilio)
                .await;
        }
    }
}