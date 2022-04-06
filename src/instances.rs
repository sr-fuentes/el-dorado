use std::convert::{TryFrom, TryInto};
use crate::exchanges::ExchangeName;
use chrono::{DateTime, Utc};



pub struct Instance {
    pub r#type: InstanceType,
    pub name: String,
    pub exchange: ExchangeName,
    pub status: InstanceStatus,
    pub restart: bool,
    pub last_retart: DateTime<Utc>,
    pub restart_count: i32,
    pub n_markets: i32,
    pub last_update: DateTime<Utc>,
}


#[derive(Debug, Clone, Copy, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum InstanceType {
    Mita,
    Ig,
}

impl InstanceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceType::Mita => "mita",
            InstanceType::Ig => "ig",
        }
    }
}

impl TryFrom<String> for InstanceType {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "mita" => Ok(Self::Mita),
            "ig" => Ok(Self::Ig),
            other => Err(format!("{} is not a supported instance.", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum InstanceStatus {
    Sync,
    Active,
    Restart,
    Paused,
    Terminated,
}

impl InstanceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceStatus::Sync => "sync",
            InstanceStatus::Active => "active",
            InstanceStatus::Restart => "restart",
            InstanceStatus::Paused => "paused",
            InstanceStatus::Terminated => "terminated",
        }
    }
}

impl TryFrom<String> for InstanceStatus {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "sync" => Ok(Self::Sync),
            "active" => Ok(Self::Active),
            "restart" => Ok(Self::Restart),
            "paused" => Ok(Self::Paused),
            "terminated" => Ok(Self::Terminated),
            other => Err(format!("{} is not a supported instance status.", other)),
        }
    }
}
