use crate::eldorado::{ElDorado, ElDoradoError};

impl ElDorado {
    // Run Inquisidor instance.
    // Fill each active market
    // Archive candles and trades if needed
    pub async fn inquisidor(&mut self) -> Result<(), ElDoradoError> {
        // Set restart value to false, error handling must explicitly set back to true
        self.instance.restart = false;
        loop {
            // Fill all active markets
            self.fill(&None, true).await?;
            // Archive all markets
            self.archive(&None).await?;
            // Sleep for 1 hour
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
        }
    }
}
