mod app_tracing;
pub mod config;
pub mod error;

#[tokio::main]
async fn main() {
    match run().await {
        Ok(_) => {
            println!("Done")
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }
}

async fn run() -> Result<(), error::SnappError> {
    let c = config::extract_config()?;
    app_tracing::enable_tracing(c);

    



    Ok(())
}
