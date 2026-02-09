use east_guard::StartUp;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    StartUp.run().await
}
