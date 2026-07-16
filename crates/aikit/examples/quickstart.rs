use aikit::Agent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let answer = Agent::new()
        .generate_text("Say hello in Turkish", "mock-1", 128)
        .await?;
    println!("{}", answer.text);
    Ok(())
}
