//! A single server, no replication: the "before OmniPaxos" setup. It chats
//! fine, but a crash makes it unreachable and a restart loses the conversation.

mod common;

use std::sync::Arc;

use common::*;
use omni_chat::{
    chat::llm::{LlmEngine, MockEngine},
    server::LocalCluster,
};

async fn single() -> LocalCluster {
    let engine: Arc<dyn LlmEngine> = Arc::new(MockEngine::default());
    LocalCluster::start(1, |_| {}, |_| engine.clone())
        .await
        .expect("single server starts")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_server_chats_but_forgets_on_restart() {
    let c = single().await;
    assert_eq!(c.wait_for_leader(T).await.unwrap(), 1, "it serves on its own");

    chat(&c, 1, "my name is Ada").await;
    let (_, reply) = chat(&c, 1, "what is my name?").await;
    assert_eq!(reply.node, 1);
    assert!(reply.content.contains("you first said \"my name is Ada\""));
    assert_eq!(transcript(&c, 1).len(), 4);

    // Crashed: nothing else can serve the chat.
    c.crash(1);
    assert!(c.alive().is_empty());

    // Restarted: up again, but the conversation is gone.
    c.restart(1).await.unwrap();
    assert!(transcript(&c, 1).is_empty());
    let (_, reply) = chat(&c, 1, "what is my name?").await;
    assert!(reply.content.contains("turn 1"), "{}", reply.content);
}
