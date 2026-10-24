//! Inference backends. Only the current leader calls [`LlmEngine::complete`].

mod mock;
mod ollama;

pub use mock::MockEngine;
pub use ollama::OllamaEngine;

use async_trait::async_trait;

use crate::chat::ChatMessage;

/// Receives reply text as it is generated. Tokens are shown to clients but
/// never written to the log; only the final reply is.
pub type OnToken<'a> = &'a (dyn Fn(&str) + Send + Sync);

#[async_trait]
pub trait LlmEngine: Send + Sync {
    /// Model name recorded on each `Assistant` entry.
    fn model(&self) -> String;

    /// Produce the reply to the last message in `messages`, calling `on_token`
    /// with each piece as it is generated. Returns the full reply.
    async fn complete(
        &self,
        system_prompt: &str,
        messages: &[ChatMessage],
        on_token: OnToken<'_>,
    ) -> anyhow::Result<String>;

    /// Called when this node becomes leader, so the first reply after failover
    /// is not a cold start. Default: no-op.
    async fn warm_up(&self) {}
}
