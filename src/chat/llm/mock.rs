use std::time::Duration;

use async_trait::async_trait;

use super::{LlmEngine, OnToken};
use crate::chat::{ChatMessage, Role};

/// Deterministic engine for tests and GPU-less demos. The reply is a pure
/// function of the context, and it echoes how much history it saw, which
/// proves context survived a failover.
#[derive(Clone, Debug, Default)]
pub struct MockEngine {
    /// Artificial latency, so a demo can kill the leader mid-generation.
    pub delay: Duration,
}

impl MockEngine {
    pub fn with_delay(delay: Duration) -> Self {
        Self { delay }
    }
}

#[async_trait]
impl LlmEngine for MockEngine {
    fn model(&self) -> String {
        "mock".into()
    }

    async fn complete(
        &self,
        _system: &str,
        messages: &[ChatMessage],
        on_token: OnToken<'_>,
    ) -> anyhow::Result<String> {
        let users: Vec<&str> = messages
            .iter()
            .filter(|m| m.role == Role::User)
            .map(|m| m.content.as_str())
            .collect();
        let last = users.last().copied().unwrap_or("");
        let mut reply = format!(
            "[mock] turn {}: you said \"{last}\" (context: {} messages)",
            users.len(),
            messages.len()
        );
        if users.len() > 1 {
            reply.push_str(&format!("; you first said \"{}\"", users[0]));
        }
        if self.delay.is_zero() {
            on_token(&reply);
            return Ok(reply);
        }
        // Stream word by word, spreading `delay` over the whole reply.
        let words: Vec<&str> = reply.split_inclusive(' ').collect();
        let per_word = self.delay / words.len() as u32;
        for w in &words {
            tokio::time::sleep(per_word).await;
            on_token(w);
        }
        Ok(reply)
    }
}
