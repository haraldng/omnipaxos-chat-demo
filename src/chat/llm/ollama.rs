use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{LlmEngine, OnToken};
use crate::chat::{ChatMessage, Role};

/// Talks to a local Ollama server (`ollama serve`) over its `/api/chat` endpoint.
pub struct OllamaEngine {
    client: reqwest::Client,
    base_url: String,
    model: String,
}

impl OllamaEngine {
    pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(300))
                .build()
                .expect("reqwest client"),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            model: model.into(),
        }
    }
}

#[derive(Serialize)]
struct Msg<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: Vec<Msg<'a>>,
    stream: bool,
}

/// One line of Ollama's streaming (NDJSON) `/api/chat` response.
#[derive(Deserialize)]
struct StreamLine {
    message: Option<RespMsg>,
    #[serde(default)]
    done: bool,
    error: Option<String>,
}

#[derive(Deserialize)]
struct RespMsg {
    content: String,
}

#[async_trait]
impl LlmEngine for OllamaEngine {
    fn model(&self) -> String {
        format!("ollama:{}", self.model)
    }

    async fn complete(
        &self,
        system: &str,
        messages: &[ChatMessage],
        on_token: OnToken<'_>,
    ) -> anyhow::Result<String> {
        let mut msgs = Vec::with_capacity(messages.len() + 1);
        if !system.is_empty() {
            msgs.push(Msg {
                role: "system",
                content: system,
            });
        }
        msgs.extend(messages.iter().map(|m| Msg {
            role: match m.role {
                Role::User => "user",
                Role::Assistant => "assistant",
            },
            content: &m.content,
        }));
        let req = ChatRequest {
            model: &self.model,
            messages: msgs,
            stream: true,
        };
        let mut resp = self
            .client
            .post(format!("{}/api/chat", self.base_url))
            .json(&req)
            .send()
            .await
            .context("ollama request failed (is `ollama serve` running?)")?
            .error_for_status()
            .context("ollama returned an error (is the model pulled?)")?;

        // Dropping this future (e.g. on lost leadership) closes the connection,
        // which makes Ollama stop generating.
        let mut buf = Vec::new();
        let mut out = String::new();
        while let Some(chunk) = resp.chunk().await.context("ollama stream failed")? {
            buf.extend_from_slice(&chunk);
            while let Some(nl) = buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = buf.drain(..=nl).collect();
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                let line: StreamLine =
                    serde_json::from_slice(&line).context("unexpected ollama response")?;
                if let Some(e) = line.error {
                    anyhow::bail!("ollama: {e}");
                }
                if let Some(m) = line.message {
                    on_token(&m.content);
                    out.push_str(&m.content);
                }
                if line.done {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    async fn warm_up(&self) {
        // An empty-prompt generate loads the model into memory.
        let body = serde_json::json!({ "model": self.model, "prompt": "", "keep_alive": "10m" });
        let _ = self
            .client
            .post(format!("{}/api/generate", self.base_url))
            .json(&body)
            .send()
            .await;
    }
}
