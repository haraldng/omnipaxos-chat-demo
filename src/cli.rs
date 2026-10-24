use std::{sync::Arc, time::Duration};

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::chat::{
    llm::{LlmEngine, MockEngine, OllamaEngine},
    NodeOptions,
};

#[derive(Parser, Debug)]
#[command(
    name = "omni_chat",
    about = "HA single-chat LLM on an OmniPaxos replicated log"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run N replicas in this process (HTTP on base-port+id; `POST /admin/crash` simulates a crash).
    Cluster {
        #[arg(long, default_value_t = 3)]
        nodes: u64,
        #[command(flatten)]
        common: Common,
    },
    /// Run one replica as its own OS process, talking TCP to its peers.
    Node {
        /// This replica's id (1..=nodes).
        #[arg(long)]
        id: u64,
        #[arg(long, default_value_t = 3)]
        nodes: u64,
        /// Peer transport listens on peer-base-port+id (default: base-port+1000).
        #[arg(long)]
        peer_base_port: Option<u16>,
        #[command(flatten)]
        common: Common,
    },
    /// Launch N replica processes and the browser UI, which can kill and
    /// restart them (restarted nodes come back empty and catch up).
    Demo {
        #[arg(long, default_value_t = 3)]
        nodes: u64,
        /// Each node's output goes to <log-dir>/node<id>.log.
        #[arg(long, default_value = "target/demo-logs")]
        log_dir: std::path::PathBuf,
        #[command(flatten)]
        common: Common,
    },
    /// Serve the browser UI (not a replica) for nodes on base-port+1..=base-port+nodes.
    Ui {
        #[arg(long, default_value_t = 8080)]
        port: u16,
        #[arg(long, default_value_t = 3)]
        nodes: u64,
        #[arg(long, default_value_t = 8080)]
        base_port: u16,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ModelKind {
    Mock,
    Ollama,
}

#[derive(Args, Debug, Clone)]
pub struct Common {
    /// HTTP API listens on base-port+id.
    #[arg(long, default_value_t = 8080)]
    pub base_port: u16,
    #[arg(long, value_enum, default_value_t = ModelKind::Mock)]
    pub model: ModelKind,
    /// Ollama model tag.
    #[arg(long, default_value = "llama3.2:1b")]
    pub model_name: String,
    #[arg(long, default_value = "http://127.0.0.1:11434")]
    pub ollama_url: String,
    /// Artificial mock latency, to make "kill the leader mid-reply" easy.
    #[arg(long, default_value_t = 0)]
    pub mock_delay_ms: u64,
    #[arg(long, default_value = "You are a concise, helpful assistant.")]
    pub system_prompt: String,
    #[arg(long, default_value_t = 50)]
    pub max_context_messages: usize,
    #[arg(long, default_value_t = 10)]
    pub tick_ms: u64,
    /// Election timeout in ticks.
    #[arg(long, default_value_t = 10)]
    pub election_ticks: u64,
}

impl Common {
    pub fn engine(&self) -> Arc<dyn LlmEngine> {
        match self.model {
            ModelKind::Mock => Arc::new(MockEngine::with_delay(Duration::from_millis(
                self.mock_delay_ms,
            ))),
            ModelKind::Ollama => Arc::new(OllamaEngine::new(&self.ollama_url, &self.model_name)),
        }
    }

    /// The same settings as command-line arguments, for launching `node` processes.
    pub fn to_args(&self) -> Vec<String> {
        let model = self
            .model
            .to_possible_value()
            .expect("model kind has a name")
            .get_name()
            .to_string();
        [
            ("--base-port", self.base_port.to_string()),
            ("--model", model),
            ("--model-name", self.model_name.clone()),
            ("--ollama-url", self.ollama_url.clone()),
            ("--mock-delay-ms", self.mock_delay_ms.to_string()),
            ("--system-prompt", self.system_prompt.clone()),
            (
                "--max-context-messages",
                self.max_context_messages.to_string(),
            ),
            ("--tick-ms", self.tick_ms.to_string()),
            ("--election-ticks", self.election_ticks.to_string()),
        ]
        .into_iter()
        .flat_map(|(k, v)| [k.to_string(), v])
        .collect()
    }

    pub fn apply(&self, o: &mut NodeOptions) {
        o.system_prompt = self.system_prompt.clone();
        o.max_context_messages = self.max_context_messages;
        o.log.tick_period = Duration::from_millis(self.tick_ms);
        o.log.election_tick_timeout = self.election_ticks;
    }
}
