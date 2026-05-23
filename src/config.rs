use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub command: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub languages: HashMap<String, String>,
    pub servers: HashMap<String, ServerConfig>,
}

impl Config {
    pub fn default_config() -> Self {
        let mut languages = HashMap::new();
        languages.insert("rs".to_string(), "rust".to_string());
        languages.insert("py".to_string(), "python".to_string());
        languages.insert("go".to_string(), "go".to_string());
        languages.insert("md".to_string(), "markdown".to_string());

        let mut servers = HashMap::new();
        servers.insert(
            "rust".to_string(),
            ServerConfig {
                command: vec!["rust-analyzer".to_string()],
            },
        );
        servers.insert(
            "python".to_string(),
            ServerConfig {
                command: vec!["pyright-langserver".to_string(), "--stdio".to_string()],
            },
        );
        servers.insert(
            "go".to_string(),
            ServerConfig {
                command: vec!["gopls".to_string()],
            },
        );

        Config { languages, servers }
    }

    pub fn load() -> Self {
        let mut config_path = std::env::var("HOME")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/home/tau"));
        config_path.push(".config/lsp-broker/config.toml");

        if config_path.exists() {
            if let Ok(content) = std::fs::read_to_string(config_path) {
                if let Ok(config) = toml::from_str::<Config>(&content) {
                    return config;
                }
            }
        }
        Self::default_config()
    }
}
