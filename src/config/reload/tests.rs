//! Tests for reload classification, over two configs that differ on purpose.

use serde_json::json;

use super::{ReloadTier, classify};
use crate::config::schema::Config;
use crate::config::validate::validate_config;

fn minimal() -> Config {
    validate_config(&json!({
        "chat": {
            "token": "a.token.value",
            "channelId": "111222333444555666",
            "allowedUserIds": ["777888999000111222"],
        },
        "agent": {
            "provider": "anthropic",
            "providers": {
                "anthropic": { "credentialName": "ANTHROPIC_API_KEY", "credential": "secret-value" },
            },
        },
        "projectRoot": "/tmp/errand/projects",
        "stateDir": "/tmp/errand/state",
    }))
    .expect("a minimal file resolves")
}

fn tiered(config: &Config, path: &str) -> Option<ReloadTier> {
    classify(&minimal(), config)
        .into_iter()
        .find(|(name, _)| name == path)
        .map(|(_, tier)| tier)
}

#[test]
fn an_unchanged_file_reports_nothing() {
    let config = minimal();
    assert!(classify(&config, &config).is_empty());
}

#[test]
fn storage_budgets_are_live_while_scratch_sizes_wait_for_launch() {
    let mut config = minimal();
    config.sandbox.disk = "10g".to_owned();
    config.sandbox.disk_check_ms = 5_000;
    config.sandbox.tmp_size = "1g".to_owned();
    config.sandbox.file_max = "2g".to_owned();

    assert_eq!(tiered(&config, "sandbox.disk"), Some(ReloadTier::Live));
    assert_eq!(
        tiered(&config, "sandbox.diskCheckMs"),
        Some(ReloadTier::Live)
    );
    assert_eq!(
        tiered(&config, "sandbox.tmpSize"),
        Some(ReloadTier::NextLaunch)
    );
    assert_eq!(
        tiered(&config, "sandbox.fileMax"),
        Some(ReloadTier::NextLaunch)
    );
}

#[test]
fn backends_brokers_and_directories_need_a_restart() {
    let mut config = minimal();
    config.sandbox.backend = crate::config::schema::SandboxBackend::Podman;
    config.sandbox.egress_ports = vec![80, 443];
    config.project_root = "/srv/elsewhere".to_owned();
    config.timeouts.idle_ms = 60_000;

    assert_eq!(
        tiered(&config, "sandbox.backend"),
        Some(ReloadTier::Restart)
    );
    assert_eq!(
        tiered(&config, "sandbox.egressPorts"),
        Some(ReloadTier::Restart)
    );
    assert_eq!(tiered(&config, "projectRoot"), Some(ReloadTier::Restart));
    assert_eq!(tiered(&config, "timeouts.idleMs"), Some(ReloadTier::Live));
}
