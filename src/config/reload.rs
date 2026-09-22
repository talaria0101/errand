//! Reloading the configuration without restarting the daemon.
//!
//! A reload re-reads the file the daemon started from and swaps the running
//! configuration for the validated one. Not every change can reach a running
//! session, so every changed leaf is classified by when it takes effect.
//! Anything the classifier does not recognise is a restart change: failing
//! safe beats applying half of a setting somebody believed was live.

use std::collections::BTreeMap;

use crate::config::load::{Environment, config_path, file_exists, load_config};
use crate::config::schema::{Config, ConfigError};

/// When a changed setting takes effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadTier {
    /// Read on every use, so running sessions pick it up within one check.
    /// Storage budgets live here: the disk watcher measures against the
    /// current number rather than the one from startup.
    Live,
    /// Written into each fresh sandbox policy, so new sessions and sandbox
    /// restarts pick it up while running sandboxes keep theirs. Scratch and
    /// single file sizes live here: a tmpfs size is fixed at mount and an
    /// rlimit at exec, so neither can move under a running agent.
    NextLaunch,
    /// Baked into objects built once at startup, so only a daemon restart
    /// applies it. Backends, the broker, directories, and admission caps live
    /// here.
    Restart,
}

impl ReloadTier {
    /// The tier as it is logged and documented.
    pub fn as_str(self) -> &'static str {
        match self {
            ReloadTier::Live => "live",
            ReloadTier::NextLaunch => "next launch",
            ReloadTier::Restart => "restart",
        }
    }
}

/// Re-reads the configuration file the daemon started from.
///
/// Resolves the path the same way startup does, from the process environment,
/// so a reload reads the file an operator edited rather than a remembered
/// path that no longer names it.
pub fn reload_config() -> Result<Config, ConfigError> {
    let env: Environment = std::env::vars().collect::<BTreeMap<_, _>>();
    let path = config_path(&env, file_exists);
    load_config(
        &path,
        |read| std::fs::read_to_string(read),
        &env,
        file_exists,
    )
}

/// Names every changed leaf and the tier at which it takes effect.
///
/// Order follows the configuration layout rather than discovery order, so two
/// reloads that change the same things report them the same way. Unchanged
/// leaves are absent. An empty report means nothing changed.
#[expect(
    clippy::too_many_lines,
    reason = "one comparison per configuration leaf, in layout order"
)]
pub fn classify(old: &Config, new: &Config) -> Vec<(String, ReloadTier)> {
    let mut changed = Vec::new();
    let mut flag = |path: &str, tier: ReloadTier, same: bool| {
        if !same {
            changed.push((path.to_owned(), tier));
        }
    };

    flag(
        "chat.token",
        ReloadTier::Restart,
        old.chat.token == new.chat.token,
    );
    flag(
        "chat.channelId",
        ReloadTier::Restart,
        old.chat.channel_id == new.chat.channel_id,
    );
    flag(
        "chat.allowedUserIds",
        ReloadTier::Live,
        old.chat.allowed_user_ids == new.chat.allowed_user_ids,
    );
    flag(
        "chat.blockedUserIds",
        ReloadTier::Live,
        old.chat.blocked_user_ids == new.chat.blocked_user_ids,
    );
    flag(
        "chat.operatorUserIds",
        ReloadTier::Live,
        old.chat.operator_user_ids == new.chat.operator_user_ids,
    );
    flag(
        "chat.startOnMention",
        ReloadTier::Restart,
        old.chat.start_on_mention == new.chat.start_on_mention,
    );

    flag(
        "agent.provider",
        ReloadTier::NextLaunch,
        old.agent.provider == new.agent.provider,
    );
    flag(
        "agent.model",
        ReloadTier::NextLaunch,
        old.agent.model == new.agent.model,
    );
    flag(
        "agent.visionModel",
        ReloadTier::NextLaunch,
        old.agent.vision_model == new.agent.vision_model,
    );
    flag(
        "agent.rulesPath",
        ReloadTier::Live,
        old.agent.rules_path == new.agent.rules_path,
    );
    flag(
        "agent.delegate",
        ReloadTier::NextLaunch,
        old.agent.delegate == new.agent.delegate,
    );
    flag(
        "agent.providers",
        ReloadTier::NextLaunch,
        old.agent.providers == new.agent.providers,
    );
    flag(
        "agent.extensions",
        ReloadTier::NextLaunch,
        old.agent.extensions == new.agent.extensions,
    );
    flag(
        "agent.aliases",
        ReloadTier::NextLaunch,
        old.agent.aliases == new.agent.aliases,
    );

    flag("github", ReloadTier::NextLaunch, old.github == new.github);
    flag(
        "projectRoot",
        ReloadTier::Restart,
        old.project_root == new.project_root,
    );
    flag(
        "stateDir",
        ReloadTier::Restart,
        old.state_dir == new.state_dir,
    );

    let old_sandbox = &old.sandbox;
    let new_sandbox = &new.sandbox;
    flag(
        "sandbox.backend",
        ReloadTier::Restart,
        old_sandbox.backend == new_sandbox.backend,
    );
    flag(
        "sandbox.requireFullEnforcement",
        ReloadTier::Restart,
        old_sandbox.require_full_enforcement == new_sandbox.require_full_enforcement,
    );
    flag(
        "sandbox.network",
        ReloadTier::Restart,
        old_sandbox.network == new_sandbox.network,
    );
    flag(
        "sandbox.egressPorts",
        ReloadTier::Restart,
        old_sandbox.egress_ports == new_sandbox.egress_ports,
    );
    flag(
        "sandbox.egress",
        ReloadTier::Restart,
        old_sandbox.egress == new_sandbox.egress,
    );
    flag(
        "sandbox.hideHostAddress",
        ReloadTier::NextLaunch,
        old_sandbox.hide_host_address == new_sandbox.hide_host_address,
    );
    flag(
        "sandbox.image",
        ReloadTier::Restart,
        old_sandbox.image == new_sandbox.image,
    );
    flag(
        "sandbox.memory",
        ReloadTier::NextLaunch,
        old_sandbox.memory == new_sandbox.memory,
    );
    flag(
        "sandbox.cpus",
        ReloadTier::NextLaunch,
        old_sandbox.cpus.to_bits() == new_sandbox.cpus.to_bits(),
    );
    flag(
        "sandbox.pids",
        ReloadTier::NextLaunch,
        old_sandbox.pids == new_sandbox.pids,
    );
    flag(
        "sandbox.fileMax",
        ReloadTier::NextLaunch,
        old_sandbox.file_max == new_sandbox.file_max,
    );
    flag(
        "sandbox.tmpSize",
        ReloadTier::NextLaunch,
        old_sandbox.tmp_size == new_sandbox.tmp_size,
    );
    flag(
        "sandbox.shmSize",
        ReloadTier::NextLaunch,
        old_sandbox.shm_size == new_sandbox.shm_size,
    );
    flag(
        "sandbox.diskTmp",
        ReloadTier::NextLaunch,
        old_sandbox.disk_tmp == new_sandbox.disk_tmp,
    );
    flag(
        "sandbox.disk",
        ReloadTier::Live,
        old_sandbox.disk == new_sandbox.disk,
    );
    flag(
        "sandbox.diskCheckMs",
        ReloadTier::Live,
        old_sandbox.disk_check_ms == new_sandbox.disk_check_ms,
    );
    flag(
        "sandbox.gracePeriodMs",
        ReloadTier::NextLaunch,
        old_sandbox.grace_period_ms == new_sandbox.grace_period_ms,
    );
    flag(
        "sandbox.policyExtra",
        ReloadTier::NextLaunch,
        old_sandbox.policy_extra == new_sandbox.policy_extra,
    );
    flag(
        "sandbox.pathExtra",
        ReloadTier::NextLaunch,
        old_sandbox.path_extra == new_sandbox.path_extra,
    );
    flag(
        "sandbox.env",
        ReloadTier::NextLaunch,
        old_sandbox.env == new_sandbox.env,
    );

    flag(
        "output.forwardToolOutput",
        ReloadTier::Live,
        old.output.forward_tool_output == new.output.forward_tool_output,
    );
    flag(
        "output.maxToolOutputChars",
        ReloadTier::Live,
        old.output.max_tool_output_chars == new.output.max_tool_output_chars,
    );
    flag(
        "output.maxAttachmentBytes",
        ReloadTier::Live,
        old.output.max_attachment_bytes == new.output.max_attachment_bytes,
    );
    flag(
        "output.maxAttachmentsPerMessage",
        ReloadTier::Live,
        old.output.max_attachments_per_message == new.output.max_attachments_per_message,
    );
    flag(
        "output.postDiffs",
        ReloadTier::Live,
        old.output.post_diffs == new.output.post_diffs,
    );

    flag(
        "shutdown.allowedUserIds",
        ReloadTier::Live,
        old.shutdown.allowed_user_ids == new.shutdown.allowed_user_ids,
    );
    flag("web", ReloadTier::Restart, old.web == new.web);
    flag("limits", ReloadTier::Restart, old.limits == new.limits);
    flag(
        "timeouts.idleMs",
        ReloadTier::Live,
        old.timeouts.idle_ms == new.timeouts.idle_ms,
    );
    flag(
        "timeouts.startupMs",
        ReloadTier::Live,
        old.timeouts.startup_ms == new.timeouts.startup_ms,
    );
    flag(
        "timeouts.questionMs",
        ReloadTier::Live,
        old.timeouts.question_ms == new.timeouts.question_ms,
    );
    flag(
        "timeouts.abortMs",
        ReloadTier::Live,
        old.timeouts.abort_ms == new.timeouts.abort_ms,
    );

    changed
}

#[cfg(test)]
mod tests;
