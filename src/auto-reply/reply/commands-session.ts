import fs from "node:fs";
import path from "node:path";

import { abortEmbeddedPiRun } from "../../agents/pi-embedded.js";
import type { SessionEntry } from "../../config/sessions.js";
import { updateSessionStore } from "../../config/sessions.js";
import { logVerbose } from "../../globals.js";
import { createInternalHookEvent, triggerInternalHook } from "../../hooks/internal-hooks.js";
import { scheduleGatewaySigusr1Restart, triggerClawdbotRestart } from "../../infra/restart.js";
import { parseActivationCommand } from "../group-activation.js";
import { parseSendPolicyCommand } from "../send-policy.js";
import {
  formatAbortReplyText,
  isAbortTrigger,
  setAbortMemory,
  stopSubagentsForRequester,
} from "./abort.js";
import type { CommandHandler } from "./commands-types.js";
import { clearSessionQueues } from "./queue.js";

// Cache for archived sessions list (per-session)
type ArchivedSession = {
  file: string;
  sessionId: string;
  deletedAt: string;
  reason: string;
  sizeBytes: number;
};
const archivedSessionsCache = new Map<string, ArchivedSession[]>();

function resolveSessionEntryForKey(
  store: Record<string, SessionEntry> | undefined,
  sessionKey: string | undefined,
) {
  if (!store || !sessionKey) return {};
  const direct = store[sessionKey];
  if (direct) return { entry: direct, key: sessionKey };
  return {};
}

function resolveAbortTarget(params: {
  ctx: { CommandTargetSessionKey?: string | null };
  sessionKey?: string;
  sessionEntry?: SessionEntry;
  sessionStore?: Record<string, SessionEntry>;
}) {
  const targetSessionKey = params.ctx.CommandTargetSessionKey?.trim() || params.sessionKey;
  const { entry, key } = resolveSessionEntryForKey(params.sessionStore, targetSessionKey);
  if (entry && key) return { entry, key, sessionId: entry.sessionId };
  if (params.sessionEntry && params.sessionKey) {
    return {
      entry: params.sessionEntry,
      key: params.sessionKey,
      sessionId: params.sessionEntry.sessionId,
    };
  }
  return { entry: undefined, key: targetSessionKey, sessionId: undefined };
}

export const handleActivationCommand: CommandHandler = async (params, allowTextCommands) => {
  if (!allowTextCommands) return null;
  const activationCommand = parseActivationCommand(params.command.commandBodyNormalized);
  if (!activationCommand.hasCommand) return null;
  if (!params.isGroup) {
    return {
      shouldContinue: false,
      reply: { text: "⚙️ Group activation only applies to group chats." },
    };
  }
  if (!params.command.isAuthorizedSender) {
    logVerbose(
      `Ignoring /activation from unauthorized sender in group: ${params.command.senderId || "<unknown>"}`,
    );
    return { shouldContinue: false };
  }
  if (!activationCommand.mode) {
    return {
      shouldContinue: false,
      reply: { text: "⚙️ Usage: /activation mention|always" },
    };
  }
  if (params.sessionEntry && params.sessionStore && params.sessionKey) {
    params.sessionEntry.groupActivation = activationCommand.mode;
    params.sessionEntry.groupActivationNeedsSystemIntro = true;
    params.sessionEntry.updatedAt = Date.now();
    params.sessionStore[params.sessionKey] = params.sessionEntry;
    if (params.storePath) {
      await updateSessionStore(params.storePath, (store) => {
        store[params.sessionKey] = params.sessionEntry as SessionEntry;
      });
    }
  }
  return {
    shouldContinue: false,
    reply: {
      text: `⚙️ Group activation set to ${activationCommand.mode}.`,
    },
  };
};

export const handleSendPolicyCommand: CommandHandler = async (params, allowTextCommands) => {
  if (!allowTextCommands) return null;
  const sendPolicyCommand = parseSendPolicyCommand(params.command.commandBodyNormalized);
  if (!sendPolicyCommand.hasCommand) return null;
  if (!params.command.isAuthorizedSender) {
    logVerbose(
      `Ignoring /send from unauthorized sender: ${params.command.senderId || "<unknown>"}`,
    );
    return { shouldContinue: false };
  }
  if (!sendPolicyCommand.mode) {
    return {
      shouldContinue: false,
      reply: { text: "⚙️ Usage: /send on|off|inherit" },
    };
  }
  if (params.sessionEntry && params.sessionStore && params.sessionKey) {
    if (sendPolicyCommand.mode === "inherit") {
      delete params.sessionEntry.sendPolicy;
    } else {
      params.sessionEntry.sendPolicy = sendPolicyCommand.mode;
    }
    params.sessionEntry.updatedAt = Date.now();
    params.sessionStore[params.sessionKey] = params.sessionEntry;
    if (params.storePath) {
      await updateSessionStore(params.storePath, (store) => {
        store[params.sessionKey] = params.sessionEntry as SessionEntry;
      });
    }
  }
  const label =
    sendPolicyCommand.mode === "inherit"
      ? "inherit"
      : sendPolicyCommand.mode === "allow"
        ? "on"
        : "off";
  return {
    shouldContinue: false,
    reply: { text: `⚙️ Send policy set to ${label}.` },
  };
};

export const handleRestartCommand: CommandHandler = async (params, allowTextCommands) => {
  if (!allowTextCommands) return null;
  if (params.command.commandBodyNormalized !== "/restart") return null;
  if (!params.command.isAuthorizedSender) {
    logVerbose(
      `Ignoring /restart from unauthorized sender: ${params.command.senderId || "<unknown>"}`,
    );
    return { shouldContinue: false };
  }
  if (params.cfg.commands?.restart !== true) {
    return {
      shouldContinue: false,
      reply: {
        text: "⚠️ /restart is disabled. Set commands.restart=true to enable.",
      },
    };
  }
  const hasSigusr1Listener = process.listenerCount("SIGUSR1") > 0;
  if (hasSigusr1Listener) {
    scheduleGatewaySigusr1Restart({ reason: "/restart" });
    return {
      shouldContinue: false,
      reply: {
        text: "⚙️ Restarting clawdbot in-process (SIGUSR1); back in a few seconds.",
      },
    };
  }
  const restartMethod = triggerClawdbotRestart();
  if (!restartMethod.ok) {
    const detail = restartMethod.detail ? ` Details: ${restartMethod.detail}` : "";
    return {
      shouldContinue: false,
      reply: {
        text: `⚠️ Restart failed (${restartMethod.method}).${detail}`,
      },
    };
  }
  return {
    shouldContinue: false,
    reply: {
      text: `⚙️ Restarting clawdbot via ${restartMethod.method}; give me a few seconds to come back online.`,
    },
  };
};

export const handleStopCommand: CommandHandler = async (params, allowTextCommands) => {
  if (!allowTextCommands) return null;
  if (params.command.commandBodyNormalized !== "/stop") return null;
  if (!params.command.isAuthorizedSender) {
    logVerbose(
      `Ignoring /stop from unauthorized sender: ${params.command.senderId || "<unknown>"}`,
    );
    return { shouldContinue: false };
  }
  const abortTarget = resolveAbortTarget({
    ctx: params.ctx,
    sessionKey: params.sessionKey,
    sessionEntry: params.sessionEntry,
    sessionStore: params.sessionStore,
  });
  if (abortTarget.sessionId) {
    abortEmbeddedPiRun(abortTarget.sessionId);
  }
  const cleared = clearSessionQueues([abortTarget.key, abortTarget.sessionId]);
  if (cleared.followupCleared > 0 || cleared.laneCleared > 0) {
    logVerbose(
      `stop: cleared followups=${cleared.followupCleared} lane=${cleared.laneCleared} keys=${cleared.keys.join(",")}`,
    );
  }
  if (abortTarget.entry && params.sessionStore && abortTarget.key) {
    abortTarget.entry.abortedLastRun = true;
    abortTarget.entry.updatedAt = Date.now();
    params.sessionStore[abortTarget.key] = abortTarget.entry;
    if (params.storePath) {
      await updateSessionStore(params.storePath, (store) => {
        store[abortTarget.key] = abortTarget.entry as SessionEntry;
      });
    }
  } else if (params.command.abortKey) {
    setAbortMemory(params.command.abortKey, true);
  }

  // Trigger internal hook for stop command
  const hookEvent = createInternalHookEvent(
    "command",
    "stop",
    abortTarget.key ?? params.sessionKey ?? "",
    {
      sessionEntry: abortTarget.entry ?? params.sessionEntry,
      sessionId: abortTarget.sessionId,
      commandSource: params.command.surface,
      senderId: params.command.senderId,
    },
  );
  await triggerInternalHook(hookEvent);

  const { stopped } = stopSubagentsForRequester({
    cfg: params.cfg,
    requesterSessionKey: abortTarget.key ?? params.sessionKey,
  });

  return { shouldContinue: false, reply: { text: formatAbortReplyText(stopped) } };
};

export const handleAbortTrigger: CommandHandler = async (params, allowTextCommands) => {
  if (!allowTextCommands) return null;
  if (!isAbortTrigger(params.command.rawBodyNormalized)) return null;
  const abortTarget = resolveAbortTarget({
    ctx: params.ctx,
    sessionKey: params.sessionKey,
    sessionEntry: params.sessionEntry,
    sessionStore: params.sessionStore,
  });
  if (abortTarget.sessionId) {
    abortEmbeddedPiRun(abortTarget.sessionId);
  }
  if (abortTarget.entry && params.sessionStore && abortTarget.key) {
    abortTarget.entry.abortedLastRun = true;
    abortTarget.entry.updatedAt = Date.now();
    params.sessionStore[abortTarget.key] = abortTarget.entry;
    if (params.storePath) {
      await updateSessionStore(params.storePath, (store) => {
        store[abortTarget.key] = abortTarget.entry as SessionEntry;
      });
    }
  } else if (params.command.abortKey) {
    setAbortMemory(params.command.abortKey, true);
  }
  return { shouldContinue: false, reply: { text: "⚙️ Agent was aborted." } };
};

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
}

function formatRelativeTime(isoDate: string): string {
  try {
    const date = new Date(isoDate);
    const now = Date.now();
    const diffMs = now - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    if (diffMins < 1) return "just now";
    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays}d ago`;
  } catch {
    return isoDate;
  }
}

function listArchivedSessions(storePath: string | undefined, agentId: string): ArchivedSession[] {
  // Derive sessions directory from store path or use default
  let sessionsDir: string;
  if (storePath) {
    sessionsDir = path.dirname(storePath);
  } else {
    const stateDir = process.env.CLAWDBOT_STATE_DIR || `${process.env.HOME}/.clawdbot`;
    sessionsDir = `${stateDir}/agents/${agentId}/sessions`;
  }

  const archived: ArchivedSession[] = [];

  try {
    if (!fs.existsSync(sessionsDir)) return archived;

    const files = fs.readdirSync(sessionsDir);
    // Match pattern: {sessionId}.jsonl.{reason}.{timestamp}
    const deletedPattern = /^(.+)\.jsonl\.([^.]+)\.(\d{4}-\d{2}-\d{2}T[\d-]+Z?)$/;

    for (const file of files) {
      const match = file.match(deletedPattern);
      if (!match) continue;

      const [, sessionId, reason, timestamp] = match;
      const filePath = path.join(sessionsDir, file);
      try {
        const stat = fs.statSync(filePath);
        archived.push({
          file: filePath,
          sessionId,
          deletedAt: timestamp.replace(/-/g, (m, i) => (i > 9 ? ":" : m)),
          reason,
          sizeBytes: stat.size,
        });
      } catch {
        // Skip files we can't stat
      }
    }

    // Sort by deletedAt descending (newest first)
    archived.sort((a, b) => b.deletedAt.localeCompare(a.deletedAt));
  } catch {
    // Ignore errors reading directory
  }

  return archived;
}

function restoreArchivedSession(
  session: ArchivedSession,
  sessionKey: string,
  storePath: string,
): { ok: boolean; error?: string; restoredPath?: string } {
  const filePath = session.file;

  if (!fs.existsSync(filePath)) {
    return { ok: false, error: `Archived file not found: ${filePath}` };
  }

  // Restore path: remove the .{reason}.{timestamp} suffix
  const restoredPath = filePath.replace(/\.[^.]+\.\d{4}-\d{2}-\d{2}T[\d-]+Z?$/, "");

  // If there's already an active file, archive it first
  if (fs.existsSync(restoredPath)) {
    try {
      const ts = new Date().toISOString().replaceAll(":", "-");
      const archived = `${restoredPath}.replaced.${ts}`;
      fs.renameSync(restoredPath, archived);
    } catch (err) {
      return { ok: false, error: `Failed to archive existing file: ${err instanceof Error ? err.message : String(err)}` };
    }
  }

  // Rename the archived file back to active
  try {
    fs.renameSync(filePath, restoredPath);
  } catch (err) {
    return { ok: false, error: `Failed to restore file: ${err instanceof Error ? err.message : String(err)}` };
  }

  return { ok: true, restoredPath };
}

export const handleRestoreCommand: CommandHandler = async (params, allowTextCommands) => {
  if (!allowTextCommands) return null;

  const normalized = params.command.commandBodyNormalized;
  if (!normalized.startsWith("/restore")) return null;

  if (!params.command.isAuthorizedSender) {
    logVerbose(
      `Ignoring /restore from unauthorized sender: ${params.command.senderId || "<unknown>"}`,
    );
    return { shouldContinue: false };
  }

  const args = normalized.slice("/restore".length).trim();
  const cacheKey = params.sessionKey ?? "default";
  const agentId = params.agentId ?? "main";

  // /restore or /restore list - show archived sessions
  if (!args || args === "list") {
    try {
      const archived = listArchivedSessions(params.storePath, agentId).slice(0, 15);

      if (!archived.length) {
        return {
          shouldContinue: false,
          reply: { text: "📦 No archived sessions found." },
        };
      }

      // Cache the list for restore by index
      archivedSessionsCache.set(cacheKey, archived);

      const lines = archived.map((s: ArchivedSession, i: number) => {
        const age = formatRelativeTime(s.deletedAt);
        const size = formatBytes(s.sizeBytes);
        const id = s.sessionId.slice(0, 8);
        return `${i + 1}. \`${id}…\` (${s.reason}) — ${age}, ${size}`;
      });

      return {
        shouldContinue: false,
        reply: {
          text: `📦 **Archived Sessions**\n\n${lines.join("\n")}\n\nRestore with \`/restore <#>\``,
        },
      };
    } catch (err) {
      return {
        shouldContinue: false,
        reply: {
          text: `⚠️ Failed to list archived sessions: ${err instanceof Error ? err.message : String(err)}`,
        },
      };
    }
  }

  // /restore <index> - restore by index
  const index = parseInt(args, 10);
  if (isNaN(index) || index < 1) {
    return {
      shouldContinue: false,
      reply: { text: "⚠️ Usage: `/restore` to list, `/restore <#>` to restore." },
    };
  }

  const cached = archivedSessionsCache.get(cacheKey);
  if (!cached || index > cached.length) {
    return {
      shouldContinue: false,
      reply: { text: "⚠️ Run `/restore` first to see available sessions." },
    };
  }

  const session = cached[index - 1];
  const storePath = params.storePath;

  if (!storePath) {
    return {
      shouldContinue: false,
      reply: { text: "⚠️ Session store path not available." },
    };
  }

  const result = restoreArchivedSession(session, params.sessionKey, storePath);

  if (!result.ok) {
    return {
      shouldContinue: false,
      reply: { text: `⚠️ ${result.error}` },
    };
  }

  // Update the session store to point to the restored session
  try {
    await updateSessionStore(storePath, (store) => {
      store[params.sessionKey] = {
        ...store[params.sessionKey],
        sessionId: session.sessionId,
        sessionFile: result.restoredPath,
        updatedAt: Date.now(),
      };
    });
  } catch (err) {
    return {
      shouldContinue: false,
      reply: {
        text: `✅ Restored session file, but failed to update store: ${err instanceof Error ? err.message : String(err)}`,
      },
    };
  }

  return {
    shouldContinue: false,
    reply: {
      text: `✅ Restored session \`${session.sessionId.slice(0, 8)}…\` (was ${session.reason}).\n\nSend a message to continue the conversation.`,
    },
  };
};
