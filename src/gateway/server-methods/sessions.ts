import { randomUUID } from "node:crypto";
import fs from "node:fs";

import { abortEmbeddedPiRun, waitForEmbeddedPiRunEnd } from "../../agents/pi-embedded.js";
import { stopSubagentsForRequester } from "../../auto-reply/reply/abort.js";
import { clearSessionQueues } from "../../auto-reply/reply/queue.js";
import { loadConfig } from "../../config/config.js";
import {
  resolveMainSessionKey,
  type SessionEntry,
  updateSessionStore,
} from "../../config/sessions.js";
import {
  ErrorCodes,
  errorShape,
  formatValidationErrors,
  validateSessionsArchivedParams,
  validateSessionsCompactParams,
  validateSessionsDeleteParams,
  validateSessionsListParams,
  validateSessionsPatchParams,
  validateSessionsResetParams,
  validateSessionsResolveParams,
  validateSessionsRestoreParams,
} from "../protocol/index.js";
import {
  archiveFileOnDisk,
  listSessionsFromStore,
  loadCombinedSessionStoreForGateway,
  loadSessionEntry,
  resolveGatewaySessionStoreTarget,
  resolveSessionTranscriptCandidates,
  type SessionsPatchResult,
} from "../session-utils.js";
import { applySessionsPatchToStore } from "../sessions-patch.js";
import { resolveSessionKeyFromResolveParams } from "../sessions-resolve.js";
import type { GatewayRequestHandlers } from "./types.js";

export const sessionsHandlers: GatewayRequestHandlers = {
  "sessions.list": ({ params, respond }) => {
    if (!validateSessionsListParams(params)) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid sessions.list params: ${formatValidationErrors(validateSessionsListParams.errors)}`,
        ),
      );
      return;
    }
    const p = params as import("../protocol/index.js").SessionsListParams;
    const cfg = loadConfig();
    const { storePath, store } = loadCombinedSessionStoreForGateway(cfg);
    const result = listSessionsFromStore({
      cfg,
      storePath,
      store,
      opts: p,
    });
    respond(true, result, undefined);
  },
  "sessions.resolve": ({ params, respond }) => {
    if (!validateSessionsResolveParams(params)) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid sessions.resolve params: ${formatValidationErrors(validateSessionsResolveParams.errors)}`,
        ),
      );
      return;
    }
    const p = params as import("../protocol/index.js").SessionsResolveParams;
    const cfg = loadConfig();

    const resolved = resolveSessionKeyFromResolveParams({ cfg, p });
    if (!resolved.ok) {
      respond(false, undefined, resolved.error);
      return;
    }
    respond(true, { ok: true, key: resolved.key }, undefined);
  },
  "sessions.patch": async ({ params, respond, context }) => {
    if (!validateSessionsPatchParams(params)) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid sessions.patch params: ${formatValidationErrors(validateSessionsPatchParams.errors)}`,
        ),
      );
      return;
    }
    const p = params as import("../protocol/index.js").SessionsPatchParams;
    const key = String(p.key ?? "").trim();
    if (!key) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "key required"));
      return;
    }

    const cfg = loadConfig();
    const target = resolveGatewaySessionStoreTarget({ cfg, key });
    const storePath = target.storePath;
    const applied = await updateSessionStore(storePath, async (store) => {
      const primaryKey = target.storeKeys[0] ?? key;
      const existingKey = target.storeKeys.find((candidate) => store[candidate]);
      if (existingKey && existingKey !== primaryKey && !store[primaryKey]) {
        store[primaryKey] = store[existingKey];
        delete store[existingKey];
      }
      return await applySessionsPatchToStore({
        cfg,
        store,
        storeKey: primaryKey,
        patch: p,
        loadGatewayModelCatalog: context.loadGatewayModelCatalog,
      });
    });
    if (!applied.ok) {
      respond(false, undefined, applied.error);
      return;
    }
    const result: SessionsPatchResult = {
      ok: true,
      path: storePath,
      key: target.canonicalKey,
      entry: applied.entry,
    };
    respond(true, result, undefined);
  },
  "sessions.reset": async ({ params, respond }) => {
    if (!validateSessionsResetParams(params)) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid sessions.reset params: ${formatValidationErrors(validateSessionsResetParams.errors)}`,
        ),
      );
      return;
    }
    const p = params as import("../protocol/index.js").SessionsResetParams;
    const key = String(p.key ?? "").trim();
    if (!key) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "key required"));
      return;
    }

    const cfg = loadConfig();
    const target = resolveGatewaySessionStoreTarget({ cfg, key });
    const storePath = target.storePath;
    const next = await updateSessionStore(storePath, (store) => {
      const primaryKey = target.storeKeys[0] ?? key;
      const existingKey = target.storeKeys.find((candidate) => store[candidate]);
      if (existingKey && existingKey !== primaryKey && !store[primaryKey]) {
        store[primaryKey] = store[existingKey];
        delete store[existingKey];
      }
      const entry = store[primaryKey];
      const now = Date.now();
      const nextEntry: SessionEntry = {
        sessionId: randomUUID(),
        updatedAt: now,
        systemSent: false,
        abortedLastRun: false,
        thinkingLevel: entry?.thinkingLevel,
        verboseLevel: entry?.verboseLevel,
        reasoningLevel: entry?.reasoningLevel,
        responseUsage: entry?.responseUsage,
        model: entry?.model,
        contextTokens: entry?.contextTokens,
        sendPolicy: entry?.sendPolicy,
        label: entry?.label,
        lastChannel: entry?.lastChannel,
        lastTo: entry?.lastTo,
        skillsSnapshot: entry?.skillsSnapshot,
      };
      store[primaryKey] = nextEntry;
      return nextEntry;
    });
    respond(true, { ok: true, key: target.canonicalKey, entry: next }, undefined);
  },
  "sessions.delete": async ({ params, respond }) => {
    if (!validateSessionsDeleteParams(params)) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid sessions.delete params: ${formatValidationErrors(validateSessionsDeleteParams.errors)}`,
        ),
      );
      return;
    }
    const p = params as import("../protocol/index.js").SessionsDeleteParams;
    const key = String(p.key ?? "").trim();
    if (!key) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "key required"));
      return;
    }

    const cfg = loadConfig();
    const mainKey = resolveMainSessionKey(cfg);
    const target = resolveGatewaySessionStoreTarget({ cfg, key });
    if (target.canonicalKey === mainKey) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, `Cannot delete the main session (${mainKey}).`),
      );
      return;
    }

    const deleteTranscript = typeof p.deleteTranscript === "boolean" ? p.deleteTranscript : true;

    const storePath = target.storePath;
    const { entry } = loadSessionEntry(key);
    const sessionId = entry?.sessionId;
    const existed = Boolean(entry);
    const queueKeys = new Set<string>(target.storeKeys);
    queueKeys.add(target.canonicalKey);
    if (sessionId) queueKeys.add(sessionId);
    clearSessionQueues([...queueKeys]);
    stopSubagentsForRequester({ cfg, requesterSessionKey: target.canonicalKey });
    if (sessionId) {
      abortEmbeddedPiRun(sessionId);
      const ended = await waitForEmbeddedPiRunEnd(sessionId, 15_000);
      if (!ended) {
        respond(
          false,
          undefined,
          errorShape(
            ErrorCodes.UNAVAILABLE,
            `Session ${key} is still active; try again in a moment.`,
          ),
        );
        return;
      }
    }
    await updateSessionStore(storePath, (store) => {
      const primaryKey = target.storeKeys[0] ?? key;
      const existingKey = target.storeKeys.find((candidate) => store[candidate]);
      if (existingKey && existingKey !== primaryKey && !store[primaryKey]) {
        store[primaryKey] = store[existingKey];
        delete store[existingKey];
      }
      if (store[primaryKey]) delete store[primaryKey];
    });

    const archived: string[] = [];
    if (deleteTranscript && sessionId) {
      for (const candidate of resolveSessionTranscriptCandidates(
        sessionId,
        storePath,
        entry?.sessionFile,
        target.agentId,
      )) {
        if (!fs.existsSync(candidate)) continue;
        try {
          archived.push(archiveFileOnDisk(candidate, "deleted"));
        } catch {
          // Best-effort.
        }
      }
    }

    respond(true, { ok: true, key: target.canonicalKey, deleted: existed, archived }, undefined);
  },
  "sessions.compact": async ({ params, respond }) => {
    if (!validateSessionsCompactParams(params)) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid sessions.compact params: ${formatValidationErrors(validateSessionsCompactParams.errors)}`,
        ),
      );
      return;
    }
    const p = params as import("../protocol/index.js").SessionsCompactParams;
    const key = String(p.key ?? "").trim();
    if (!key) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "key required"));
      return;
    }

    const maxLines =
      typeof p.maxLines === "number" && Number.isFinite(p.maxLines)
        ? Math.max(1, Math.floor(p.maxLines))
        : 400;

    const cfg = loadConfig();
    const target = resolveGatewaySessionStoreTarget({ cfg, key });
    const storePath = target.storePath;
    // Lock + read in a short critical section; transcript work happens outside.
    const compactTarget = await updateSessionStore(storePath, (store) => {
      const primaryKey = target.storeKeys[0] ?? key;
      const existingKey = target.storeKeys.find((candidate) => store[candidate]);
      if (existingKey && existingKey !== primaryKey && !store[primaryKey]) {
        store[primaryKey] = store[existingKey];
        delete store[existingKey];
      }
      return { entry: store[primaryKey], primaryKey };
    });
    const entry = compactTarget.entry;
    const sessionId = entry?.sessionId;
    if (!sessionId) {
      respond(
        true,
        {
          ok: true,
          key: target.canonicalKey,
          compacted: false,
          reason: "no sessionId",
        },
        undefined,
      );
      return;
    }

    const filePath = resolveSessionTranscriptCandidates(
      sessionId,
      storePath,
      entry?.sessionFile,
      target.agentId,
    ).find((candidate) => fs.existsSync(candidate));
    if (!filePath) {
      respond(
        true,
        {
          ok: true,
          key: target.canonicalKey,
          compacted: false,
          reason: "no transcript",
        },
        undefined,
      );
      return;
    }

    const raw = fs.readFileSync(filePath, "utf-8");
    const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
    if (lines.length <= maxLines) {
      respond(
        true,
        {
          ok: true,
          key: target.canonicalKey,
          compacted: false,
          kept: lines.length,
        },
        undefined,
      );
      return;
    }

    const archived = archiveFileOnDisk(filePath, "bak");
    const keptLines = lines.slice(-maxLines);
    fs.writeFileSync(filePath, `${keptLines.join("\n")}\n`, "utf-8");

    await updateSessionStore(storePath, (store) => {
      const entryKey = compactTarget.primaryKey;
      const entryToUpdate = store[entryKey];
      if (!entryToUpdate) return;
      delete entryToUpdate.inputTokens;
      delete entryToUpdate.outputTokens;
      delete entryToUpdate.totalTokens;
      entryToUpdate.updatedAt = Date.now();
    });

    respond(
      true,
      {
        ok: true,
        key: target.canonicalKey,
        compacted: true,
        archived,
        kept: keptLines.length,
      },
      undefined,
    );
  },
  "sessions.archived": ({ params, respond }) => {
    if (!validateSessionsArchivedParams(params)) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid sessions.archived params: ${formatValidationErrors(validateSessionsArchivedParams.errors)}`,
        ),
      );
      return;
    }
    const p = params as { agentId?: string; limit?: number };
    const cfg = loadConfig();
    const limit = p.limit ?? 20;

    // Find all session directories to scan
    const agentId = p.agentId?.trim() || "main";
    const stateDir =
      process.env.CLAWDBOT_STATE_DIR || `${process.env.HOME}/.clawdbot`;
    const sessionsDir = `${stateDir}/agents/${agentId}/sessions`;

    const archived: Array<{
      file: string;
      sessionId: string;
      deletedAt: string;
      reason: string;
      sizeBytes: number;
    }> = [];

    try {
      if (!fs.existsSync(sessionsDir)) {
        respond(true, { ok: true, archived: [] }, undefined);
        return;
      }

      const files = fs.readdirSync(sessionsDir);
      // Match pattern: {sessionId}.jsonl.{reason}.{timestamp}
      // Timestamp may include milliseconds like 2026-01-17T03-05-14.440Z
      const deletedPattern = /^(.+)\.jsonl\.([a-z]+)\.(\d{4}-\d{2}-\d{2}T[\d.-]+Z?)$/;

      for (const file of files) {
        const match = file.match(deletedPattern);
        if (!match) continue;

        const [, sessionId, reason, timestamp] = match;
        const filePath = `${sessionsDir}/${file}`;
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

      respond(true, { ok: true, archived: archived.slice(0, limit) }, undefined);
    } catch (err) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INTERNAL_ERROR,
          `Failed to list archived sessions: ${err instanceof Error ? err.message : String(err)}`,
        ),
      );
    }
  },
  "sessions.restore": async ({ params, respond }) => {
    if (!validateSessionsRestoreParams(params)) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `invalid sessions.restore params: ${formatValidationErrors(validateSessionsRestoreParams.errors)}`,
        ),
      );
      return;
    }
    const p = params as { file: string; key?: string };
    const filePath = p.file.trim();

    if (!fs.existsSync(filePath)) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.NOT_FOUND, `Archived file not found: ${filePath}`),
      );
      return;
    }

    // Parse the archived filename to extract session info
    // Pattern: {path}/{sessionId}.jsonl.{reason}.{timestamp}
    const basename = filePath.split("/").pop() || "";
    const match = basename.match(/^(.+)\.jsonl\.([^.]+)\.(\d{4}-\d{2}-\d{2}T[\d-]+Z?)$/);
    if (!match) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          `File does not appear to be an archived session: ${basename}`,
        ),
      );
      return;
    }

    const [, sessionId, reason] = match;
    const restoredPath = filePath.replace(/\.[^.]+\.\d{4}-\d{2}-\d{2}T[\d-]+Z?$/, "");

    // Check if there's already an active file at the restore path
    if (fs.existsSync(restoredPath)) {
      // Archive the current file first
      try {
        archiveFileOnDisk(restoredPath, "replaced");
      } catch (err) {
        respond(
          false,
          undefined,
          errorShape(
            ErrorCodes.INTERNAL_ERROR,
            `Failed to archive existing file: ${err instanceof Error ? err.message : String(err)}`,
          ),
        );
        return;
      }
    }

    // Rename the archived file back to active
    try {
      fs.renameSync(filePath, restoredPath);
    } catch (err) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INTERNAL_ERROR,
          `Failed to restore file: ${err instanceof Error ? err.message : String(err)}`,
        ),
      );
      return;
    }

    // Optionally update the session store if a key is provided
    const sessionKey = p.key?.trim();
    if (sessionKey) {
      const cfg = loadConfig();
      const target = resolveGatewaySessionStoreTarget({ cfg, key: sessionKey });
      const storePath = target.storePath;

      try {
        await updateSessionStore(storePath, (store) => {
          const primaryKey = target.storeKeys[0] ?? sessionKey;
          store[primaryKey] = {
            ...store[primaryKey],
            sessionId,
            sessionFile: restoredPath,
            updatedAt: Date.now(),
          };
        });
      } catch (err) {
        // Non-fatal: file was restored but store update failed
        respond(
          true,
          {
            ok: true,
            restored: true,
            file: restoredPath,
            sessionId,
            warning: `Session file restored but store update failed: ${err instanceof Error ? err.message : String(err)}`,
          },
          undefined,
        );
        return;
      }
    }

    respond(
      true,
      {
        ok: true,
        restored: true,
        file: restoredPath,
        sessionId,
        fromReason: reason,
      },
      undefined,
    );
  },
};
