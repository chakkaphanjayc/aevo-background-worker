import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { createHash } from "node:crypto";
import {
  buildPlaceSourceImportPlan,
  type PlaceSourceImportPlan,
  type PlaceSourceLicensePacket,
  type PlaceSourceRawRecord
} from "@aevocado/contracts";

export interface Env {
  AEVO_ENVIRONMENT?: string;
  AEVO_DATABASE_URL?: string;
  AEVO_PUBSUB_TOPIC?: string;
  AEVO_TASK_QUEUE?: string;
  AEVO_WORKER_SHARED_SECRET?: string;
  AEVO_FEED_EVENT_WORKER_TOKEN?: string;
  AEVO_CORE_API_ORIGIN?: string;
  SUPABASE_URL?: string;
  SUPABASE_SECRET_KEY?: string;
  AEVO_CONNECTION_PROBE_TOKEN?: string;
  AEVO_HANDSHAKE_SHARED_SECRET?: string;
  AEVO_PROBE_TARGETS?: string;
  AEVO_EVENT_MAX_ATTEMPTS?: string;
  AEVO_PLACE_SOURCE_MODE?: "disabled" | "fixture" | "approved";
}

interface ProbeTarget {
  appCode: string;
  baseUrl: string;
  healthPath?: string;
  handshakePath?: string;
}

interface WorkerEventEnvelope {
  schemaVersion: "v1";
  eventId: string;
  eventType: "FEED_EVENT_ACCEPTED";
  occurredAt?: string;
  organizationId?: string;
  storeId?: string;
  payload?: unknown;
}

interface ParsedDelivery {
  envelope: WorkerEventEnvelope;
  deliveryAttempt: number;
}

interface SupabaseRpcCall {
  ok: boolean;
  status: number;
  payload: unknown;
}

type SupabaseRpcFunction =
  | "tracedee_enqueue_profile_projection_job"
  | "tracedee_process_profile_projection_job"
  | "tracedee_get_media_cleanup_plan"
  | "tracedee_complete_media_cleanup";

const eventMetrics = {
  received: 0,
  acknowledged: 0,
  retried: 0,
  rejected: 0,
  deadLettered: 0,
  unavailable: 0
};

const defaultHandshakePath = "/.well-known/aevo-handshake";
const handshakeProtocol = "aevo.application-handshake";

function hex(bytes: ArrayBuffer): string {
  return Array.from(new Uint8Array(bytes), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function handshakeSignature(secret: string, timestamp: string, nonce: string, appCode: string, path: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const payload = [timestamp, "GET", path, nonce, appCode].join("\n");
  return hex(await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload)));
}

function requestIdFor(request: Request): string {
  const incoming = request.headers.get("x-request-id")?.trim();
  return incoming ? incoming.slice(0, 128) : crypto.randomUUID();
}

function json(requestId: string, body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "x-request-id": requestId
    }
  });
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${stableJson(record[key])}`).join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function workerAuthorized(request: Request, env: Env): boolean {
  const expected = env.AEVO_WORKER_SHARED_SECRET?.trim();
  return Boolean(expected && request.headers.get("x-aevo-worker-token") === expected);
}

function eventMaxAttempts(env: Env): number {
  const value = Number(env.AEVO_EVENT_MAX_ATTEMPTS ?? "3");
  return Number.isInteger(value) ? Math.min(5, Math.max(1, value)) : 3;
}

function decodePubSubData(value: string): unknown {
  try {
    const binary = atob(value);
    const bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0));
    return JSON.parse(new TextDecoder().decode(bytes)) as unknown;
  } catch {
    return null;
  }
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(value);
}

function parseWorkerEvent(value: unknown): WorkerEventEnvelope | null {
  if (!isRecord(value)
    || typeof value.eventId !== "string"
    || value.eventId.trim().length < 1
    || value.eventId.trim().length > 200
    || typeof value.eventType !== "string"
    || value.eventType.trim().toUpperCase() !== "FEED_EVENT_ACCEPTED") return null;
  const schemaVersion = typeof value.schemaVersion === "string" ? value.schemaVersion.trim() : "v1";
  if (schemaVersion !== "v1") return null;
  if (value.occurredAt !== undefined && (typeof value.occurredAt !== "string" || !Number.isFinite(Date.parse(value.occurredAt)))) return null;
  if (value.organizationId !== undefined && (typeof value.organizationId !== "string" || !isUuid(value.organizationId))) return null;
  if (value.storeId !== undefined && (typeof value.storeId !== "string" || !isUuid(value.storeId))) return null;
  return {
    schemaVersion: "v1",
    eventId: value.eventId.trim(),
    eventType: "FEED_EVENT_ACCEPTED",
    ...(typeof value.occurredAt === "string" ? { occurredAt: value.occurredAt } : {}),
    ...(typeof value.organizationId === "string" ? { organizationId: value.organizationId.toLowerCase() } : {}),
    ...(typeof value.storeId === "string" ? { storeId: value.storeId.toLowerCase() } : {}),
    ...(value.payload !== undefined ? { payload: value.payload } : {})
  };
}

async function parseDelivery(request: Request): Promise<ParsedDelivery | null> {
  const raw = await request.json().catch(() => null) as unknown;
  if (!isRecord(raw)) return null;
  if (isRecord(raw.message) && typeof raw.message.data === "string") {
    const decoded = decodePubSubData(raw.message.data);
    const candidate = isRecord(decoded) ? { ...decoded } : {};
    if (candidate.eventId === undefined && typeof raw.message.messageId === "string") candidate.eventId = raw.message.messageId;
    if (candidate.eventType === undefined && isRecord(raw.message.attributes) && typeof raw.message.attributes.eventType === "string") {
      candidate.eventType = raw.message.attributes.eventType;
    }
    const envelope = parseWorkerEvent(candidate);
    const deliveryAttempt = isRecord(raw.message.attributes) && typeof raw.message.attributes.deliveryAttempt === "string"
      ? Number(raw.message.attributes.deliveryAttempt)
      : 1;
    return envelope && Number.isInteger(deliveryAttempt) && deliveryAttempt >= 1 ? { envelope, deliveryAttempt } : null;
  }
  const envelope = parseWorkerEvent(raw);
  return envelope ? { envelope, deliveryAttempt: 1 } : null;
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function parseSourcePlanInput(value: unknown): {
  runId: string;
  source: PlaceSourceLicensePacket;
  observedAt: string;
  records: PlaceSourceRawRecord[];
  previousExternalIds: string[];
} | null {
  if (!isRecord(value)
    || typeof value.runId !== "string"
    || typeof value.observedAt !== "string"
    || !isRecord(value.source)
    || !Array.isArray(value.records)) return null;
  const source = value.source as Partial<PlaceSourceLicensePacket>;
  if (typeof source.sourceKind !== "string"
    || typeof source.namespace !== "string"
    || typeof source.sourceVersion !== "string"
    || typeof source.license !== "string"
    || typeof source.attributionText !== "string"
    || typeof source.termsApproved !== "boolean"
    || (source.sourceUrl !== null && typeof source.sourceUrl !== "string")
    || (source.approvedAt !== null && typeof source.approvedAt !== "string")) return null;
  const records = value.records.filter((record): record is PlaceSourceRawRecord => {
    if (!isRecord(record)) return false;
    return typeof record.externalId === "string"
      && typeof record.name === "string"
      && (record.geometry === null || typeof record.geometry === "object")
      && typeof record.categoryId === "string"
      && typeof record.categoryLabel === "string"
      && typeof record.observedAt === "string";
  });
  if (records.length !== value.records.length) return null;
  const previousExternalIds = Array.isArray(value.previousExternalIds)
    && value.previousExternalIds.every((item): item is string => typeof item === "string")
    ? value.previousExternalIds
    : [];
  return {
    runId: value.runId,
    source: source as PlaceSourceLicensePacket,
    observedAt: value.observedAt,
    records,
    previousExternalIds
  };
}

async function normalizePlaceSource(request: Request, env: Env, requestId: string): Promise<Response> {
  if (!env.AEVO_WORKER_SHARED_SECRET?.trim()) {
    return json(requestId, { error: { code: "WORKER_NOT_CONFIGURED", message: "Worker authentication is not configured.", requestId } }, 503);
  }
  if (request.headers.get("x-aevo-worker-token") !== env.AEVO_WORKER_SHARED_SECRET) {
    return json(requestId, { error: { code: "UNAUTHORIZED", message: "Worker authentication failed.", requestId } }, 401);
  }
  if ((env.AEVO_PLACE_SOURCE_MODE ?? "disabled") === "disabled") {
    return json(requestId, { error: { code: "PLACE_SOURCE_DISABLED", message: "Place source normalization is disabled until a source/legal packet is approved.", requestId } }, 503);
  }
  const contentLength = Number(request.headers.get("content-length") ?? "0");
  if (Number.isFinite(contentLength) && contentLength > 512 * 1024) {
    return json(requestId, { error: { code: "REQUEST_BODY_TOO_LARGE", message: "Place source batches are limited to 512 KiB.", requestId } }, 413);
  }
  const rawBody = await request.json().catch(() => null);
  const input = parseSourcePlanInput(rawBody);
  if (!input) {
    return json(requestId, { error: { code: "INVALID_PLACE_SOURCE_REQUEST", message: "A valid source packet and record batch are required.", requestId } }, 400);
  }
  try {
    const sortedRecords = [...input.records].sort((left, right) => left.externalId.localeCompare(right.externalId));
    const replayFingerprint = `sha256:${createHash("sha256").update(stableJson({
      source: input.source,
      observedAt: input.observedAt,
      records: sortedRecords
    })).digest("hex")}`;
    const plan: PlaceSourceImportPlan = buildPlaceSourceImportPlan({
      ...input,
      records: sortedRecords,
      replayFingerprint
    });
    return json(requestId, {
      success: true,
      publishable: false,
      reviewRequired: true,
      reason: "PROJECTION_REVIEW_REQUIRED",
      plan,
      requestId
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Place source normalization failed.";
    const code = error && typeof error === "object" && "code" in error && typeof error.code === "string"
      ? error.code
      : "INVALID_PLACE_SOURCE_REQUEST";
    return json(requestId, { error: { code, message, requestId } }, 422);
  }
}

function readinessMissingConfiguration(env: Env): string[] {
  const missing: string[] = [];
  if (!env.AEVO_DATABASE_URL?.trim()) missing.push("AEVO_DATABASE_URL");
  if (!env.AEVO_PUBSUB_TOPIC?.trim()) missing.push("AEVO_PUBSUB_TOPIC");
  if (!env.AEVO_TASK_QUEUE?.trim()) missing.push("AEVO_TASK_QUEUE");
  if (!env.AEVO_WORKER_SHARED_SECRET?.trim()) missing.push("AEVO_WORKER_SHARED_SECRET");

  const environment = env.AEVO_ENVIRONMENT?.trim().toLowerCase();
  if (environment === "nonprod" || environment === "staging" || environment === "production") {
    if (!env.AEVO_CORE_API_ORIGIN?.trim()) missing.push("AEVO_CORE_API_ORIGIN");
    if (!env.AEVO_FEED_EVENT_WORKER_TOKEN?.trim()) missing.push("AEVO_FEED_EVENT_WORKER_TOKEN");
  }

  return missing;
}

function configured(env: Env): boolean {
  return readinessMissingConfiguration(env).length === 0;
}

function probeTargets(env: Env): ProbeTarget[] {
  if (!env.AEVO_PROBE_TARGETS?.trim()) return [];
  try {
    const parsed = JSON.parse(env.AEVO_PROBE_TARGETS) as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed.flatMap((value): ProbeTarget[] => {
      if (!value || typeof value !== "object") return [];
      const candidate = value as Record<string, unknown>;
      if (typeof candidate.appCode !== "string" || typeof candidate.baseUrl !== "string") return [];
      try {
        const url = new URL(candidate.baseUrl);
        if (url.protocol !== "https:" && url.hostname !== "localhost" && url.hostname !== "127.0.0.1") return [];
      } catch {
        return [];
      }
      return [{
        appCode: candidate.appCode.trim().toUpperCase(),
        baseUrl: candidate.baseUrl.replace(/\/$/u, ""),
        ...(typeof candidate.healthPath === "string" && candidate.healthPath.startsWith("/") ? { healthPath: candidate.healthPath } : {}),
        ...(typeof candidate.handshakePath === "string" && candidate.handshakePath.startsWith("/") ? { handshakePath: candidate.handshakePath } : {})
      }];
    });
  } catch {
    return [];
  }
}

async function probeApplications(env: Env, requestId: string): Promise<Response> {
  const coreOrigin = env.AEVO_CORE_API_ORIGIN?.trim().replace(/\/$/u, "");
  const token = env.AEVO_CONNECTION_PROBE_TOKEN?.trim();
  const targets = probeTargets(env);
  if (!coreOrigin || !token || targets.length === 0) {
    return json(requestId, { error: { code: "CONNECTION_PROBE_NOT_CONFIGURED", message: "Core API origin, probe token and targets are required.", requestId } }, 503);
  }

  const results: Array<{ appCode: string; status: string; compatibilityStatus: string; compatibilityReason: string; latencyMs: number | null }> = [];
  for (const target of targets) {
    const healthPath = target.healthPath ?? "/health";
    const handshakePath = target.handshakePath ?? defaultHandshakePath;
    const handshakeSecret = env.AEVO_HANDSHAKE_SHARED_SECRET?.trim();
    const startedAt = performance.now();
    let status: "connected" | "degraded" | "not_connected" = "not_connected";
    let compatibilityStatus = handshakeSecret ? "INCOMPATIBLE" : "HEALTH_ONLY";
    let compatibilityReason = handshakeSecret ? "HANDSHAKE_NOT_VERIFIED" : "HANDSHAKE_NOT_CONFIGURED";
    let lastErrorCode: string | null = null;
    let latencyMs: number | null = null;
    let probePath = handshakeSecret ? handshakePath : healthPath;
    try {
      const headers: Record<string, string> = { accept: "application/json", "x-request-id": requestId };
      if (handshakeSecret) {
        const timestamp = Math.floor(Date.now() / 1000).toString();
        const nonce = crypto.randomUUID().replaceAll("-", "");
        headers["x-aevo-handshake-timestamp"] = timestamp;
        headers["x-aevo-handshake-nonce"] = nonce;
        headers["x-aevo-handshake-signature"] = await handshakeSignature(handshakeSecret, timestamp, nonce, target.appCode, handshakePath);
      }
      let response = await fetch(`${target.baseUrl}${probePath}`, {
        headers,
        signal: AbortSignal.timeout(3_000)
      });
      if (handshakeSecret && (response.status === 404 || response.status === 405)) {
        response = await fetch(`${target.baseUrl}${healthPath}`, {
          headers: { accept: "application/json", "x-request-id": requestId },
          signal: AbortSignal.timeout(3_000)
        });
        probePath = healthPath;
        compatibilityStatus = "HEALTH_ONLY";
        compatibilityReason = response.ok ? "HANDSHAKE_NOT_SUPPORTED" : "HEALTH_CHECK_FAILED";
      } else if (handshakeSecret && (response.status === 401 || response.status === 403)) {
        compatibilityStatus = "UNAUTHORIZED";
        compatibilityReason = "HANDSHAKE_REQUEST_UNAUTHORIZED";
      } else if (handshakeSecret && response.ok) {
        const payload = await response.json().catch(() => null) as Record<string, unknown> | null;
        const checkedAt = typeof payload?.checkedAt === "string" ? Date.parse(payload.checkedAt) : Number.NaN;
        const compatible = payload?.protocol === handshakeProtocol
          && payload.protocolVersion === "1"
          && payload.appCode === target.appCode
          && payload.contractVersion === "v1"
          && (payload.status === "ready" || payload.status === "healthy")
          && Number.isFinite(checkedAt)
          && Math.abs(Date.now() - checkedAt) <= 300_000
          && Array.isArray(payload.capabilities)
          && payload.capabilities.every((capability) => typeof capability === "string");
        compatibilityStatus = compatible ? "COMPATIBLE" : "INCOMPATIBLE";
        compatibilityReason = compatible ? "HANDSHAKE_COMPATIBLE" : "HANDSHAKE_CONTRACT_MISMATCH";
      }
      latencyMs = Math.max(0, Math.round(performance.now() - startedAt));
      status = response.ok && compatibilityStatus === "COMPATIBLE" ? "connected" : response.ok ? "degraded" : "not_connected";
      if (!response.ok) lastErrorCode = `HTTP_${response.status}`;
    } catch {
      latencyMs = Math.max(0, Math.round(performance.now() - startedAt));
      lastErrorCode = "PROBE_FETCH_FAILED";
      compatibilityStatus = "OFFLINE";
      compatibilityReason = "HANDSHAKE_FETCH_FAILED";
    }

    const writeResponse = await fetch(`${coreOrigin}/internal/application-connections/probe`, {
      method: "POST",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
        "x-aevo-worker-token": token,
        "x-request-id": requestId
      },
      body: JSON.stringify({
        appCode: target.appCode,
        environment: env.AEVO_ENVIRONMENT ?? "unknown",
        status,
        baseUrl: target.baseUrl,
        healthPath: probePath,
        checkedAt: new Date().toISOString(),
        latencyMs,
        lastErrorCode,
        metadata: {
          compatibilityStatus,
          compatibilityReason,
          handshakePath,
          capabilities: []
        }
      })
    });
    if (!writeResponse.ok) {
      return json(requestId, { error: { code: "CONNECTION_PROBE_WRITE_FAILED", message: "Core API rejected a connection probe result.", requestId } }, 502);
    }
    results.push({ appCode: target.appCode, status, compatibilityStatus, compatibilityReason, latencyMs });
  }

  return json(requestId, { success: true, checkedAt: new Date().toISOString(), results });
}

async function callSupabaseRpc(
  env: Env,
  functionName: SupabaseRpcFunction,
  body: Record<string, unknown>
): Promise<SupabaseRpcCall> {
  const origin = env.SUPABASE_URL?.trim().replace(/\/$/u, "");
  const secretKey = env.SUPABASE_SECRET_KEY?.trim();
  if (!origin || !secretKey) {
    return { ok: false, status: 503, payload: { error: { code: "SUPABASE_NOT_CONFIGURED" } } };
  }

  try {
    const response = await fetch(`${origin}/rest/v1/rpc/${functionName}`, {
      method: "POST",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
        apikey: secretKey,
        authorization: `Bearer ${secretKey}`
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(10_000)
    });
    return {
      ok: response.ok,
      status: response.status,
      payload: await response.json().catch(() => null)
    };
  } catch {
    return { ok: false, status: 503, payload: { error: { code: "SUPABASE_RPC_UNAVAILABLE" } } };
  }
}

function mediaCleanupFailure(requestId: string, result: SupabaseRpcCall): Response {
  return json(requestId, {
    error: {
      code: result.status === 503 ? "MEDIA_CLEANUP_UNAVAILABLE" : "MEDIA_CLEANUP_REJECTED",
      message: "The private media cleanup boundary did not complete.",
      requestId
    },
    retryable: result.status === 503
  }, result.status === 503 ? 503 : 502);
}

function mediaCleanupPlan(value: unknown): {
  assetId: string;
  bucketId: string;
  status: "REJECTED" | "DELETED";
  version: number;
  cleanupRequired: boolean;
  cleanupPaths: string[];
} | null {
  if (!isRecord(value)
    || value.ok !== true
    || typeof value.assetId !== "string"
    || typeof value.bucketId !== "string"
    || (value.status !== "REJECTED" && value.status !== "DELETED")
    || typeof value.version !== "number"
    || !Number.isInteger(value.version)
    || typeof value.cleanupRequired !== "boolean"
    || !Array.isArray(value.cleanupPaths)
    || !value.cleanupPaths.every((path): path is string => typeof path === "string")) return null;
  return {
    assetId: value.assetId,
    bucketId: value.bucketId,
    status: value.status,
    version: value.version,
    cleanupRequired: value.cleanupRequired,
    cleanupPaths: value.cleanupPaths
  };
}

function safeQuarantinePath(path: string): boolean {
  return path.length >= 10
    && path.length <= 500
    && (path.startsWith("quarantine/") || path.startsWith("approved/"))
    && !path.includes("..")
    && !path.includes("\\")
    && !path.includes("//");
}

async function deletePrivateMediaObject(
  env: Env,
  bucketId: string,
  objectPath: string
): Promise<{ ok: boolean; status: number }> {
  const origin = env.SUPABASE_URL?.trim().replace(/\/$/u, "");
  const secretKey = env.SUPABASE_SECRET_KEY?.trim();
  if (!origin || !secretKey) return { ok: false, status: 503 };
  const encodedPath = objectPath.split("/").map((part) => encodeURIComponent(part)).join("/");
  try {
    const response = await fetch(`${origin}/storage/v1/object/${encodeURIComponent(bucketId)}/${encodedPath}`, {
      method: "DELETE",
      headers: {
        accept: "application/json",
        apikey: secretKey,
        authorization: `Bearer ${secretKey}`
      },
      signal: AbortSignal.timeout(10_000)
    });
    return { ok: response.ok || response.status === 404, status: response.status };
  } catch {
    return { ok: false, status: 503 };
  }
}

async function cleanupMediaAsset(
  env: Env,
  requestId: string,
  assetId: string
): Promise<Response> {
  const planResult = await callSupabaseRpc(env, "tracedee_get_media_cleanup_plan", { p_asset_id: assetId });
  if (!planResult.ok) return mediaCleanupFailure(requestId, planResult);
  const plan = mediaCleanupPlan(planResult.payload);
  if (!plan) {
    return json(requestId, {
      error: { code: "MEDIA_CLEANUP_PLAN_INVALID", message: "The cleanup plan failed closed validation.", requestId },
      retryable: false
    }, 502);
  }
  if (plan.bucketId !== "tracedee-quarantine") {
    return json(requestId, {
      error: { code: "MEDIA_CLEANUP_BUCKET_UNSUPPORTED", message: "The cleanup plan referenced an unsupported private bucket.", requestId },
      retryable: false
    }, 422);
  }
  if (!plan.cleanupRequired) {
    return json(requestId, {
      success: true,
      assetId: plan.assetId,
      status: plan.status,
      version: plan.version,
      cleaned: false,
      reason: "NO_OBJECT_CLEANUP_REQUIRED",
      requestId
    });
  }
  if (plan.cleanupPaths.length > 2 || !plan.cleanupPaths.every(safeQuarantinePath)) {
    return json(requestId, {
      error: { code: "MEDIA_CLEANUP_PATH_INVALID", message: "The cleanup plan contained an unsafe object path.", requestId },
      retryable: false
    }, 422);
  }

  for (const objectPath of plan.cleanupPaths) {
    const deleted = await deletePrivateMediaObject(env, plan.bucketId, objectPath);
    if (!deleted.ok) {
      return json(requestId, {
        error: { code: "MEDIA_OBJECT_DELETE_FAILED", message: "A private media object could not be deleted.", requestId },
        retryable: true,
        status: deleted.status
      }, 503);
    }
  }

  const completeResult = await callSupabaseRpc(env, "tracedee_complete_media_cleanup", {
    p_asset_id: assetId,
    p_expected_version: plan.version
  });
  if (!completeResult.ok) return mediaCleanupFailure(requestId, completeResult);
  return json(requestId, {
    success: true,
    assetId: plan.assetId,
    status: plan.status,
    version: plan.version,
    cleaned: true,
    completed: completeResult.payload,
    requestId
  });
}

function profileProjectionRpcResponse(requestId: string, result: SupabaseRpcCall): Response {
  if (!result.ok) {
    return json(requestId, {
      error: {
        code: result.status === 503 ? "PROFILE_PROJECTION_UNAVAILABLE" : "PROFILE_PROJECTION_REJECTED",
        message: "The profile projection RPC did not complete.",
        requestId
      },
      retryable: result.status === 503
    }, result.status === 503 ? 503 : 502);
  }

  const payload = result.payload;
  if (isRecord(payload) && payload.ok === false) {
    const retryable = payload.retryable === true;
    return json(requestId, { success: false, result: payload, retryable }, retryable ? 503 : 422);
  }
  return json(requestId, { success: true, result: payload, requestId });
}

async function consumeFeedDelivery(
  request: Request,
  env: Env,
  requestId: string,
  delivery: ParsedDelivery
): Promise<Response> {
  const coreOrigin = env.AEVO_CORE_API_ORIGIN?.trim().replace(/\/$/u, "");
  const feedEventToken = env.AEVO_FEED_EVENT_WORKER_TOKEN?.trim();
  if (!coreOrigin || !feedEventToken) {
    eventMetrics.unavailable += 1;
    return json(requestId, { error: { code: "FEED_EVENT_CONSUMER_NOT_CONFIGURED", message: "Core API origin and Feed event worker token are required.", requestId }, acknowledged: false, retryable: false }, 503);
  }

  const maxAttempts = eventMaxAttempts(env);
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const consumeResponse = await fetch(`${coreOrigin}/internal/feed/events/consume?consumer=aevo-background-worker&limit=50`, {
        method: "POST",
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          "x-aevo-worker-token": feedEventToken,
          "x-request-id": requestId
        },
        body: JSON.stringify(delivery.envelope),
        signal: AbortSignal.timeout(10_000)
      });
      const consumeBody = await consumeResponse.json().catch(() => null) as Record<string, unknown> | null;
      const failed = typeof consumeBody?.failed === "number" ? consumeBody.failed : 0;
      const deadLettered = typeof consumeBody?.deadLettered === "number" ? consumeBody.deadLettered : 0;

      if (consumeResponse.ok && failed === 0) {
        eventMetrics.received += 1;
        eventMetrics.acknowledged += 1;
        if (deadLettered > 0) eventMetrics.deadLettered += deadLettered;
        return json(requestId, {
          success: true,
          acknowledged: true,
          eventId: delivery.envelope.eventId,
          eventType: delivery.envelope.eventType,
          attempts: attempt,
          deadLettered,
          consumer: consumeBody
        });
      }

      if (!consumeResponse.ok && consumeResponse.status >= 400 && consumeResponse.status < 500 && consumeResponse.status !== 408 && consumeResponse.status !== 429) {
        eventMetrics.rejected += 1;
        return json(requestId, { error: { code: "FEED_EVENT_CONSUMER_REJECTED", message: "Core API rejected the Feed event delivery.", requestId }, acknowledged: false, retryable: false }, 422);
      }
    } catch {
      // A timeout or transport failure must leave the Pub/Sub delivery
      // unacknowledged so the platform retry/DLQ policy remains authoritative.
    }

    if (attempt < maxAttempts) {
      eventMetrics.retried += 1;
      await wait(Math.min(250, 50 * 2 ** (attempt - 1)));
    }
  }

  eventMetrics.unavailable += 1;
  return json(requestId, {
    error: { code: "FEED_EVENT_CONSUMER_UNAVAILABLE", message: "Core API Feed event consumer is unavailable after bounded retries.", requestId },
    acknowledged: false,
    retryable: true,
    attempts: maxAttempts
  }, 503);
}

export async function handleRequest(request: Request, env: Env): Promise<Response> {
  const requestId = requestIdFor(request);
  const url = new URL(request.url);

  if (url.pathname === "/health") {
    return json(requestId, {
      status: "healthy",
      service: "aevo-background-worker",
      environment: env.AEVO_ENVIRONMENT ?? "local"
    });
  }

  if (url.pathname === "/ready") {
    const missingConfiguration = readinessMissingConfiguration(env);
    return missingConfiguration.length === 0
      ? json(requestId, { status: "ready", service: "aevo-background-worker" })
      : json(requestId, { error: { code: "WORKER_NOT_CONFIGURED", message: "Worker dependencies are not configured.", requestId, missingConfiguration } }, 503);
  }

  if (request.method === "GET" && url.pathname === "/internal/metrics") {
    if (!env.AEVO_WORKER_SHARED_SECRET?.trim()) {
      return json(requestId, { error: { code: "WORKER_NOT_CONFIGURED", message: "Worker authentication is not configured.", requestId } }, 503);
    }
    if (!workerAuthorized(request, env)) {
      return json(requestId, { error: { code: "UNAUTHORIZED", message: "Worker authentication failed.", requestId } }, 401);
    }
    return json(requestId, { service: "aevo-background-worker", eventDelivery: { ...eventMetrics } });
  }

  if (request.method === "POST" && url.pathname === "/internal/pubsub") {
    if (!env.AEVO_WORKER_SHARED_SECRET?.trim()) {
      return json(requestId, { error: { code: "WORKER_NOT_CONFIGURED", message: "Worker authentication is not configured.", requestId } }, 503);
    }
    if (!workerAuthorized(request, env)) {
      return json(requestId, { error: { code: "UNAUTHORIZED", message: "Worker authentication failed.", requestId } }, 401);
    }
    const delivery = await parseDelivery(request);
    if (!delivery) {
      eventMetrics.rejected += 1;
      return json(requestId, { error: { code: "INVALID_EVENT_ENVELOPE", message: "A valid versioned event envelope is required.", requestId }, acknowledged: false }, 400);
    }
    return consumeFeedDelivery(request, env, requestId, delivery);
  }

  if (request.method === "POST" && url.pathname === "/internal/tasks") {
    if (!env.AEVO_WORKER_SHARED_SECRET?.trim()) {
      return json(requestId, { error: { code: "WORKER_NOT_CONFIGURED", message: "Worker authentication is not configured.", requestId } }, 503);
    }
    if (!workerAuthorized(request, env)) {
      return json(requestId, { error: { code: "UNAUTHORIZED", message: "Worker authentication failed.", requestId } }, 401);
    }
    const body = await request.clone().json().catch(() => null) as Record<string, unknown> | null;
    const taskType = typeof body?.taskType === "string" ? body.taskType.trim().toUpperCase() : "";
    if (taskType === "APPLICATION_CONNECTION_PROBE") return probeApplications(env, requestId);
    if (taskType === "FEED_EVENT_ACCEPTED") {
      const delivery = await parseDelivery(request);
      if (!delivery) return json(requestId, { error: { code: "INVALID_TASK_ENVELOPE", message: "A valid Feed task envelope is required.", requestId } }, 400);
      return consumeFeedDelivery(request, env, requestId, delivery);
    }
    if (taskType === "TRACEDEE_PROFILE_PROJECTION_ENQUEUE") {
      const profileId = typeof body?.profileId === "string" ? body.profileId.trim() : "";
      const operation = typeof body?.operation === "string" ? body.operation.trim().toUpperCase() : "";
      const reason = typeof body?.reason === "string" ? body.reason.trim() : "";
      const requestKey = typeof body?.requestKey === "string" ? body.requestKey.trim() : "";
      const requestHash = typeof body?.requestHash === "string" ? body.requestHash.trim() : "";
      if (!isUuid(profileId) || !["REBUILD", "DELETE"].includes(operation)
        || reason.length < 3 || reason.length > 500
        || requestKey.length < 8 || requestKey.length > 200
        || requestHash.length < 8 || requestHash.length > 200) {
        return json(requestId, { error: { code: "INVALID_PROFILE_PROJECTION_ENQUEUE", message: "A valid profile projection job request is required.", requestId } }, 400);
      }
      return profileProjectionRpcResponse(requestId, await callSupabaseRpc(
        env,
        "tracedee_enqueue_profile_projection_job",
        { p_profile_id: profileId, p_operation: operation, p_reason: reason, p_request_key: requestKey, p_request_hash: requestHash }
      ));
    }
    if (taskType === "TRACEDEE_PROFILE_PROJECTION_PROCESS") {
      const jobId = typeof body?.jobId === "string" ? body.jobId.trim() : "";
      if (!isUuid(jobId)) {
        return json(requestId, { error: { code: "INVALID_PROFILE_PROJECTION_JOB", message: "A valid profile projection job ID is required.", requestId } }, 400);
      }
      return profileProjectionRpcResponse(requestId, await callSupabaseRpc(
        env,
        "tracedee_process_profile_projection_job",
        { p_job_id: jobId }
      ));
    }
    if (taskType === "TRACEDEE_MEDIA_OBJECT_CLEANUP") {
      const assetId = typeof body?.assetId === "string" ? body.assetId.trim() : "";
      if (!isUuid(assetId)) {
        return json(requestId, { error: { code: "INVALID_MEDIA_CLEANUP_ASSET", message: "A valid media asset ID is required.", requestId } }, 400);
      }
      return cleanupMediaAsset(env, requestId, assetId);
    }
    return json(requestId, { error: { code: "TASK_TYPE_UNSUPPORTED", message: "The task type is not registered with this worker.", requestId } }, 400);
  }

  if (request.method === "POST" && url.pathname === "/internal/probe") {
    if (!env.AEVO_WORKER_SHARED_SECRET?.trim()) {
      return json(requestId, { error: { code: "WORKER_NOT_CONFIGURED", message: "Worker authentication is not configured.", requestId } }, 503);
    }
    if (request.headers.get("x-aevo-worker-token") !== env.AEVO_WORKER_SHARED_SECRET) {
      return json(requestId, { error: { code: "UNAUTHORIZED", message: "Worker authentication failed.", requestId } }, 401);
    }
    return probeApplications(env, requestId);
  }

  if (request.method === "POST" && url.pathname === "/internal/place-source/normalize") {
    return normalizePlaceSource(request, env, requestId);
  }

  return json(requestId, { error: { code: "NOT_FOUND", message: "Route not found.", requestId } }, 404);
}

async function readBody(request: IncomingMessage): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of request) {
    chunks.push(typeof chunk === "string" ? new TextEncoder().encode(chunk) : chunk);
  }
  const totalLength = chunks.reduce((total, chunk) => total + chunk.byteLength, 0);
  const body = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function nodeRequest(request: IncomingMessage, body: Uint8Array): Request {
  const protocol = request.headers["x-forwarded-proto"] === "https" ? "https" : "http";
  const host = request.headers.host ?? "localhost";
  const headers = new Headers();
  for (const [key, value] of Object.entries(request.headers)) {
    if (Array.isArray(value)) headers.set(key, value.join(", "));
    else if (value !== undefined) headers.set(key, value);
  }
  return new Request(`${protocol}://${host}${request.url ?? "/"}`, {
    method: request.method ?? "GET",
    headers,
    body: request.method === "GET" || request.method === "HEAD" ? undefined : body
  });
}

async function writeResponse(response: Response, nodeResponse: ServerResponse): Promise<void> {
  nodeResponse.statusCode = response.status;
  response.headers.forEach((value, key) => nodeResponse.setHeader(key, value));
  const body = new Uint8Array(await response.arrayBuffer());
  nodeResponse.end(body);
}

export function startServer(env: Env): ReturnType<typeof createServer> {
  const server = createServer(async (request, response) => {
    try {
      const body = await readBody(request);
      await writeResponse(await handleRequest(nodeRequest(request, body), env), response);
    } catch (error) {
      console.error("aevo.background_worker.request_failed", error);
      response.statusCode = 500;
      response.end("Internal Server Error");
    }
  });
  const port = Number(process.env.PORT ?? "8080");
  server.listen(port, "0.0.0.0");
  return server;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  startServer({
    AEVO_ENVIRONMENT: process.env.AEVO_ENVIRONMENT,
    AEVO_DATABASE_URL: process.env.AEVO_DATABASE_URL,
    AEVO_PUBSUB_TOPIC: process.env.AEVO_PUBSUB_TOPIC,
    AEVO_TASK_QUEUE: process.env.AEVO_TASK_QUEUE,
    AEVO_WORKER_SHARED_SECRET: process.env.AEVO_WORKER_SHARED_SECRET,
    AEVO_FEED_EVENT_WORKER_TOKEN: process.env.AEVO_FEED_EVENT_WORKER_TOKEN,
    AEVO_CORE_API_ORIGIN: process.env.AEVO_CORE_API_ORIGIN,
    SUPABASE_URL: process.env.SUPABASE_URL,
    SUPABASE_SECRET_KEY: process.env.SUPABASE_SECRET_KEY,
    AEVO_CONNECTION_PROBE_TOKEN: process.env.AEVO_CONNECTION_PROBE_TOKEN,
    AEVO_PROBE_TARGETS: process.env.AEVO_PROBE_TARGETS,
    AEVO_EVENT_MAX_ATTEMPTS: process.env.AEVO_EVENT_MAX_ATTEMPTS,
    AEVO_PLACE_SOURCE_MODE: process.env.AEVO_PLACE_SOURCE_MODE as Env["AEVO_PLACE_SOURCE_MODE"]
  });
}
