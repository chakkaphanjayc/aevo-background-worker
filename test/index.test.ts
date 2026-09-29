import { describe, expect, test } from "bun:test";
import { createHmac } from "node:crypto";
import { handleRequest } from "../src/index.js";

describe("aevo-background-worker", () => {
  test("keeps liveness available before eventing is provisioned", async () => {
    const response = await handleRequest(new Request("http://worker.test/health"), {});
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ status: "healthy", service: "aevo-background-worker" });
  });

  test("fails non-production-like readiness closed when Feed delivery wiring is incomplete", async () => {
    const response = await handleRequest(new Request("http://worker.test/ready"), {
      AEVO_ENVIRONMENT: "nonprod",
      AEVO_DATABASE_URL: "postgres://database",
      AEVO_PUBSUB_TOPIC: "projects/test/topics/events",
      AEVO_TASK_QUEUE: "projects/test/locations/asia-southeast1/queues/feed",
      AEVO_WORKER_SHARED_SECRET: "worker-shared-secret"
    });

    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({
      error: {
        code: "WORKER_NOT_CONFIGURED",
        missingConfiguration: ["AEVO_CORE_API_ORIGIN", "AEVO_FEED_EVENT_WORKER_TOKEN"]
      }
    });
  });

  test("reports worker ready when production Feed delivery wiring is complete", async () => {
    const response = await handleRequest(new Request("http://worker.test/ready"), {
      AEVO_ENVIRONMENT: "production",
      AEVO_DATABASE_URL: "postgres://database",
      AEVO_PUBSUB_TOPIC: "projects/test/topics/events",
      AEVO_TASK_QUEUE: "projects/test/locations/asia-southeast1/queues/feed",
      AEVO_WORKER_SHARED_SECRET: "worker-shared-secret",
      AEVO_CORE_API_ORIGIN: "https://core.test",
      AEVO_FEED_EVENT_WORKER_TOKEN: "feed-event-worker-token"
    });

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ status: "ready", service: "aevo-background-worker" });
  });

  test("keeps profile projection jobs disabled until Supabase worker credentials exist", async () => {
    const response = await handleRequest(
      new Request("http://worker.test/internal/tasks", {
        method: "POST",
        headers: { "x-aevo-worker-token": "worker-secret", "content-type": "application/json" },
        body: JSON.stringify({
          taskType: "TRACEDEE_PROFILE_PROJECTION_PROCESS",
          jobId: "00000000-0000-4000-8000-000000000001"
        })
      }),
      { AEVO_WORKER_SHARED_SECRET: "worker-secret" }
    );
    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({ error: { code: "PROFILE_PROJECTION_UNAVAILABLE" }, retryable: true });
  });

  test("rejects media cleanup tasks with an invalid asset id", async () => {
    const response = await handleRequest(
      new Request("http://worker.test/internal/tasks", {
        method: "POST",
        headers: { "x-aevo-worker-token": "worker-secret", "content-type": "application/json" },
        body: JSON.stringify({ taskType: "TRACEDEE_MEDIA_OBJECT_CLEANUP", assetId: "not-an-asset" })
      }),
      { AEVO_WORKER_SHARED_SECRET: "worker-secret" }
    );
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ error: { code: "INVALID_MEDIA_CLEANUP_ASSET" } });
  });

  test("deletes only the private media paths returned by the server cleanup plan", async () => {
    const originalFetch = globalThis.fetch;
    const calls: Array<{ url: string; method: string; authorization: string | null }> = [];
    const assetId = "00000000-0000-4000-8000-000000000010";
    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const headers = new Headers(init?.headers);
      calls.push({ url, method: init?.method ?? "GET", authorization: headers.get("authorization") });
      if (url.endsWith("/rpc/tracedee_get_media_cleanup_plan")) {
        return Response.json({
          ok: true,
          assetId,
          bucketId: "tracedee-quarantine",
          status: "DELETED",
          version: 4,
          cleanupRequired: true,
          cleanupPaths: ["quarantine/test/file.webp", "approved/test/file.webp"]
        });
      }
      if (url.includes("/storage/v1/object/tracedee-quarantine/")) return Response.json({});
      return Response.json({ ok: true, assetId, status: "DELETED", version: 4, changed: true });
    }) as typeof globalThis.fetch;

    try {
      const response = await handleRequest(
        new Request("http://worker.test/internal/tasks", {
          method: "POST",
          headers: { "x-aevo-worker-token": "worker-secret", "content-type": "application/json" },
          body: JSON.stringify({ taskType: "TRACEDEE_MEDIA_OBJECT_CLEANUP", assetId })
        }),
        {
          AEVO_WORKER_SHARED_SECRET: "worker-secret",
          SUPABASE_URL: "https://project.supabase.co",
          SUPABASE_SECRET_KEY: "sb_secret_test"
        }
      );

      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ success: true, assetId, cleaned: true });
      expect(calls.map((call) => call.url)).toEqual([
        "https://project.supabase.co/rest/v1/rpc/tracedee_get_media_cleanup_plan",
        "https://project.supabase.co/storage/v1/object/tracedee-quarantine/quarantine/test/file.webp",
        "https://project.supabase.co/storage/v1/object/tracedee-quarantine/approved/test/file.webp",
        "https://project.supabase.co/rest/v1/rpc/tracedee_complete_media_cleanup"
      ]);
      expect(calls.slice(1).every((call) => call.authorization === "Bearer sb_secret_test")).toBe(true);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("fails closed when a cleanup plan names another bucket", async () => {
    const originalFetch = globalThis.fetch;
    let storageCalled = false;
    const assetId = "00000000-0000-4000-8000-000000000011";
    globalThis.fetch = (async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/rpc/tracedee_get_media_cleanup_plan")) {
        return Response.json({
          ok: true,
          assetId,
          bucketId: "unexpected-bucket",
          status: "DELETED",
          version: 2,
          cleanupRequired: true,
          cleanupPaths: ["quarantine/test/file.webp"]
        });
      }
      storageCalled = true;
      return Response.json({});
    }) as typeof globalThis.fetch;

    try {
      const response = await handleRequest(
        new Request("http://worker.test/internal/tasks", {
          method: "POST",
          headers: { "x-aevo-worker-token": "worker-secret", "content-type": "application/json" },
          body: JSON.stringify({ taskType: "TRACEDEE_MEDIA_OBJECT_CLEANUP", assetId })
        }),
        {
          AEVO_WORKER_SHARED_SECRET: "worker-secret",
          SUPABASE_URL: "https://project.supabase.co",
          SUPABASE_SECRET_KEY: "sb_secret_test"
        }
      );

      expect(response.status).toBe(422);
      expect(await response.json()).toMatchObject({ error: { code: "MEDIA_CLEANUP_BUCKET_UNSUPPORTED" }, retryable: false });
      expect(storageCalled).toBe(false);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("forwards a validated profile projection enqueue task to the private Supabase RPC", async () => {
    const originalFetch = globalThis.fetch;
    let forwarded: { url: string; headers: Headers; body: unknown } | null = null;
    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      forwarded = {
        url: String(input),
        headers: new Headers(init?.headers),
        body: JSON.parse(String(init?.body)) as unknown
      };
      return Response.json({ ok: true, jobId: "00000000-0000-4000-8000-000000000002", status: "PENDING", deduped: false });
    }) as typeof globalThis.fetch;

    try {
      const response = await handleRequest(
        new Request("http://worker.test/internal/tasks", {
          method: "POST",
          headers: { "x-aevo-worker-token": "worker-secret", "content-type": "application/json" },
          body: JSON.stringify({
            taskType: "TRACEDEE_PROFILE_PROJECTION_ENQUEUE",
            profileId: "00000000-0000-4000-8000-000000000003",
            operation: "REBUILD",
            reason: "test projection rebuild",
            requestKey: "profile-rebuild-001",
            requestHash: "sha256:test-profile-rebuild"
          })
        }),
        {
          AEVO_WORKER_SHARED_SECRET: "worker-secret",
          SUPABASE_URL: "https://project.supabase.co",
          SUPABASE_SECRET_KEY: "sb_secret_test"
        }
      );

      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ success: true, result: { jobId: "00000000-0000-4000-8000-000000000002" } });
      expect(forwarded).toMatchObject({
        url: "https://project.supabase.co/rest/v1/rpc/tracedee_enqueue_profile_projection_job",
        body: {
          p_profile_id: "00000000-0000-4000-8000-000000000003",
          p_operation: "REBUILD",
          p_reason: "test projection rebuild",
          p_request_key: "profile-rebuild-001",
          p_request_hash: "sha256:test-profile-rebuild"
        }
      });
      const request = forwarded as unknown as { headers: Headers };
      expect(request.headers.get("authorization")).toBe("Bearer sb_secret_test");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("keeps a retryable profile projection failure retryable for the task platform", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => Response.json({
      ok: false,
      jobId: "00000000-0000-4000-8000-000000000004",
      status: "PENDING",
      retryable: true,
      error: "temporary database failure"
    })) as unknown as typeof globalThis.fetch;

    try {
      const response = await handleRequest(
        new Request("http://worker.test/internal/tasks", {
          method: "POST",
          headers: { "x-aevo-worker-token": "worker-secret", "content-type": "application/json" },
          body: JSON.stringify({
            taskType: "TRACEDEE_PROFILE_PROJECTION_PROCESS",
            jobId: "00000000-0000-4000-8000-000000000004"
          })
        }),
        {
          AEVO_WORKER_SHARED_SECRET: "worker-secret",
          SUPABASE_URL: "https://project.supabase.co",
          SUPABASE_SECRET_KEY: "sb_secret_test"
        }
      );

      expect(response.status).toBe(503);
      expect(await response.json()).toMatchObject({ success: false, retryable: true });
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("does not acknowledge events without a shared secret", async () => {
    const response = await handleRequest(
      new Request("http://worker.test/internal/pubsub", { method: "POST", body: JSON.stringify({ eventId: "1", eventType: "test" }) }),
      { AEVO_DATABASE_URL: "postgres://redacted", AEVO_PUBSUB_TOPIC: "topic", AEVO_TASK_QUEUE: "queue" }
    );
    expect(response.status).toBe(503);
  });

  test("forwards an accepted Feed event to the Core consumer before acknowledging", async () => {
    const originalFetch = globalThis.fetch;
    let forwarded: { url: string; headers: Headers; body: unknown } | null = null;
    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      forwarded = {
        url: String(input),
        headers: new Headers(init?.headers),
        body: JSON.parse(String(init?.body)) as unknown
      };
      return Response.json({ claimed: 1, processed: 1, duplicates: 0, failed: 0, deadLettered: 0 });
    }) as typeof globalThis.fetch;

    try {
      const response = await handleRequest(
        new Request("http://worker.test/internal/pubsub", {
          method: "POST",
          headers: { "x-aevo-worker-token": "pubsub-secret", "x-request-id": "feed-request-1" },
          body: JSON.stringify({ eventId: "feed-event-1", eventType: "FEED_EVENT_ACCEPTED" })
        }),
        {
          AEVO_WORKER_SHARED_SECRET: "pubsub-secret",
          AEVO_FEED_EVENT_WORKER_TOKEN: "core-feed-secret",
          AEVO_CORE_API_ORIGIN: "https://core.test"
        }
      );

      expect(response.status).toBe(200);
      expect(forwarded).toMatchObject({
        url: "https://core.test/internal/feed/events/consume?consumer=aevo-background-worker&limit=50",
        body: { eventId: "feed-event-1", eventType: "FEED_EVENT_ACCEPTED" }
      });
      const forwardedRequest = forwarded as unknown as { headers: Headers };
      expect(forwardedRequest.headers.get("x-aevo-worker-token")).toBe("core-feed-secret");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("accepts a Pub/Sub envelope and retries a transient Core failure before ack", async () => {
    const originalFetch = globalThis.fetch;
    let calls = 0;
    globalThis.fetch = (async () => {
      calls += 1;
      if (calls === 1) return new Response("unavailable", { status: 503 });
      return Response.json({ claimed: 1, processed: 1, duplicates: 0, failed: 0, deadLettered: 0 });
    }) as unknown as typeof globalThis.fetch;
    try {
      const encoded = btoa(JSON.stringify({
        schemaVersion: "v1",
        eventId: "pubsub-event-1",
        eventType: "FEED_EVENT_ACCEPTED",
        organizationId: "00000000-0000-4000-8000-000000000001"
      }));
      const response = await handleRequest(
        new Request("http://worker.test/internal/pubsub", {
          method: "POST",
          headers: { "x-aevo-worker-token": "worker-secret" },
          body: JSON.stringify({ message: { messageId: "message-1", data: encoded } })
        }),
        {
          AEVO_WORKER_SHARED_SECRET: "worker-secret",
          AEVO_FEED_EVENT_WORKER_TOKEN: "core-feed-secret",
          AEVO_CORE_API_ORIGIN: "https://core.test",
          AEVO_EVENT_MAX_ATTEMPTS: "2"
        }
      );
      expect(response.status).toBe(200);
      expect(calls).toBe(2);
      expect(await response.json()).toMatchObject({ acknowledged: true, eventId: "pubsub-event-1", attempts: 2 });
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("does not acknowledge a poison delivery after bounded retries", async () => {
    const originalFetch = globalThis.fetch;
    let calls = 0;
    globalThis.fetch = (async () => {
      calls += 1;
      return Response.json({ claimed: 1, processed: 0, duplicates: 0, failed: 1, deadLettered: 0 });
    }) as unknown as typeof globalThis.fetch;
    try {
      const response = await handleRequest(
        new Request("http://worker.test/internal/pubsub", {
          method: "POST",
          headers: { "x-aevo-worker-token": "worker-secret" },
          body: JSON.stringify({ eventId: "poison-event-1", eventType: "FEED_EVENT_ACCEPTED" })
        }),
        {
          AEVO_WORKER_SHARED_SECRET: "worker-secret",
          AEVO_FEED_EVENT_WORKER_TOKEN: "core-feed-secret",
          AEVO_CORE_API_ORIGIN: "https://core.test",
          AEVO_EVENT_MAX_ATTEMPTS: "2"
        }
      );
      expect(response.status).toBe(503);
      expect(calls).toBe(2);
      expect(await response.json()).toMatchObject({ acknowledged: false, retryable: true, attempts: 2 });
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("rejects malformed event envelopes before contacting Core", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (() => { throw new Error("Core must not be called"); }) as unknown as typeof globalThis.fetch;
    try {
      const response = await handleRequest(
        new Request("http://worker.test/internal/pubsub", {
          method: "POST",
          headers: { "x-aevo-worker-token": "worker-secret" },
          body: JSON.stringify({ eventId: "not-a-uuid", eventType: "UNKNOWN" })
        }),
        { AEVO_WORKER_SHARED_SECRET: "worker-secret" }
      );
      expect(response.status).toBe(400);
      expect(await response.json()).toMatchObject({ error: { code: "INVALID_EVENT_ENVELOPE" }, acknowledged: false });
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("keeps connection probes disabled until targets and Core API are configured", async () => {
    const response = await handleRequest(
      new Request("http://worker.test/internal/probe", {
        method: "POST",
        headers: { "x-aevo-worker-token": "worker-secret" }
      }),
      { AEVO_WORKER_SHARED_SECRET: "worker-secret" }
    );
    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({ error: { code: "CONNECTION_PROBE_NOT_CONFIGURED" } });
  });

  test("records a compatible signed application handshake", async () => {
    const originalFetch = globalThis.fetch;
    let recorded: Record<string, unknown> | null = null;
    const mockedFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url === "https://pos.test/.well-known/aevo-handshake") {
        const headers = new Headers(init?.headers);
        const timestamp = headers.get("x-aevo-handshake-timestamp") ?? "";
        const nonce = headers.get("x-aevo-handshake-nonce") ?? "";
        const signature = createHmac("sha256", "handshake-secret")
          .update([timestamp, "GET", "/.well-known/aevo-handshake", nonce, "POS"].join("\n"))
          .digest("hex");
        expect(headers.get("x-aevo-handshake-signature")).toBe(signature);
        return Response.json({
          protocol: "aevo.application-handshake",
          protocolVersion: "1",
          appCode: "POS",
          contractVersion: "v1",
          environment: "development",
          status: "ready",
          capabilities: ["catalog"],
          checkedAt: new Date().toISOString()
        });
      }
      if (url === "https://core.test/internal/application-connections/probe") {
        recorded = JSON.parse(String(init?.body)) as Record<string, unknown>;
        return new Response(null, { status: 200 });
      }
      throw new Error(`Unexpected fetch: ${url}`);
    }) as typeof globalThis.fetch;
    globalThis.fetch = mockedFetch;

    try {
      const response = await handleRequest(
        new Request("http://worker.test/internal/probe", {
          method: "POST",
          headers: { "x-aevo-worker-token": "worker-secret" }
        }),
        {
          AEVO_WORKER_SHARED_SECRET: "worker-secret",
          AEVO_CONNECTION_PROBE_TOKEN: "probe-token",
          AEVO_CORE_API_ORIGIN: "https://core.test",
          AEVO_HANDSHAKE_SHARED_SECRET: "handshake-secret",
          AEVO_PROBE_TARGETS: JSON.stringify([{ appCode: "POS", baseUrl: "https://pos.test" }])
        }
      );
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ results: [{ appCode: "POS", status: "connected", compatibilityStatus: "COMPATIBLE" }] });
      expect(recorded).toMatchObject({
        appCode: "POS",
        status: "connected",
        metadata: { compatibilityStatus: "COMPATIBLE", compatibilityReason: "HANDSHAKE_COMPATIBLE" }
      });
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("keeps Place source normalization disabled until an approved source is configured", async () => {
    const response = await handleRequest(
      new Request("http://worker.test/internal/place-source/normalize", {
        method: "POST",
        headers: {
          "x-aevo-worker-token": "worker-secret",
          "content-type": "application/json"
        },
        body: "{}"
      }),
      { AEVO_WORKER_SHARED_SECRET: "worker-secret" }
    );
    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({ error: { code: "PLACE_SOURCE_DISABLED" } });
  });

  test("returns a deterministic review-only Place source plan", async () => {
    const body = {
      runId: "run-1",
      observedAt: "2026-09-25T00:00:00Z",
      source: {
        sourceKind: "government",
        namespace: "fixture",
        sourceVersion: "v1",
        sourceUrl: null,
        license: "fixture-terms",
        attributionText: "Fixture",
        termsApproved: true,
        approvedAt: "2026-09-25T00:00:00Z",
        rawRetentionUntil: null
      },
      records: [{
        externalId: "one",
        name: "One",
        geometry: { type: "Point", coordinates: [100.5, 13.7] },
        categoryId: "venue",
        categoryLabel: "Venue",
        observedAt: "2026-09-25T00:00:00Z"
      }]
    };
    const makeRequest = () => new Request("http://worker.test/internal/place-source/normalize", {
      method: "POST",
      headers: {
        "x-aevo-worker-token": "worker-secret",
        "content-type": "application/json"
      },
      body: JSON.stringify(body)
    });
    const env = { AEVO_WORKER_SHARED_SECRET: "worker-secret", AEVO_PLACE_SOURCE_MODE: "fixture" as const };
    const first = await handleRequest(makeRequest(), env);
    const firstPayload = await first.json() as { plan: { replayFingerprint: string }; publishable: boolean; reviewRequired: boolean };
    const second = await handleRequest(makeRequest(), env);
    const secondPayload = await second.json() as { plan: { replayFingerprint: string } };

    expect(first.status).toBe(200);
    expect(firstPayload.publishable).toBe(false);
    expect(firstPayload.reviewRequired).toBe(true);
    expect(firstPayload.plan.replayFingerprint).toBe(secondPayload.plan.replayFingerprint);
  });
});
