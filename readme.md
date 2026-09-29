# Aevo Background Worker

The Background Worker consumes outbox events through Pub/Sub and schedules retryable commands through Cloud Tasks. It must persist an inbox/idempotency decision before acknowledging a message, and dead-letter handling must be observable.

The initial host exposes health/readiness, an authenticated Pub/Sub boundary
and an authenticated `/internal/probe` boundary. The probe runner checks only
operator-configured app health URLs and writes the observed result to Core API;
it never marks an app healthy without a successful response. It deliberately
returns `503 WORKER_NOT_CONFIGURED` until the relevant Cloud SQL/eventing or
probe dependencies are provisioned; it never acknowledges an event that it
cannot persist.

The authenticated `POST /internal/place-source/normalize` boundary is a
provider-neutral MAP-005 dry-run adapter. It requires an approved source/legal
packet, normalizes a bounded batch deterministically, reports disappeared source
records, and never assigns canonical Place IDs or publishes projections. It is
disabled by default with `AEVO_PLACE_SOURCE_MODE=disabled`; enabling it still
produces review-required plans only.

The authenticated `POST /internal/tasks` boundary also supports the controlled
TraceDee profile-projection lifecycle tasks
`TRACEDEE_PROFILE_PROJECTION_ENQUEUE` and
`TRACEDEE_PROFILE_PROJECTION_PROCESS`. They call only the service-role
Supabase RPCs when `SUPABASE_URL` and `SUPABASE_SECRET_KEY` are configured;
missing credentials fail closed with a retryable `503`. This boundary does not
enable a production schedule by itself, and the service-role key is never
returned to callers or sent to browser clients.

The same private task boundary supports
`TRACEDEE_MEDIA_OBJECT_CLEANUP`. It reads a server-generated cleanup plan,
deletes only `tracedee-quarantine` objects under the `quarantine/` or
`approved/` prefixes, and marks the metadata cleanup complete through an
idempotent service-role RPC. Unsupported buckets, unsafe paths, missing
credentials, and Storage failures fail closed; no public media URL is issued.

Required runtime settings for a managed environment are `AEVO_DATABASE_URL`,
`AEVO_PUBSUB_TOPIC`, `AEVO_TASK_QUEUE`, `AEVO_CORE_API_ORIGIN`,
`AEVO_WORKER_SHARED_SECRET`, and the separate `AEVO_FEED_EVENT_WORKER_TOKEN`.
For `nonprod`, `staging`, and `production`, `/ready` returns `503` with the
missing configuration names until the authenticated event delivery path is
complete; it does not acknowledge Feed events while the worker auth secret,
Core origin, or event token is missing.
