-- The last export ingest refused for each project, so a producer that keeps
-- sending something sensorium cannot store (a bad body, a payload over the
-- size cap, a content type it does not take) shows up in storage_stats
-- instead of only in the ingest container's log. Written at most every 30
-- seconds per project; a database outage (503) cannot be recorded here.
alter table projects
  add column if not exists last_rejected_at timestamptz,
  add column if not exists last_rejected_status integer,
  add column if not exists last_rejected_reason text;
