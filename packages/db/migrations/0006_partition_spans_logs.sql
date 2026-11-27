-- Partition spans (by start_ts) and logs (by ts) by UTC day, like metric_points
-- in 0004, so retention drops whole days and the space goes back to the OS.
--
-- Both were expired with DELETE on the theory that they stay small. They did
-- not: on 2026-11-25 one instance's spans table reached 25 GB (about a million
-- spans a day, 30 days kept) and filled its disk. DELETE leaves the bytes
-- behind for the table to reuse; the file never shrinks.
--
-- Changes beyond the partitioning:
-- - spans' primary key gains start_ts (a partitioned table's unique keys must
--   include the partition key). A retried export carries the same start time,
--   so the upsert still deduplicates it.
-- - spans_project_trace_idx goes: the primary key (project, trace_id, ...)
--   already serves every lookup by trace.
-- - logs_attributes_gin_idx goes: no query can use it (they read single
--   keys with ->>, which a GIN index on the whole document does not serve),
--   and both production instances showed 0 scans.
-- - logs.id stays (readers follow logs in id order), as a plain sequence
--   default instead of an identity column, with an index rather than a key.
-- - Privileges granted on the old tables (a read-only role, say) are granted
--   again on the new ones.
--
-- The copy runs inside this migration, so it needs free disk about the size
-- of the two tables. For a table too large for that, see deploy/README.md
-- ("Large spans tables"): empty it into a compressed file first, run this
-- migration, then load the file back.

alter table spans rename to spans_legacy;
alter index spans_pkey rename to spans_legacy_pkey;
alter table logs rename to logs_legacy;
alter index logs_pkey rename to logs_legacy_pkey;
alter sequence logs_id_seq rename to logs_legacy_id_seq;

create table spans (
  project text not null references projects (name),
  trace_id text not null,
  span_id text not null,
  parent_span_id text,
  name text not null,
  kind text not null check (
    kind in ('unspecified', 'internal', 'server', 'client', 'producer', 'consumer')
  ),
  start_ts timestamptz not null,
  end_ts timestamptz not null,
  duration_ms double precision not null,
  status_code text,
  resource jsonb not null default '{}',
  attributes jsonb not null default '{}',
  http_status_code integer,
  primary key (project, trace_id, span_id, start_ts)
) partition by range (start_ts);

create sequence logs_id_seq;

create table logs (
  id bigint not null default nextval('logs_id_seq'),
  project text not null references projects (name),
  ts timestamptz not null,
  severity text,
  body text,
  trace_id text,
  span_id text,
  resource jsonb not null default '{}',
  attributes jsonb not null default '{}'
) partition by range (ts);

alter sequence logs_id_seq owned by logs.id;
select setval('logs_id_seq', coalesce((select max(id) from logs_legacy), 0) + 1, false);

-- Day partitions from the oldest row (or 31 days back, whichever is earlier,
-- so a table loaded back after this migration finds its days) to a week ahead.
-- Bounds are UTC midnights, matching partitionBounds() in src/retention.ts.
do $$
declare
  t record;
  day date;
  last_day date;
begin
  for t in
    select * from (values ('spans', 'spans_legacy', 'start_ts'), ('logs', 'logs_legacy', 'ts')) v(parent, legacy, col)
  loop
    execute format('select least(min(%I at time zone ''UTC'')::date, (now() at time zone ''UTC'')::date - 31) from %I', t.col, t.legacy)
      into day;
    day := coalesce(day, (now() at time zone 'UTC')::date - 31);
    execute format('select max(%I at time zone ''UTC'')::date from %I', t.col, t.legacy) into last_day;
    last_day := greatest(coalesce(last_day, day), (now() at time zone 'UTC')::date) + 7;
    while day <= last_day loop
      execute format(
        'create table %I partition of %I for values from (%L) to (%L)',
        t.parent || '_' || to_char(day, 'YYYYMMDD'),
        t.parent,
        (day::timestamp at time zone 'UTC'),
        ((day + 1)::timestamp at time zone 'UTC')
      );
      day := day + 1;
    end loop;
    execute format('create table %I partition of %I default', t.parent || '_default', t.parent);
  end loop;
end $$;

insert into spans (project, trace_id, span_id, parent_span_id, name, kind, start_ts, end_ts,
                   duration_ms, status_code, resource, attributes, http_status_code)
select project, trace_id, span_id, parent_span_id, name, kind, start_ts, end_ts,
       duration_ms, status_code, resource, attributes, http_status_code
from spans_legacy;

insert into logs (id, project, ts, severity, body, trace_id, span_id, resource, attributes)
select id, project, ts, severity, body, trace_id, span_id, resource, attributes
from logs_legacy;

-- Grant again whatever the old tables granted to other roles.
do $$
declare
  g record;
begin
  for g in
    select table_name, grantee, privilege_type
      from information_schema.role_table_grants
     where table_schema = 'public'
       and table_name in ('spans_legacy', 'logs_legacy')
       and grantee <> current_user
  loop
    execute format('grant %s on %I to %s', g.privilege_type,
                   replace(g.table_name, '_legacy', ''),
                   case when g.grantee = 'PUBLIC' then 'public' else quote_ident(g.grantee) end);
  end loop;
end $$;

drop table spans_legacy;
drop table logs_legacy;

-- Built after the copy, on the parents, so they cascade to every partition.
create index spans_project_start_idx on spans (project, start_ts desc);
create index spans_start_idx on spans (start_ts);
create index spans_project_http_status_idx on spans (project, http_status_code)
  where http_status_code is not null;
create index logs_id_idx on logs (id);
create index logs_project_ts_idx on logs (project, ts desc);
create index logs_project_trace_idx on logs (project, trace_id);
