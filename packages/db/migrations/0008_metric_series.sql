-- Store each metric series once, and only (series_id, ts, value) per point (#25).
--
-- A metric_points row repeated metric_name, kind, attributes and resource:
-- about 313 bytes a row, 21 GB a day on the zyx instance, for about 7,600
-- distinct series an hour. A samples row is 24 bytes of data, about 90 bytes
-- on disk with its indexes.
--
-- No row is copied. The old table becomes metric_points_legacy and keeps its
-- day partitions until retention drops them. metric_points becomes a view over
-- both, so readers (golem's read-only role, the MCP tools) keep their queries
-- and their history. A later migration drops the legacy branch once it is empty.
--
-- metric_rollups holds 5-minute buckets per series (count, sum, min, max, last),
-- kept longer than raw samples. The retention job fills it before it drops a
-- samples partition.

-- When a project last sent a metric, kept by ingest at most every 30 s. golem
-- asks which projects report continuously; over the view that question would
-- read a day of points. Seeded with one index hop per project (a skip scan).
alter table projects add column last_metric_at timestamptz;
with recursive t as (
  (select project from metric_points where ts > now() - interval '1 day' order by project limit 1)
  union all
  select (select project from metric_points where project > t.project and ts > now() - interval '1 day' order by project limit 1)
    from t where t.project is not null
)
update projects set last_metric_at = now() where name in (select project from t where project is not null);

alter table metric_points rename to metric_points_legacy;

-- The one definition of a series' identity. jsonb's text form is canonical
-- (keys sorted, duplicates dropped), so key order never splits a series.
create function metric_series_identity(metric_name text, kind text, attributes jsonb, resource jsonb)
  returns bytea language sql immutable parallel safe
  return sha256(convert_to(jsonb_build_array(metric_name, kind, attributes, resource)::text, 'UTF8'));

create table metric_series (
  id bigint generated always as identity primary key,
  project text not null references projects (name),
  identity bytea not null,
  metric_name text not null,
  kind text not null check (kind in ('gauge', 'sum', 'histogram')),
  attributes jsonb not null default '{}',
  resource jsonb not null default '{}',
  created_at timestamptz not null default now(),
  unique (project, identity)
);
create index metric_series_project_name_idx on metric_series (project, metric_name);

create table metric_samples (
  series_id bigint not null,
  ts timestamptz not null,
  value double precision not null
) partition by range (ts);

create table metric_rollups (
  series_id bigint not null,
  bucket timestamptz not null,
  count integer not null,
  sum double precision not null,
  min double precision not null,
  max double precision not null,
  last double precision not null,
  primary key (series_id, bucket)
) partition by range (bucket);

-- How far metric_rollups is complete: every bucket before this instant is final.
create table metric_rollup_progress (
  id boolean primary key default true check (id),
  rolled_until timestamptz not null
);
insert into metric_rollup_progress (rolled_until) values (date_bin('5 minutes', now(), timestamptz '2000-01-01'));

-- Buckets that got points after they were rolled up (a producer that sends
-- late, or a backfill). Ingest adds them; the next rollup recomputes and clears them.
create table metric_rollup_dirty (
  bucket timestamptz primary key
);

-- Today and 7 days ahead, as UTC days (partitionBounds in src/retention.ts).
do $$
declare
  day date := (now() at time zone 'UTC')::date;
  t text;
begin
  foreach t in array array['metric_samples', 'metric_rollups'] loop
    for i in 0..7 loop
      execute format('create table %I partition of %I for values from (%L) to (%L)',
        t || '_' || to_char(day + i, 'YYYYMMDD'), t,
        ((day + i)::timestamp at time zone 'UTC'), ((day + i + 1)::timestamp at time zone 'UTC'));
    end loop;
    execute format('create table %I partition of %I default', t || '_default', t);
  end loop;
end $$;

create index metric_samples_series_ts_idx on metric_samples (series_id, ts desc);
-- The rollup job and golem's checks read recent points of every series. Points
-- arrive in time order, so a BRIN index answers that at a fraction of a btree's size.
create index metric_samples_ts_brin on metric_samples using brin (ts);

create view metric_points as
  select s.project, s.metric_name, p.ts, s.kind, p.value, s.attributes, s.resource
    from metric_samples p
    join metric_series s on s.id = p.series_id
  union all
  select project, metric_name, ts, kind, value, attributes, resource
    from metric_points_legacy;

-- An ingest from before 0008 (still running during a deploy, or a rolled-back
-- image) inserts into metric_points. Route those rows to the new tables.
create function metric_points_insert() returns trigger language plpgsql as $$
declare
  sid bigint;
  ident bytea := metric_series_identity(new.metric_name, new.kind, coalesce(new.attributes, '{}'), coalesce(new.resource, '{}'));
begin
  select id into sid from metric_series where project = new.project and identity = ident;
  if sid is null then
    insert into metric_series (project, identity, metric_name, kind, attributes, resource)
      values (new.project, ident, new.metric_name, new.kind, coalesce(new.attributes, '{}'), coalesce(new.resource, '{}'))
      on conflict (project, identity) do nothing;
    select id into sid from metric_series where project = new.project and identity = ident;
  end if;
  insert into metric_samples (series_id, ts, value) values (sid, new.ts, new.value);
  if new.ts < now() - interval '15 minutes' then
    insert into metric_rollup_dirty (bucket) values (date_bin('5 minutes', new.ts, timestamptz '2000-01-01')) on conflict do nothing;
  end if;
  return new;
end $$;
create trigger metric_points_insert instead of insert on metric_points
  for each row execute function metric_points_insert();

-- Whoever could read the old table can read the view (golem_ro). The view runs
-- with its owner's rights, so no grant on the new tables is needed for that.
do $$
declare
  r record;
begin
  for r in
    select distinct grantee from information_schema.role_table_grants
     where table_schema = current_schema() and table_name = 'metric_points_legacy'
       and privilege_type = 'SELECT' and grantee <> current_user
  loop
    execute format('grant select on metric_points to %s',
      case when r.grantee = 'PUBLIC' then 'public' else quote_ident(r.grantee) end);
    execute format('grant select on metric_series, metric_rollups to %s',
      case when r.grantee = 'PUBLIC' then 'public' else quote_ident(r.grantee) end);
  end loop;
end $$;
