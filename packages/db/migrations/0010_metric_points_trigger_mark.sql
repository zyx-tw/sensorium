-- The metric_points view trigger (0008) marked a late bucket with "do nothing",
-- which takes no lock on a mark that already exists. A concurrent rollUpDirty
-- could then delete the mark and recompute before this insert committed, and
-- leave the point out. "do update" row-locks the mark, so the recompute waits,
-- the same as ingest since #49.
create or replace function metric_points_insert() returns trigger language plpgsql as $$
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
    insert into metric_rollup_dirty (bucket) values (date_bin('5 minutes', new.ts, timestamptz '2000-01-01'))
      on conflict (bucket) do update set bucket = excluded.bucket;
  end if;
  return new;
end $$;
