-- agent_usage reads only spans that carry gen_ai.* attributes, a small share of
-- all spans. This partial index lets it skip the rest instead of reading each
-- span's attributes from the heap.
--
-- "on only spans" builds nothing on the existing partitions, so the migration
-- does not hold up span writes on a large table. Partitions created from now on
-- get the index automatically. The existing partitions from today on (today's
-- and the empty days ahead) get it here; older days stay without it until
-- retention drops them.
create index spans_gen_ai_idx on only spans (project, start_ts)
  where (
    attributes ? 'gen_ai.operation.name'
    or attributes ? 'gen_ai.request.model'
    or attributes ? 'gen_ai.provider.name'
    or attributes ? 'gen_ai.system'
  );

do $$
declare
  r record;
  child_index text;
begin
  for r in
    select c.relname
      from pg_inherits i join pg_class c on c.oid = i.inhrelid
     where i.inhparent = 'spans'::regclass
       and c.relname ~ '^spans_\d{8}$'
       and to_date(substring(c.relname from '(\d{8})$'), 'YYYYMMDD') >= (now() at time zone 'UTC')::date
  loop
    child_index := r.relname || '_gen_ai_idx';
    execute format(
      $sql$create index %I on %I (project, start_ts)
             where (
               attributes ? 'gen_ai.operation.name'
               or attributes ? 'gen_ai.request.model'
               or attributes ? 'gen_ai.provider.name'
               or attributes ? 'gen_ai.system'
             )$sql$,
      child_index, r.relname);
    execute format('alter index spans_gen_ai_idx attach partition %I', child_index);
  end loop;
end $$;
