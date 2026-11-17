create table public.psdata_order_entry_sync_runs (
  id uuid primary key,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null check (status in ('running','complete','failed')),
  row_count integer not null default 0 check (row_count >= 0),
  source_start_date date not null,
  source_max_date date,
  error_code text
);
create index psdata_order_entry_complete_idx on public.psdata_order_entry_sync_runs (finished_at desc) where status='complete';

create table public.psdata_order_entries (
  sync_run_id uuid not null references public.psdata_order_entry_sync_runs(id) on delete cascade,
  setup_number bigint not null,
  entered_by text,
  entered_date date not null,
  source_input_date integer not null,
  source_status text,
  source_order_type text,
  primary key (sync_run_id, setup_number)
);
create index psdata_order_entries_date_idx on public.psdata_order_entries (sync_run_id,entered_date,setup_number);
alter table public.psdata_order_entry_sync_runs enable row level security;
alter table public.psdata_order_entries enable row level security;
revoke all on public.psdata_order_entry_sync_runs,public.psdata_order_entries from public,anon,authenticated;
grant all on public.psdata_order_entry_sync_runs,public.psdata_order_entries to service_role;
create policy service_sync_runs on public.psdata_order_entry_sync_runs to service_role using (true) with check (true);
create policy service_order_entries on public.psdata_order_entries to service_role using (true) with check (true);

create function public.finalize_psdata_order_entry_sync(run_id uuid, expected_rows integer)
returns void language plpgsql security invoker set search_path=pg_catalog,public as $$
declare actual_rows integer;
begin
  if expected_rows < 1 then raise exception 'Empty order-entry snapshot rejected'; end if;
  perform 1 from public.psdata_order_entry_sync_runs where id=run_id and status='running' for update;
  if not found then raise exception 'Running order-entry sync not found'; end if;
  select count(*) into actual_rows from public.psdata_order_entries where sync_run_id=run_id;
  if actual_rows <> expected_rows then raise exception 'Incomplete order-entry snapshot'; end if;
  update public.psdata_order_entry_sync_runs set status='complete',finished_at=now(),row_count=actual_rows where id=run_id;
  -- Keep two successful snapshots. Failed refreshes cannot replace the last good one.
  delete from public.psdata_order_entry_sync_runs where status='complete' and id not in (
    select id from public.psdata_order_entry_sync_runs where status='complete' order by finished_at desc,id desc limit 2
  );
end;
$$;
revoke all on function public.finalize_psdata_order_entry_sync(uuid,integer) from public,anon,authenticated;
grant execute on function public.finalize_psdata_order_entry_sync(uuid,integer) to service_role;
