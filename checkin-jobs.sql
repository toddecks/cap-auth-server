-- Private durable instruction jobs. The official check-in stays in the driver database.
create table if not exists public.shipping_checkin_jobs (
 id text primary key,
 payload jsonb not null,
 status text not null default 'pending' check(status in ('pending','processing','complete','needs_review')),
 created_at timestamptz not null default now(),
 started_at timestamptz,
 finished_at timestamptz,
 error text
);
create index if not exists shipping_checkin_jobs_pending_idx on public.shipping_checkin_jobs(created_at) where status='pending';
alter table public.shipping_checkin_jobs enable row level security;
revoke all on public.shipping_checkin_jobs from public,anon,authenticated;
grant select,insert,update on public.shipping_checkin_jobs to service_role;
