begin;
create table if not exists public.shipping_release_verifications (
 conversation_id uuid primary key references public.driver_conversations(id) on delete cascade,
 session_key text not null,
 attempt_id text not null,
 release_number text not null,
 requested_at timestamptz not null default now(),
 verified_at timestamptz,
 verified_by uuid references auth.users(id)
);
alter table public.shipping_release_verifications enable row level security;
revoke all on public.shipping_release_verifications from anon, authenticated;
grant all on public.shipping_release_verifications to service_role;
commit;
select tablename,rowsecurity from pg_tables where schemaname='public' and tablename='shipping_release_verifications';
