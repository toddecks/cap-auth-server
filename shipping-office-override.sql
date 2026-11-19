-- Shared office notice override. Server service role only; staff authorization is enforced by the API.
create table if not exists public.shipping_office_override (
 id integer primary key check(id=1), closed_on date, activated_at timestamptz,
 updated_at timestamptz not null default now(), updated_by uuid
);
alter table public.shipping_office_override enable row level security;
revoke all on public.shipping_office_override from anon, authenticated;
grant select,insert,update on public.shipping_office_override to service_role;
insert into public.shipping_office_override(id) values(1) on conflict(id) do nothing;
