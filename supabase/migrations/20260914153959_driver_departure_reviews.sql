-- New departures only. Server worker owns this queue; clients cannot enqueue texts.
create table public.driver_review_settings (
  id boolean primary key default true check (id),
  enabled_at timestamptz not null default now()
);
insert into public.driver_review_settings(id) values (true);
create table public.driver_review_requests (
  arrival_id bigint primary key references public.driver_sms_arrivals(id),
  conversation_id uuid not null references public.driver_conversations(id),
  phone text not null,
  status text not null default 'pending' check (status in ('pending','processing','sent','failed','uncertain')),
  created_at timestamptz not null default now(),
  claimed_at timestamptz,
  sent_at timestamptz,
  provider_message_id text,
  error_code text
);
alter table public.driver_review_settings enable row level security;
alter table public.driver_review_requests enable row level security;
revoke all on public.driver_review_settings, public.driver_review_requests from public, anon, authenticated;
grant select on public.driver_review_settings to service_role;
grant select,insert,update on public.driver_review_requests to service_role;

create function public.claim_driver_review_request()
returns setof public.driver_review_requests
language plpgsql security invoker set search_path = '' as $$
begin
  insert into public.driver_review_requests(arrival_id,conversation_id,phone)
  select a.id,a.conversation_id,a.phone_e164
  from public.driver_sms_arrivals a
  join public.driver_conversations c on c.id=a.conversation_id and c.channel='sms'
  cross join public.driver_review_settings s
  where a.departed_at >= s.enabled_at and a.departed_at <= now()
    and a.phone_e164 is not null
  on conflict (arrival_id) do nothing;

  -- Never re-send an uncertain external request after a server restart.
  update public.driver_review_requests set status='uncertain',error_code='worker_interrupted'
  where status='processing' and claimed_at < now()-interval '10 minutes';
  return query
  update public.driver_review_requests r set status='processing',claimed_at=now()
  where r.arrival_id in (
    select q.arrival_id from public.driver_review_requests q
    where q.status='pending' order by q.created_at,q.arrival_id
    for update skip locked limit 1
  ) returning r.*;
end;
$$;
revoke all on function public.claim_driver_review_request() from public,anon,authenticated;
grant execute on function public.claim_driver_review_request() to service_role;
