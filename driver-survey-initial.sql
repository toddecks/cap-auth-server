create table public.driver_survey_initial_campaigns (
 id text primary key check(id='driver-checkin-20261123-initial'),
 approved_by uuid not null,approved_at timestamptz not null default now(),
 approved_body text not null,audience_count integer not null check(audience_count>=0)
);
create table public.driver_survey_initial_outbox (
 campaign_id text not null references public.driver_survey_initial_campaigns(id),
 phone_e164 text not null,invitation_id uuid not null references public.driver_survey_invitations(id),
 message_body text not null,status text not null default 'pending',
 provider_message_id text,error_code text,created_at timestamptz not null default now(),updated_at timestamptz not null default now(),
 primary key(campaign_id,phone_e164)
);
create index driver_survey_initial_pending_idx on public.driver_survey_initial_outbox(status,created_at);
alter table public.driver_survey_initial_campaigns enable row level security;
alter table public.driver_survey_initial_outbox enable row level security;
revoke all on public.driver_survey_initial_campaigns,public.driver_survey_initial_outbox from public,anon,authenticated;
grant select,insert,update on public.driver_survey_initial_campaigns,public.driver_survey_initial_outbox to service_role;
create or replace function public.prepare_driver_survey_initial(p_approved_by uuid,p_body text,p_audience jsonb)
returns integer language plpgsql security definer set search_path='' as $$
declare inserted text; item jsonb; invite_id uuid; total integer:=0;
begin
 if jsonb_typeof(p_audience)<>'array' or jsonb_array_length(p_audience)>10000 then raise exception 'Invalid survey audience'; end if;
 insert into public.driver_survey_initial_campaigns(id,approved_by,approved_body,audience_count)
 values('driver-checkin-20261123-initial',p_approved_by,p_body,jsonb_array_length(p_audience)) on conflict(id) do nothing returning id into inserted;
 if inserted is null then return (select count(*)::integer from public.driver_survey_initial_outbox where campaign_id='driver-checkin-20261123-initial'); end if;
 for item in select * from jsonb_array_elements(p_audience) loop
  select i.id into invite_id from public.driver_survey_invitations i where i.campaign='driver-checkin-20261123' and i.phone_e164=item->>'phone_e164';
  if invite_id is not null and exists(select 1 from public.driver_survey_responses r where r.invitation_id=invite_id) then continue; end if;
  if invite_id is null then
   insert into public.driver_survey_invitations(campaign,phone_e164,driver_name,driver_company,last_checked_in_at,token_hash,delivery_status)
   values('driver-checkin-20261123',item->>'phone_e164',coalesce(item->>'driver_name',''),coalesce(item->>'driver_company',''),(item->>'last_checked_in_at')::timestamptz,item->>'token_hash','prepared')
   on conflict(campaign,phone_e164) do update set token_hash=excluded.token_hash returning id into invite_id;
  else
   update public.driver_survey_invitations set token_hash=item->>'token_hash' where id=invite_id;
  end if;
  -- A public response may have completed while the invitation was being created.
  if exists(select 1 from public.driver_survey_responses r where r.invitation_id=invite_id) then continue; end if;
  insert into public.driver_survey_initial_outbox(campaign_id,phone_e164,invitation_id,message_body)
  values('driver-checkin-20261123-initial',item->>'phone_e164',invite_id,item->>'message_body');total:=total+1;
 end loop;
 return total;
end $$;
revoke all on function public.prepare_driver_survey_initial(uuid,text,jsonb) from public,anon,authenticated;
grant execute on function public.prepare_driver_survey_initial(uuid,text,jsonb) to service_role;
create or replace function public.claim_driver_survey_initial(p_limit integer default 8)
returns setof public.driver_survey_initial_outbox language sql security definer set search_path='' as $$
 with jobs as (
  select o.campaign_id,o.phone_e164 from public.driver_survey_initial_outbox o
  join public.driver_survey_initial_campaigns c on c.id=o.campaign_id
  where o.status='pending' order by o.created_at,o.phone_e164 for update of o skip locked limit least(greatest(p_limit,1),20)
 ) update public.driver_survey_initial_outbox o set status='sending',updated_at=clock_timestamp()
 from jobs j where o.campaign_id=j.campaign_id and o.phone_e164=j.phone_e164 returning o.*;
$$;
revoke all on function public.claim_driver_survey_initial(integer) from public,anon,authenticated;
grant execute on function public.claim_driver_survey_initial(integer) to service_role;
