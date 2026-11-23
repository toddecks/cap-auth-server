create table public.driver_survey_invitations (
 id uuid primary key default gen_random_uuid(),
 campaign text not null default 'driver-checkin-20261123' check(campaign='driver-checkin-20261123'),
 token_hash text not null unique check(token_hash ~ '^[a-f0-9]{64}$'),
 phone_e164 text not null check(phone_e164 ~ '^\+[1-9][0-9]{7,14}$'),
 driver_name text not null default '', driver_company text not null default '',
 last_checked_in_at timestamptz not null,
 created_at timestamptz not null default now(), expires_at timestamptz not null default (now()+interval '90 days'),
 sent_at timestamptz, provider_message_id text, delivery_status text not null default 'prepared',
 unique(campaign,phone_e164)
);
create table public.driver_survey_responses (
 invitation_id uuid primary key references public.driver_survey_invitations(id),
 easy_to_use text not null check(easy_to_use in ('Yes','No')),
 clear_instructions text not null check(clear_instructions in ('Yes','No')),
 preferred_method text not null check(preferred_method in ('Text message','Phone call','No preference')),
 notes text not null default '' check(char_length(notes)<=2000),
 submitted_at timestamptz not null default now()
);
alter table public.driver_survey_invitations enable row level security;
alter table public.driver_survey_responses enable row level security;
revoke all on public.driver_survey_invitations,public.driver_survey_responses from public,anon,authenticated;
grant select,insert,update on public.driver_survey_invitations,public.driver_survey_responses to service_role;
comment on table public.driver_survey_responses is 'Driver-linked survey responses. No direct client access; owner-only API authorization. No notifications to Shipping.';
