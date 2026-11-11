create table public.town_hall_submissions (
 submission_token uuid primary key,
 submitted_at timestamptz not null default now(),
 employee_name text check (employee_name is null or (length(btrim(employee_name)) between 1 and 160)),
 questions text not null default '' check(length(questions)<=6000),
 concerns text not null default '' check(length(concerns)<=6000),
 leadership_notes text not null default '' check(length(leadership_notes)<=6000),
 payload_hash text not null check(length(payload_hash)=64),
 email_sent_at timestamptz,
 email_provider_id text,
 constraint town_hall_has_response check(length(btrim(questions)||btrim(concerns)||btrim(leadership_notes))>0)
);
alter table public.town_hall_submissions enable row level security;
revoke all on public.town_hall_submissions from anon, authenticated;
grant select,insert,update on public.town_hall_submissions to service_role;
create index town_hall_submissions_submitted_at_idx on public.town_hall_submissions(submitted_at);
comment on table public.town_hall_submissions is 'Town Hall questions. Optional employee name; submissions and results are accessed only through the authorized forms server.';
