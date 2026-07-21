create table if not exists public.expansion_leads (
  id bigint generated always as identity primary key,
  submission_token uuid not null unique,
  submitted_at timestamptz not null default now(),
  name text not null,
  company text not null,
  email text not null,
  phone text not null,
  material_need text not null,
  opportunity_timing text not null,
  message text,
  page_url text,
  referrer text,
  visitor_id text,
  session_id text,
  ip_address text,
  user_agent text,
  email_status text not null default 'pending'
    check (email_status in ('pending', 'sent', 'failed')),
  email_sent_at timestamptz,
  resend_email_id text,
  email_error text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists expansion_leads_submitted_at_idx
  on public.expansion_leads (submitted_at desc);
create index if not exists expansion_leads_company_idx
  on public.expansion_leads (lower(company));
create index if not exists expansion_leads_email_idx
  on public.expansion_leads (lower(email));

alter table public.expansion_leads enable row level security;
revoke all on table public.expansion_leads from anon, authenticated;
grant all on table public.expansion_leads to service_role;
grant usage, select on sequence public.expansion_leads_id_seq to service_role;
