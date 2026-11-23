-- Private ambiguity decisions: no SMS and no shared inbox messages.
create table if not exists public.driver_release_reviews (
 client_message_id text primary key,
 conversation_id text not null,
 sent_at timestamptz not null
);
create index if not exists driver_release_reviews_conversation_idx on public.driver_release_reviews(conversation_id);
alter table public.driver_release_reviews enable row level security;
revoke all on public.driver_release_reviews from public,anon,authenticated;
grant select,insert,update on public.driver_release_reviews to service_role;
