alter table public.pro_form_submissions
  add column if not exists idempotency_key text;

create unique index if not exists pro_form_submissions_idempotency_key_uidx
  on public.pro_form_submissions (idempotency_key)
  where idempotency_key is not null;
