-- Applied to the driver project on 2026-11-17. Preserve all existing app labels.
begin;
alter table public.driver_messages drop constraint if exists driver_messages_original_language_check;
alter table public.driver_messages add constraint driver_messages_original_language_check check (original_language = any (array['English','Spanish','French','Russian','Ukrainian','Portuguese','Polish','Arabic','Romanian','German','Italian','Turkish','Hindi','Punjabi','Chinese','Vietnamese','Unknown','Español','Français','Українська','Русский']::text[]));
alter table public.driver_messages drop constraint if exists driver_messages_translated_language_check;
alter table public.driver_messages add constraint driver_messages_translated_language_check check (translated_language is null or translated_language = any (array['English','Spanish','French','Russian','Ukrainian','Portuguese','Polish','Arabic','Romanian','German','Italian','Turkish','Hindi','Punjabi','Chinese','Vietnamese','Español','Français','Українська','Русский']::text[]));
commit;
