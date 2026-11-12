begin;
alter table public.driver_conversations add column if not exists purged_at timestamptz;
alter table public.driver_conversations add column if not exists purged_attachment_paths text[];
-- Keep only the internal visit reference so historical arrivals/check-ins survive.
-- Conversation contents are physically erased; this is not a restoreable archive.
create or replace function public.shipping_purge_closed_conversation(target_id uuid)
returns jsonb language plpgsql security invoker set search_path='' as $$
declare c public.driver_conversations; paths text[];
begin
 select * into c from public.driver_conversations where id=target_id for update;
 if not found then return jsonb_build_object('deleted',false); end if;
 if c.status<>'closed' then raise exception 'Only closed conversations can be permanently deleted.' using errcode='22023'; end if;
 if c.purged_at is not null then return jsonb_build_object('deleted',true,'attachmentPaths',coalesce(c.purged_attachment_paths,'{}'::text[])); end if;
 select coalesce(array_agg(distinct attachment_path) filter(where attachment_path is not null),'{}'::text[]) into paths from public.driver_messages where conversation_id=target_id;
 delete from public.driver_messages where conversation_id=target_id;
 update public.driver_conversations set purged_at=clock_timestamp(),purged_attachment_paths=paths where id=target_id;
 return jsonb_build_object('deleted',true,'attachmentPaths',paths);
end $$;
revoke all on function public.shipping_purge_closed_conversation(uuid) from public,anon,authenticated;
grant execute on function public.shipping_purge_closed_conversation(uuid) to service_role;
-- Preserve the existing view's columns, grants, security options and staff scope.
do $$ declare definition text; begin
 definition:=pg_get_viewdef('public.shipping_conversation_feed'::regclass,true);
 if position('purge_guard.purged_at' in definition)=0 then
 execute 'create or replace view public.shipping_conversation_feed as select existing.* from ('||rtrim(definition,E';\n ')||') existing join public.driver_conversations purge_guard on purge_guard.id=existing.conversation_id where purge_guard.purged_at is null';
 end if;
end $$;
commit;
