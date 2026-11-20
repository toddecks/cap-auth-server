begin;
alter table public.driver_sms_contacts add column if not exists customer_name text;
create or replace function public.native_dropoff_reply()
returns trigger language plpgsql security definer set search_path = '' as $function$
declare
 c public.driver_conversations;
 local_time timestamp;
 message text;
begin
 if new.direction <> 'driver_to_shipping' or new.body not in ('Drop-off check-in','Drop-off and pick-up check-in') then return new; end if;
 select * into c from public.driver_conversations where id=new.conversation_id and user_id=new.sender_user_id
 and visit_type in ('dropoff','both') and channel='app' and status='open' and session_ended_at is null;
 if not found then return new; end if;
 if exists (select 1 from public.shipping_staff_checkins s where s.conversation_id=c.id and s.checked_in_at >= coalesce(c.session_started_at,c.created_at)) then return new; end if;
 local_time := coalesce((select occurred_at from public.driver_arrivals where id=c.arrival_id),c.session_started_at,c.created_at) at time zone 'America/New_York';
 if local_time::time < time '05:30' or local_time::time >= time '18:00' then
  message := 'Our shipping office is currently closed. Please call 419-269-9706, select 5 or stay on the line to be connected. Thank you';
 elsif c.visit_type='both' then
  message := 'Please call 419-269-9706 and select option 5 for further instructions.';
 else
  message := 'Dropping off coils or sheets?';
 end if;
 insert into public.driver_messages(client_message_id,conversation_id,related_arrival_id,direction,body,original_body,original_language,sent_at,delivery_status)
 values('dropoff-instructions:'||c.id::text||coalesce(':'||c.session_started_at::text,''),c.id,c.arrival_id,'shipping_to_driver',message,message,'English',clock_timestamp(),'sent')
 on conflict(client_message_id) do nothing;
 return new;
end
$function$;
revoke all on function public.native_dropoff_reply() from public, anon, authenticated;
commit;
