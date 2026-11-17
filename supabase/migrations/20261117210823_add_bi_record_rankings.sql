create function public.get_bi_record_rankings(p_period text, p_scope text, p_year integer, p_metrics text[])
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,public as $$
declare result jsonb;
begin
 if p_period not in ('day','month') or p_scope not in ('company','people') or (p_year is not null and p_year not between 2000 and 2100) then
  raise exception 'Invalid records filters';
 end if;
 with latest as (
  select id,finished_at from public.psdata_order_entry_sync_runs where status='complete' order by finished_at desc limit 1
 ), source as (
  select 'shipping' metric,ship_date as activity_date,coalesce(nullif(btrim(loaded_by_name),''),'Unassigned') person,
   case when shipping_tons>0 then shipping_tons else weight/2000 end value
  from public.psdata_loads where not coalesce(cancel_load,false)
  union all
  select 'production',processing_start_date,
   case machine_id when 4 then 'RBI' when 2 then '1/2 Line' else 'Line '||coalesce(machine_id::text,'unassigned') end,tag_tons
  from public.psdata_production_tags
  union all
  select 'appointments',created_date,coalesce(nullif(btrim(created_by),''),'Unassigned'),1
  from public.psdata_loads where not coalesce(cancel_load,false)
  union all
  select 'appointments',entered_date,coalesce(nullif(btrim(created_by),''),'Unassigned'),1
  from (select distinct on (coalesce(nullif(btrim(appt_number),''),id::text)) * from public.psdata_appt_in
   order by coalesce(nullif(btrim(appt_number),''),id::text),inserted_at desc,id desc) a
  where lower(coalesce(cancelled,'')) not in ('y','yes','1','true','t')
  union all
  select 'orders',entered_date,coalesce(nullif(btrim(entered_by),''),'Unassigned'),1
  from public.psdata_order_entries where sync_run_id=(select id from latest)
  union all
  select 'receiving',departure_date,coalesce(nullif(btrim(audited_by),''),'Unassigned'),tons
  from public.psdata_appt_in where lower(coalesce(cancelled,'')) not in ('y','yes','1','true','t')
 ), valid as (
  select *,case when p_scope='company' then 'CSP' else regexp_replace(person,'\s+',' ','g') end entity
  from source where metric=any(p_metrics) and activity_date between '2000-01-01'::date and (now() at time zone 'America/New_York')::date and value>0
 ), coverage as (
  select metric,min(activity_date) history_start,max(activity_date) source_through,count(*) source_count from valid group by metric
 ), totals as (
  select metric,date_trunc(p_period,activity_date)::date period_start,min(entity) entity,sum(value) value
  from valid where (p_year is null or extract(year from activity_date)=p_year)
   and (p_scope='company' or entity<>'Unassigned')
  group by metric,date_trunc(p_period,activity_date)::date,lower(entity)
 ), ranked as (
  select *,dense_rank() over(partition by metric order by value desc) rank,
   row_number() over(partition by metric order by value desc,period_start desc,entity) position
  from totals
 ), cards as (
  select c.*,coalesce((select jsonb_agg(to_jsonb(r)-'metric'-'position' order by r.position) from ranked r where r.metric=c.metric and position<=10),'[]'::jsonb) records
  from coverage c
 )
 select jsonb_build_object('period',p_period,'scope',p_scope,'year',p_year,
  'generatedAt',now(),'orderSyncedAt',(select finished_at from latest),
  'years',(select jsonb_agg(y order by y desc) from (select distinct extract(year from activity_date)::integer y from valid)t),
  'cards',coalesce((select jsonb_agg(to_jsonb(c)) from cards c),'[]'::jsonb)) into result;
 return result;
end;
$$;
revoke all on function public.get_bi_record_rankings(text,text,integer,text[]) from public,anon,authenticated;
grant execute on function public.get_bi_record_rankings(text,text,integer,text[]) to service_role;
