'use strict';
const {references}=require('./release-doors');
const normalize=v=>String(v||'').normalize('NFKC').toUpperCase().replace(/[^A-Z0-9]/g,'');
const localDay=ms=>new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(ms));
// PS timestamps are facility wall time, not UTC. Resolve the Eastern offset,
// then round-trip the wall time so invalid/DST-skipped times never close visits.
function easternTimestamp(date,time){
 const wall=`${date}T${String(time||'').slice(0,8)}`;
 if(!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/.test(wall))return NaN;
 const target=Date.parse(wall+'Z');if(!Number.isFinite(target))return NaN;
 const format=ms=>Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}).formatToParts(new Date(ms)).map(p=>[p.type,p.value]));
 const asWall=p=>`${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}:${p.second}`;
 let ms=target;
 for(let i=0;i<3;i++)ms+=target-Date.parse(asWall(format(ms))+'Z');
 return asWall(format(ms))===wall?ms:NaN;
}
function confirmedDeparture(visit,rows,now=Date.now()){
 if(visit.departed_at||!['pickup','both'].includes(visit.visit_type))return null;
 const entered=Date.parse(visit.entered_at),checked=Date.parse(visit.checked_in_at);
 if(!Number.isFinite(entered)||!Number.isFinite(checked)||checked<entered||checked>now)return null;
 const location={'csp-toledo-main':'PROSPY','csp-toledo-enterprise':'ENTPRS'}[visit.facility_id];
 const release=normalize(visit.release_number),digits=release.replace(/[A-Z]/g,'');
 if(!location||!digits)return null;
 const candidates=rows.filter(r=>normalize(r.location)===location&&r.cancelLoad!==true&&String(r.cancelLoad).toLowerCase()!=='true'&&
  r.arrivalDate===localDay(entered)&&(normalize(r.poRel)===release||references(r.poRel).includes(digits)));
 if(!candidates.length)return null;
 // Multiple BOLs could represent different trucks using the same release.
 const bols=new Set(candidates.map(r=>String(r.bolNumber||'').trim()));
 if(bols.size!==1||bols.has(''))return null;
 const times=candidates.map(r=>({arrival:easternTimestamp(r.arrivalDate,r.arrivalTime),ship:easternTimestamp(r.shipDate,r.shipTime)}));
 if(times.some(t=>!Number.isFinite(t.arrival)||!Number.isFinite(t.ship)||t.arrival<entered-15*60000||t.arrival>checked+15*60000||t.ship<checked||t.ship<t.arrival||t.ship>now))return null;
 // Conflicting snapshots are not confirmation of one departure.
 if(new Set(times.map(t=>t.ship)).size!==1||new Set(times.map(t=>t.arrival)).size!==1)return null;
 return {departedAt:new Date(times[0].ship).toISOString(),bolNumber:[...bols][0]};
}
function createWorker({db,chart,now=()=>Date.now(),candidates=require('./release-catalog').releaseCandidatesForMany}){
 const health={mode:'ps-shipped-departures-v2',state:'idle',lastChecked:null,confirmed:0,removed:0,totalRemoved:0,error:null};
 let running=false,timer;
 async function result(query){const r=await query;if(r.error)throw r.error;return r.data;}
 async function pages(table,build){const rows=[];for(let offset=0;;offset+=500){const page=await result(build(db.from(table).select('*')).order('id').range(offset,offset+499));rows.push(...page);if(page.length<500)return rows;}}
 async function appVisit(c){
  if(!c?.arrival_id||c.status!=='open'||c.session_ended_at)return null;
  const arrival=await result(db.from('driver_arrivals').select('*').eq('id',c.arrival_id).maybeSingle());
  if(!arrival||!['geofence_enter','manual_check_in'].includes(arrival.event_type))return null;
  const checked=await result(db.from('shipping_staff_checkins').select('checked_in_at').eq('conversation_id',c.id).maybeSingle());
  if(!checked||Date.parse(checked.checked_in_at)<Date.parse(c.session_started_at||c.created_at))return null;
  const exits=await result(db.from('driver_arrivals').select('id').eq('user_id',arrival.user_id).eq('facility_id',arrival.facility_id).gt('occurred_at',arrival.occurred_at).in('event_type',['geofence_exit','manual_check_out','manual_check_in']).limit(1));
  if(exits.length)return null;
  return {...arrival,arrival_id:arrival.id,conversation_id:c.id,entered_at:arrival.occurred_at,checked_in_at:checked.checked_in_at,release_number:c.release_number||arrival.release_number,visit_type:c.visit_type||arrival.visit_type,source:'app'};
 }
 async function activeVisits(){
  // Staff-only UI views deliberately exclude service identities. Read protected
  // base tables with the existing server service client instead of changing RLS.
  const [sms,conversations]=await Promise.all([
   pages('driver_sms_arrivals',q=>q.is('departed_at',null).not('checked_in_at','is',null).gte('entered_at',new Date(now()-30*86400000).toISOString())),
   pages('driver_conversations',q=>q.eq('channel','app').eq('status','open').is('session_ended_at',null))
  ]);
  const app=[];for(const c of conversations){const visit=await appVisit(c);if(visit&&Date.parse(visit.entered_at)>=now()-30*86400000)app.push(visit);}
  return [...sms.map(v=>({...v,arrival_id:v.id,source:'sms'})),...app];
 }
 async function tick(){
  if(running||!db||!chart)return;
  running=true;Object.assign(health,{state:'running',confirmed:0,removed:0,error:null});
  try{
   const visits=(await activeVisits()).filter(v=>['pickup','both'].includes(v.visit_type));
   health.activeVisits=visits.length;
   const loads=visits.length?await candidates(chart,visits.map(v=>v.release_number)):[];
   for(const visit of visits){
    const departure=confirmedDeparture(visit,loads,now());if(!departure)continue;health.confirmed++;
    // Recheck active state after source lookup. Do not overwrite manual checkout.
    const current=visit.source==='sms'
     ? await result(db.from('driver_sms_arrivals').select('*').eq('id',visit.arrival_id).is('departed_at',null).maybeSingle())
     : await appVisit(await result(db.from('driver_conversations').select('*').eq('id',visit.conversation_id).maybeSingle()));
    const currentDeparture=current&&confirmedDeparture(current,loads,now());
    if(!currentDeparture||currentDeparture.departedAt!==departure.departedAt||currentDeparture.bolNumber!==departure.bolNumber)continue;
    if(visit.source==='sms'){
     const changed=await result(db.from('driver_sms_arrivals').update({departed_at:departure.departedAt}).eq('id',visit.arrival_id).is('departed_at',null).select('id'));
     health.removed+=(changed||[]).length;
    }else{
     const arrival=await result(db.from('driver_arrivals').select('id,user_id,facility_id,facility_name,profile_snapshot').eq('id',visit.arrival_id).single());
     const changed=await result(db.from('driver_arrivals').upsert({client_event_id:`ps-shipped-${visit.arrival_id}`,event_type:'geofence_exit',user_id:arrival.user_id,facility_id:arrival.facility_id,facility_name:arrival.facility_name,release_number:null,occurred_at:departure.departedAt,profile_snapshot:arrival.profile_snapshot||{}},{onConflict:'client_event_id',ignoreDuplicates:true}).select('id'));
     health.removed+=(changed||[]).length;
    }
   }
   health.totalRemoved+=health.removed;health.state='idle';
  }catch(error){health.state='error';health.error=String(error.message||error);console.error('PS departure sync:',health.error);}
  finally{health.lastChecked=new Date(now()).toISOString();running=false;}
 }
 return {health,tick,start(){if(timer)return;void tick();timer=setInterval(()=>void tick(),60000);timer.unref?.();},stop(){clearInterval(timer);timer=null;}};
}
module.exports={confirmedDeparture,easternTimestamp,createWorker};
