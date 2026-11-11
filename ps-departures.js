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
 const health={mode:'ps-shipped-departures-v1',state:'idle',lastChecked:null,confirmed:0,removed:0,totalRemoved:0,error:null};
 let running=false,timer;
 async function result(query){const r=await query;if(r.error)throw r.error;return r.data;}
 async function active(table){const rows=[];for(let offset=0;;offset+=500){const page=await result(db.from(table).select('*').is('departed_at',null).not('checked_in_at','is',null).gte('entered_at',new Date(now()-30*86400000).toISOString()).order('arrival_id').range(offset,offset+499));rows.push(...page);if(page.length<500)return rows;}}
 async function tick(){
  if(running||!db||!chart)return;
  running=true;Object.assign(health,{state:'running',confirmed:0,removed:0,error:null});
  try{
   const [sms,app]=await Promise.all([active('shipping_sms_arrival_feed'),active('driver_arrival_history_feed')]);
   const visits=[...sms.map(v=>({...v,source:'sms'})),...app.map(v=>({...v,source:'app'}))].filter(v=>['pickup','both'].includes(v.visit_type));
   const loads=visits.length?await candidates(chart,visits.map(v=>v.release_number)):[];
   for(const visit of visits){
    const departure=confirmedDeparture(visit,loads,now());if(!departure)continue;health.confirmed++;
    // Recheck active state after source lookup. Do not overwrite manual checkout.
    const table=visit.source==='sms'?'shipping_sms_arrival_feed':'driver_arrival_history_feed';
    const current=await result(db.from(table).select('*').eq('arrival_id',visit.arrival_id).is('departed_at',null).maybeSingle());
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
