'use strict';
const {doorMatches}=require('./release-doors');
const {driverName,cleanIdentity}=require('./driver-identity');
const day=value=>new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(value));
const normalize=value=>String(value||'').trim().toUpperCase().replace(/[^A-Z0-9]/g,'');
function displayRows(visits,loads,now=Date.now()){
 const today=day(now);
 const active=visits.filter(v=>v.checked_in_at&&!v.departed_at&&['pickup','both'].includes(v.visit_type)&&day(v.entered_at)===today);
 const matches=doorMatches(loads,active.map(v=>v.release_number),today);
 const seen=new Set();
 return active.sort((a,b)=>Date.parse(b.entered_at)-Date.parse(a.entered_at)).filter(v=>{
  const key=v.conversation_id||`${v.source}:${v.arrival_id}`;if(seen.has(key))return false;seen.add(key);return true;
 }).reverse().map(v=>{
  const location={'csp-toledo-main':'PROSPY','csp-toledo-enterprise':'ENTPRS'}[v.facility_id];
  const m=matches[normalize(v.release_number)+':'+location];
  // Deliberate public projection. Never return phone numbers, users, messages,
  // coordinates, attachments, or internal conversation identifiers.
  return {driver_name:driverName(v.driver_name||v.profile_snapshot?.name)||'Name not provided',driver_company:cleanIdentity(v.driver_company||v.profile_snapshot?.driverCompany)||'—',release_number:String(v.release_number||''),entered_at:v.entered_at,checked_in_at:v.checked_in_at,appointment_at:m?.appointment_at||null,bol_number:m?.bol_number||null,bol_ambiguous:!!m?.bol_ambiguous,door:m?.door||null,customer:m?.customer||null,match_status:m?.status||'unmatched'};
 });
}
function register({app,visits,chart,candidates=require('./release-catalog').releaseCandidatesForMany,now=()=>Date.now()}){
 let cache=null,inflight=null;
 async function load(){
  if(cache&&now()-cache.timestamp<15000)return cache.payload;
  if(inflight)return inflight;
  inflight=(async()=>{
   const current=(await visits()).filter(v=>day(v.entered_at)===day(now())&&v.checked_in_at&&!v.departed_at&&['pickup','both'].includes(v.visit_type));
   const loads=current.length?await candidates(chart,current.map(v=>v.release_number)):[];
   const payload={updated_at:new Date(now()).toISOString(),date:day(now()),arrivals:displayRows(current,loads,now())};
   cache={timestamp:now(),payload};return payload;
  })().finally(()=>{inflight=null});return inflight;
 }
 app.get('/api/shipping/public-arrivals',async(_req,res)=>{
  res.set('Cache-Control','no-store');res.set('X-Robots-Tag','noindex, nofollow');
  try{return res.json(await load())}catch(error){console.error('Public arrivals:',error.message);return res.status(503).json({error:'Arrivals temporarily unavailable. Retrying automatically.'})}
 });
 return {load};
}
module.exports={displayRows,register};
