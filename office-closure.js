'use strict';
const formatter=new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
function periodDay(now){const p=Object.fromEntries(formatter.formatToParts(new Date(now)).map(x=>[x.type,x.value]));if(Number(p.hour)*60+Number(p.minute)<330)return periodDay(now-6*3600000);return `${p.year}-${p.month}-${p.day}`;}
function active(override,now){const parts=Object.fromEntries(formatter.formatToParts(new Date(now)).map(p=>[p.type,p.value]));const minutes=Number(parts.hour)*60+Number(parts.minute);return Boolean(minutes>=330&&minutes<1080&&override?.closed_on===periodDay(now)&&Date.parse(override.activated_at)<=now);}
function createStore({db,auth,now=Date.now}){
 let cached=null,loadedAt=-Infinity;
 async function load(fresh=false){if(!fresh&&now()-loadedAt<3000)return cached;const r=await db.from('shipping_office_override').select('closed_on,activated_at').eq('id',1).maybeSingle();if(r.error)throw r.error;cached=r.data;loadedAt=now();return cached;}
 async function cancelForStaffReply(staffId){
  const value=await load(true);if(!active(value,now()))return false;
  // Conditional update cannot erase a newer button activation from another clerk.
  const r=await db.from('shipping_office_override').update({closed_on:null,activated_at:null,updated_at:new Date(now()).toISOString(),updated_by:staffId}).eq('id',1).eq('activated_at',value.activated_at);
  if(r.error)throw r.error;
  cached=null;loadedAt=-Infinity;return true;
 }
 async function handler(req,res){
  res.set('Cache-Control','no-store');
  try{
   const token=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');
   if(!token)return res.status(401).json({error:'Sign in to Shipping.'});
   const r=await auth.auth.getUser(token),user=r.data?.user;
   if(r.error||!user)return res.status(401).json({error:'Your Shipping session has expired.'});
   if(!['shipping','admin'].includes(String(user.app_metadata?.csp_role||'').toLowerCase()))return res.status(403).json({error:'Shipping access is required.'});
   if(req.method==='POST'){
    if(typeof req.body?.enabled!=='boolean')return res.status(400).json({error:'Choose on or off.'});
    const at=now(),value={id:1,closed_on:req.body.enabled?periodDay(at):null,activated_at:req.body.enabled?new Date(at).toISOString():null,updated_at:new Date(at).toISOString(),updated_by:user.id};
    const saved=await db.from('shipping_office_override').upsert(value,{onConflict:'id'});if(saved.error)throw saved.error;
    cached=value;loadedAt=at;
   }
   const value=await load();return res.json({enabled:active(value,now()),closedOn:value?.closed_on||null,message:require('./after-hours-reply').BODY});
  }catch(error){console.error('Office closure setting:',error.message);return res.status(503).json({error:'The office closing setting could not be loaded or saved.'});}
 }
 return {load,handler,cancelForStaffReply};
}
module.exports={periodDay,active,createStore};
