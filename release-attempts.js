'use strict';
const session=require('./visit-session');
const {parseDetails}=require('./sms-checkin-details');
const flow=require('./driver-visit-flow');
const {doorMatches}=require('./release-doors');
const ENABLED_AT=Date.parse('2026-10-12T18:38:00Z');
const INVALID=[
 'The release number provided is not valid. Please verify the number and try again.',
 'The release number provided is still not valid. Please contact your dispatch to confirm the correct release number before trying again.',
 'Please call 419-269-9706 for assistance.'
];
const ACCEPTED='Thank you! We’re verifying your release number and will send instructions shortly.';
const AMBIGUOUS='More than one appointment matches that number. Please send the complete release number, including any suffix, for verification.';
function releaseFromMessage(body){
 const value=flow.stripVisitWords(body).trim();
 const list=require('./release-list').extract(value);
 if(list.releases.length>1)return list.releases.join(' & ');
 // Keep a punctuated release together before the general details parser splits fields.
 const bare=value.replace(/^(?:release|rel)(?:\s*(?:number|no\.?))?\s*[:#]?\s*/i,'').replace(/^#\s*[:=]?\s*/,'');
 if(/^[A-Za-z0-9][A-Za-z0-9./_-]{2,39}$/.test(bare)&&/\d/.test(bare)&&!/^\+?1?\d{10}$/.test(bare))return bare;
 return parseDetails(value,{}).last_release_number||'';
}
async function lookupRelease(chart,release,c,at){
 if(!chart)throw Error('Release database is unavailable.');
 release=require('./release-list').primary(release);
 const date=new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(at));
 const rows=await require('./release-catalog').releaseCandidates(chart,release);
 const location={'csp-toledo-main':'PROSPY','csp-toledo-enterprise':'ENTPRS'}[c.facility_id];
 if(!location)throw Error('Release facility is unknown.');
 const key=release.normalize('NFKC').toUpperCase().replace(/[^A-Z0-9]/g,'');
 const match=doorMatches(rows,[release],date)[key+':'+location];
 return !match?'invalid':match.status==='ambiguous'?'ambiguous':'matched';
}
async function processAttempts({chart,c,messages,pickupAt,send,now=Date.now,lookup=lookupRelease,validationEnabled=true,silentStore}){
 const key=session.key(c);
 const marker=(kind,id)=>`release-wait:${kind}-${id}:${key}`;
 // Temporary PS outage mode: record receipt for staff review without querying
 // stale appointments or claiming that the release matched a PS record.
 if(!validationEnabled){
  const attempts=messages.filter(m=>m.id&&m.direction==='driver_to_shipping'&&Date.parse(m.sent_at)>=Math.max(ENABLED_AT,pickupAt)&&releaseFromMessage(m.body)).sort((a,b)=>Date.parse(a.sent_at)-Date.parse(b.sent_at)||String(a.id).localeCompare(String(b.id)));
  const latest=attempts.at(-1);
  if(!latest)return {state:null,initial:null};
  const prior=messages.find(m=>m.client_message_id===marker('manual',latest.id)||m.client_message_id===marker('matched',latest.id));
  if(prior)return {state:'manual',initial:prior};
  await send(c,`manual-${latest.id}`,ACCEPTED);
  const initial={client_message_id:marker('manual',latest.id),direction:'shipping_to_driver',sent_at:new Date(now()).toISOString(),delivery_status:'sent',body:ACCEPTED};
  messages.push(initial);
  return {state:'manual',initial};
 }
 // Private outcomes preserve manual-review state without creating a chat message.
 if(silentStore){
  const saved=await silentStore.load(c.id);
  for(const outcome of saved)if(outcome.client_message_id.endsWith(':'+key)&&!messages.some(m=>m.client_message_id===outcome.client_message_id))messages.push(outcome);
 }
 const outcomes=messages.filter(m=>['invalid','matched','ambiguous','manual'].some(kind=>m.client_message_id?.startsWith(`release-wait:${kind}-`))).sort((a,b)=>{
  const attemptedAt=outcome=>{const attempt=messages.find(m=>m.id&&['invalid','matched','ambiguous','manual'].some(kind=>outcome.client_message_id===marker(kind,m.id)));return Date.parse(attempt?.sent_at||outcome.sent_at);};
  return attemptedAt(a)-attemptedAt(b);
 });
 let count=0,state=null,initial=null;
 for(const m of outcomes){if(m.client_message_id.startsWith('release-wait:manual-')){count=0;state='manual';initial=m;}else if(m.client_message_id.startsWith('release-wait:matched-')){count=0;state='matched';initial=m;}else if(m.client_message_id.startsWith('release-wait:ambiguous-')){state='ambiguous';}else{count++;state='invalid';}}
 const attempts=messages.filter(m=>m.id&&m.direction==='driver_to_shipping'&&Date.parse(m.sent_at)>=Math.max(ENABLED_AT,pickupAt)&&releaseFromMessage(m.body)).sort((a,b)=>Date.parse(a.sent_at)-Date.parse(b.sent_at)||String(a.id).localeCompare(String(b.id)));
 for(const m of attempts){
  // Do not replay responses already acknowledged during the API outage.
  const manual=messages.find(x=>x.client_message_id===marker('manual',m.id));
  if(manual)continue;
  if(messages.some(x=>['invalid','matched','ambiguous'].some(kind=>x.client_message_id===marker(kind,m.id))))continue;
  const result=await lookup(chart,releaseFromMessage(m.body),c,m.sent_at);
  const kind=result==='ambiguous'?'ambiguous':result==='matched'?'matched':'invalid';
  const body=result==='ambiguous'?AMBIGUOUS:result==='matched'?ACCEPTED:INVALID[Math.min(count,2)];
  const sameAccepted=kind==='matched'&&messages.find(x=>x.client_message_id?.startsWith('release-wait:matched-')&&messages.some(prior=>prior.id&&x.client_message_id===marker('matched',prior.id)&&require('./release-list').primary(releaseFromMessage(prior.body))===require('./release-list').primary(releaseFromMessage(m.body))));
  if(sameAccepted){state='matched';initial=sameAccepted;continue;}
  if(kind!=='ambiguous')await send(c,`${kind}-${m.id}`,body);
  const outcome={client_message_id:marker(kind,m.id),direction:'shipping_to_driver',sent_at:new Date(now()).toISOString(),delivery_status:'sent',body};
  if(kind==='ambiguous'){
   outcome.body='';
   if(silentStore)await silentStore.save(c.id,outcome);
  }
  messages.push(outcome);
  if(result==='matched'){count=0;state='matched';initial=outcome;}else if(result==='ambiguous'){state='ambiguous';}else{count++;state='invalid';}
 }
 return {state,initial};
}
module.exports={processAttempts,lookupRelease,releaseFromMessage,INVALID,ACCEPTED,AMBIGUOUS,ENABLED_AT};
