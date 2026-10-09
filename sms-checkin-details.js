'use strict';
const clean=v=>String(v||'').trim();
const companyPattern=/\b(trucking|transport(?:ation)?|logistics|carriers?|freight|express|inc|llc|corp|company|steel|viqtory)\b/i;
const chatPattern=/\b(thanks?|thank you|hello|hi|ok(?:ay)?|yes|no|waiting|door|dock|where|when|how|wrong|sorry|please|arrived|here|need|have|can|will|ready|morning|afternoon)\b/i;
function parseDetails(text, existing={}) {
 const found={},rest=[];
 // Labels are accepted in any order, even when separated only by spaces.
 const marked=clean(text).replace(/\.{2,}|\.\s+(?=(?:release|load|name|driver|carrier|company)\b)/gi, ', ').replace(/\b((?:full\s+)?name|driver(?:\s+name)?|(?:release|pickup|pick-up|load)(?:\s*(?:number|no\.?|#))?|(?:trucking\s+)?company|carrier(?:\s+name)?)\s*[:=]\s*/gi,'\n$1: ');
 for(let part of marked.split(/[\n,;|/]+/).map(clean).filter(Boolean)){
  part=part.replace(/^\d[.)]\s*/,'');
  if(/^(?:\+?1[ .-]?)?\(?\d{3}\)?[ .-]\d{3}[ .-]\d{4}$/.test(part)||/^\+?1\d{10}$/.test(part))continue;
  let m=part.match(/^(?:full\s+name|name|driver(?:\s+name)?)\s*[:=]\s*(.+)$/i);
  if(m){found.full_name=clean(m[1]).slice(0,120);continue;}
  m=part.match(/^(?:(?:trucking\s+)?company|carrier(?:\s+name)?)\s*[:=]\s*(.+)$/i);
  if(m){found.driver_company=clean(m[1]).slice(0,160);continue;}
  m=part.match(/^(?:(?:(?:correct|new|actual)\s+)?(?:release|pickup|pick-up|load)(?:\s*(?:number|no\.?|#))?\s*(?:is|[:=#])?\s*)([a-z0-9][a-z0-9-]{2,29})[.!]?$/i);
  if(m&&/\d/.test(m[1])){found.last_release_number=m[1];continue;}
  m=part.match(/^(?:(?:sorry|correction|correct number|actually|it's|its|it is)\s*[:=]?\s*)?#?([a-z]*\d[a-z0-9-]{0,29})[.!]?$/i);
  if(m){found.last_release_number=m[1];continue;}
  // Extract an unmistakable release number embedded in a complete text.
  m=part.match(/\b\d{5,10}\b/g);
  if(m?.length===1 && !chatPattern.test(part) && !/\b(phone|tel|mobile|call me|truck|trailer|pounds|lbs|weight|zip|address)\b/i.test(part)){
   found.last_release_number=m[0];part=clean(part.replace(m[0],'').replace(/\b(?:release|number|pickup|load)\b\s*[:#]?/gi,''));
  }
  if(part)rest.push(part);
 }
 for(const part of rest){
  // A known multiword carrier can follow a driver's name without punctuation.
  // Keep standalone carrier names intact; do not guess a boundary for unknown carriers.
  const carrierIntroduction=part.match(/^([\p{L}.'’-]+(?:\s+[\p{L}.'’-]+){0,3})\s+(Center\s+Express[.]?)$/iu);
  if(carrierIntroduction&&!chatPattern.test(carrierIntroduction[1])&&!companyPattern.test(carrierIntroduction[1])&&!/^(?:pick(?:[ -]?up)?|release(?: number)?)$/i.test(carrierIntroduction[1])){
   found.full_name=clean(carrierIntroduction[1]);found.driver_company=clean(carrierIntroduction[2]);continue;
  }
  const split=part.match(/^(.+?)\s+(?:with|from|driving for)\s+(.+)$/i);
  if(split&&!chatPattern.test(split[1])&&!existing.full_name){found.full_name=clean(split[1]).replace(/^I'm\s+|^I am\s+/i,'').slice(0,120);found.driver_company=clean(split[2]).slice(0,160);continue;}
  if(companyPattern.test(part)&&!chatPattern.test(part)){
   if(!existing.driver_company)found.driver_company=part.slice(0,160);
   continue;
  }
  if(chatPattern.test(part)||/^(?:pick(?:[ -]?up)?|release(?: number)?)$/i.test(part))continue;
  if(!found.full_name&&!existing.full_name&&/^[\p{L}.'’-]+(?:\s+[\p{L}.'’-]+){0,3}$/u.test(part))found.full_name=part.slice(0,120);
  else if(!found.driver_company&&!existing.driver_company&&(found.full_name||existing.full_name)&&/^[\p{L}\d .&'’-]{2,160}$/u.test(part))found.driver_company=part;
 }
 return found;
}
function nextCheckin({existing,matchedProfile,body,conversationCreated,now=new Date()}){
 const flow=require('./driver-visit-flow');
 let prior={...(existing||{})};
 // Recover legacy contacts where punctuation caused the name to be stored with the carrier.
 if(!clean(prior.full_name)&&/\.{2,}/.test(prior.driver_company||'')){
  const recovered=parseDetails(prior.driver_company,{});
  if(recovered.full_name&&recovered.driver_company)prior={...prior,...recovered};
 }
 if(/^(?:pick(?:[ -]?up)?|release(?: number)?)$/i.test(prior.full_name||''))prior.full_name='';
 const fresh=conversationCreated||!existing;
 const previousType=fresh?null:prior.visit_type;
 const chosen=flow.visitType(body);
 const introduction=parseDetails(flow.stripVisitWords(body),{});
 const completeIntroduction=Boolean(introduction.full_name&&introduction.driver_company&&introduction.last_release_number);
 // Numbers supplied for the pickup leg must not erase a Both visit.
 const type=previousType==='both'?'both':chosen||(introduction.last_release_number?'pickup':previousType)||null;
 const started=fresh?new Date(now).toISOString():(prior.visit_started_at||new Date(now).toISOString());
 const base={full_name:clean(prior.full_name||matchedProfile?.full_name),driver_company:clean(prior.driver_company||matchedProfile?.driver_company||matchedProfile?.hauling_for),last_release_number:fresh?'':clean(prior.last_release_number)};
 const detailText=flow.stripVisitWords(body);
 const parsed=completeIntroduction?introduction:parseDetails(detailText,base),contact={...base,...parsed,visit_type:type,visit_started_at:started};
 contact.onboarding_step=type==='dropoff'||(['pickup','both'].includes(type)&&contact.full_name&&contact.driver_company&&contact.last_release_number)?'ready':'awaiting_details';
 const changed=Object.keys(parsed).some(k=>parsed[k]!==base[k]);
 let reply='';
 if(!type&&fresh)reply=flow.TYPE_PROMPT;
 else if(type&&(fresh||type!==previousType||(type==='dropoff'&&chosen==='dropoff'))) {
  if(type==='dropoff')reply=flow.dropoffReply(now);
  else {
   const missing=[!contact.full_name&&'name',!contact.last_release_number&&'release number',!contact.driver_company&&'carrier name'].filter(Boolean);
   if(missing.length)reply=`Please reply with your ${missing.length===3?missing.slice(0,2).join(', ')+', and '+missing[2]:missing.join(' and ')}.`;
   if(type==='both')reply=[flow.bothReply(now),reply].filter(Boolean).join(' ');
  }
 }
 return {contact,reply,releaseNumber:parsed.last_release_number||'',detailsChanged:changed,visitType:type};
}
module.exports={parseDetails,nextCheckin};
