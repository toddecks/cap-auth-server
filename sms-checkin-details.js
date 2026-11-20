'use strict';
const clean=v=>String(v||'').trim();
const {driverName,cleanIdentity}=require('./driver-identity');
const companyPattern=/\b(trucking|transport(?:ation)?|logistics|carriers?|freight|express|inc|llc|corp|company|steel|viqtory)\b/i;
const chatPattern=/\b(thanks?|thank you|hello|hi|ok(?:ay)?|yes|no|waiting|door|dock|where|when|how|wrong|sorry|please|arrived|here|need|have|can|will|ready|morning|afternoon)\b/i;
function parseDetails(text, existing={}) {
 text=String(text||'').normalize('NFC');
 const found={},rest=[];
 const multiple=require('./release-list').extract(text);
 const multiRelease=multiple.releases.length>1?multiple.releases.join(' & '):'';
 // Separate a driver's natural introduction from delivery/customer commentary.
 // Only explicit with/from introductions establish a carrier boundary.
 let input=clean(text).replace(/([.!?])(?=my\s+release\b)/gi,'$1 ');
 if(multiRelease)for(const segment of multiple.segments)input=input.replace(segment,'');
 const intro=input.match(/^(?:(?:this\s+is|is\s+this|my\s+name\s+is|i\s+am|i['’]m)\s+)?([\p{L}\p{M}.'’-]+(?:\s+[\p{L}\p{M}.'’-]+){0,3})\s+(?:with|from|driving for)\s+(.+?)(?=\s+(?:here\b|to\b|for\b|my\s+release\b|release\b|\d{4,}\b)|[.!?](?:\s|$)|$)/iu);
 if(intro&&!chatPattern.test(intro[1])&&!companyPattern.test(intro[1])){
  found.full_name=driverName(intro[1]).slice(0,120);
  found.driver_company=cleanIdentity(intro[2].replace(/[,;]+$/, '')).slice(0,160);
  input=clean(input.slice(intro[0].length));
 }
 const customer=input.match(/\bfor\s+([\p{L}\p{M}][\p{L}\p{M}\d .&'’-]+)\s*$/iu);
 if(customer&&/\d{4,}/.test(input.slice(0,customer.index))){found.customer_name=clean(customer[1]);input=input.slice(0,customer.index).trim();}
 // A labeled release can be anywhere in the sentence. Do not confuse a
 // weight, phone, trailer or customer number with a release.
 const tagIntro=input.match(/^([\p{L}\p{M}.'’\-]+)\s+(.+?)\s+(?:tag|release(?: number)?)\s*[:#]?\s*([a-z0-9][a-z0-9./_\-]{2,39})$/iu);
 if(tagIntro&&!/\.{2}/.test(tagIntro[1])&&!chatPattern.test(tagIntro[1])&&/\d/.test(tagIntro[3])){
  found.full_name=driverName(tagIntro[1]);found.driver_company=cleanIdentity(tagIntro[2]);found.last_release_number=tagIntro[3];input='';
 }
 const labeled=input.match(/\b(?:my\s+)?(?:release|tag)(?:\s+(?:number|no\.?))?\s*(?:is|[:=#])?\s*([a-z0-9][a-z0-9./_-]{2,39})/i);
 if(labeled&&/\d/.test(labeled[1])){
  found.last_release_number=labeled[1].replace(/[.!]+$/,'');
  input=input.replace(labeled[0],'');
 }
 input=input.replace(/[.!?]\s*(?=\d{4,12}\b)/g,'; ');

 // Labels are accepted in any order, even when separated only by spaces.
 const marked=input.replace(/\.{2,}|\.\s+(?=(?:release|load|name|driver|carrier|company)\b)/gi, ', ').replace(/\b((?:full\s+)?name|driver(?:\s+name)?|(?:release|pickup|pick-up|load)(?:\s*(?:number|no\.?|#))?|(?:trucking\s+)?company|carrier(?:\s+name)?)\s*[:=]\s*/gi,'\n$1: ');
 // Slashes inside a release (CB7201P1-2/7, 968751/510) are part of
 // the reference. Keep ordinary slash-separated names/carriers as separate fields.
 const protectedText=marked.replace(/\b(?=[a-z0-9._-]*\d)[a-z0-9][a-z0-9._-]*(?:\/[a-z0-9._-]+)+/gi,token=>token.replace(/\//g,'\uE000'));
 for(let part of protectedText.split(/[\n,;|/]+/).map(clean).filter(Boolean)){
  part=part.replace(/\uE000/g,'/');
  part=part.replace(/^\d[.)]\s*/,'');
  if(/^(?:\+?1[ .-]?)?\(?\d{3}\)?[ .-]\d{3}[ .-]\d{4}$/.test(part)||/^\+?1\d{10}$/.test(part))continue;
  // Drivers often provide multiple complete release numbers after PU #.
  // Use the first complete reference for lookup; retain all supplied references
  // in the original message rather than turning a prefix into the release.
  const releaseList=part.match(/^(?:(?:pu|pickup|pick-up|release|load)(?:\s*(?:number|no\.?))?\s*[:=#]?\s*|#\s*)(\d{4,12})(?:\s+(?:and\s+)?#?\d{4,12})+[.!]?$/i);
  if(releaseList){found.last_release_number=releaseList[1];continue;}
  let m=part.match(/^(?:full\s+name|name|driver(?:\s+name)?)\s*[:=]\s*(.+)$/i);
  if(m){found.full_name=driverName(m[1]).slice(0,120);continue;}
  m=part.match(/^(?:(?:trucking\s+)?company|carrier(?:\s+name)?)\s*[:=]\s*(.+)$/i);
  if(m){found.driver_company=clean(m[1]).slice(0,160);continue;}
  m=part.match(/^(?:(?:(?:correct|new|actual)\s+)?(?:release|pickup|pick-up|load)(?:\s*(?:number|no\.?|#))?\s*(?:is|[:=#])?\s*)([a-z0-9][a-z0-9./_-]{2,39})[.!]?$/i);
  if(m&&/\d/.test(m[1])){found.last_release_number=m[1];continue;}
  m=part.match(/^(?:(?:sorry|correction|correct number|actually|it's|its|it is)\s*[:=]?\s*)?(?:#\s*[:=]?\s*)?([a-z0-9][a-z0-9./_-]{0,39})[.!]?$/i);
  if(m&&/\d/.test(m[1])){found.last_release_number=m[1];continue;}
  // A release between a driver name and a carrier is an explicit field boundary.
  // Do this before removing the number, which would merge both identity fields.
  const separated=part.match(/^([\p{L}\p{M}.'’-]+(?:\s+[\p{L}\p{M}.'’-]+){0,3})\s+(\d{4,10})\s+([\p{L}\p{M}][\p{L}\p{M}\d .&'’-]{1,159})$/u);
  if(separated&&!chatPattern.test(separated[1])&&!companyPattern.test(separated[1])&&!chatPattern.test(separated[3])){
   found.full_name=driverName(separated[1]).slice(0,120);
   found.last_release_number=separated[2];
   found.driver_company=cleanIdentity(separated[3]).slice(0,160);
   continue;
  }
  // Extract an unmistakable release number embedded in a complete text.
  m=part.match(/\b\d{5,10}\b/g);
  if(m?.length===1 && !chatPattern.test(part) && !/\b(phone|tel|mobile|call me|truck|trailer|pounds|lbs|weight|zip|address)\b/i.test(part)){
   found.last_release_number=m[0];part=clean(part.replace(m[0],'').replace(/\b(?:release|number|pickup|load)\b\s*[:#]?/gi,''));
  }
  if(part&&!/^(?:truck|trailer|tractor|unit)\s*(?:number|no\.?|#)?\s*[:#-]?\s*\d+$/i.test(part))rest.push(part);
 }
 for(const rawPart of rest){
  const part=driverName(rawPart);
  if(!part)continue;
  const split=part.match(/^(.+?)\s+(?:with|from|driving for)\s+(.+)$/i);
  if(split&&!found.full_name&&!chatPattern.test(split[1])&&!existing.full_name){found.full_name=driverName(split[1]).slice(0,120);found.driver_company=clean(split[2]).slice(0,160);continue;}
  // A known multiword carrier can follow a driver's name without punctuation.
  // Keep standalone carrier names intact; do not guess a boundary for unknown carriers.
  const carrierIntroduction=part.match(/^([\p{L}\p{M}.'’-]+(?:\s+[\p{L}\p{M}.'’-]+){0,3})\s+((?:Center\s+Express|Ace\s+Doran)[.]?)$/iu);
  if(carrierIntroduction&&!chatPattern.test(carrierIntroduction[1])&&!companyPattern.test(carrierIntroduction[1])&&!/^(?:pick(?:[ -]?up)?|release(?: number)?)$/i.test(carrierIntroduction[1])){
   found.full_name=clean(carrierIntroduction[1]);found.driver_company=clean(carrierIntroduction[2]);continue;
  }
  if(/^Ace\s+Doran[.]?$/i.test(part)){found.driver_company=part.slice(0,160);continue;}
  if(companyPattern.test(part)&&!chatPattern.test(part)){
   if(!found.driver_company&&!existing.driver_company)found.driver_company=part.slice(0,160);
   continue;
  }
  if(chatPattern.test(part)||/^(?:pick(?:[ -]?up)?|release(?: number)?)$/i.test(part))continue;
  if(!found.full_name&&!existing.full_name&&/^[\p{L}\p{M}.'’-]+(?:\s+[\p{L}\p{M}.'’-]+){0,3}$/u.test(part))found.full_name=part.slice(0,120);
  else if(!found.driver_company&&!existing.driver_company&&(found.full_name||existing.full_name)&&/^[\p{L}\p{M}\d .&'’-]{2,160}$/u.test(part))found.driver_company=part;
 }
 if(multiRelease)found.last_release_number=multiRelease;
 if(found.last_release_number)found.last_release_number=found.last_release_number.replace(/[.!]+$/,'');
 if('full_name' in found)found.full_name=driverName(found.full_name);
 if('driver_company' in found)found.driver_company=cleanIdentity(found.driver_company);
 return found;
}
function nextCheckin({existing,matchedProfile,body,conversationCreated,now=new Date()}){
 const flow=require('./driver-visit-flow');
 let prior={...(existing||{})};
 prior.full_name=driverName(prior.full_name);
 prior.driver_company=cleanIdentity(prior.driver_company);
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
 const type=previousType==='both'?'both':chosen||previousType||null;
 const started=fresh?new Date(now).toISOString():(prior.visit_started_at||new Date(now).toISOString());
 const base={full_name:driverName(prior.full_name||matchedProfile?.full_name),driver_company:cleanIdentity(prior.driver_company||matchedProfile?.driver_company||matchedProfile?.hauling_for),last_release_number:fresh?'':clean(prior.last_release_number),customer_name:fresh?'':clean(prior.customer_name)};
 const materialWords=String(body||'').match(/[\p{L}\p{M}]+/gu)||[];
 const materialOnly=type==='dropoff'&&materialWords.length&&materialWords.every(word=>require('./dropoff-material').material(word));
 const detailText=materialOnly?'':flow.stripVisitWords(body);
 const parsed=completeIntroduction?introduction:parseDetails(detailText,base);
 const contact={...base,...parsed,visit_type:type,visit_started_at:started};
 contact.onboarding_step=type==='dropoff'||(['pickup','both'].includes(type)&&contact.full_name&&contact.driver_company&&contact.last_release_number)?'ready':'awaiting_details';
 const changed=Object.keys(parsed).some(k=>parsed[k]!==base[k]);
 let reply='';
 if(!type)reply=flow.TYPE_PROMPT;
 else if(type&&(fresh||type!==previousType||(type==='dropoff'&&chosen==='dropoff')||(previousType==='both'&&chosen==='pickup'))) {
  if(type==='both'&&chosen!=='pickup')reply=flow.bothReply(now);
  else if(type==='dropoff')reply=flow.dropoffReply(now,body);
  // Pickup's first response is sent once by the reminder worker, including
  // when the release is missing; this prevents two competing auto replies.
 }
 if(type==='dropoff'&&!reply&&require('./dropoff-material').material(body))reply=flow.dropoffReply(now,body);
 return {contact,reply,releaseNumber:parsed.last_release_number||'',detailsChanged:changed,visitType:type};
}
module.exports={parseDetails,nextCheckin};
