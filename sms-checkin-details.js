'use strict';
const clean=v=>String(v||'').trim();
const companyPattern=/\b(trucking|transport(?:ation)?|logistics|carriers?|freight|express|inc|llc|corp|company|steel|viqtory)\b/i;
const chatPattern=/\b(thanks?|thank you|hello|hi|ok(?:ay)?|yes|no|waiting|door|dock|where|when|how|wrong|sorry|please|arrived|here|need|have|can|will|ready|morning|afternoon)\b/i;
function parseDetails(text, existing={}) {
 const found={},rest=[];
 // Labels are accepted in any order, even when separated only by spaces.
 const marked=clean(text).replace(/\b((?:full\s+)?name|driver(?:\s+name)?|(?:release|pickup|pick-up|load)(?:\s*(?:number|no\.?|#))?|(?:trucking\s+)?company)\s*[:=]\s*/gi,'\n$1: ');
 for(let part of marked.split(/[\n,;|]+/).map(clean).filter(Boolean)){
  part=part.replace(/^\d[.)]\s*/,'');
  let m=part.match(/^(?:full\s+name|name|driver(?:\s+name)?)\s*[:=]\s*(.+)$/i);
  if(m){found.full_name=clean(m[1]).slice(0,120);continue;}
  m=part.match(/^(?:trucking\s+)?company\s*[:=]\s*(.+)$/i);
  if(m){found.driver_company=clean(m[1]).slice(0,160);continue;}
  m=part.match(/^(?:(?:(?:correct|new|actual)\s+)?(?:release|pickup|pick-up|load)(?:\s*(?:number|no\.?|#))?\s*(?:is|[:=#])?\s*)([a-z0-9][a-z0-9-]{2,29})[.!]?$/i);
  if(m&&/\d/.test(m[1])){found.last_release_number=m[1];continue;}
  m=part.match(/^(?:(?:sorry|correction|correct number|actually|it's|its|it is)\s*[:=]?\s*)?#?([a-z]*\d[a-z0-9-]{3,29})[.!]?$/i);
  if(m){found.last_release_number=m[1];continue;}
  // Extract an unmistakable release number embedded in a complete text.
  m=part.match(/\b\d{5,10}\b/g);
  if(m?.length===1 && !chatPattern.test(part) && !/\b(phone|tel|mobile|call me|truck|trailer|pounds|lbs|weight|zip|address)\b/i.test(part)){
   found.last_release_number=m[0];part=clean(part.replace(m[0],'').replace(/\b(?:release|number|pickup|load)\b\s*[:#]?/gi,''));
  }
  if(part)rest.push(part);
 }
 for(const part of rest){
  const split=part.match(/^(.+?)\s+(?:with|from|driving for)\s+(.+)$/i);
  if(split&&!chatPattern.test(split[1])&&!existing.full_name){found.full_name=clean(split[1]).replace(/^I'm\s+|^I am\s+/i,'').slice(0,120);found.driver_company=clean(split[2]).slice(0,160);continue;}
  if(companyPattern.test(part)&&!chatPattern.test(part)){
   if(!existing.driver_company)found.driver_company=part.slice(0,160);
   continue;
  }
  if(chatPattern.test(part))continue;
  if(!found.full_name&&!existing.full_name&&/^[\p{L}.'’-]+(?:\s+[\p{L}.'’-]+){1,3}$/u.test(part))found.full_name=part.slice(0,120);
  else if(!found.driver_company&&!existing.driver_company&&(found.full_name||existing.full_name)&&/^[\p{L}\d .&'’-]{2,160}$/u.test(part))found.driver_company=part;
 }
 return found;
}
function nextCheckin({existing,matchedProfile,body,conversationCreated}){
 const prior=existing||{},base={full_name:clean(prior.full_name||matchedProfile?.full_name),driver_company:clean(prior.driver_company||matchedProfile?.driver_company||matchedProfile?.hauling_for),last_release_number:conversationCreated?'':clean(prior.last_release_number)};
 const parsed=parseDetails(body,base),contact={...base,...parsed};
 const missing=[!contact.full_name&&'Name: your full name',!contact.driver_company&&'Company: your trucking company',!contact.last_release_number&&'Release: your release or pickup number'].filter(Boolean);
 contact.onboarding_step=missing.length?'awaiting_details':'ready';
 const changed=Object.keys(parsed).some(k=>parsed[k]!==base[k]);
 const reply=(conversationCreated||!existing)
  ? "Please reply with:\n\n1. Full name\n2. Release Number\n3. Company\n\nFor faster check-ins, Download the CSP Driver app."
  : '';
 return {contact,reply,releaseNumber:parsed.last_release_number||'',detailsChanged:changed};
}
module.exports={parseDetails,nextCheckin};
