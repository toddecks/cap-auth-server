'use strict';
const {cleanIdentity,driverName}=require('./driver-identity');
const {primary}=require('./release-list');
const {doorMatches,references}=require('./release-doors');
const normalize=value=>String(value||'').normalize('NFKC').toUpperCase().replace(/[^A-Z0-9]/g,'');
async function carrierFromRelease(chart,conversation,contact,at){
 // Keep an explicitly supplied carrier; PS may list a booking carrier instead.
 if(cleanIdentity(contact?.driver_company))return null;
 const release=primary(conversation.release_number);
 const location={'csp-toledo-main':'PROSPY','csp-toledo-enterprise':'ENTPRS'}[conversation.facility_id];
 if(!chart||!release||!location)return null;
 const date=new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(at));
 const rows=await require('./release-catalog').releaseCandidates(chart,release);
 const match=doorMatches(rows,[release],date)[normalize(release)+':'+location];
 if(!match||match.status==='ambiguous'||!['exact','abbreviated'].includes(match.match_type)||!match.appointment_at)return null;
 const [scheduleDate,scheduleTime]=match.appointment_at.split('T');
 const eligible=rows.filter(row=>String(row.location||'').trim().toUpperCase()===location&&row.cancelLoad!==true&&String(row.cancelLoad).toLowerCase()!=='true'&&String(row.scheduleDate)===scheduleDate&&String(row.scheduleTime).slice(0,8)===scheduleTime&&(match.match_type==='abbreviated'?normalize(row.poRel)===normalize(match.release_reference):(references(row.poRel).includes(normalize(release).replace(/[A-Z]/g,''))||normalize(row.poRel)===normalize(release))));
 const names=eligible.map(row=>cleanIdentity(row.carrierName));
 const unique=new Set(names.map(normalize));
 if(!names.length||names.some(name=>!name)||unique.size!==1)return null;
 const carrier=names[0],patch={driver_company:carrier};
 // Recover a legacy name that accidentally included this confirmed carrier.
 const name=driverName(contact?.full_name),words=carrier.match(/[\p{L}\p{N}]+/gu)||[];
 const suffix=new RegExp('(?:[\\s,;]+)'+words.map(word=>word.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')).join('[\\s.,&-]+')+'[.]*$','iu');
 const corrected=driverName(name.replace(suffix,''));
 if(corrected&&corrected!==name)patch.full_name=corrected;
 return patch;
}
module.exports={carrierFromRelease};
