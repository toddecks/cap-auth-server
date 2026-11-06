'use strict';
const {doorMatches}=require('./release-doors');
const {PICKUP_CHECKIN}=require('./driver-visit-flow');
const COIL_CHECKIN='You have been successfully checked in. Please pull around back and stay to the RIGHT.';
async function pickupInstructions(chartDb,conversation,now=new Date()){
 if(!chartDb)throw Error('Loading-door lookup is unavailable.');
 const date=new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(now);
 const data=await require('./release-catalog').releaseCandidates(chartDb,conversation.release_number);
 const release=String(conversation.release_number||'').normalize('NFKC').trim().toUpperCase().replace(/[^A-Z0-9]/g,'');
 const facility={'csp-toledo-main':'PROSPY','csp-toledo-enterprise':'ENTPRS'}[conversation.facility_id];
 const match=doorMatches(data||[],[release],date)[release+':'+facility];
 if(match?.status==='ambiguous')throw Error('Loading door needs confirmation.');
 return String(match?.door||'').trim().toUpperCase()==='COIL'?COIL_CHECKIN:PICKUP_CHECKIN;
}
module.exports={pickupInstructions,COIL_CHECKIN};
