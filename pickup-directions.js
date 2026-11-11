'use strict';
const {doorMatches}=require('./release-doors');
const {PICKUP_CHECKIN,PICKUP_PPE}=require('./driver-visit-flow');
const COIL_CHECKIN='Please pull around back and stay to the RIGHT. '+PICKUP_PPE;
async function pickupInstructions(chartDb,conversation,now=new Date(),loadingDoor=''){
 const routing=String(loadingDoor||'').trim().toUpperCase();
 if(routing&&!['COIL','OTHER'].includes(routing))throw Error('Choose COIL or Other loading door.');
 // Only the authenticated Shipping check-in endpoint accepts this explicit clerk choice.
 if(routing)return routing==='COIL'?COIL_CHECKIN:PICKUP_CHECKIN;
 if(!chartDb)throw Error('Loading-door lookup is unavailable.');
 const date=new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(now);
 const data=await require('./release-catalog').releaseCandidates(chartDb,conversation.release_number);
 const release=String(conversation.release_number||'').normalize('NFKC').trim().toUpperCase().replace(/[^A-Z0-9]/g,'');
 const facility={'csp-toledo-main':'PROSPY','csp-toledo-enterprise':'ENTPRS'}[conversation.facility_id];
 const match=doorMatches(data||[],[release],date)[release+':'+facility];
 if(!match?.door||match.status==='ambiguous'){
  const error=Error('Loading door could not be determined. Select COIL (RIGHT) or Other loading door (LEFT) in driver details before check-in.');
  error.code='LOADING_DOOR_REQUIRED';throw error;
 }
 return String(match?.door||'').trim().toUpperCase()==='COIL'?COIL_CHECKIN:PICKUP_CHECKIN;
}
module.exports={pickupInstructions,COIL_CHECKIN};
