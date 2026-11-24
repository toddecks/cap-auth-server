'use strict';
const {doorMatches}=require('./release-doors');
const {PICKUP_CHECKIN}=require('./driver-visit-flow');
const COIL_CHECKIN=PICKUP_CHECKIN.replace('LEFT','RIGHT');
const UNKNOWN_CHECKIN='You have been successfully checked in. Please remain in your truck. A shipping team member will assist you shortly.';
async function pickupInstructions(chartDb,conversation,now=new Date(),loadingDoor='',routingEnabled=false){
 if(!routingEnabled)return PICKUP_CHECKIN;
 const releases=require('./release-list').split(conversation.release_number);
 if(releases.length>1){const instructions=await Promise.all(releases.map(r=>pickupInstructions(chartDb,{...conversation,release_number:r},now,loadingDoor,routingEnabled)));if(new Set(instructions).size>1)throw Error('These loads use different loading-door routes. Confirm the route with Shipping before check-in.');return instructions[0];}
 const routing=String(loadingDoor||'').trim().toUpperCase();
 if(routing&&!['COIL','OTHER'].includes(routing))throw Error('Choose COIL or Other loading door.');
 // A manual routing selection is a fallback, never an override of PS.
 if(!chartDb){if(routing)return routing==='COIL'?COIL_CHECKIN:PICKUP_CHECKIN;throw Error('Loading-door lookup is unavailable.');}
 const date=new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(now);
 let data;
 try{data=await require('./release-catalog').releaseCandidates(chartDb,conversation.release_number);}
 catch(error){if(routing)return routing==='COIL'?COIL_CHECKIN:PICKUP_CHECKIN;throw error;}
 const release=String(conversation.release_number||'').normalize('NFKC').trim().toUpperCase().replace(/[^A-Z0-9]/g,'');
 const facility={'csp-toledo-main':'PROSPY','csp-toledo-enterprise':'ENTPRS'}[conversation.facility_id];
 const match=doorMatches(data||[],[release],date)[release+':'+facility];
 if(!match?.door||match.status==='ambiguous'){
  if(routing)return routing==='COIL'?COIL_CHECKIN:PICKUP_CHECKIN;
  const error=Error('Loading door could not be determined. Select COIL (RIGHT) or Other loading door (LEFT) in driver details before check-in.');
  error.code='LOADING_DOOR_REQUIRED';throw error;
 }
 return String(match?.door||'').trim().toUpperCase()==='COIL'?COIL_CHECKIN:PICKUP_CHECKIN;
}
async function automaticPickupInstructions(chartDb,conversation,now=new Date()){
 const release=require('./release-list').primary(conversation.release_number);
 if(!release)return UNKNOWN_CHECKIN;
 try{return await pickupInstructions(chartDb,{...conversation,release_number:release},now,'',true);}
 catch(error){console.error('Automatic pickup routing unavailable:',error.message);return UNKNOWN_CHECKIN;}
}
module.exports={pickupInstructions,automaticPickupInstructions,COIL_CHECKIN,UNKNOWN_CHECKIN};
