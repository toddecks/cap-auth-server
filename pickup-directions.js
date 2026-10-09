'use strict';
const {doorMatches}=require('./release-doors');
const {PICKUP_CHECKIN}=require('./driver-visit-flow');
const COIL_CHECKIN='Please pull around back and stay to the RIGHT. Please have all required PPE ready before entering the facility, including a hard hat, safety glasses, and fully enclosed shoes.';
async function pickupInstructions(chartDb,conversation,now=new Date()){
 if(!chartDb)throw Error('Loading-door lookup is unavailable.');
 const date=new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(now);
 const start=new Date(date+'T12:00:00Z'),end=new Date(start);start.setUTCDate(start.getUTCDate()-1);end.setUTCDate(end.getUTCDate()+1);
 const {data,error}=await chartDb.from('psdata_loads_api').select('scheduleDate,scheduleTime,poRel,location,cancelLoad,unloadingDoor,customerNo').gte('scheduleDate',start.toISOString().slice(0,10)).lte('scheduleDate',end.toISOString().slice(0,10)).limit(5000);
 if(error)throw error;
 const release=String(conversation.release_number||'').normalize('NFKC').trim().toUpperCase().replace(/[^A-Z0-9]/g,'');
 const facility={'csp-toledo-main':'PROSPY','csp-toledo-enterprise':'ENTPRS'}[conversation.facility_id];
 const match=doorMatches(data||[],[release],date)[release+':'+facility];
 if(match?.status==='ambiguous')throw Error('Loading door needs confirmation.');
 return String(match?.door||'').trim().toUpperCase()==='COIL'?COIL_CHECKIN:PICKUP_CHECKIN;
}
module.exports={pickupInstructions,COIL_CHECKIN};
