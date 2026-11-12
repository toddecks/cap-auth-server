'use strict';
const {driverName,cleanIdentity}=require('./driver-identity');
function missingPrompt(contact){
 const missing=[];
 if(!driverName(contact?.full_name))missing.push('your name');
 if(!String(contact?.last_release_number||'').trim())missing.push('your release number');
 if(!cleanIdentity(contact?.driver_company))missing.push('your carrier name');
 if(!missing.length)return null;
 if(missing.length===3)return require('./driver-visit-flow').PICKUP_PROMPT;
 return `Please send ${missing.length===1?missing[0]:missing.join(' and ')} for verification and further instructions.`;
}
module.exports={missingPrompt};
