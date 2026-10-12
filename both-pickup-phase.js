 'use strict';
const flow=require('./driver-visit-flow');
function pickupRequestedAt(messages){
 const inbound=messages.filter(m=>m.direction==='driver_to_shipping').sort((a,b)=>Date.parse(a.sent_at)-Date.parse(b.sent_at));
 let pickup=null;
 for(const m of inbound){
  const type=flow.visitType(m.body);
  if(type==='both')pickup=null;
  else if(!pickup&&type==='pickup'&&flow.stripVisitWords(m.body)!==String(m.body||''))pickup=Date.parse(m.sent_at);
 }
 return pickup;
}
module.exports={pickupRequestedAt};
