'use strict';
const {driverName,cleanIdentity}=require('./driver-identity');
const {sendCheckinInstructions} = require('./checkin-instructions');
function createHandler(deps) {
 const {db} = deps;
 return async (req,res) => {
  res.set('Cache-Control','no-store');
  const token=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');
  const id=String(req.body?.conversationId||'');
  if(!db)return res.status(503).json({error:'Check-in is unavailable.'});
  if(!token)return res.status(401).json({error:'Sign in to Shipping.'});
  if(!/^[0-9a-f-]{36}$/i.test(id))return res.status(400).json({error:'Choose a valid conversation before selecting Check In.'});
  try {
   const {data:auth,error:authError}=await db.auth.getUser(token);
   const staff=auth?.user;
   if(authError||!staff)return res.status(401).json({error:'Sign in to Shipping.'});
   if(!['shipping','admin'].includes(staff.app_metadata?.csp_role))return res.status(403).json({error:'Shipping access is required.'});
   const {data:c,error:ce}=await db.from('driver_conversations').select('*').eq('id',id).single();
   if(ce||!c||c.status!=='open')return res.status(409).json({error:'This conversation is no longer open.'});
   if(!['pickup','both'].includes(c.visit_type)||!String(c.release_number||'').trim()||/^(TEXT\s*\d*|drop[ -]?off)$/i.test(c.release_number))return res.status(409).json({error:'A pick-up and verified release number are required.'});
   if(c.visit_type==='both'&&req.body?.dropOffComplete!==true)return res.status(409).json({error:'Confirm unloading is complete before checking this Both visit in for pickup.'});
   if(c.session_ended_at)return res.status(409).json({error:'This visit has ended. The driver must start a new visit before check-in.'});
   let preparedInstructions;
   const deferred=typeof deps.enqueueInstructions==='function';
   try {preparedInstructions=deps.routingEnabled===true?await require('./pickup-directions').pickupInstructions(deps.chartDb,c,new Date(),req.body?.loadingDoor,true):deps.routingEnabled===false?require('./pickup-directions').UNKNOWN_CHECKIN:deferred?undefined:await require('./pickup-directions').automaticPickupInstructions(deps.chartDb,c);}
   catch(error){return res.status(409).json({error:error.message,code:'LOADING_DOOR_REQUIRED'});}
   const [priorResult,contactResult,arrivalResult]=await Promise.all([
    db.from('shipping_staff_checkins').select('checked_in_at').eq('conversation_id',id).maybeSingle(),
    c.channel==='sms'?db.from('driver_sms_contacts').select('full_name,driver_company').eq('phone_e164',c.sms_phone_e164).single():Promise.resolve({data:null}),
    c.channel==='sms'?db.from('driver_sms_arrivals').select('id').eq('conversation_id',id).is('departed_at',null).maybeSingle():Promise.resolve({data:null})
   ]);
   const {data:priorCheckin,error:priorError}=priorResult;
   let arrivalId=c.arrival_id;
   if(c.channel==='sms'){
    const {data:contact,error:e}=contactResult;
    if(e||!driverName(contact?.full_name)||!cleanIdentity(contact?.driver_company))return res.status(409).json({error:'Complete the driver name and company before checking in.'});
    if(arrivalResult.error)throw arrivalResult.error;
    arrivalId=arrivalResult.data?.id;
    if(!arrivalId){
     const {data:arrival,error:ae}=await db.from('driver_sms_arrivals').insert({phone_e164:c.sms_phone_e164,conversation_id:id,facility_id:c.facility_id,release_number:c.release_number,driver_name:contact.full_name,driver_company:contact.driver_company,visit_type:c.visit_type,added_by:staff.id}).select('id').single();
     if(ae)throw ae;arrivalId=arrival.id;
    }
   }
   if(priorError)throw priorError;
   const alreadyChecked=require('./visit-session').currentCheckin(c,priorCheckin);
   const {data:verified,error:writeError}=alreadyChecked?{data:priorCheckin}:await db.from('shipping_staff_checkins').upsert({conversation_id:id,checked_in_by:staff.id,checked_in_at:new Date().toISOString()},{onConflict:'conversation_id'}).select('checked_in_at').single();
   if(writeError)throw writeError;
   require('./manual-reply-pause').clear(c);
   if(c.channel==='sms'){const {error:e}=await db.from('driver_sms_arrivals').update({checked_in_at:verified.checked_in_at}).eq('id',arrivalId);if(e)throw e;}
   let warning=null,instructionsJobId=null;
   try {const payload={conversation:c,arrivalId,staffId:staff.id,visitType:c.visit_type,preparedInstructions};
    if(deferred)instructionsJobId=await deps.enqueueInstructions(payload);else await sendCheckinInstructions({...deps,...payload});}
   catch(error){console.error('Staff check-in instructions:',error.message);warning='Check-in recorded, but instructions could not be confirmed. Review the conversation before sending them manually.';}
   return res.json({ok:true,arrivalId,checkedInAt:verified.checked_in_at,warning,instructionsJobId});
  } catch(error){console.error('Staff check-in:',error.message);return res.status(500).json({error:'Check-in could not be completed. Refresh and try again.'});}
 };
}
module.exports={createHandler};
