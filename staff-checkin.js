'use strict';
const {sendCheckinInstructions} = require('./checkin-instructions');
function createHandler(deps) {
 const {db} = deps;
 return async (req,res) => {
  res.set('Cache-Control','no-store');
  const token=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');
  const id=String(req.body?.conversationId||'');
  if(!db)return res.status(503).json({error:'Check-in is unavailable.'});
  if(!token)return res.status(401).json({error:'Sign in to Shipping.'});
  if(!/^[0-9a-f-]{36}$/i.test(id)||req.body?.releaseVerified!==true)return res.status(400).json({error:'Verify the release number before selecting Check In.'});
  try {
   const {data:auth,error:authError}=await db.auth.getUser(token);
   const staff=auth?.user;
   if(authError||!staff)return res.status(401).json({error:'Sign in to Shipping.'});
   if(!['shipping','admin'].includes(staff.app_metadata?.csp_role))return res.status(403).json({error:'Shipping access is required.'});
   const {data:c,error:ce}=await db.from('driver_conversations').select('*').eq('id',id).single();
   if(ce||!c||c.status!=='open')return res.status(409).json({error:'This conversation is no longer open.'});
   if(c.visit_type!=='pickup'||!String(c.release_number||'').trim()||/^(TEXT\s*\d*|drop[ -]?off)$/i.test(c.release_number))return res.status(409).json({error:'A pick-up and verified release number are required.'});
   let arrivalId=c.arrival_id;
   if(c.channel==='sms'){
    const {data:contact,error:e}=await db.from('driver_sms_contacts').select('*').eq('phone_e164',c.sms_phone_e164).single();
    if(e||!contact?.full_name||!contact?.driver_company)return res.status(409).json({error:'Complete the driver name and company before checking in.'});
    const {data:existing,error:ee}=await db.from('driver_sms_arrivals').select('id').eq('conversation_id',id).is('departed_at',null).maybeSingle();
    if(ee)throw ee;
    arrivalId=existing?.id;
    if(!arrivalId){
     const {data:arrival,error:ae}=await db.from('driver_sms_arrivals').insert({phone_e164:c.sms_phone_e164,conversation_id:id,facility_id:c.facility_id,release_number:c.release_number,driver_name:contact.full_name,driver_company:contact.driver_company,visit_type:'pickup',added_by:staff.id}).select('id').single();
     if(ae)throw ae;arrivalId=arrival.id;
    }
   }
   const {error:writeError}=await db.from('shipping_staff_checkins').upsert({conversation_id:id,checked_in_by:staff.id},{onConflict:'conversation_id',ignoreDuplicates:true});
   if(writeError)throw writeError;
   const {data:verified,error:ve}=await db.from('shipping_staff_checkins').select('checked_in_at').eq('conversation_id',id).single();
   if(ve)throw ve;
   let warning=null;
   try {await sendCheckinInstructions({...deps,conversation:c,arrivalId,staffId:staff.id,visitType:'pickup'});}
   catch(error){console.error('Staff check-in instructions:',error.message);warning='Check-in recorded, but instructions could not be confirmed. Review the conversation before sending them manually.';}
   return res.json({ok:true,arrivalId,checkedInAt:verified.checked_in_at,warning});
  } catch(error){console.error('Staff check-in:',error.message);return res.status(500).json({error:'Check-in could not be completed. Refresh and try again.'});}
 };
}
module.exports={createHandler};
