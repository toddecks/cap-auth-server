'use strict';
function parseInput(body = {}) {
 const driverName=String(body.driverName||'').trim();
 const driverCompany=String(body.driverCompany||'').trim();
 const releaseNumber=String(body.releaseNumber||'').trim();
 const digits=String(body.phone||'').replace(/\D/g,'');
 const phone=digits.length===10?`+1${digits}`:digits.length===11&&digits[0]==='1'?`+${digits}`:/^\+[1-9]\d{7,14}$/.test(String(body.phone||'').trim())?String(body.phone).trim():'';
 if(!driverName||driverName.length>120||!driverCompany||driverCompany.length>160||!releaseNumber||releaseNumber.length>100||!phone)throw new Error('Enter a driver name, company, valid phone number, and release number.');
 return {driverName,driverCompany,releaseNumber,phone};
}
function createHandler({db,findOrCreateConversation}) {
 const pending=new Set();
 return async(req,res)=>{
  res.set('Cache-Control','no-store');
  if(!db)return res.status(503).json({error:'Arrivals are temporarily unavailable.'});
  const token=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');
  if(!token)return res.status(401).json({error:'Sign in to Shipping.'});
  let input;
  try{input=parseInput(req.body);}catch(e){return res.status(400).json({error:e.message});}
  let locked=false;
  try{
   const {data,error}=await db.auth.getUser(token);
   if(error||!data?.user)return res.status(401).json({error:'Your session expired. Sign in again.'});
   const staff=data.user;
   if(!['shipping','admin'].includes(staff.app_metadata?.csp_role))return res.status(403).json({error:'Shipping access is required.'});
   if(pending.has(input.phone))return res.status(409).json({error:'This driver is already being added. Refresh arrivals.'});
   pending.add(input.phone);locked=true;
   // Never overwrite an existing active visit or create a second arrival for it.
   const active=await db.from('driver_sms_arrivals').select('id').eq('phone_e164',input.phone).is('departed_at',null).limit(1);
   if(active.error)throw active.error;
   if(active.data?.length)return res.status(409).json({error:'This driver already has an active arrival. Use their existing arrival card.'});
   const found=await findOrCreateConversation(input.phone,'');
   if(found.sessionEnded)return res.status(409).json({error:'This driver has an ended visit in an open conversation. Close that conversation before adding a new arrival.'});
   const conversationId=found.id;
   const contact=await db.from('driver_sms_contacts').upsert({phone_e164:input.phone,full_name:input.driverName,driver_company:input.driverCompany,last_release_number:input.releaseNumber,visit_type:'pickup',onboarding_step:'ready',updated_at:new Date().toISOString()},{onConflict:'phone_e164'});
   if(contact.error)throw contact.error;
   const conversation=await db.from('driver_conversations').update({release_number:input.releaseNumber,visit_type:'pickup'}).eq('id',conversationId);
   if(conversation.error)throw conversation.error;
   const arrival=await db.from('driver_sms_arrivals').insert({phone_e164:input.phone,conversation_id:conversationId,facility_id:'csp-toledo-main',release_number:input.releaseNumber,driver_name:input.driverName,driver_company:input.driverCompany,visit_type:'pickup',added_by:staff.id}).select('id').single();
   if(arrival.error)throw arrival.error;
   // Record arrival only: clerk check-in and outgoing messages remain separate actions.
   return res.json({ok:true,arrivalId:arrival.data.id,conversationId});
  }catch(e){console.error('Manual arrival:',e.message);return res.status(500).json({error:'The driver could not be added. Refresh arrivals before trying again.'});}
  finally{if(locked)pending.delete(input.phone);}
 };
}
module.exports={parseInput,createHandler};
