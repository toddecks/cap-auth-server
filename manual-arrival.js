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
function createHandler({db,chartDb,findOrCreateConversation,now=()=>new Date().toISOString()}) {
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
   const timestamp=now();
   const facilityId='csp-toledo-main';
   // Match against the same catalog and punctuation/slash rules used by the cards.
   let appointment;
   try {
    const rows=await require('./release-catalog').releaseCandidates(chartDb,input.releaseNumber);
    const key=input.releaseNumber.normalize('NFKC').toUpperCase().replace(/[^A-Z0-9]/g,'');
    appointment=require('./release-doors').doorMatches(rows,[input.releaseNumber],timestamp.slice(0,10))[key+':PROSPY']||null;
   } catch(error) {
    console.error('Manual arrival release lookup:',error.message);
    return res.status(503).json({error:'The release lookup is unavailable. No arrival was added. Please try again.'});
   }
   const active=await db.from('driver_sms_arrivals').select('id,conversation_id,release_number,checked_in_at').eq('phone_e164',input.phone).eq('facility_id',facilityId).is('departed_at',null).limit(1);
   if(active.error)throw active.error;
   const existing=active.data?.[0];
   // Retry a partial add using its existing arrival; never create a duplicate thread.
   const found=existing?{id:existing.conversation_id}:await findOrCreateConversation(input.phone,'PICK UP');
   if(found.sessionEnded)return res.status(409).json({error:'This driver has an ended visit. Refresh Shipping before adding a new arrival.'});
   const conversationId=found.id;
   const current=await db.from('driver_conversations').select('id,status,session_started_at,session_ended_at').eq('id',conversationId).single();
   if(current.error)throw current.error;
   if(!current.data||current.data.status!=='open'||current.data.session_ended_at)return res.status(409).json({error:'This visit has ended. Refresh Shipping before adding a new arrival.'});
   const checked=await db.from('shipping_staff_checkins').select('checked_in_at').eq('conversation_id',conversationId).maybeSingle();
   if(checked.error)throw checked.error;
   const checkedInAt=require('./visit-session').currentCheckin(current.data,checked.data)?checked.data.checked_in_at:null;
   if(existing&&checkedInAt){
    const normalize=value=>String(value||'').normalize('NFKC').toUpperCase().replace(/[^A-Z0-9]/g,'');
    if(normalize(existing.release_number)!==normalize(input.releaseNumber))return res.status(409).json({error:'This driver is already checked in with a different release. Edit their existing arrival card instead.',arrivalId:existing.id,conversationId});
    return res.json({ok:true,arrivalId:existing.id,conversationId,checkedInAt,appointment,alreadyAdded:true});
   }
   const contact=await db.from('driver_sms_contacts').upsert({phone_e164:input.phone,full_name:input.driverName,driver_company:input.driverCompany,last_release_number:input.releaseNumber,visit_type:'pickup',onboarding_step:'ready',updated_at:timestamp},{onConflict:'phone_e164'});
   if(contact.error)throw contact.error;
   const conversation=await db.from('driver_conversations').update({release_number:input.releaseNumber,visit_type:'pickup'}).eq('id',conversationId);
   if(conversation.error)throw conversation.error;
   const arrivalValues={phone_e164:input.phone,conversation_id:conversationId,facility_id:facilityId,release_number:input.releaseNumber,driver_name:input.driverName,driver_company:input.driverCompany,visit_type:'pickup',added_by:staff.id,entered_at:timestamp,checked_in_at:timestamp};
   const arrival=existing
    ? await db.from('driver_sms_arrivals').update(arrivalValues).eq('id',existing.id).is('departed_at',null).select('id').single()
    : await db.from('driver_sms_arrivals').insert(arrivalValues).select('id').single();
   if(arrival.error)throw arrival.error;
   // Both feeds use the official check-in record, not just the raw arrival row.
   const checkin=await db.from('shipping_staff_checkins').upsert({conversation_id:conversationId,checked_in_by:staff.id,checked_in_at:timestamp},{onConflict:'conversation_id'});
   if(checkin.error)throw checkin.error;
   // A clerk is entering an arrival, not composing a message: do not send SMS here.
   return res.json({ok:true,arrivalId:arrival.data.id,conversationId,checkedInAt:timestamp,appointment,recovered:Boolean(existing)});

  }catch(e){console.error('Manual arrival:',e.message);return res.status(500).json({error:'The driver could not be added. Refresh arrivals before trying again.'});}
  finally{if(locked)pending.delete(input.phone);}
 };
}
module.exports={parseInput,createHandler};
