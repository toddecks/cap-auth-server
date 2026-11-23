'use strict';
const{randomBytes,createHash}=require('node:crypto');
const APPROVAL='20261123-survey-test-2',PHONES=['+14199732000','+14193460240'];
const BODY=url=>'Coil Steel Processing Driver Check-In Survey.\n\nComplete our 3-question survey to be entered to win a $50 Visa gift card. '+url;
function createSender({db,twilio,messagingServiceSid}){
 async function existing(phone){const r=await db.from('driver_survey_sms_tests').select('phone_e164,status,provider_message_id,error_code').eq('approval_key',APPROVAL).eq('phone_e164',phone).maybeSingle();if(r.error)throw r.error;return r.data;}
 async function send(){
  if(!twilio||!messagingServiceSid)throw Error('SMS provider is unavailable.');const outcomes=[];
  for(const phone of PHONES){
   const prior=await existing(phone);if(prior){outcomes.push(prior);continue;}
   const token=randomBytes(24).toString('base64url'),url='https://coilsteelprocessing.com/driver-survey/#'+token;
   const invite=await db.from('driver_survey_invitations').insert({campaign:'driver-checkin-20261123',token_hash:createHash('sha256').update(token).digest('hex'),is_test:true,phone_e164:null,driver_name:'SMS test '+phone.slice(-4),driver_company:'Test response',last_checked_in_at:new Date().toISOString(),delivery_status:'test'}).select('id').single();if(invite.error)throw invite.error;
   const claim=await db.from('driver_survey_sms_tests').insert({approval_key:APPROVAL,phone_e164:phone,invitation_id:invite.data.id,status:'sending'});
   if(claim.error?.code==='23505'){outcomes.push(await existing(phone));continue;}if(claim.error)throw claim.error;
   let sent;
   try{sent=await twilio.messages.create({to:phone,body:BODY(url),messagingServiceSid});}
   catch(error){const status=error.status>=400&&error.status<500?'failed':'uncertain';await db.from('driver_survey_sms_tests').update({status,error_code:String(error.code||'provider_response_unknown'),updated_at:new Date().toISOString()}).eq('approval_key',APPROVAL).eq('phone_e164',phone);outcomes.push({phone_e164:phone,status,error_code:String(error.code||'provider_response_unknown')});continue;}
   const saved=await db.from('driver_survey_sms_tests').update({status:sent.status||'queued',provider_message_id:sent.sid,updated_at:new Date().toISOString()}).eq('approval_key',APPROVAL).eq('phone_e164',phone);if(saved.error)throw saved.error;
   outcomes.push({phone_e164:phone,status:sent.status||'queued',provider_message_id:sent.sid});
  }
  return outcomes;
 }
 async function status(){const r=await db.from('driver_survey_sms_tests').select('phone_e164,status,provider_message_id,error_code').eq('approval_key',APPROVAL);if(r.error)throw r.error;const out=[];for(const row of r.data||[]){if(row.provider_message_id&&twilio){try{const p=await twilio.messages(row.provider_message_id).fetch();row.status=p.status;row.error_code=p.errorCode?String(p.errorCode):null;await db.from('driver_survey_sms_tests').update({status:row.status,error_code:row.error_code,updated_at:new Date().toISOString()}).eq('approval_key',APPROVAL).eq('phone_e164',row.phone_e164);}catch{}}out.push(row);}return out;}
 return{send,status};
}
module.exports={createSender,APPROVAL,PHONES,BODY};
