'use strict';
const session=require('./visit-session');
const {PICKUP_CHECKIN}=require('./driver-visit-flow');
const {prepareMap,preparePpe}=require('./checkin-map');
async function sendCheckinInstructions({db,twilio,messagingServiceSid,publicBaseUrl,scheduleStatusSync,conversation,arrivalId,staffId,visitType}){
 if(visitType!=='pickup')return null;
 async function sendOnce(id,body,attachment) {
  const {data:message,error}=await db.from('driver_messages').insert({...attachment?.fields,client_message_id:id,conversation_id:conversation.id,sender_user_id:staffId,direction:'shipping_to_driver',driver_phone:conversation.sms_phone_e164,body,original_body:body,original_language:'English',sent_at:new Date().toISOString(),delivery_status:conversation.channel==='sms'?'queued':'sent'}).select('id').single();
  if(error?.code==='23505')return;
  if(error)throw error;
  if(conversation.channel!=='sms')return;
  try{
   if(!twilio||!messagingServiceSid)throw Error('Text messaging is not configured.');
   const sent=await twilio.messages.create({to:conversation.sms_phone_e164,body,...(attachment?.mediaUrl ? {mediaUrl:[attachment.mediaUrl]} : {}),messagingServiceSid,statusCallback:`${publicBaseUrl}/api/twilio/message-status`});
   const result=await db.from('driver_messages').update({provider_message_id:sent.sid,provider_status:sent.status,delivery_status:['queued','sent','delivered','failed'].includes(sent.status)?sent.status:'queued',provider_status_updated_at:new Date().toISOString()}).eq('id',message.id);
   if(result.error)throw result.error;
   scheduleStatusSync?.(sent.sid);
  }catch(error){await db.from('driver_messages').update({delivery_status:'failed',provider_error_message:String(error.message||error).slice(0,500)}).eq('id',message.id);throw error;}
 }
 await sendOnce(`staff-pickup-checkin:${session.key(conversation)}`,PICKUP_CHECKIN,await prepareMap(db,conversation));
 await sendOnce(`staff-pickup-ppe:${session.key(conversation)}`,'PPE requirements',await preparePpe(db,conversation));
 return null;
}
module.exports={sendCheckinInstructions};
