'use strict';
const {PICKUP_CHECKIN}=require('./driver-visit-flow');
async function sendCheckinInstructions({db,twilio,messagingServiceSid,publicBaseUrl,scheduleStatusSync,conversation,arrivalId,staffId,visitType}){
 if(visitType!=='pickup')return null;
 const id=`pickup-checkin:${arrivalId}`;
 const {data:message,error}=await db.from('driver_messages').insert({client_message_id:id,conversation_id:conversation.id,sender_user_id:staffId,direction:'shipping_to_driver',driver_phone:conversation.sms_phone_e164,body:PICKUP_CHECKIN,original_body:PICKUP_CHECKIN,original_language:'English',sent_at:new Date().toISOString(),delivery_status:'queued'}).select('id').single();
 if(error?.code==='23505')return null;
 if(error)throw error;
 try{
  if(!twilio||!messagingServiceSid)throw Error('Text messaging is not configured.');
  const sent=await twilio.messages.create({to:conversation.sms_phone_e164,body:PICKUP_CHECKIN,messagingServiceSid,statusCallback:`${publicBaseUrl}/api/twilio/message-status`});
  const result=await db.from('driver_messages').update({provider_message_id:sent.sid,provider_status:sent.status,delivery_status:'sent',provider_status_updated_at:new Date().toISOString()}).eq('id',message.id);
  if(result.error)throw result.error;
  scheduleStatusSync(sent.sid);
 }catch(error){await db.from('driver_messages').update({delivery_status:'failed',provider_error_message:String(error.message||error).slice(0,500)}).eq('id',message.id);throw error;}
 return null;
}
module.exports={sendCheckinInstructions};
