'use strict';
const session=require('./visit-session');
const {needsImages}=require('./first-visit-images');
const {prepareMap,preparePpe}=require('./checkin-map');
async function sendBothImages({db,twilio,messagingServiceSid,publicBaseUrl,scheduleStatusSync,conversation:c,messages,prepare=[prepareMap,preparePpe]}) {
 if(!messages.some(m=>m.direction==='shipping_to_driver'&&m.body?.startsWith('STEP 1')&&!['failed','undelivered'].includes(m.delivery_status)))return;
 const ids=['map','ppe'].map(kind=>`staff-pickup-both-${kind}:${session.key(c)}`);
 if(ids.every(id=>messages.some(m=>m.client_message_id===id)))return;
 if(!await needsImages(db,c,ids))return;
 for(let i=0;i<ids.length;i++){
  if(messages.some(m=>m.client_message_id===ids[i]))continue;
  const attachment=await prepare[i](db,c);
  if(!attachment)continue;
  const body=i===0?'Property map':'PPE requirements';
  const {data,error}=await db.from('driver_messages').insert({...attachment.fields,client_message_id:ids[i],conversation_id:c.id,sender_user_id:null,direction:'shipping_to_driver',driver_phone:c.sms_phone_e164,body,original_body:body,original_language:'English',sent_at:new Date().toISOString(),delivery_status:c.channel==='sms'?'queued':'sent'}).select('id').single();
  if(error?.code==='23505')continue;
  if(error)throw error;
  if(c.channel!=='sms')continue;
  try {
   if(!twilio||!messagingServiceSid)throw Error('Text messaging is not configured.');
   const sent=await twilio.messages.create({to:c.sms_phone_e164,body,mediaUrl:[attachment.mediaUrl],messagingServiceSid,statusCallback:`${publicBaseUrl}/api/twilio/message-status`});
   const result=await db.from('driver_messages').update({provider_message_id:sent.sid,provider_status:sent.status,delivery_status:['queued','sent','delivered','failed','undelivered'].includes(sent.status)?sent.status:'queued',provider_status_updated_at:new Date().toISOString()}).eq('id',data.id);
   if(result.error)throw result.error;
   scheduleStatusSync?.(sent.sid);
  }catch(error){await db.from('driver_messages').update({delivery_status:'failed',provider_error_message:String(error.message||error).slice(0,500)}).eq('id',data.id);throw error;}
 }
}
module.exports={sendBothImages};
