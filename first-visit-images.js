'use strict';
const session=require('./visit-session');
async function needsImages(db,conversation,excludeIds=[]){
 const ids=new Set([conversation.id]);
 for(const [field,value] of [['user_id',conversation.user_id],['sms_phone_e164',conversation.sms_phone_e164]]){
  if(!value)continue;
  let priorVisits=db.from('driver_conversations').select('id').eq(field,value);
  if(conversation.facility_id)priorVisits=priorVisits.eq('facility_id',conversation.facility_id);
  const {data,error}=await priorVisits;
  if(error)throw error;
  for(const row of data||[])ids.add(row.id);
 }
 const current=session.key(conversation);
 let query=db.from('driver_messages').select('id,conversation_id,client_message_id,sent_at').in('conversation_id',[...ids])
  .or('client_message_id.like.staff-pickup-%,client_message_id.like.dropoff-instructions:%').not('attachment_path','is',null)
  .neq('client_message_id',`staff-pickup-checkin:${current}`).neq('client_message_id',`staff-pickup-ppe:${current}`)
  .neq('client_message_id',`staff-pickup-dropoff-map:${current}`)
  .in('delivery_status',['sent','delivered','queued']);
 for(const id of excludeIds)query=query.neq('client_message_id',id);
 const {data,error}=await query;
 if(error)throw error;
 return !(data||[]).some(message=>{
  // Current-visit images must not suppress a later PPE/map stage or retry.
  if(message.conversation_id===conversation.id&&Number.isFinite(session.start(conversation))&&Date.parse(message.sent_at)>=session.start(conversation))return false;
  return true;
 });
}
module.exports={needsImages};
