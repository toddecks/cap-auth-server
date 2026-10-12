'use strict';
const session=require('./visit-session');
async function needsImages(db,conversation,excludeIds=[]){
 const ids=new Set([conversation.id]);
 for(const [field,value] of [['user_id',conversation.user_id],['sms_phone_e164',conversation.sms_phone_e164]]){
  if(!value)continue;
  const {data,error}=await db.from('driver_conversations').select('id').eq(field,value);
  if(error)throw error;
  for(const row of data||[])ids.add(row.id);
 }
 const current=session.key(conversation);
 let query=db.from('driver_messages').select('id').in('conversation_id',[...ids])
  .like('client_message_id','staff-pickup-%').not('attachment_path','is',null)
  .neq('client_message_id',`staff-pickup-checkin:${current}`).neq('client_message_id',`staff-pickup-ppe:${current}`)
  .in('delivery_status',['sent','delivered','queued']);
 for(const id of excludeIds)query=query.neq('client_message_id',id);
 const {data,error}=await query.limit(1);
 if(error)throw error;
 return !data?.length;
}
module.exports={needsImages};
