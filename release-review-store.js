
'use strict';
// Server-only records, never inserted into the shared Shipping transcript.
function createStore(db){return {
 async load(conversationId){
  const {data,error}=await db.from('driver_release_reviews').select('client_message_id,sent_at').eq('conversation_id',conversationId);
  if(error)throw error;
  return (data||[]).map(row=>({...row,direction:'shipping_to_driver',delivery_status:'sent',body:''}));
 },
 async save(conversationId,outcome){
  const {error}=await db.from('driver_release_reviews').upsert({conversation_id:conversationId,client_message_id:outcome.client_message_id,sent_at:outcome.sent_at},{onConflict:'client_message_id'});
  if(error)throw error;
 }
};}
module.exports={createStore};
