'use strict';
const session=require('./visit-session');
// Drafts pause immediately. Sent clerk messages are the durable source of truth,
// including after a worker/server restart. Holds are scoped to a single visit.
const drafts=new Map();
function begin(c,now=Date.now()) {
 for(const [key,at] of drafts)if(now-at>86400000)drafts.delete(key);
 drafts.set(session.key(c),now);
}
function clear(c){drafts.delete(session.key(c));}
function paused(c,messages=[],checked){
 if(!c||c.status!=='open'||c.session_ended_at)return false;
 if(session.currentCheckin(c,checked))return false;
 return drafts.has(session.key(c))||messages.some(m=>m.direction==='shipping_to_driver'&&m.sender_user_id
  &&(!c.session_started_at||Date.parse(m.sent_at)>=session.start(c))
  &&!String(m.client_message_id||'').startsWith('manual-review:')
  &&!['failed','undelivered'].includes(m.delivery_status));
}
async function load(db,id){
 const query=async q=>{const r=await q;if(r.error)throw r.error;return r.data;};
 const c=await query(db.from('driver_conversations').select('*').eq('id',id).maybeSingle());
 if(!c||c.status!=='open'||c.session_ended_at)return false;
 const [checked,messages]=await Promise.all([
  query(db.from('shipping_staff_checkins').select('checked_in_at').eq('conversation_id',id).maybeSingle()),
  query(db.from('driver_messages').select('sender_user_id,direction,sent_at,delivery_status,client_message_id').eq('conversation_id',id))
 ]);
 return paused(c,messages||[],checked);
}
function handler(db){return async(req,res)=>{
 res.set('Cache-Control','no-store');
 if(!db)return res.status(503).json({error:'Shipping is unavailable.'});
 const token=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');
 if(!token)return res.status(401).json({error:'Sign in to Shipping.'});
 const id=String(req.body?.conversationId||'');
 if(!/^[0-9a-f-]{36}$/i.test(id))return res.status(400).json({error:'Invalid conversation.'});
 try{
  const {data,error}=await db.auth.getUser(token);
  if(error||!data?.user)return res.status(401).json({error:'Sign in to Shipping.'});
  if(!['shipping','admin'].includes(data.user.app_metadata?.csp_role))return res.status(403).json({error:'Shipping access is required.'});
  const {data:c,error:ce}=await db.from('driver_conversations').select('*').eq('id',id).maybeSingle();
  if(ce)throw ce;
  if(!c||c.status!=='open'||c.session_ended_at)return res.status(409).json({error:'This visit is no longer open.'});
  begin(c);return res.json({ok:true});
 }catch(error){console.error('Clerk reply pause:',error.message);return res.status(500).json({error:'Could not pause automated replies.'});}
};}
module.exports={begin,clear,paused,load,handler};
