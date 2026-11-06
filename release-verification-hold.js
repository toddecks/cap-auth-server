'use strict';
const session=require('./visit-session');
const TABLE='shipping_release_verifications';
async function result(query){const r=await query;if(r.error)throw r.error;return r.data;}
const current=(hold,c)=>hold?.session_key===session.key(c);
function createStore(db){return {
 async get(c){const hold=await result(db.from(TABLE).select('*').eq('conversation_id',c.id).maybeSingle());return current(hold,c)?hold:null;},
 async pause(c,m,release){return result(db.from(TABLE).upsert({conversation_id:c.id,session_key:session.key(c),attempt_id:String(m.id),release_number:release,requested_at:m.sent_at,verified_at:null,verified_by:null},{onConflict:'conversation_id'}).select('*').single());}
};}
function createHandlers({db,onVerified=()=>{}}){
 async function staff(req,res){
  if(!db){res.status(503).json({error:'Release verification is unavailable.'});return null;}
  const token=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');
  if(!token){res.status(401).json({error:'Sign in to Shipping.'});return null;}
  const {data,error}=await db.auth.getUser(token);const user=data?.user;
  if(error||!user){res.status(401).json({error:'Sign in to Shipping.'});return null;}
  if(!['shipping','admin'].includes(user.app_metadata?.csp_role)){res.status(403).json({error:'Shipping access is required.'});return null;}
  return user;
 }
 return {
 async list(req,res){
  res.set('Cache-Control','no-store');
  try{
   if(!await staff(req,res))return;
   const conversations=await result(db.from('driver_conversations').select('id,status,session_started_at,session_ended_at,created_at').eq('status','open').is('session_ended_at',null).limit(1000));
   if(!conversations.length)return res.json({verifications:[]});
   const ids=conversations.map(c=>c.id);
   const [holds,checks]=await Promise.all([
    result(db.from(TABLE).select('*').in('conversation_id',ids).order('requested_at')),
    result(db.from('shipping_staff_checkins').select('conversation_id,checked_in_at').in('conversation_id',ids))
   ]);
   const map=new Map(conversations.map(c=>[c.id,c])),checked=new Map(checks.map(c=>[c.conversation_id,c]));
   res.json({verifications:holds.filter(h=>{const c=map.get(h.conversation_id);return c&&c.status==='open'&&!c.session_ended_at&&current(h,c)&&!session.currentCheckin(c,checked.get(c.id));})});
  }catch(error){console.error('Release verification list:',error.message);res.status(503).json({error:'Release warnings could not be loaded.'});}
 },
 async verify(req,res){
  res.set('Cache-Control','no-store');
  try{
   const user=await staff(req,res);if(!user)return;
   const id=String(req.body?.conversationId||''),attemptId=String(req.body?.attemptId||'');
   if(!/^[0-9a-f-]{36}$/i.test(id)||!attemptId||req.body?.verified!==true)return res.status(400).json({error:'Review the driver and confirm the release before selecting Verified.'});
   const c=await result(db.from('driver_conversations').select('*').eq('id',id).single());
   if(!c||c.status!=='open'||c.session_ended_at)return res.status(409).json({error:'This visit is no longer open.'});
   const hold=await createStore(db).get(c);
   if(!hold||hold.attempt_id!==attemptId)return res.status(409).json({error:'The release has changed. Refresh and review the latest number.'});
   // A newly received number must be reviewed, even before the worker polls it.
   const messages=await result(db.from('driver_messages').select('id,body,sent_at').eq('conversation_id',id).eq('direction','driver_to_shipping').gte('sent_at',hold.requested_at));
   const newer=messages.some(m=>String(m.id)!==attemptId&&require('./release-attempts').releaseFromMessage(m.body)&&Date.parse(m.sent_at)>=Date.parse(hold.requested_at));
   if(newer)return res.status(409).json({error:'A newer release was received. Refresh and verify the latest number.'});
   if(hold.verified_at)return res.json({ok:true,verification:hold});
   if(!String(c.release_number||'').trim())return res.status(400).json({error:'Enter the verified release number first.'});
   const verified=await result(db.from(TABLE).update({verified_at:new Date().toISOString(),verified_by:user.id,release_number:c.release_number}).eq('conversation_id',id).eq('session_key',session.key(c)).eq('attempt_id',attemptId).is('verified_at',null).select('*').maybeSingle());
   if(!verified)return res.status(409).json({error:'The release changed during verification. Review it again.'});
   res.json({ok:true,verification:verified});
   void Promise.resolve().then(onVerified).catch(error=>console.error('Resume release verification:',error.message));
  }catch(error){console.error('Verify release:',error.message);res.status(500).json({error:'Release verification could not be saved.'});}
 }
 };
}
module.exports={TABLE,current,createStore,createHandlers};
