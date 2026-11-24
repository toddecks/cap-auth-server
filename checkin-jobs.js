'use strict';
const session=require('./visit-session');
function createQueue({db,send,now=Date.now,defer=setImmediate}){
 const health={mode:'durable-checkin-instructions-v1',state:'idle',lastChecked:null,error:null};
 let running=false,timer,waking=false;
 async function read(query){const r=await query;if(r.error)throw r.error;return r.data;}
 async function enqueue({conversation,arrivalId,staffId,visitType,preparedInstructions}){
  const c={};for(const key of ['id','user_id','channel','sms_phone_e164','facility_id','release_number','visit_type','session_started_at','created_at','session_ended_at','status'])if(conversation[key]!==undefined)c[key]=conversation[key];
  const id=session.key(c);
  await read(db.from('shipping_checkin_jobs').upsert({id,payload:{conversation:c,arrivalId,staffId,visitType,preparedInstructions}},{onConflict:'id',ignoreDuplicates:true}));
  wake();return id;
 }
 function wake(){if(waking)return;waking=true;defer(()=>{waking=false;void tick();});}
 async function tick(){
  if(running||!db)return;running=true;health.state='running';health.error=null;
  try{
   // A interrupted job is safe to resume: the sender's per-visit message ID
   // never resends an SMS whose provider outcome is unknown.
   await read(db.from('shipping_checkin_jobs').update({status:'pending',started_at:null}).eq('status','processing').lt('started_at',new Date(now()-180000).toISOString()));
   const pending=await read(db.from('shipping_checkin_jobs').select('id,payload').eq('status','pending').order('created_at').limit(12));
   let next=0;
   await Promise.all(Array.from({length:Math.min(3,pending.length)},async()=>{while(next<pending.length){
    const job=pending[next++];
    const claimed=await read(db.from('shipping_checkin_jobs').update({status:'processing',started_at:new Date(now()).toISOString()}).eq('id',job.id).eq('status','pending').select('id').maybeSingle());
    if(!claimed)continue;
    let status='complete',error=null;
    try{await send(job.payload);}catch(e){status='needs_review';error=String(e.message||e).slice(0,500);health.error=error;}
    await read(db.from('shipping_checkin_jobs').update({status,error,finished_at:new Date(now()).toISOString()}).eq('id',job.id).eq('status','processing'));
   }}));
  }catch(e){health.error=String(e.message||e);console.error('Check-in instruction jobs:',health.error);}
  finally{running=false;health.state=health.error?'error':'idle';health.lastChecked=new Date(now()).toISOString();}
 }
 async function status(id){return read(db.from('shipping_checkin_jobs').select('status,error').eq('id',id).maybeSingle());}
 return{health,enqueue,tick,status,start(){if(timer)return;wake();timer=setInterval(()=>void tick(),2000);timer.unref?.();},stop(){clearInterval(timer);timer=null;}};
}
function statusHandler({queue,auth}){return async(req,res)=>{
 res.set('Cache-Control','no-store');
 const token=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');
 if(!token)return res.status(401).json({error:'Sign in to Shipping.'});
 try{
  const {data,error}=await auth.auth.getUser(token);
  if(error||!data?.user)return res.status(401).json({error:'Sign in to Shipping.'});
  if(!['shipping','admin'].includes(data.user.app_metadata?.csp_role))return res.status(403).json({error:'Shipping access is required.'});
  const id=String(req.query?.jobId||'');if(!id||id.length>250)return res.status(400).json({error:'Invalid check-in.'});
  const job=await queue.status(id);if(!job)return res.status(404).json({error:'Check-in instructions not found.'});
  return res.json({status:job.status});
 }catch{return res.status(503).json({error:'Could not check instruction status.'});}
};}
module.exports={createQueue,statusHandler};
