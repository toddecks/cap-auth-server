'use strict';
function register({app,db}){
 const health={mode:'closed-permanent-delete-v1',error:null};let running=false;
 async function cleanup(id,paths){
  const owned=(paths||[]).filter(path=>typeof path==='string'&&path.includes(`/${id}/`)&&!path.split('/').includes('..'));
  if(owned.length){const {error}=await db.storage.from('driver-paperwork').remove(owned);if(error)throw error;}
  const result=await db.from('driver_conversations').update({purged_attachment_paths:null}).eq('id',id).not('purged_at','is',null);if(result.error)throw result.error;
 }
 async function drain(){
  if(!db||running)return;running=true;
  try{
   const {data,error}=await db.from('driver_conversations').select('id,purged_attachment_paths').not('purged_at','is',null).not('purged_attachment_paths','is',null).limit(50);if(error)throw error;
   for(const row of data||[])await cleanup(row.id,row.purged_attachment_paths);
   health.error=null;
  }catch(error){health.error=String(error.message||error);console.error('Deleted conversation attachment cleanup:',health.error);}finally{running=false;}
 }
 const handler=async(req,res)=>{
  res.set('Cache-Control','no-store');
  if(!db)return res.status(503).json({error:'Conversation deletion is unavailable.'});
  const token=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');
  if(!token)return res.status(401).json({error:'Sign in to Shipping.'});
  const id=String(req.body?.conversationId||'');
  if(!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id))return res.status(400).json({error:'Choose a closed conversation.'});
  try{
   const {data:auth,error}=await db.auth.getUser(token);if(error||!auth?.user)return res.status(401).json({error:'Sign in to Shipping.'});
   if(!['shipping','admin'].includes(auth.user.app_metadata?.csp_role))return res.status(403).json({error:'Shipping access is required.'});
   const result=await db.rpc('shipping_purge_closed_conversation',{target_id:id});
   if(result.error){if(result.error.code==='22023')return res.status(409).json({error:result.error.message});throw result.error;}
   if(!result.data?.deleted)return res.status(404).json({error:'Conversation no longer exists.'});
   let warning=null;
   try{await cleanup(id,result.data.attachmentPaths);}catch(error){health.error=String(error.message||error);warning='Messages deleted. Attachment cleanup is queued for retry.';}
   return res.json({ok:true,warning});
  }catch(error){console.error('Permanent conversation deletion:',error.message);return res.status(500).json({error:'Could not permanently delete this conversation. Try again.'});}
 };
 app.post('/api/shipping/permanently-delete-conversation',handler);
 const timer=setInterval(()=>void drain(),60000);timer.unref?.();void drain();
 return{health,handler,cleanup,drain,stop:()=>clearInterval(timer)};
}
module.exports={register};
