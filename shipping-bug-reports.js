'use strict';
module.exports = function registerShippingBugReports({app,supabase,resolveBiShippingRole,apiKey,from,send=fetch}) {
 const recent=new Map(),pending=new Set();
 app.post('/api/shipping/bug-report',async(req,res)=>{
  res.set('Cache-Control','no-store');
  let userId;
  try {
   const token=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');
   if(!token)return res.status(401).json({error:'Sign in before sending a bug report.'});
   const {data,error}=await supabase.auth.getUser(token);
   if(error||!data?.user)return res.status(401).json({error:'Your session expired. Sign in again.'});
   const user=data.user;
   if(!await resolveBiShippingRole(user))return res.status(403).json({error:'Shipping access required.'});
   const {reportId,summary,description,reporterName,page}=req.body||{};
   if(typeof reportId!=='string'||!/^[a-f0-9-]{36}$/i.test(reportId)||typeof summary!=='string'||!summary.trim()||summary.length>150||/[\r\n]/.test(summary)||typeof description!=='string'||!description.trim()||description.length>6000||typeof reporterName!=='string'||reporterName.length>100||typeof page!=='string'||page.length>100)return res.status(400).json({error:'Enter a short summary and describe the problem.'});
   if(!apiKey||!from)return res.status(503).json({error:'Email reporting is temporarily unavailable.'});
   const previous=recent.get(user.id);
   if(previous?.id===reportId)return res.json({ok:true});
   if(pending.has(user.id)||(previous&&Date.now()-previous.at<60000))return res.status(429).json({error:'Please wait one minute before sending another report.'});
   userId=user.id;pending.add(userId);
   const text=['Shipping web app bug report','',`Summary: ${summary.trim()}`,`Reported by: ${reporterName.trim()||user.user_metadata?.full_name||user.email}`,`Account: ${user.email}`,`Page: ${page}`,`Received: ${new Date().toISOString()}`,'','What happened:',description.trim()].join('\n');
   const response=await send('https://api.resend.com/emails',{method:'POST',headers:{Authorization:`Bearer ${apiKey}`,'Content-Type':'application/json','Idempotency-Key':`shipping-bug-${user.id}-${reportId}`},body:JSON.stringify({from,to:['todd@coilsteelprocessing.com'],reply_to:user.email,subject:`[Shipping bug] ${summary.trim()}`,text}),signal:AbortSignal.timeout(20000)});
   if(!response.ok)return res.status(502).json({error:'The report could not be sent. Your text is saved here; please try again.'});
   for(const [id,item] of recent)if(Date.now()-item.at>86400000)recent.delete(id);
   recent.set(user.id,{id:reportId,at:Date.now()});
   return res.json({ok:true});
  }catch{return res.status(502).json({error:'The report could not be sent. Please try again.'});}
  finally{if(userId)pending.delete(userId);}
 });
};
