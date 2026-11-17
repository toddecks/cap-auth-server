'use strict';
const {createHash,timingSafeEqual}=require('node:crypto');
const {sync}=require('./scripts/sync-azure-order-entries.cjs');
const jobKey=key=>createHash('sha256').update('csp-order-entry-sync-v1:'+key).digest('hex');
function handler({url,key,run=sync}){
 let busy=false;
 return async(req,res)=>{
  const supplied=String(req.get('x-csp-sync-key')||'');
  if(!key||supplied.length!==64||!timingSafeEqual(Buffer.from(supplied),Buffer.from(jobKey(key))))return res.status(401).json({error:'Sync authentication required.'});
  if(busy)return res.status(409).json({error:'An order-entry refresh is already running.'});
  const user=req.body?.sqlUser,password=req.body?.sqlPassword;
  if(typeof user!=='string'||!user||user.length>128||typeof password!=='string'||!password||password.length>1024)return res.status(400).json({error:'SQL credentials are required.'});
  busy=true;
  try {
   const result=await run({PSDATA_SQL_SERVER:'psdata-azure.database.windows.net',PSDATA_SQL_DATABASE:'psdata-azure',
    PSDATA_SQL_USER:user,PSDATA_SQL_PASSWORD:password,SUPABASE_URL:url,SUPABASE_SERVICE_ROLE_KEY:key});
   return res.json({ok:true,...result});
  }catch(error){
   console.error('Cloud order-entry sync failed:',error.code||'SYNC_FAILED');
   const blocked=String(error.message||'').match(/Client with IP address '([\d.]+)' is not allowed/i)?.[1];
   return res.status(502).json({error:blocked?'Azure firewall blocked the cloud server.':'Unable to refresh order entries.',code:error.code||'SYNC_FAILED',...(blocked?{blockedCloudIp:blocked}:{})});
  }finally{busy=false;}
 };
}
module.exports={handler,jobKey};
