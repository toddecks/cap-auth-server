'use strict';
const{randomBytes,createHash}=require('node:crypto');
const CAMPAIGN='driver-checkin-20261123-initial',APPROVAL='20261123-approved-first-invitation';
function createCampaign({db,twilio,messagingServiceSid,body,logger=console}){
 let running=false;const health={mode:'approved-initial-survey-v1',error:null};
 const checked=async q=>{const r=await q;if(r.error)throw r.error;return r.data;};
 async function prepare(recipients,userId){const audience=recipients.map(row=>{const token=randomBytes(24).toString('base64url');return{...row,token_hash:createHash('sha256').update(token).digest('hex'),message_body:body.replace('[personal survey link]','https://coilsteelprocessing.com/driver-survey/#'+token)}});const queued=await checked(db.rpc('prepare_driver_survey_initial',{p_approved_by:userId,p_body:body,p_audience:audience}));return{queued};}
 async function patch(row,values){await checked(db.from('driver_survey_initial_outbox').update({...values,updated_at:new Date().toISOString()}).eq('campaign_id',CAMPAIGN).eq('phone_e164',row.phone_e164));}
 async function send(row){
  try{
   const answer=await checked(db.from('driver_survey_responses').select('invitation_id').eq('invitation_id',row.invitation_id).maybeSingle());
   if(answer){await patch(row,{status:'skipped_completed'});return;}
  }catch(error){await patch(row,{status:'uncertain',error_code:'response_check_failed'});return;}
  let sent;
  try{sent=await twilio.messages.create({to:row.phone_e164,body:row.message_body,messagingServiceSid});}
  catch(error){await patch(row,{status:error.status>=400&&error.status<500?'failed':'uncertain',error_code:String(error.code||'provider_response_unknown')});return;}
  const values={status:sent.status||'queued',provider_message_id:sent.sid};
  await patch(row,values);
  await checked(db.from('driver_survey_invitations').update({sent_at:new Date().toISOString(),provider_message_id:sent.sid,delivery_status:sent.status||'queued'}).eq('id',row.invitation_id));
 }
 async function refresh(){
  const rows=await checked(db.from('driver_survey_initial_outbox').select('phone_e164,invitation_id,provider_message_id,status').eq('campaign_id',CAMPAIGN).in('status',['accepted','queued','sent']).order('updated_at').limit(20));
  for(const row of rows||[]){if(!row.provider_message_id)continue;try{const p=await twilio.messages(row.provider_message_id).fetch();await patch(row,{status:p.status,error_code:p.errorCode?String(p.errorCode):null});await checked(db.from('driver_survey_invitations').update({delivery_status:p.status}).eq('id',row.invitation_id));}catch{}}
 }
 async function tick(){if(running||!db||!twilio||!messagingServiceSid)return;running=true;try{const jobs=await checked(db.rpc('claim_driver_survey_initial',{p_limit:8}));for(let i=0;i<(jobs||[]).length;i+=2)await Promise.all(jobs.slice(i,i+2).map(send));await refresh();health.error=null;}catch(error){health.error='Survey send or delivery check incomplete.';logger.error('Survey initial worker:',error.code||'database_or_provider_error');}finally{running=false;}}
 async function status(){const c=await checked(db.from('driver_survey_initial_campaigns').select('approved_at,audience_count').eq('id',CAMPAIGN).maybeSingle());if(!c)return null;const rows=await checked(db.from('driver_survey_initial_outbox').select('status,error_code').eq('campaign_id',CAMPAIGN));const counts={};for(const row of rows||[])counts[row.status]=(counts[row.status]||0)+1;return{...c,total:rows.length,counts,remaining:(counts.pending||0)+(counts.sending||0),errors:rows.filter(row=>row.error_code).map(row=>row.error_code)};}
 function start(){void tick();const timer=setInterval(()=>void tick(),5000);timer.unref?.();return timer;}
 return{prepare,tick,status,start,health};
}
module.exports={createCampaign,CAMPAIGN,APPROVAL};
