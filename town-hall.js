'use strict';
const {createHash}=require('node:crypto');
const TABLE='town_hall_submissions',RECIPIENTS=['kim@coilsteelprocessing.com'];
function validate(body={}){
 const text=(key,max)=>{if(body[key]!=null&&typeof body[key]!=='string')throw Error('Please check the '+key+' field.');const value=String(body[key]||'').trim();if(value.length>max)throw Error('Please shorten the '+key+' field to '+max+' characters or less.');return value;};
 const token=text('submissionToken',36);
 if(!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(token))throw Error('Refresh the page and try again.');
 const row={submission_token:token,employee_name:text('employeeName',160)||null,questions:text('questions',6000),concerns:text('concerns',6000),leadership_notes:text('leadershipNotes',6000)};
 if(![row.questions,row.concerns,row.leadership_notes].some(Boolean))throw Error('Please answer at least one question before submitting.');
 row.payload_hash=createHash('sha256').update(JSON.stringify(row)).digest('hex');return row;
}
function emailHtml(row,escapeHtml){
 const sections=[['What questions do you have about the new production line?',row.questions],['Do you have any concerns about the upcoming changes?',row.concerns],["Is there anything else you'd like leadership to know or discuss?",row.leadership_notes]];
 return '<div style="background:#f3f6fb;padding:28px 16px;font-family:Arial,sans-serif;color:#233658"><div style="max-width:680px;margin:auto;background:#fff;border:1px solid #dce4ef;border-radius:12px;overflow:hidden"><div style="background:#233658;border-bottom:5px solid #f1a91e;padding:28px;color:#fff"><p style="color:#f1a91e;font-size:12px;font-weight:bold;letter-spacing:2px;margin:0 0 12px">CSP NEW LINE TOWN HALL</p><h1 style="font-size:27px;line-height:1.2;margin:0">Question submission</h1></div><div style="padding:28px"><p style="margin:0 0 25px;font-size:16px"><strong>Submitted by:</strong> '+escapeHtml(row.employee_name||'Anonymous')+'</p>'+sections.map(([label,value],i)=>'<div style="margin:0 0 24px"><h2 style="font-size:17px;line-height:1.4;margin:0 0 10px">'+(i+1)+'. '+escapeHtml(label)+'</h2><div style="background:#f5f7fb;border:1px solid #e2e8f0;border-radius:8px;padding:15px;white-space:pre-wrap;line-height:1.6;color:#24344e">'+escapeHtml(value||'No response provided.')+'</div></div>').join('')+'<p style="font-size:14px;color:#65738a">Submitted for leadership’s review ahead of the CSP New Line Town Hall.</p><a href="https://coilsteelprocessing.com/forms/" style="display:inline-block;background:#f1a91e;color:#233658;padding:13px 18px;border-radius:7px;font-weight:bold;text-decoration:none">View Forms &amp; Results</a></div></div></div>';
}
function register(app,{db,sendEmail,escapeHtml,consumeAttempt}){
 app.post('/api/town-hall/submissions',async(req,res)=>{
  res.set('Cache-Control','no-store');
  if(req.body?.website)return res.json({success:true});
  let row;try{row=validate(req.body)}catch(error){return res.status(400).json({error:error.message})}
  if(!consumeAttempt(req,'town-hall-submission'))return res.status(429).json({error:'Too many attempts. Please try again in 15 minutes.'});
  if(!db)return res.status(503).json({error:'Submissions are temporarily unavailable. Please try again shortly.'});
  try{
   let {data:stored,error}=await db.from(TABLE).select('*').eq('submission_token',row.submission_token).maybeSingle();if(error)throw error;
   if(!stored){const created=await db.from(TABLE).insert(row).select('*').single();if(created.error){if(created.error.code!=='23505')throw created.error;const prior=await db.from(TABLE).select('*').eq('submission_token',row.submission_token).single();if(prior.error)throw prior.error;stored=prior.data;}else stored=created.data;}
   if(stored.payload_hash!==row.payload_hash)return res.status(409).json({error:'This submission was already saved with different responses. Refresh to start a new submission.'});
   if(!stored.email_sent_at){
    const id=await sendEmail({to:RECIPIENTS,subject:'CSP New Line Town Hall — '+(row.employee_name||'Anonymous submission'),html:emailHtml(row,escapeHtml),idempotencyKey:'town-hall-'+row.submission_token});
    const updated=await db.from(TABLE).update({email_sent_at:new Date().toISOString(),email_provider_id:id}).eq('submission_token',row.submission_token);if(updated.error)throw updated.error;
   }
   return res.json({success:true});
  }catch(error){console.error('Town Hall submission failed:',error.message);return res.status(503).json({error:'We could not finish sending your submission. Please retry without refreshing; your responses will not be saved twice.'});}
 });
}
module.exports={validate,emailHtml,register,RECIPIENTS};
