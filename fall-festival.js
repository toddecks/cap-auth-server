'use strict';
const CHOICES=new Set(['yes','no','maybe']);
function validate(body={}){
 const name=String(body.employeeName??'').trim(),count=String(body.familyCount??'').trim();
 if(!name||name.length>120)throw Error('Enter your employee name.');
 if(!CHOICES.has(body.attendance)||!CHOICES.has(body.chili))throw Error('Choose an attendance and Chili Cook-Off response.');
 if(!/^\d+$/.test(count)||Number(count)>100)throw Error('Enter a whole number of family members from 0 to 100.');
 if(!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(body.submissionToken??''))throw Error('Refresh the page and try again.');
 return {submission_token:body.submissionToken,employee_name:name,attendance:body.attendance,family_count:Number(count),chili:body.chili};
}
function register(app,{db,sendEmail,escapeHtml,consumeAttempt}){
 app.post('/api/fall-festival-signups',async(req,res)=>{
  res.set('Cache-Control','no-store');
  if(req.body?.website)return res.json({success:true});
  let signup;try{signup=validate(req.body)}catch(e){return res.status(400).json({error:e.message})}
  if(!consumeAttempt(req,'fall-festival'))return res.status(429).json({error:'Too many attempts. Please try again in 15 minutes.'});
  if(!db)return res.status(503).json({error:'Signups are temporarily unavailable. Please try again shortly.'});
  try{
   const {data:prior,error:lookupError}=await db.from('fall_festival_signups').select('*').eq('submission_token',signup.submission_token).maybeSingle();if(lookupError)throw lookupError;
   let stored=prior;
   if(stored&&Object.keys(signup).some(k=>stored[k]!==signup[k]))return res.status(409).json({error:'This response was already saved. Refresh to send a new response.'});
   if(!stored){const {data,error}=await db.from('fall_festival_signups').insert(signup).select('*').single();if(error){if(error.code==='23505')return res.status(409).json({error:'Your response is being processed. Please try again in a moment.'});throw error}stored=data;}
   if(!stored.email_sent_at){
    const labels={yes:'Yes',no:'No',maybe:'Maybe – not ready to commit'};
    const rows=[['Employee',signup.employee_name],['Attending',labels[signup.attendance]],['Family members (excluding employee)',signup.family_count],['Chili Cook-Off',labels[signup.chili]]];
    const id=await sendEmail({to:'lindsey@coilsteelprocessing.com',subject:'CSP Family Fall Festival — '+signup.employee_name,html:'<div style="font-family:Arial,sans-serif;color:#233658;max-width:650px"><h1>CSP Family Fall Festival</h1><h2>New signup response</h2><table cellpadding="12" style="border-collapse:collapse;width:100%">'+rows.map(([label,value])=>'<tr><th align="left" style="border-bottom:1px solid #ddd">'+escapeHtml(label)+'</th><td style="border-bottom:1px solid #ddd">'+escapeHtml(String(value))+'</td></tr>').join('')+'</table></div>',idempotencyKey:'fall-festival-'+signup.submission_token});
    const {error}=await db.from('fall_festival_signups').update({email_sent_at:new Date().toISOString(),email_provider_id:id}).eq('submission_token',signup.submission_token);if(error)throw error;
   }
   return res.json({success:true});
  }catch(error){console.error('Fall Festival signup failed:',error?.message);return res.status(503).json({error:'We could not finish sending your response. Please try again; your response will not be recorded twice.'})}
 });
}
module.exports={validate,register};
