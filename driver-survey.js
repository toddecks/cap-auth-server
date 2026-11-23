'use strict';
const {createHash}=require('node:crypto');
const CAMPAIGN='driver-checkin-20261123',OWNER='todd@coilsteelprocessing.com';
const QUESTIONS=[
 {key:'easy_to_use',text:'Was the text message check-in process easy to use?',options:['Yes','No']},
 {key:'clear_instructions',text:'Did the text messages clearly explain what you needed to do?',options:['Yes','No']},
 {key:'preferred_method',text:'Which check-in method do you prefer?',options:['Text message','Phone call','No preference']}
];
const INVITATION="Coil Steel Processing: We'd appreciate your feedback on driver check-in. Please take this 3-question survey when safely parked: [personal survey link] Reply STOP to opt out.";
const hash=token=>createHash('sha256').update(String(token)).digest('hex');
const owner=user=>Boolean(user?.email_confirmed_at&&String(user.email||'').toLowerCase()===OWNER);
function validate(input){const out={};for(const q of QUESTIONS){if(!q.options.includes(input?.[q.key]))throw Error('Please answer all three questions.');out[q.key]=input[q.key];}return out;}
async function rows(query){const r=await query;if(r.error)throw r.error;return r.data||[];}
async function pages(makeQuery){let all=[];for(let offset=0;;offset+=500){const page=await rows(makeQuery().range(offset,offset+499));all.push(...page);if(page.length<500)return all;}}
function mergeRecipients(sms,app,profiles){
 const people=new Map(profiles.map(row=>[row.user_id,row])),phones=new Map();let missingPhone=0;
 function add(row){if(!/^\+[1-9][0-9]{7,14}$/.test(row.phone_e164||'')){missingPhone++;return;}const prior=phones.get(row.phone_e164);if(!prior||Date.parse(row.last_checked_in_at)>Date.parse(prior.last_checked_in_at))phones.set(row.phone_e164,row);}
 for(const row of sms)add({phone_e164:row.phone_e164,driver_name:row.driver_name||'',driver_company:row.driver_company||'',last_checked_in_at:row.checked_in_at});
 for(const row of app){const p=people.get(row.user_id)||{};add({phone_e164:p.phone_e164,driver_name:p.full_name||row.profile_snapshot?.name||'',driver_company:p.driver_company||'',last_checked_in_at:row.occurred_at});}
 return{recipients:[...phones.values()].sort((a,b)=>a.phone_e164.localeCompare(b.phone_e164)),missingPhone};
}
async function recipientPreview(driver){
 const [sms,app]=await Promise.all([
  pages(()=>driver.from('driver_sms_arrivals').select('id,phone_e164,driver_name,driver_company,checked_in_at').not('checked_in_at','is',null).order('id')),
  pages(()=>driver.from('driver_arrivals').select('id,user_id,occurred_at,profile_snapshot').eq('event_type','manual_check_in').order('id'))
 ]);
 const profiles=[],ids=[...new Set(app.map(row=>row.user_id).filter(Boolean))];
 for(let i=0;i<ids.length;i+=100)profiles.push(...await rows(driver.from('driver_profiles').select('user_id,phone_e164,full_name,driver_company').in('user_id',ids.slice(i,i+100))));
 return mergeRecipients(sms,app,profiles);
}
function csv(rows){const keys=['driver_name','driver_company','phone_e164','easy_to_use','clear_instructions','preferred_method','submitted_at'];const cell=v=>'"'+String(v??'').replace(/^[=+@-]/,"'$&").replace(/"/g,'""')+'"';return '\uFEFF'+[keys.map(cell).join(','),...rows.map(row=>keys.map(key=>cell(row[key])).join(','))].join('\r\n');}
function register(app,{db,driver,auth,consumeAttempt}){
 const fail=(res)=>res.status(503).json({error:'The survey is temporarily unavailable. Please try again.'});
 async function requireOwner(req,res,next){res.set('Cache-Control','no-store');const token=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');if(!token)return res.status(401).json({error:'Sign in with your CSP account.'});try{const r=await auth.auth.getUser(token);if(r.error||!r.data?.user)return res.status(401).json({error:'Please sign in again.'});if(!owner(r.data.user))return res.status(403).json({error:'This survey is restricted to its owner.'});next();}catch{return fail(res);}}
 async function invitation(token){if(!/^[A-Za-z0-9_-]{32,64}$/.test(String(token||'')))return null;const r=await db.from('driver_survey_invitations').select('id,expires_at').eq('campaign',CAMPAIGN).eq('token_hash',hash(token)).maybeSingle();if(r.error)throw r.error;return r.data&&Date.parse(r.data.expires_at)>Date.now()?r.data:null;}
 app.get('/api/driver-survey/config',(_req,res)=>{res.set('Cache-Control','no-store');res.json({title:'Driver Check-In Survey',questions:QUESTIONS,sendingEnabled:false});});
 app.post('/api/driver-survey/check',async(req,res)=>{res.set('Cache-Control','no-store');if(!consumeAttempt(req,'driver-survey-check'))return res.status(429).json({error:'Please wait a few minutes before trying again.'});try{const invite=await invitation(req.body?.token);if(!invite)return res.status(404).json({error:'Please use the personal survey link in your invitation.'});const r=await db.from('driver_survey_responses').select('invitation_id').eq('invitation_id',invite.id).maybeSingle();if(r.error)throw r.error;res.json({valid:true,submitted:Boolean(r.data)});}catch{return fail(res);}});
 app.post('/api/driver-survey/responses',async(req,res)=>{res.set('Cache-Control','no-store');let answers;try{answers=validate(req.body?.answers);}catch(e){return res.status(400).json({error:e.message});}if(!consumeAttempt(req,'driver-survey-submit'))return res.status(429).json({error:'Please wait a few minutes before trying again.'});try{const invite=await invitation(req.body?.token);if(!invite)return res.status(404).json({error:'Please use the personal survey link in your invitation.'});const r=await db.from('driver_survey_responses').insert({invitation_id:invite.id,...answers});if(r.error&&r.error.code!=='23505')throw r.error;res.json({success:true});}catch{return fail(res);}});
 app.get('/api/driver-survey/owner/recipients',requireOwner,async(_req,res)=>{try{const preview=await recipientPreview(driver);res.json({...preview,count:preview.recipients.length,invitation:INVITATION,sendingEnabled:false});}catch{return fail(res);}});
 async function results(){const invites=await pages(()=>db.from('driver_survey_invitations').select('id,driver_name,driver_company,phone_e164,sent_at,delivery_status').eq('campaign',CAMPAIGN).order('created_at').order('id'));const responses=await pages(()=>db.from('driver_survey_responses').select('*').order('submitted_at').order('invitation_id'));const map=new Map(invites.map(row=>[row.id,row]));return{invited:invites.filter(row=>row.sent_at).length,rows:responses.filter(row=>map.has(row.invitation_id)).map(row=>({...map.get(row.invitation_id),...row}))};}
 app.get('/api/driver-survey/owner/results',requireOwner,async(_req,res)=>{try{res.json({...await results(),sendingEnabled:false});}catch{return fail(res);}});
 app.get('/api/driver-survey/owner/results.csv',requireOwner,async(_req,res)=>{try{res.set({'Content-Type':'text/csv; charset=utf-8','Content-Disposition':'attachment; filename="driver-checkin-survey.csv"'});res.send(csv((await results()).rows));}catch{return fail(res);}});
 // Sending is deliberately absent. No scheduler, campaign worker, or send route.
}
module.exports={register,owner,validate,mergeRecipients,QUESTIONS,CAMPAIGN,INVITATION,hash,csv};
