'use strict';
const games=require('./walleye-games.json');
const TABLE='walleye_ticket_requests';
function validate(body={},now=Date.now()){
 const text=(key,max)=>{const value=String(body[key]??'').trim();if(value.length>max)throw Error('Please shorten the '+key+' field.');return value;};
 const first_name=text('firstName',80),last_name=text('lastName',80),phone=text('phone',40),email=text('email',254).toLowerCase();
 if(!first_name||!last_name)throw Error('Enter your first and last name.');
 if(!/^\+?[\d\s().-]+$/.test(phone)||phone.replace(/\D/g,'').length<10||phone.replace(/\D/g,'').length>15)throw Error('Enter a valid phone number.');
 if(!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))throw Error('Enter a valid email address for your ticket request.');
 const game=games.find(g=>g.id===body.game);
 if(!game||Date.parse(game.startsAt)<=now)throw Error('Choose an upcoming game from the list.');
 if(!['1','2','3','4'].includes(String(body.tickets)))throw Error('Choose 1, 2, 3, or 4 tickets.');
 if(!['electronic','printed'].includes(body.delivery))throw Error('Choose how you would like to receive your tickets.');
 if(!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(body.submissionToken??''))throw Error('Refresh the page and try again.');
 return {submission_token:body.submissionToken,first_name,last_name,phone,email,game_id:game.id,game_label:game.label,tickets:Number(body.tickets),delivery:body.delivery};
}
function register(app,{db,sendEmail,escapeHtml,consumeAttempt}){
 app.get('/api/walleye/games',(req,res)=>res.json({games:games.filter(g=>Date.parse(g.startsAt)>Date.now())}));
 app.post('/api/walleye/requests',async(req,res)=>{
  res.set('Cache-Control','no-store');
  if(req.body?.website)return res.json({success:true});
  let signup;try{signup=validate(req.body)}catch(e){return res.status(400).json({error:e.message})}
  if(!consumeAttempt(req,'walleye-request'))return res.status(429).json({error:'Too many attempts. Please try again in 15 minutes.'});
  if(!db)return res.status(503).json({error:'Ticket requests are temporarily unavailable. Please try again shortly.'});
  try{
   const {data:prior,error:lookupError}=await db.from(TABLE).select('*').eq('submission_token',signup.submission_token).maybeSingle();if(lookupError)throw lookupError;
   let stored=prior;
   if(stored&&Object.keys(signup).some(k=>stored[k]!==signup[k]))return res.status(409).json({error:'This request was already saved. Start a new request to change your selection.'});
   if(!stored){const {data,error}=await db.from(TABLE).insert(signup).select('*').single();if(error){if(error.code==='23505')return res.status(409).json({error:'Your request is being processed. Please try again in a moment.'});throw error}stored=data;}
   if(!stored.email_sent_at){
    const rows=[['Employee',signup.first_name+' '+signup.last_name],['Phone',signup.phone],['Email',signup.email],['Game',signup.game_label],['Tickets',signup.tickets],['Delivery preference',signup.delivery==='electronic'?'Electronically':'Printed tickets']];
    const id=await sendEmail({to:'kim@coilsteelprocessing.com',replyTo:signup.email,subject:'Walleye ticket request — '+signup.first_name+' '+signup.last_name,html:'<div style="font-family:Arial,sans-serif;color:#233658;max-width:680px"><h1>Toledo Walleye</h1><h2>Employee ticket request</h2><table cellpadding="12" style="border-collapse:collapse;width:100%">'+rows.map(([label,value])=>'<tr><th align="left" style="border-bottom:1px solid #ddd">'+escapeHtml(label)+'</th><td style="border-bottom:1px solid #ddd">'+escapeHtml(String(value))+'</td></tr>').join('')+'</table><p>This is a ticket request, pending confirmation.</p></div>',idempotencyKey:'walleye-'+signup.submission_token});
    const {error}=await db.from(TABLE).update({email_sent_at:new Date().toISOString(),email_provider_id:id}).eq('submission_token',signup.submission_token);if(error)throw error;
   }
   return res.json({success:true});
  }catch(error){console.error('Walleye request failed:',error?.message);return res.status(503).json({error:'We could not finish sending your request. Please try again; your request will not be recorded twice.'})}
 });
}
module.exports={validate,register};
