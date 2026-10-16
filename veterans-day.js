'use strict';
const crypto=require('node:crypto');
const TABLE='veterans_day_submissions',BUCKET='veterans-day-photos',MAX=20*1024*1024;
function validate(body={}){
 const text=(key,max,required=false)=>{if(typeof body[key]!=='string'&&body[key]!=null)throw Error('Check the '+key+' field.');const v=String(body[key]||'').trim();if(v.length>max||required&&!v)throw Error('Please complete '+key+' (maximum '+max+' characters).');return v;};
 const token=text('submissionToken',36,true);if(!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(token))throw Error('Refresh the page and try again.');
 const row={submission_token:token,first_name:text('firstName',80,true),last_name:text('lastName',80,true),veteran_name:text('veteranName',160,true),relationship:text('relationship',160,true),branch:text('branch',160,true),years_of_service:text('yearsOfService',160),message:text('message',4000),permission_to_share:body.permissionToShare===true};
 if(!Array.isArray(body.photos)||body.photos.length>4)throw Error('Choose up to four photos.');
 let total=0;const files=body.photos.map((p,i)=>{if(typeof p.base64!=='string'||p.base64.length>Math.ceil(MAX/3)*4||! /^[A-Za-z0-9+/]*={0,2}$/.test(p.base64))throw Error('The selected photo could not be read.');const bytes=Buffer.from(p.base64,'base64');total+=bytes.length;if(!bytes.length||total>MAX)throw Error('Photos must total 20 MB or less.');let ext,type;
 if(bytes.subarray(0,3).equals(Buffer.from([255,216,255]))){ext='jpg';type='image/jpeg';}
 else if(bytes.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10]))){ext='png';type='image/png';}
 else if(bytes.toString('ascii',0,4)==='RIFF'&&bytes.toString('ascii',8,12)==='WEBP'){ext='webp';type='image/webp';}
 else throw Error('Use JPG, PNG, or WebP photos.');
 const hash=crypto.createHash('sha256').update(bytes).digest('hex');return{bytes,type,path:token+'/'+i+'-'+hash+'.'+ext,name:'photo-'+(i+1)+'.'+ext,hash};});
 row.photos=files.map(({path,name,type,bytes})=>({path,name,type,size:bytes.length}));row.payload_hash=crypto.createHash('sha256').update(JSON.stringify(row)).digest('hex');return{row,files};
}
function register(app,{db,sendEmail,escapeHtml,consumeAttempt}){
 app.post('/api/veterans-day/submissions',async(req,res)=>{
 res.set('Cache-Control','no-store');if(req.body?.website)return res.json({success:true});
 let parsed;try{parsed=validate(req.body)}catch(e){return res.status(400).json({error:e.message})}
 if(!consumeAttempt(req,'veterans-day'))return res.status(429).json({error:'Too many attempts. Please try again in 15 minutes.'});
 if(!db)return res.status(503).json({error:'Submissions are temporarily unavailable. Please try again shortly.'});
 const{row,files}=parsed;
 try{
 let {data:stored,error}=await db.from(TABLE).select('*').eq('submission_token',row.submission_token).maybeSingle();if(error)throw error;
 if(!stored){const created=await db.from(TABLE).insert(row).select('*').single();if(created.error){if(created.error.code!=='23505')throw created.error;const prior=await db.from(TABLE).select('*').eq('submission_token',row.submission_token).single();if(prior.error)throw prior.error;stored=prior.data;}else stored=created.data;}
 if(stored.payload_hash!==row.payload_hash)return res.status(409).json({error:'This submission was already saved with different details. Refresh to start a new submission.'});
 if(stored.email_sent_at)return res.json({success:true});
 for(const file of files){const uploaded=await db.storage.from(BUCKET).upload(file.path,file.bytes,{contentType:file.type,upsert:false});if(uploaded.error&&!/already exists|duplicate/i.test(uploaded.error.message))throw uploaded.error;}
 const rows=[['Submitted by',row.first_name+' '+row.last_name],['Veteran',row.veteran_name],['Relationship',row.relationship],['Branch',row.branch],['Years of service',row.years_of_service||'Not provided'],['Message',row.message||'Not provided'],['Permission to share',row.permission_to_share?'Yes':'No — do not publish this submission or photos']];
 const links=[];for(const f of files){const {data,error}=await db.storage.from(BUCKET).createSignedUrl(f.path,604800);if(error)throw error;links.push('<li><a href="'+escapeHtml(data.signedUrl)+'">'+escapeHtml(f.name)+'</a></li>');}
 const id=await sendEmail({to:'kim@coilsteelprocessing.com',subject:'Veterans Day tribute — '+row.veteran_name,html:'<div style="font-family:Arial,sans-serif;color:#233658"><h1>Veterans Day submission</h1><table cellpadding="10">'+rows.map(([k,v])=>'<tr><th align="left">'+escapeHtml(k)+'</th><td style="white-space:pre-wrap">'+escapeHtml(v)+'</td></tr>').join('')+'</table>'+(links.length?'<h2>Photos</h2><ul>'+links.join('')+'</ul><p>Photo links expire in 7 days. Open Forms &amp; Results for fresh links.</p>':'')+'<p><a href="https://coilsteelprocessing.com/forms/">View Forms &amp; Results</a></p></div>',idempotencyKey:'veterans-day-'+row.submission_token});
 const updated=await db.from(TABLE).update({email_sent_at:new Date().toISOString(),email_provider_id:id}).eq('submission_token',row.submission_token);if(updated.error)throw updated.error;
 res.json({success:true});
 }catch(e){console.error('Veterans Day submission failed:',e.message);res.status(503).json({error:'We could not finish sending your submission. Please retry without refreshing; it will not be recorded twice.'});}
 });
}
module.exports={validate,register};
