'use strict';
const {makeContext,generate,fingerprint,MODEL,VERSION}=require('./sad-diagnosis');
function registerAI(app,{db,requireAccess,snapshot,library,analyze,getOpenAIClient}){
 let active=0;const limits=new Map();
 const full=req=>req.authRoles.some(r=>['admin','production'].includes(r));
 const checked=async q=>{const {data,error}=await q;if(error)throw error;return data;};
 const validDays=v=>[1,7,30].includes(Number(v));
 const publicRun=row=>row&&({id:row.id,equipmentId:row.equipment_id,days:row.window_days,createdAt:row.created_at,reviewEnd:row.review_end,latestAlarm:row.latest_alarm,model:row.model,result:row.result,sources:row.context.sources,limitations:row.context.limitations,libraryCoverage:row.context.libraryCoverage});
 async function outcomes(id){return checked(db.from('sad_v2_maintenance_feedback').select('id,assessment_id,created_at,outcome,note').eq('equipment_id',id).order('created_at',{ascending:false}).limit(8));}
 app.get('/api/sad-v2/assessments',requireAccess,async(req,res)=>{res.set('Cache-Control','no-store');if(!validDays(req.query.days))return res.status(400).json({error:'Choose 1, 7, or 30 days.'});try{
 const rows=await checked(db.from('sad_v2_assessments').select('id,equipment_id,created_at,review_end,latest_alarm,result').eq('window_days',Number(req.query.days)).eq('reports_included',full(req)).order('created_at',{ascending:false}).limit(150));
 const seen=new Set();res.json({assessments:rows.filter(r=>!seen.has(r.equipment_id)&&seen.add(r.equipment_id))});
 }catch{res.status(503).json({error:'Saved assessments could not load.'});}});
 app.get('/api/sad-v2/assessment/:group',requireAccess,async(req,res)=>{res.set('Cache-Control','no-store');if(!validDays(req.query.days))return res.status(400).json({error:'Choose 1, 7, or 30 days.'});try{
 const rows=await checked(db.from('sad_v2_assessments').select('*').eq('equipment_id',req.params.group).eq('window_days',Number(req.query.days)).eq('reports_included',full(req)).order('created_at',{ascending:false}).limit(1));
 res.json({assessment:publicRun(rows[0]),feedback:full(req)?await outcomes(req.params.group):[],canRecordFeedback:full(req)});
 }catch{res.status(503).json({error:'Assessment history unavailable.'});}});
 app.post('/api/sad-v2/assessment/:group',requireAccess,async(req,res)=>{
 res.set('Cache-Control','no-store');if(!validDays(req.body?.days))return res.status(400).json({error:'Choose 1, 7, or 30 days.'});
 if(active>=2)return res.status(429).json({error:'Two equipment reviews are running. Try again shortly.'});
 const client=getOpenAIClient();if(!client)return res.status(503).json({error:'AI analysis is not configured. Alarm evidence remains available.'});
 active++;try{
 const s=await snapshot(),review=analyze(s.raw,full(req)?s.reports:[],Number(req.body.days));
 const group=review.groups.find(g=>g.id===req.params.group);
 if(!group||group.id==='unmapped')return res.status(400).json({error:'Choose a mapped equipment group.'});
 if(!group.patterns.some(p=>['AL','SF'].includes(p.alarm_type)))return res.status(422).json({error:'No AL/SF evidence is available for this group and period. Operating messages alone cannot support a fault assessment.'});
 const feedback=full(req)?await outcomes(group.id):[];
 const context={...makeContext(review,group,await library(),feedback),reportsIncluded:full(req)},hash=fingerprint(context);
 const cached=await checked(db.from('sad_v2_assessments').select('*').eq('input_hash',hash).eq('reports_included',full(req)).limit(1));
 if(cached.length)return res.json({assessment:publicRun(cached[0]),reused:true,feedback,canRecordFeedback:full(req)});
 const uid=req.authUser.id,now=Date.now();
 for(const [key,value] of limits)if(now-value.start>600000)limits.delete(key);
 const rate=limits.get(uid)||{start:now,count:0};if(rate.count>=12)return res.status(429).json({error:'Analysis limit reached for this ten-minute period. Saved results remain available.'});rate.count++;limits.set(uid,rate);
 const result=await generate(client,context);
 const row={equipment_id:group.id,window_days:review.days,reports_included:full(req),input_hash:hash,model:MODEL,prompt_version:VERSION,created_by:uid,review_end:review.end,latest_alarm:review.latest,result,context};
 const saved=await db.from('sad_v2_assessments').insert(row).select('*').single();
 if(saved.error){if(saved.error.code!=='23505')throw saved.error;const same=await checked(db.from('sad_v2_assessments').select('*').eq('input_hash',hash).single());return res.json({assessment:publicRun(same),reused:true,feedback,canRecordFeedback:full(req)});}
 res.json({assessment:publicRun(saved.data),reused:false,feedback,canRecordFeedback:full(req)});
 }catch(e){console.error('SAD maintenance analysis:',e.message);res.status(503).json({error:'The AI review could not be completed and verified. No diagnosis has been substituted. Please retry.'});}finally{active--;}
 });
 app.post('/api/sad-v2/assessment/:id/feedback',requireAccess,async(req,res)=>{
 res.set('Cache-Control','no-store');if(!full(req))return res.status(403).json({error:'Production or administrator access is required to record findings.'});
 const outcome=String(req.body?.outcome||''),note=String(req.body?.note||'').trim();
 if(!['inspection_planned','issue_found','no_issue_found','resolved'].includes(outcome)||note.length<5||note.length>2000)return res.status(400).json({error:'Choose an outcome and enter a finding between 5 and 2,000 characters.'});
 try{const rows=await checked(db.from('sad_v2_assessments').select('id,equipment_id').eq('id',req.params.id).limit(1));if(!rows.length)return res.status(404).json({error:'Assessment not found.'});
 await checked(db.from('sad_v2_maintenance_feedback').insert({assessment_id:rows[0].id,equipment_id:rows[0].equipment_id,created_by:req.authUser.id,outcome,note}));
 res.json({feedback:await outcomes(rows[0].equipment_id)});
 }catch{res.status(503).json({error:'The finding was not saved. Please retry.'});}
 });
}
module.exports={registerAI};
