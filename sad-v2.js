'use strict';
const GROUPS=[
 ['entry','Entry coil car',/entry_coil|coil_stage|coil_car/i,/entry coil|coil car|coil stage/i],
 ['uncoiler','Uncoiler',/uncoiler/i,/uncoiler|uncoiling|coil slip/i],
 ['peeler','Peeler / threading',/peeler|thread_hg/i,/peeler|threading|thread shelf/i],
 ['straightener','Straightener',/straightener/i,/straightener/i],
 ['pinch','Leveler pinch rolls',/leveler_pinch/i,/pinch roll|pinch drive/i],
 ['feed','Roll feed',/roll_feed/i,/roll feed|feed roll/i],
 ['shear','Prime shear',/prime_shear/i,/prime shear|\bshear\b/i],
 ['stretcher','Stretcher',/stretcher/i,/stretcher|stretching/i],
 ['stacker','Drop stacker',/drop_stacker/i,/drop stacker|\bstacker\b|backstop/i],
 ['table','Stack table / rollout',/stack_table|rollout/i,/stack table|rollout/i],
 ['safety','Safety / services',/services|twinsafe/i,/light curtain|safety gate|e.?stop|emergency stop|guard door/i],
 ['power','Main power unit',/main_power/i,/power unit|hydraulic pump|hydraulic pressure/i],
 ['tracker','Time tracker / data server',/time_tracker|data_server/i,/time tracker|data server/i],
 ['dust','Dust collector',/dust_collector/i,/dust collector/i],
 ['control','Line control / HMI',/main_control|start_stop|hmi/i,/\bhmi\b|yield test|line control|screen|touchscreen/i],
 ['unmapped','Mapping review',/$a/,/$a/]
];
const key=p=>JSON.stringify([p.alarm_type,p.plc_tag,p.message]);
function groupFor(p){return GROUPS.find(g=>g[2].test(p.plc_tag||''))?.[0]||'unmapped';}
const day=t=>new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(t));
function normalizeReports(rows){const unique=new Map();for(const r of rows){if(/\btest\b/i.test(r.operator||''))continue;const k=[r.report_date,r.shift,r.operator].join('|');if(!unique.has(k)||r.submitted_at>unique.get(k).submitted_at)unique.set(k,r);}return [...unique.values()].map(r=>({...r,text:[r.maintenance_reason,r.unplanned_downtime_details,r.planned_downtime_details,r.additional_comments].filter(Boolean).join('\n')})).filter(r=>r.text.trim()&&!/^(n\/?a|none|no)$/i.test(r.text.trim()));}
function analyze(raw,reports,days){
 const end=Date.parse(raw.end),start=end-days*864e5,priorStart=start-days*864e5;
 const daily=raw.daily||[],patterns=(raw.patterns||[]).map(p=>({...p,id:key(p),group:groupFor(p),current:Number(p['n'+days]||0),previous:Number(p['p'+days]||0)}));
 const indexed=new Map();for(const d of daily){const k=key(d);if(!indexed.has(k))indexed.set(k,[]);indexed.get(k).push({date:d.day,count:Number(d.n)});}
 for(const p of patterns){p.days=(indexed.get(p.id)||[]).filter(d=>d.date>=day(start)&&d.date<=day(end));p.activeDays=p.days.filter(d=>d.count).length;p.ratio=p.previous?p.current/p.previous:null;
 p.signals=[];if(p.current>=5&&p.previous===0)p.signals.push('Not seen in prior window');if(p.current>=5&&p.previous>0&&p.ratio>=2)p.signals.push('At least twice the prior count');if(p.current>=5&&p.activeDays>=2)p.signals.push('Recurring across dates');
 p.reviewable=['AL','SF'].includes(p.alarm_type)&&p.signals.length>0;
 p.score=p.reviewable?Math.round(Math.min(100,20+Math.log2(p.current+1)*4+(p.ratio>=2?20:0)+(p.previous===0?10:0)+Math.min(p.activeDays,7)*2)):0;
 }
 const normalized=normalizeReports(reports).filter(r=>r.report_date>=day(start)&&r.report_date<=day(end));
 const groups=GROUPS.map(([id,name,,rx])=>{
  const list=patterns.filter(p=>p.group===id&&p.current>0).sort((a,b)=>b.score-a.score||b.current-a.current);
  const reportMatches=normalized.filter(r=>rx.test(r.text)).map(r=>{
   const dates=list.flatMap(p=>p.days.map(d=>d.date));const exact=dates.includes(r.report_date),near=dates.some(d=>Math.abs(Date.parse(d)-Date.parse(r.report_date))<=864e5);
   return {...r,correlation:exact?'Equipment mention + same date':near?'Equipment mention + adjacent date':'Equipment mention only',matchedDate:exact||near};
  });
  return {id,name,patterns:list,reports:reportMatches,counts:Object.fromEntries(['AL','SF','MC'].map(t=>[t,list.filter(p=>p.alarm_type===t).reduce((n,p)=>n+p.current,0)])),score:Math.max(0,...list.map(p=>p.score)),reviewCount:list.filter(p=>p.reviewable).length};
 });
 return {generatedAt:new Date().toISOString(),end:raw.end,start:new Date(start).toISOString(),priorStart:new Date(priorStart).toISOString(),days,first:raw.first,latest:raw.latest,groups,patterns:patterns.filter(p=>p.current>0).length,reportCount:normalized.length,unmatchedReports:normalized.filter(r=>!groups.some(g=>g.reports.some(x=>x.submission_id===r.submission_id))),coverage:{partial:!raw.first||Date.parse(raw.first)>priorStart,clock:'Database timestamps displayed in Eastern time; source clock and line identity need confirmation.',basis:'Counts of recorded messages, not failure counts or runtime-normalized rates. MC records are context only.'}};
}
function register(app,{db,requireAccess}){
 let cache=null,pending=null,manuals=null;
 async function checked(q){const {data,error}=await q;if(error)throw error;return data;}
 async function snapshot(){if(cache&&Date.now()-cache.at<300000)return cache;if(pending)return pending;pending=(async()=>{
 const raw=await checked(db.rpc('sad_v2_alarm_review',{p_end:new Date().toISOString()}));
 const reports=await checked(db.from('pro_shift_report_submissions').select('submission_id,submitted_at,report_date,operator,shift,maintenance_reason,maintenance_tech,maintenance_times,unplanned_downtime_minutes,unplanned_downtime_details,planned_downtime_details,additional_comments').gte('report_date',day(Date.parse(raw.end)-31*864e5)).order('report_date',{ascending:false}).limit(2000));
 cache={raw,reports,at:Date.now()};return cache;
 })().finally(()=>pending=null);return pending;}
 app.get('/api/sad-v2/review',requireAccess,async(req,res)=>{res.set('Cache-Control','no-store');try{const days=Number(req.query.days||7);if(![1,7,30].includes(days))return res.status(400).json({error:'Choose 1, 7, or 30 days.'});const s=await snapshot();const canReadReports=req.authRoles.some(r=>['admin','production'].includes(r));res.json({...analyze(s.raw,canReadReports?s.reports:[],days),reportsAuthorized:canReadReports,cachedAt:new Date(s.at).toISOString()});}catch(e){console.error('SAD v2 review',e.message);res.status(503).json({error:'The equipment review could not load. Retry shortly; no data has been changed.'});}});
 async function library(){if(!manuals)manuals=await checked(db.from('sad_v2_manuals').select('*'));return manuals;}
 app.get('/api/sad-v2/manuals',requireAccess,async(req,res)=>{res.set('Cache-Control','no-store');try{
 const docs=await library();const q=String(req.query.q||'').trim().slice(0,100).toLowerCase();
 const terms=q.split(/\s+/).filter(t=>t.length>2);const hits=[];
 if(terms.length)for(const d of docs)for(const p of d.pages){const txt=p.text.toLowerCase(),score=terms.reduce((s,t)=>s+(txt.includes(t)?1:0),0)+(txt.includes(q)?3:0);if(score<terms.length)continue;const at=Math.max(0,txt.indexOf(terms[0])-100);hits.push({id:d.id,filename:d.filename,page:p.page,score:score+(/maintenance|operation/i.test(d.filename)?1:0)-(/table of contents/i.test(p.text)?3:0),excerpt:p.text.slice(at,at+800)});}
 res.json({documents:docs.map(({pages,...d})=>d),hits:hits.sort((a,b)=>b.score-a.score||a.page-b.page).slice(0,20)});
 }catch(e){res.status(503).json({error:'Manual index unavailable.'});}});
 app.get('/api/sad-v2/manuals/:id/:page',requireAccess,async(req,res)=>{try{const d=(await library()).find(d=>d.id===req.params.id),p=d?.pages.find(p=>p.page===Number(req.params.page));if(!p)return res.status(404).json({error:'No text is indexed for this page.'});res.set('Cache-Control','private, no-store').json({filename:d.filename,...p});}catch{res.status(503).json({error:'Manual page unavailable.'});}});
}
module.exports={register,analyze,groupFor,normalizeReports,GROUPS};
