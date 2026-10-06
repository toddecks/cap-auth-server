'use strict';
const ALIASES={entry:['coil stage','coil car','coil lift','entry coil'],uncoiler:['uncoiler','mandrel'],peeler:['peeler','threading'],straightener:['straightener'],pinch:['pinch roll','leveler pinch'],feed:['roll feed','feed roll'],shear:['prime shear','shear'],stretcher:['stretcher'],stacker:['drop stacker','backstop'],table:['stack table','rollout'],safety:['light curtain','safety gate','emergency stop','interlock'],power:['power unit','hydraulic'],tracker:['time tracker','data server'],dust:['dust collector'],control:['line control','hmi','touch screen','touchscreen']};
const stop=new Set('the and for from with this that alarm fault error warning signal system machine auto manual current previous about out into material red bud industries operation maintenance'.split(' '));
const normalize=s=>String(s||'').toLowerCase().replace(/[^a-z0-9]+/g,' ').replace(/\s+/g,' ').trim();
const words=s=>[...new Set(normalize(s).split(' ').filter(x=>x.length>2&&!stop.has(x)))];
function retrieve(docs,group,limit=12){
 const aliases=(ALIASES[group.id]||[group.name]).map(normalize);
 const equipmentWords=new Set(words(aliases.join(' ')));
 const query=words(group.patterns.filter(p=>['AL','SF'].includes(p.alarm_type)).slice(0,12).map(p=>p.message).join(' ')).filter(t=>!equipmentWords.has(t));
 const found=[];
 for(const doc of docs){
  // Parts lists alone do not support a diagnostic or a repair procedure.
  if(!/maintenance|operation|safety|product literature|schematic|assembly/i.test(doc.filename))continue;
  for(const page of doc.pages||[]){
   const text=[page.reviewedLabels,page.text].filter(Boolean).join('\n'),norm=normalize(text);
   if(text.length<250||text.split('\n').filter(l=>/(?:\.\s*){5}/.test(l)).length>3)continue;
   if(!aliases.some(a=>norm.includes(a)))continue;
   for(let start=0;start<text.length;start+=1900){
    const excerpt=text.slice(start,start+2600),chunk=normalize(excerpt);
    const local=aliases.some(a=>chunk.includes(a));
    const score=query.reduce((n,t)=>n+(chunk.includes(t)?3:0),0)+(local?4:0)+(/maintenance/i.test(doc.filename)?4:0)+(/troubleshoot|cause|remedy/i.test(excerpt)?5:0)+(local?0:-3);
    if(score<5)continue;
    found.push({kind:'manual',documentId:doc.id,filename:doc.filename,page:page.page,excerpt,extraction:page.extraction||'pdf_text',ocrConfidence:page.ocrConfidence??null,reviewedLabels:page.reviewedLabels||null,score:score+(/schematic/i.test(doc.filename)?3:0),start,sourceSha:doc.source_sha256});
   }
  }
 }
 const picked=[],perPage=new Map();
 for(const hit of found.sort((a,b)=>b.score-a.score)){
  const key=hit.documentId+':'+hit.page;
  if(perPage.has(key))continue;
  perPage.set(key,true);picked.push(hit);if(picked.length===limit)break;
 }
 return picked.map((p,i)=>({...p,id:'M'+(i+1)}));
}
module.exports={retrieve};
