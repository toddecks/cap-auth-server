'use strict';
// Customer account names from the PS customer export (pro/vendor list.csv).
// Ship-to names describe destinations and must not be used as account names.
const customerNames=require('./customer-names.json');
const customerName=row=>customerNames[String(row.customerNo||'').trim().toUpperCase()]||String(row.customerNo||'').trim();
const normalize=v=>String(v||'').normalize('NFKC').trim().toUpperCase().replace(/[^A-Z0-9]/g,'');
function references(value){
 const parts=String(value||'').normalize('NFKC').split(/[\/,;|\n]+/).map(p=>normalize(p.replace(/^\s*(?:release|rel)(?:\s*(?:number|no\.?))?\s*[:#-]?\s*/i,''))).filter(Boolean);
 let base='';return [...new Set(parts.map(p=>{if(/^\d+$/.test(p)){if(base&&p.length<base.length&&p.length<=4)return base.slice(0,-p.length)+p;base=p;}return p;}))];
}
function doorMatches(rows,releases,date){
 const result={},facilities=new Map();
 for(const row of rows){
  if(row.cancelLoad===true||String(row.cancelLoad).toLowerCase()==='true')continue;
  const location=String(row.location||'').trim().toUpperCase();
  if(!facilities.has(location))facilities.set(location,[]);
  facilities.get(location).push({...row,refs:references(row.poRel)});
 }
 for(const input of releases){const release=normalize(input);if(!release)continue;
  for(const [location,loads] of facilities){
   let candidates=loads.filter(r=>r.refs.includes(release)),matchType='exact';
   if(!candidates.length&&release.length>=4&&/\d/.test(release)){
    candidates=loads.filter(r=>r.refs.some(ref=>ref.includes(release)));matchType='partial';
   }
   if(!candidates.length)continue;
   const key=release+':'+location;
   // Resolve a fragment to one full reference before comparing its dated occurrences.
   const matchedReferences=new Set(candidates.flatMap(r=>r.refs.filter(ref=>ref.includes(release))));
   if(matchType==='partial'&&matchedReferences.size>1){result[key]={door:null,status:'ambiguous',appointment_at:null,match_type:'partial'};continue;}
   const dated=candidates.filter(r=>Number.isFinite(Date.parse(r.scheduleDate)));
   if(!dated.length)continue;
   const latestDate=Math.max(...dated.map(r=>Date.parse(r.scheduleDate)));
   const latest=dated.filter(r=>Date.parse(r.scheduleDate)===latestDate);
   const doors=[...new Set(latest.map(r=>String(r.unloadingDoor||'').trim()).filter(Boolean))];
   const incomplete=latest.some(r=>!String(r.unloadingDoor||'').trim());
   const appointments=[...new Set(latest.map(r=>r.scheduleDate&&r.scheduleTime?`${r.scheduleDate}T${String(r.scheduleTime).slice(0,8)}`:null))];
   const ambiguous=doors.length>1||(doors.length&&incomplete)||appointments.length>1;
   result[key]={door:!ambiguous&&doors.length===1&&!incomplete?doors[0]:null,status:ambiguous?'ambiguous':doors.length?'matched':'unassigned',appointment_at:!ambiguous&&appointments.length===1?appointments[0]:null,match_type:matchType,release_reference:latest[0]?.poRel||null,customer:!ambiguous?[...new Set(latest.map(r=>customerName(r)).filter(Boolean))].join(' / '):null};
  }
 }
 return result;
}
module.exports={doorMatches,references};
