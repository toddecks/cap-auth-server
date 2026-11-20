'use strict';
// Customer account names from the PS customer export (pro/vendor list.csv).
// Ship-to names describe destinations and must not be used as account names.
const customerNames=require('./customer-names.json');
const {resolveCombo}=require('./combo-loads');
const customerName=row=>customerNames[String(row.customerNo||'').trim().toUpperCase()]||String(row.customerNo||'').trim();
const normalize=v=>String(v||'').normalize('NFKC').trim().toUpperCase().replace(/[^A-Z0-9]/g,'');
function references(value){
 const parts=String(value||'').normalize('NFKC').split(/[\/,;|\n]+/).map(p=>normalize(p.replace(/^\s*(?:release|rel)(?:\s*(?:number|no\.?))?\s*[:#-]?\s*/i,''))).filter(Boolean);
 let base='';return [...new Set(parts.map(p=>p.replace(/[A-Z]/g,'')).filter(Boolean).map(p=>{if(base&&p.length<base.length&&p.length<=4)return base.slice(0,-p.length)+p;base=p;return p;}))];
}
// Only a separated numeric child qualifies as an omitted suffix, not an arbitrary prefix.
function suffixBase(value){
 const raw=String(value||'').normalize('NFKC').trim().replace(/^\s*(?:release|rel)(?:\s*(?:number|no\.?))?\s*[:#]?\s*/i,'');
 return raw.match(/^(\d{4,})[-_.:#\s\u2010-\u2015]+\d+$/)?.[1]||'';
}
function appointmentDateRows(rows,date){
 const dated=rows.filter(r=>Number.isFinite(Date.parse(r.scheduleDate)));
 if(!dated.length)return [];
 const target=Date.parse(date);
 if(!Number.isFinite(target)){
  const latest=Math.max(...dated.map(r=>Date.parse(r.scheduleDate)));
  return dated.filter(r=>Date.parse(r.scheduleDate)===latest);
 }
 const distance=Math.min(...dated.map(r=>Math.abs(Date.parse(r.scheduleDate)-target)));
 // Equal-distance different dates stay unresolved rather than choosing a truck.
 return dated.filter(r=>Math.abs(Date.parse(r.scheduleDate)-target)===distance);
}
function doorMatches(rows,releases,date){
 const result={},facilities=new Map();
 for(const row of rows){
  if(row.cancelLoad===true||String(row.cancelLoad).toLowerCase()==='true')continue;
  const location=String(row.location||'').trim().toUpperCase();
  if(!facilities.has(location))facilities.set(location,[]);
  facilities.get(location).push({...row,refs:references(row.poRel)});
 }
 for(const input of releases){const primary=require('./release-list').primary(input),release=normalize(primary),digits=release.replace(/[A-Z]/g,''),inputKey=normalize(input);if(!digits)continue;
  for(const [location,loads] of facilities){
   // Match a complete slash-separated reference as a whole, too.
   const fullReferenceExists=loads.some(r=>normalize(r.poRel)===release);
   // Letter suffixes distinguish appointments such as JFB1119A and JFB1119B.
   // Prefer a complete reference before the legacy numeric/label fallback.
   const letterSuffix=/^[A-Z]+\d+[A-Z]+$/.test(release);
   const exact=r=>fullReferenceExists||letterSuffix?normalize(r.poRel)===release:r.refs.includes(digits)||normalize(r.poRel)===release;
   const abbreviated=r=>/^\d{6,12}$/.test(release)&&String(r.poRel||'').split(/[\/,;|\n]+/).slice(1).some(part=>{const tail=part.trim();return /^\d{3,4}$/.test(tail)&&digits.endsWith(tail)});
   const exactLoads=loads.filter(exact);
   const datedMatches=appointmentDateRows(loads.filter(r=>exact(r)||suffixBase(r.poRel)===digits||(!exactLoads.length&&abbreviated(r))),date);
   if(new Set(datedMatches.map(r=>r.scheduleDate)).size>1){
    result[inputKey+':'+location]={door:null,status:'ambiguous',appointment_at:null,match_type:'date-conflict'};
    continue;
   }
   let candidates=datedMatches.filter(exact),matchType='exact';
   if(!candidates.length){candidates=datedMatches.filter(abbreviated);if(candidates.length)matchType='abbreviated';}
   const combo=!candidates.length?resolveCombo(datedMatches,digits):null;
   if(combo)candidates=[combo.primary];
   else if(!candidates.length&&datedMatches.length){
    candidates=datedMatches;matchType='suffix-base';
    // A base is enough only for a single full reference on the selected date.
    if(new Set(candidates.map(r=>normalize(r.poRel))).size>1){
     result[inputKey+':'+location]={door:null,status:'ambiguous',appointment_at:null,match_type:matchType};
     continue;
    }
   }
   if(!candidates.length&&digits.length>=4&&/\d/.test(release)){
    candidates=loads.filter(r=>r.refs.some(ref=>ref.includes(digits)));matchType='partial';
   }
   if(!candidates.length)continue;
   const key=inputKey+':'+location;
   // Resolve a fragment to one full reference before comparing its dated occurrences.
   const matchedReferences=new Set(candidates.flatMap(r=>r.refs.filter(ref=>ref.includes(digits))));
   if(matchType==='partial'){
    // Fragments may request clarification, but never validate or assign a door.
    if(matchedReferences.size>1)result[key]={door:null,status:'ambiguous',appointment_at:null,match_type:'partial'};
    continue;
   }
   const dated=candidates.filter(r=>Number.isFinite(Date.parse(r.scheduleDate)));
   if(!dated.length)continue;
   const latest=appointmentDateRows(dated,date);
   const doors=[...new Set(latest.map(r=>String(r.unloadingDoor||'').trim()).filter(Boolean))];
   const incomplete=latest.some(r=>!String(r.unloadingDoor||'').trim());
   const appointments=[...new Set(latest.map(r=>r.scheduleDate&&r.scheduleTime?`${r.scheduleDate}T${String(r.scheduleTime).slice(0,8)}`:null))];
   const ambiguous=new Set(latest.map(r=>r.scheduleDate)).size>1||doors.length>1||(doors.length&&incomplete)||appointments.length>1;
   const bols=[...new Set(latest.map(r=>String(r.bolNumber||'').trim()).filter(b=>b&&!/^0+$/.test(b)))];
   const bolAmbiguous=bols.length>1||(bols.length===1&&latest.some(r=>!String(r.bolNumber||'').trim()||/^0+$/.test(String(r.bolNumber).trim())));
   result[key]={...(combo?{combo_load:true,master_bol_number:combo.masterBolNumber,release_base:combo.base,combo_bol_numbers:[...new Set(combo.rows.map(r=>String(r.bolNumber).trim()))],carrier_name:combo.primary.carrierName}:{}),bol_number:!ambiguous&&!bolAmbiguous&&bols.length===1?bols[0]:null,bol_ambiguous:bolAmbiguous,door:!ambiguous&&doors.length===1&&!incomplete?doors[0]:null,status:ambiguous?'ambiguous':doors.length?'matched':'unassigned',appointment_at:!ambiguous&&appointments.length===1?appointments[0]:null,match_type:matchType,release_reference:latest[0]?.poRel||null,carrier_name:!ambiguous?latest[0]?.carrierName||null:null,customer:!ambiguous?[...new Set(latest.map(r=>customerName(r)).filter(Boolean))].join(' / '):null};
  }
 }
 return result;
}
module.exports={doorMatches,references};
