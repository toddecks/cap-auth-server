'use strict';
const normalize=v=>String(v||'').trim().toUpperCase().replace(/[^A-Z0-9]/g,'');
function doorMatches(rows,releases,date){
 const requested=new Set(releases.map(normalize)),groups=new Map(),result={};
 for(const row of rows){
  if(row.cancelLoad===true||String(row.cancelLoad).toLowerCase()==='true')continue;
  // Only explicit release references; do not guess abbreviated release suffixes.
  const keys=String(row.poRel||'').split(/[\/,;|]+/).map(normalize).filter(k=>k&&requested.has(k));
  for(const release of keys){
   const key=release+':'+String(row.location||'').trim().toUpperCase();
   if(!groups.has(key))groups.set(key,[]);groups.get(key).push(row);
  }
 }
 for(const [key,candidates] of groups){
  const distance=r=>Math.abs(Date.parse(r.scheduleDate)-Date.parse(date));
  const best=Math.min(...candidates.map(distance));
  const nearest=candidates.filter(r=>distance(r)===best);
  const doors=[...new Set(nearest.map(r=>String(r.unloadingDoor||'').trim()).filter(Boolean))];
  const incomplete=nearest.some(r=>!String(r.unloadingDoor||'').trim());
  const appointments=[...new Set(nearest.map(r=>r.scheduleDate&&r.scheduleTime?`${r.scheduleDate}T${String(r.scheduleTime).slice(0,8)}`:null))];
  const ambiguous=doors.length>1||(doors.length&&incomplete)||appointments.length>1;
  result[key]={door:!ambiguous&&doors.length===1&&!incomplete?doors[0]:null,status:ambiguous?'ambiguous':doors.length?'matched':'unassigned',appointment_at:!ambiguous&&appointments.length===1?appointments[0]:null};
 }
 return result;
}
module.exports={doorMatches};
