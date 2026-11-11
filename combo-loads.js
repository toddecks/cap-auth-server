'use strict';
const normalize=v=>String(v||'').normalize('NFKC').trim().toUpperCase().replace(/[^A-Z0-9]/g,'');
const validBol=v=>Boolean(String(v||'').trim())&&!/^0+$/.test(String(v).trim());
// A base release is valid only when its numbered children are explicitly linked
// by PS's master BOL and agree on the physical load. Never accept arbitrary prefixes.
function resolveCombo(rows,input){
 const base=normalize(input);if(!/^\d{4,}$/.test(base))return null;
 const family=rows.flatMap(row=>{
  if(row.cancelLoad===true||String(row.cancelLoad).toLowerCase()==='true')return [];
  const match=String(row.poRel||'').trim().match(/^(\d+)\s*-\s*(\d+)$/);
  return match&&match[1]===base&&Number.isSafeInteger(Number(match[2]))&&Number(match[2])>0?[{row,suffix:Number(match[2])}]:[];
 });
 const dated=family.filter(x=>Number.isFinite(Date.parse(x.row.scheduleDate)));
 if(!dated.length)return null;
 const newest=Math.max(...dated.map(x=>Date.parse(x.row.scheduleDate)));
 const current=dated.filter(x=>Date.parse(x.row.scheduleDate)===newest);
 const suffixes=new Set(current.map(x=>x.suffix));
 if(suffixes.size<2||!suffixes.has(1))return null;
 for(const field of ['masterBolNumber','customerNo','carrierName','location','unloadingDoor']){
  const values=new Set(current.map(x=>normalize(x.row[field])));
  if(values.size!==1||values.has('')||(field==='masterBolNumber'&&/^0+$/.test([...values][0])))return null;
 }
 if(current.some(x=>!validBol(x.row.bolNumber)))return null;
 for(const suffix of suffixes){if(new Set(current.filter(x=>x.suffix===suffix).map(x=>String(x.row.bolNumber).trim())).size!==1)return null;}
 const first=current.filter(x=>x.suffix===1);
 if(new Set(first.map(x=>String(x.row.scheduleTime||''))).size!==1)return null;
 return {primary:first[0].row,rows:current.map(x=>x.row),base,masterBolNumber:String(first[0].row.masterBolNumber).trim()};
}
module.exports={resolveCombo};
