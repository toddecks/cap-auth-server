(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.ShippingReleaseList=api;})(typeof globalThis!=='undefined'?globalThis:this,function(){
 const key=v=>String(v||'').normalize('NFKC').toUpperCase().replace(/[^A-Z0-9]/g,'');
 const valid=v=>/^[A-Za-z0-9][A-Za-z0-9./_\-\u2010-\u2015]{2,39}$/.test(v)&&/\d/.test(v)&&!/^\+?1?\d{10}$/.test(v);
 function split(value){
  const raw=String(value||'').trim();
  const parts=raw.split(/\s*(?:&|\band\b|\+|[,;\n])\s*/i).map(s=>s.replace(/^#\s*/, '').trim()).filter(Boolean);
  if(parts.length<2||!parts.every(valid))return raw?[raw]:[];
  return [...new Map(parts.map(v=>[key(v),v])).values()];
 }
 function extract(text){
  const refs=[],segments=[];
  for(const raw of String(text||'').split(/[\n,;]+/)){
   const part=raw.trim().replace(/^(?:release|rel|pu|pick[ -]?up|load|wo)(?:\s*(?:numbers?|no\.?))?\s*[:#]?\s*/i,'').replace(/^#\s*/, '');
   const values=split(part);
   if(values.length>1&&values.every(valid)||values.length===1&&valid(part)&&/^\d{4,12}(?:[-_.]\d+)?$/.test(part)){
    refs.push(...values);segments.push(raw);
   }
  }
  return {releases:[...new Map(refs.map(v=>[key(v),v])).values()],segments};
 }
 function expand(rows){return rows.flatMap(row=>{const releases=split(row.release_number);return releases.length>1?releases.map(release=>({...row,release_number:release,visit_release_numbers:row.release_number})): [row];});}
 return {split,extract,expand};
});
