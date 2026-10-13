'use strict';
// Shared, freshly completed catalog; concurrent callers share an in-flight refresh.
const cache=new WeakMap();
const PAGE_SIZE=1000, PARALLEL_PAGES=8, TTL_MS=60000;
async function releaseCatalog(db,now=Date.now()) {
 if(!db)throw Error('Release database is unavailable.');
 const prior=cache.get(db);
 if(prior&&(prior.pending||now-prior.at<TTL_MS))return prior.promise;
 const entry={at:now,pending:true,promise:null};
 const started=Date.now();
 const page=async offset=>{
  const {data,error}=await db.from('psdata_loads_api')
   .select('scheduleDate,scheduleTime,poRel,carrierName,location,bolNumber,masterBolNumber,cancelLoad,unloadingDoor,customerNo')
   .order('scheduleDate').order('bolNumber').range(offset,offset+PAGE_SIZE-1);
  if(error)throw error;
  return data||[];
 };
 entry.promise=(async()=>{
  const rows=await page(0);
  if(rows.length<PAGE_SIZE)return rows;
  for(let offset=PAGE_SIZE;;offset+=PAGE_SIZE*PARALLEL_PAGES){
   const pages=await Promise.all(Array.from({length:PARALLEL_PAGES},(_,i)=>page(offset+i*PAGE_SIZE)));
   for(const batch of pages){rows.push(...batch);if(batch.length<PAGE_SIZE)return rows;}
  }
 })().then(rows=>{entry.pending=false;entry.at=now+(Date.now()-started);return rows;})
 .catch(error=>{if(cache.get(db)===entry)cache.delete(db);throw error;});
 cache.set(db,entry);return entry.promise;
}
module.exports={releaseCatalog};

// Filter on the server before applying the exact reference/list rules locally.
// Ordered characters allow punctuation and expanded shorthand (968751/510)
// while the final matcher still rejects incomplete and unrelated references.
async function releaseCandidates(db,release){
 if(!db)throw Error('Release database is unavailable.');
 const key=String(release||'').normalize('NFKC').toUpperCase().replace(/[^0-9]/g,'');
 if(!key)return [];
 const pattern='%'+key.split('').join('%')+'%';
 const rows=[];
 for(let offset=0;;offset+=PAGE_SIZE){
  const {data,error}=await db.from('psdata_loads_api')
   .select('scheduleDate,scheduleTime,poRel,carrierName,location,bolNumber,masterBolNumber,cancelLoad,unloadingDoor,customerNo')
   .ilike('poRel',pattern).order('scheduleDate').order('bolNumber').range(offset,offset+PAGE_SIZE-1);
  if(error)throw error;rows.push(...(data||[]));if(!data||data.length<PAGE_SIZE)return rows;
 }
}
module.exports.releaseCandidates=releaseCandidates;
