'use strict';
// The same dated catalog drives validation, appointment cards and loading directions.
// Cache per client so the five-second message worker does not reread the catalog per driver.
const cache=new WeakMap();
async function releaseCatalog(db,now=Date.now()) {
 if(!db)throw Error('Release database is unavailable.');
 const prior=cache.get(db);
 if(prior&&now-prior.at<60000)return prior.promise;
 const entry={at:now,promise:null};
 entry.promise=(async()=>{
  const rows=[];
  for(let offset=0;;offset+=1000){
   const {data,error}=await db.from('psdata_loads_api')
    .select('scheduleDate,scheduleTime,poRel,carrierName,location,bolNumber,masterBolNumber,cancelLoad,unloadingDoor,customerNo')
    .order('scheduleDate').order('bolNumber').range(offset,offset+999);
   if(error)throw error;
   rows.push(...(data||[]));if(!data||data.length<1000)return rows;
  }
 })().catch(error=>{if(cache.get(db)===entry)cache.delete(db);throw error;});
 cache.set(db,entry);return entry.promise;
}
module.exports={releaseCatalog};
