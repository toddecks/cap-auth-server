'use strict';
const {driverName,isAcknowledgement}=require('./driver-identity');
const introduction=/^(?:(?:hello|hi|hey)[,!]?\s+)?(?:this\s+is|my\s+name\s+is|i\s+am|i['’]m|it['’]s)\s+/i;
function repairedName(value){
 const original=String(value||'').trim(),name=driverName(original);
 if(isAcknowledgement(original))return '';
 // Only repair unmistakable introductions, never infer a name from a sentence.
 return introduction.test(original)&&name!==original&&/^[\p{L}.'’-]+(?:\s+[\p{L}.'’-]+){0,3}$/u.test(name)&&!/\b(?:with|from|for|pickup|release|dropping|loading)\b/i.test(name)?name:null;
}
function createRepair(db){
 const health={version:'driver-name-acknowledgements-v3',state:'pending',updated:0,verified:0,error:null};
 async function run(){
  if(!db){health.state='unavailable';return;}
  health.state='running';
  try{
   for(const [table,key,column] of [['driver_sms_contacts','phone_e164','full_name'],['driver_sms_arrivals','id','driver_name']]){
    const candidates=[];
    for(let offset=0;;offset+=500){
     const {data,error}=await db.from(table).select(`${key},${column}`).order(key).range(offset,offset+499);
     if(error)throw error;
     for(const row of data||[]){const name=repairedName(row[column]);if(name!==null)candidates.push({row,name:name || (table==='driver_sms_arrivals'?'Name not provided':'')});}
     if((data||[]).length<500)break;
    }
    for(const {row,name} of candidates){
     // Compare the original value so concurrent clerk corrections always win.
     const {data,error}=await db.from(table).update({[column]:name}).eq(key,row[key]).eq(column,row[column]).select(`${key},${column}`);
     if(error)throw error;
     health.updated+=(data||[]).length;
     for(const changed of data||[]){
      const check=await db.from(table).select(column).eq(key,changed[key]).single();
      if(check.error)throw check.error;
      if(check.data?.[column]!==name)throw Error('Driver name repair verification failed');
      health.verified++;
     }
    }
   }
   health.state='complete';
  }catch(error){health.state='error';health.error=error.message;console.error('Driver name repair:',error.message);}
 }
 return{health,run};
}
module.exports={repairedName,createRepair};
