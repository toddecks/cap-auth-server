const {test}=require('node:test'),assert=require('node:assert/strict');
const {createQueue,statusHandler}=require('./checkin-jobs');
function database(){const rows=[];return{rows,from(table){assert.equal(table,'shipping_checkin_jobs');let filters=[],op='select',value,single=false,limit=Infinity;
 const q={select(){return q},eq(k,v){filters.push(r=>r[k]===v);return q},lt(k,v){filters.push(r=>r[k]<v);return q},order(){return q},limit(n){limit=n;return q},maybeSingle(){single=true;return q},upsert(v){op='upsert';value=v;return q},update(v){op='update';value=v;return q},then(resolve,reject){return Promise.resolve().then(()=>{
  let matches=rows.filter(r=>filters.every(f=>f(r))).slice(0,limit);
  if(op==='upsert'){const old=rows.find(r=>r.id===value.id);if(!old)rows.push({status:'pending',...structuredClone(value)});matches=[];}
  if(op==='update')for(const row of matches)Object.assign(row,value);
  return{data:single?(matches[0]||null):matches.map(r=>({...r}))};
 }).then(resolve,reject)}};return q;}};}
const payload={conversation:{id:'visit',status:'open',channel:'sms',sms_phone_e164:'+15555550100',created_at:'2026-11-24T12:00:00Z'},staffId:'staff',visitType:'pickup',arrivalId:4};
test('enqueue completes while delivery remains blocked, repeated clicks do not overwrite or duplicate',async()=>{
 const db=database();let finish,calls=0;const gate=new Promise(r=>finish=r);
 const queue=createQueue({db,defer:()=>{},send:async()=>{calls++;await gate;}});
 const id=await queue.enqueue(payload);assert.equal(id,'visit');assert.equal(db.rows.length,1);
 const sending=queue.tick();while(!calls)await Promise.resolve();assert.equal((await queue.status(id)).status,'processing');
 await queue.enqueue({...payload,staffId:'other'});assert.equal(db.rows[0].payload.staffId,'staff');
 finish();await sending;await queue.tick();assert.equal(calls,1);assert.equal((await queue.status(id)).status,'complete');
});
test('restart recovers interrupted jobs and failures remain available for staff review without automatic resend',async()=>{
 const db=database(),q=createQueue({db,defer:()=>{},send:async()=>{throw Error('provider outcome uncertain')}});
 await q.enqueue(payload);Object.assign(db.rows[0],{status:'processing',started_at:'2026-11-24T00:00:00Z'});
 await q.tick();assert.equal(db.rows[0].status,'needs_review');assert.match(db.rows[0].error,/uncertain/);
 let retried=false;await createQueue({db,send:async()=>{retried=true}}).tick();assert.equal(retried,false);
});
test('overlapping workers claim each job only once',async()=>{
 const db=database();let calls=0;const deps={db,defer:()=>{},send:async()=>{calls++;}};
 await createQueue(deps).enqueue(payload);await Promise.all([createQueue(deps).tick(),createQueue(deps).tick()]);assert.equal(calls,1);
});
test('only authenticated Shipping staff can inspect job status; private payload and errors never return',async()=>{
 for(const role of ['driver','shipping']){
  let reads=0,status=200,body;const fn=statusHandler({auth:{auth:{getUser:async()=>({data:{user:{app_metadata:{csp_role:role}}}})}},queue:{status:async()=>{reads++;return{status:'needs_review',payload:'private',error:'private'}}}});
  const res={set(){},status(n){status=n;return this},json(v){body=v;return this}};await fn({headers:{authorization:'Bearer test'},query:{jobId:'visit'}},res);
  assert.equal(status,role==='driver'?403:200);assert.equal(reads,role==='driver'?0:1);if(role==='shipping')assert.deepEqual(body,{status:'needs_review'});
 }
});
