const {test}=require('node:test');
const assert=require('node:assert/strict');
const {createWorker,VERIFY,ASSIST}=require('./release-validation');
function fixture(channel='sms') {
 let clock=Date.parse('2026-09-29T17:00:00Z');
 const tables={driver_conversations:[{id:'visit-1',status:'open',visit_type:'pickup',release_number:'12345',channel,sms_phone_e164:'+15555550100',created_at:new Date(clock).toISOString()}],shipping_staff_checkins:[],driver_messages:[]};
 const sends=[];
 const db={from(table){let filters=[],op='select',value,start=0,end=Infinity;
  const q={select(){return q},eq(k,v){filters.push(r=>r[k]===v);return q},gte(k,v){filters.push(r=>r[k]>=v);return q},order(){return q},range(a,b){start=a;end=b;return q},maybeSingle(){q.singleRow=true;return q},single(){q.singleRow=true;return q},insert(v){op='insert';value=v;return q},update(v){op='update';value=v;return q},then(resolve,reject){try{let rows=tables[table].filter(r=>filters.every(f=>f(r)));
   if(op==='insert'){if(tables[table].some(r=>r.client_message_id===value.client_message_id))return Promise.resolve({error:{code:'23505'}}).then(resolve,reject);const row={...value,id:tables[table].length+1};tables[table].push(row);rows=[row];}
   if(op==='update')rows.forEach(r=>Object.assign(r,value));
   rows=rows.slice(start,end+1);return Promise.resolve({data:q.singleRow?(rows[0]||null):rows,error:null}).then(resolve,reject);
  }catch(e){return Promise.reject(e).then(resolve,reject)}}};return q;}};
 const options={db,twilio:{messages:{async create(message){sends.push(message);return {sid:'SM'+sends.length,status:'sent'}}}},messagingServiceSid:'test',publicBaseUrl:'https://example.test',now:()=>clock};
 return {tables,sends,options,worker:createWorker(options),advance(ms){clock+=ms}};
}
test('exact initial reply and one follow-up after three minutes, persistent across restart',async()=>{
 const f=fixture();await f.worker.tick();assert.equal(f.sends[0].body,VERIFY);
 f.advance(179999);await f.worker.tick();assert.equal(f.sends.length,1);
 f.advance(1);await createWorker(f.options).tick();assert.equal(f.sends[1].body,ASSIST);
 await f.worker.tick();assert.equal(f.sends.length,2);assert.equal(f.worker.health.error,null);
});
for(const action of ['reply','checkin','close','dropoff'])test(action+' cancels the reminder',async()=>{
 const f=fixture();await f.worker.tick();f.advance(180001);
 if(action==='reply')f.tables.driver_messages.push({conversation_id:'visit-1',direction:'shipping_to_driver',sender_user_id:'staff',delivery_status:'sent'});
 if(action==='checkin')f.tables.shipping_staff_checkins.push({conversation_id:'visit-1'});
 if(action==='close')f.tables.driver_conversations[0].status='closed';
 if(action==='dropoff')f.tables.driver_conversations[0].visit_type='dropoff';
 await f.worker.tick();assert.equal(f.sends.length,1);
});
test('app channel records both messages without texting',async()=>{
 const f=fixture('app');await f.worker.tick();f.advance(180000);await f.worker.tick();assert.equal(f.sends.length,0);assert.deepEqual(f.tables.driver_messages.map(m=>m.body),[VERIFY,ASSIST]);
});
test('old visits and placeholder release numbers are excluded',async()=>{
 for(const patch of [{created_at:'2026-09-28T00:00:00Z'},{release_number:'TEXT1234'},{release_number:''},{visit_type:'dropoff'}]){
  const f=fixture();Object.assign(f.tables.driver_conversations[0],patch);await f.worker.tick();assert.equal(f.sends.length,0);
 }
});
test('failed initial message does not start a follow-up',async()=>{
 const f=fixture();f.options.twilio.messages.create=async()=>{throw Error('Test delivery failure')};await f.worker.tick();f.advance(180000);await f.worker.tick();assert.equal(f.tables.driver_messages.length,1);assert.equal(f.tables.driver_messages[0].delivery_status,'failed');
});

for(const placeholder of ['Text 2000',' TEXT 2000 ', 'text\t2000','TEXT2000','Text'])test('SMS placeholder '+JSON.stringify(placeholder)+' waits for a real release',async()=>{
 const f=fixture();f.tables.driver_conversations[0].release_number=placeholder;
 await f.worker.tick();f.advance(180001);await f.worker.tick();
 assert.equal(f.sends.length,0);assert.equal(f.tables.driver_messages.length,0);
 f.tables.driver_conversations[0].release_number='AB2000';
 await f.worker.tick();await f.worker.tick();assert.deepEqual(f.sends.map(m=>m.body),[VERIFY]);
});
test('premature acknowledgement cannot schedule assistance for an SMS placeholder',async()=>{
 const f=fixture();await f.worker.tick();f.tables.driver_conversations[0].release_number='Text 2000';
 f.advance(180001);await f.worker.tick();assert.equal(f.sends.length,1);
});
