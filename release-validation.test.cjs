const {test}=require('node:test');
const assert=require('node:assert/strict');
const {createWorker,VERIFY,ASSIST,SOON,CLOSED,REMAIN,officeClosed}=require('./release-validation');
function fixture(channel='sms') {
 let clock=Date.parse('2026-10-01T17:00:00Z');
 const tables={driver_conversations:[{id:'visit-1',status:'open',visit_type:'pickup',release_number:'12345',channel,sms_phone_e164:'+15555550100',created_at:new Date(clock).toISOString()}],shipping_staff_checkins:[],driver_messages:[]};
 const sends=[];const checkouts=[];
 const db={async rpc(name,args){checkouts.push({name,args});tables.driver_conversations[0].status='closed';return {data:true,error:null}},from(table){let filters=[],op='select',value,start=0,end=Infinity;
  const q={select(){return q},eq(k,v){filters.push(r=>r[k]===v);return q},gte(k,v){filters.push(r=>r[k]>=v);return q},order(){return q},range(a,b){start=a;end=b;return q},maybeSingle(){q.singleRow=true;return q},single(){q.singleRow=true;return q},insert(v){op='insert';value=v;return q},update(v){op='update';value=v;return q},then(resolve,reject){try{let rows=tables[table].filter(r=>filters.every(f=>f(r)));
   if(op==='insert'){if(tables[table].some(r=>r.client_message_id===value.client_message_id))return Promise.resolve({error:{code:'23505'}}).then(resolve,reject);const row={...value,id:tables[table].length+1};tables[table].push(row);rows=[row];}
   if(op==='update')rows.forEach(r=>Object.assign(r,value));
   rows=rows.slice(start,end+1);return Promise.resolve({data:q.singleRow?(rows[0]||null):rows,error:null}).then(resolve,reject);
  }catch(e){return Promise.reject(e).then(resolve,reject)}}};return q;}};
 const options={db,twilio:{messages:{async create(message){sends.push(message);return {sid:'SM'+sends.length,status:'sent'}}}},messagingServiceSid:'test',publicBaseUrl:'https://example.test',now:()=>clock};
 return {tables,sends,checkouts,options,worker:createWorker(options),advance(ms){clock+=ms}};
}
test('exact initial reply and three- and five-minute follow-ups, persistent across restart',async()=>{
 const f=fixture();await f.worker.tick();assert.equal(f.sends[0].body,VERIFY);
 f.advance(179999);await f.worker.tick();assert.equal(f.sends.length,1);
 f.advance(1);await createWorker(f.options).tick();assert.equal(f.sends[1].body,SOON);
 f.advance(119999);await f.worker.tick();assert.equal(f.sends.length,2);
 f.advance(1);await createWorker(f.options).tick();assert.equal(f.sends[2].body,ASSIST);
 await f.worker.tick();assert.equal(f.sends.length,3);assert.equal(f.worker.health.error,null);
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
 const f=fixture('app');await f.worker.tick();f.advance(180000);await f.worker.tick();f.advance(120000);await f.worker.tick();assert.equal(f.sends.length,0);assert.deepEqual(f.tables.driver_messages.map(m=>m.body),[VERIFY,SOON,ASSIST]);
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

test('office hours use Eastern time at exact boundaries and in winter',()=>{
 for(const [at,closed] of [['2026-10-01T09:59:59Z',true],['2026-10-01T10:00:00Z',false],['2026-10-01T21:59:59Z',false],['2026-10-01T22:00:00Z',true],['2026-12-01T10:59:59Z',true],['2026-12-01T11:00:00Z',false],['2026-10-04T22:00:00Z',true]])assert.equal(officeClosed(at),closed,at);
});
test('after-hours pickup gets one closed reply and no daytime reminders',async()=>{
 const f=fixture();f.advance(5*60*60*1000);await f.worker.tick();f.advance(301000);await f.worker.tick();assert.deepEqual(f.sends.map(m=>m.body),[CLOSED]);
});
function checkedFixture(){const f=fixture();f.tables.shipping_staff_checkins.push({conversation_id:'visit-1',checked_in_at:'2026-10-01T17:00:00Z'});f.tables.driver_messages.push({client_message_id:'staff-pickup-checkin:visit-1',conversation_id:'visit-1',sent_at:'2026-10-01T17:00:00Z',delivery_status:'sent',sender_user_id:'staff',direction:'shipping_to_driver'});return f;}
test('one-minute pickup followup and checkout only at 30 minutes from staff checkin',async()=>{
 const f=checkedFixture();f.advance(59999);await f.worker.tick();assert.equal(f.sends.length,0);
 f.advance(1);await createWorker(f.options).tick();assert.deepEqual(f.sends.map(m=>m.body),[REMAIN]);
 f.advance(1739999);await f.worker.tick();assert.equal(f.checkouts.length,0);
 f.advance(1);await f.worker.tick();assert.equal(f.checkouts.length,1);assert.equal(f.tables.driver_conversations[0].status,'closed');
 await f.worker.tick();assert.equal(f.checkouts.length,1);assert.equal(f.sends.length,1);
});
for(const action of ['closed','dropoff','failed','old'])test('no checkin followup for '+action,async()=>{
 const f=checkedFixture();if(action==='closed')f.tables.driver_conversations[0].status='closed';if(action==='dropoff')f.tables.driver_conversations[0].visit_type='dropoff';if(action==='failed')f.tables.driver_messages[0].delivery_status='failed';if(action==='old')f.tables.shipping_staff_checkins[0].checked_in_at='2026-09-29T17:00:00Z';f.advance(60000);await f.worker.tick();assert.equal(f.sends.length,0);
});
test('restart after five minutes sends assistance without two simultaneous reminders',async()=>{const f=fixture();await f.worker.tick();f.advance(310000);await createWorker(f.options).tick();assert.deepEqual(f.sends.map(m=>m.body),[VERIFY,ASSIST]);});

test('a new staff check-in on an older open conversation still receives its timer',async()=>{const f=checkedFixture();f.tables.driver_conversations[0].created_at='2026-09-29T17:00:00Z';f.advance(1800000);await f.worker.tick();assert.equal(f.checkouts.length,1);});
test('new visit in an existing thread gets exactly one verification despite old check-in and replies',async()=>{
 const f=checkedFixture();
 const c=f.tables.driver_conversations[0];c.created_at='2026-09-29T10:00:00Z';
 f.tables.driver_messages.push({conversation_id:c.id,client_message_id:'release-wait:verify:'+c.id,sent_at:'2026-09-30T12:00:00Z',direction:'shipping_to_driver',delivery_status:'sent'});
 f.advance(60000);c.session_started_at='2026-10-01T17:01:00Z';
 await f.worker.tick();await createWorker(f.options).tick();
 assert.deepEqual(f.sends.map(m=>m.body),[VERIFY]);assert.equal(f.checkouts.length,0);
 f.advance(180000);await f.worker.tick();assert.equal(f.sends[1].body,SOON);
});
