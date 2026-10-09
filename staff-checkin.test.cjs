const {test}=require('node:test'),assert=require('node:assert/strict');
const {createHandler}=require('./staff-checkin');
const {dropoffReply,PICKUP_CHECKIN}=require('./driver-visit-flow');
const {sendCheckinInstructions}=require('./checkin-instructions');
function harness(role='shipping',channel='app',visitType='pickup'){
 const messages=[],checks=[],arrivals=[];let clock=0;
 const db={storage:{from:()=>({upload:async()=>({}),createSignedUrl:async()=>({data:{signedUrl:"https://example.invalid/test.png"}})})},auth:{getUser:async()=>({data:{user:{id:'staff',app_metadata:{csp_role:role}}}})},from(table){let op='read',value,singular=false;const q={select(){return q},in(){return q},like(){return q},not(){return q},neq(){return q},limit(){return q},eq(){return q},single(){singular=true;return q},maybeSingle(){singular=true;return q},is(){return q},update(v){op='update';value=v;return q},upsert(v){op='write';value=v;return q},insert(v){op='insert';value=v;return q},then(resolve){
  if(table==='driver_conversations'&&!singular)return Promise.resolve({data:[]}).then(resolve);
  if(table==='driver_conversations')return Promise.resolve({data:{id:'12345678-1234-1234-1234-123456789abc',status:'open',channel,sms_phone_e164:channel==='sms'?'+15555550101':null,visit_type:visitType,release_number:'12345',arrival_id:1}}).then(resolve);
  if(table==='driver_sms_contacts')return Promise.resolve({data:{full_name:'Test Driver',driver_company:'Test Carrier'}}).then(resolve);
  if(table==='driver_sms_arrivals'){if(op==='insert')arrivals.push({id:91,...value});if(op==='update')Object.assign(arrivals[0],value);return Promise.resolve({data:singular?(arrivals[0]||null):arrivals}).then(resolve);}
  if(table==='shipping_staff_checkins'){if(op==='write'&&!checks.length)checks.push({...value,checked_in_at:'2026-09-29T12:00:00Z'});return Promise.resolve({data:checks[0]}).then(resolve);}
  if(table==='driver_messages'){if(op==='update')return Promise.resolve({data:null}).then(resolve);if(op==='read')return Promise.resolve({data:[]}).then(resolve);if(messages.some(m=>m.client_message_id===value.client_message_id))return Promise.resolve({error:{code:'23505'}}).then(resolve);messages.push(value);return Promise.resolve({data:{id:1}}).then(resolve);}
  throw Error('Unexpected table '+table);
 }};return q;}};
 const handler=createHandler({db,twilio:{messages:{create:async()=>({sid:'test',status:'sent'})}},messagingServiceSid:'test',publicBaseUrl:'https://example.invalid',chartDb:{from(){const q={select(){return q},gte(){return q},lte(){return q},limit:async()=>({data:[]})};return q;}}});
 async function call(verified=true,dropOffComplete=false){let status=200,payload;await handler({headers:{authorization:'Bearer token'},body:{conversationId:'12345678-1234-1234-1234-123456789abc',releaseVerified:verified,dropOffComplete}},{set(){},status(n){status=n;return this},json(p){payload=p;return this}});return{status,payload};}
 return {call,messages,checks,arrivals};
}
test('staff check-in records official time once and sends app instructions once',async()=>{const h=harness();const a=await h.call(),b=await h.call();assert.equal(a.status,200);assert.equal(a.payload.checkedInAt,b.payload.checkedInAt);assert.equal(h.checks.length,1);assert.equal(h.messages.length,2);assert.equal(h.messages[0].body,PICKUP_CHECKIN);assert.equal(h.messages[0].delivery_status,'sent');});
test('driver role and unverified release cannot create check-ins or messages',async()=>{const h=harness('driver');assert.equal((await h.call()).status,403);assert.equal(h.messages.length,0);const staff=harness();assert.equal((await staff.call(false)).status,400);assert.equal(staff.checks.length,0);});
test('drop-off Eastern boundaries and weekends',()=>{for(const [time,phrase] of [['09:59','Receiving is closed'],['10:00','stay to the right'],['21:29','stay to the right'],['21:30','cut-off'],['21:59','cut-off'],['22:00','Receiving is closed']])assert(dropoffReply('2026-09-29T'+time+':00Z').includes(phrase));assert(dropoffReply('2026-10-03T14:00:00Z').includes('Receiving is closed'));assert(dropoffReply('2026-12-01T11:00:00Z').includes('stay to the right'));});

test('SMS staff check-in creates one active arrival and records its official check-in time',async()=>{const h=harness('shipping','sms');const result=await h.call();assert.equal(result.status,200);assert.equal(result.payload.warning,null);assert.equal(h.arrivals.length,1);assert.equal(h.arrivals[0].checked_in_at,result.payload.checkedInAt);assert.equal(h.arrivals[0].visit_type,'pickup');assert.equal(h.arrivals[0].release_number,'12345');await h.call();assert.equal(h.arrivals.length,1);});

for(const channel of ['app','sms'])test('Both '+channel+' requires unloading complete and starts pickup once',async()=>{
 const h=harness('shipping',channel,'both');
 assert.equal((await h.call()).status,409);assert.equal(h.checks.length,0);assert.equal(h.messages.length,0);
 const a=await h.call(true,true);assert.equal(a.status,200);assert.equal(a.payload.warning,null);
 const b=await h.call(true,true);assert.equal(b.payload.checkedInAt,a.payload.checkedInAt);assert.equal(h.checks.length,1);assert.equal(h.messages.length,2);
 if(channel==='sms'){assert.equal(h.arrivals.length,1);assert.equal(h.arrivals[0].visit_type,'both');}
});
