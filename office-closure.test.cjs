const {test}=require('node:test'),assert=require('node:assert/strict');
const {periodDay,active,createStore}=require('./office-closure');
const {plan,BODY,closedPeriod}=require('./after-hours-reply');
const now=Date.parse('2026-11-19T15:00:00-05:00'),override={closed_on:'2026-11-19',activated_at:new Date(now).toISOString()};
const c={id:'visit',status:'open',created_at:'2026-11-19T12:00:00Z'};
const incoming=at=>({direction:'driver_to_shipping',sent_at:new Date(at).toISOString(),body:'Pickup 72967'});
test('early override uses existing notice, expires 6pm, keeps normal schedule',()=>{
 assert.equal(closedPeriod(now),null);assert.equal(closedPeriod(now,override),'2026-11-19');
 assert.equal(plan(c,[incoming(now+1000)],now+2000,override).body,require('./after-hours-reply').CALL_OFFICE);
 assert.equal(active(override,Date.parse('2026-11-19T17:59:59-05:00')),true);
 assert.equal(active(override,Date.parse('2026-11-19T18:00:00-05:00')),false);
 assert.equal(active(override,Date.parse('2026-11-20T05:59:00-05:00')),false);
 assert.equal(active(override,Date.parse('2026-11-20T06:00:00-05:00')),false);
 assert.equal(closedPeriod(Date.parse('2026-11-20T18:00:00-05:00'),override),'2026-11-20');
 assert.equal(periodDay(Date.parse('2026-11-20T02:00:00-05:00')),'2026-11-19');
});
test('no retroactive or duplicate notices when enabling early, crossing 6pm or midnight',()=>{
 assert.equal(plan(c,[incoming(now-1000)],now+1000,override),null);
 const sent={direction:'shipping_to_driver',sent_at:new Date(now+2000).toISOString(),body:BODY,client_message_id:'release-wait:after-hours-2026-11-19:visit'};
 for(const at of [now+3000,Date.parse('2026-11-19T18:01:00-05:00'),Date.parse('2026-11-20T01:00:00-05:00')])assert.equal(plan(c,[sent,incoming(at)],at+1000,override).send,false);
});
function fixture(role='shipping'){
 let value={closed_on:null,activated_at:null},writes=0;
 const db={from(){const q={select(){return q},eq(){return q},maybeSingle:async()=>({data:value}),upsert:async v=>{value=v;writes++;return {data:null}}};return q;}};
 const auth={auth:{getUser:async()=>({data:{user:{id:'staff',app_metadata:{csp_role:role}}}})}};
 const store=createStore({db,auth,now:()=>now});
 const call=async(method,body={},token='test')=>{let status=200,result;const res={set(){},status(v){status=v;return res},json(v){result=v;return res}};await store.handler({method,body,headers:{authorization:token?'Bearer '+token:''}},res);return {status,result}};
 return {call,get value(){return value},get writes(){return writes},db,auth};
}
test('staff can persist early close and return to automatic; new process reads shared value',async()=>{
 const f=fixture();assert.equal((await f.call('GET')).result.enabled,false);assert.equal((await f.call('POST',{enabled:true})).result.enabled,true);assert.equal(f.value.updated_by,'staff');
 assert.equal(active(await createStore({db:f.db,auth:f.auth,now:()=>now}).load(),now),true);
 assert.equal((await f.call('POST',{enabled:false})).result.enabled,false);
});
test('no writes for unauthenticated, unauthorized or malformed requests',async()=>{
 for(const role of ['driver','viewer']){const f=fixture(role);assert.equal((await f.call('POST',{enabled:true})).status,403);assert.equal(f.writes,0);}
 const f=fixture();assert.equal((await f.call('POST',{enabled:true},'')).status,401);assert.equal((await f.call('POST',{enabled:'true'})).status,400);assert.equal(f.writes,0);
});
