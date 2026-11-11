const {test}=require('node:test'),assert=require('node:assert/strict');
const {confirmedDeparture,easternTimestamp,createWorker}=require('./ps-departures');
const now=Date.parse('2026-11-11T16:00:00Z');
const visit={arrival_id:1,conversation_id:'thread',facility_id:'csp-toledo-main',visit_type:'pickup',release_number:'255479.',entered_at:'2026-11-11T12:59:00Z',checked_in_at:'2026-11-11T12:59:00Z'};
const load={poRel:'255479',bolNumber:'94804',location:'PROSPY',arrivalDate:'2026-11-11',arrivalTime:'07:59:00',shipDate:'2026-11-11',shipTime:'08:57:00'};
test('uses Eastern standard/daylight time and rejects invalid wall times',()=>{
 assert.equal(easternTimestamp('2026-11-11','08:57:00'),Date.parse('2026-11-11T13:57:00Z'));
 assert.equal(easternTimestamp('2026-07-01','08:57:00'),Date.parse('2026-07-01T12:57:00Z'));
 assert(Number.isNaN(easternTimestamp('2026-03-08','02:30:00')));
 assert(Number.isNaN(easternTimestamp('2026-02-30','08:00:00')));
});
test('real completed PS pickup matches full reference with punctuation',()=>assert.deepEqual(confirmedDeparture(visit,[load],now),{departedAt:'2026-11-11T13:57:00.000Z',bolNumber:'94804'}));
test('full slash/shorthand references match but fragments do not',()=>{
 assert(confirmedDeparture({...visit,release_number:'968310'},[{...load,poRel:'968310/354'}],now));
 assert(confirmedDeparture({...visit,release_number:'968354'},[{...load,poRel:'968310/354'}],now));
 assert.equal(confirmedDeparture({...visit,release_number:'5310'},[{...load,poRel:'968310/354'}],now),null);
});
for(const [name,change] of Object.entries({unshipped:{shipTime:null},future:{shipTime:'16:00:00'},previous_day:{arrivalDate:'2026-11-10'},wrong_facility:{location:'ENTPRS'},cancelled:{cancelLoad:true},missing_BOL:{bolNumber:null},before_checkin:{shipTime:'07:58:00'},stale_arrival:{arrivalTime:'05:00:00'}}))test(`keeps ${name} load visible`,()=>assert.equal(confirmedDeparture(visit,[{...load,...change}],now),null));
test('multiple BOLs and conflicting snapshots remain visible',()=>{
 assert.equal(confirmedDeparture(visit,[load,{...load,bolNumber:'other'}],now),null);
 assert.equal(confirmedDeparture(visit,[load,{...load,shipTime:'09:00:00'}],now),null);
});
test('Both waits until pickup check-in and ignores receiving completion',()=>{
 assert.equal(confirmedDeparture({...visit,visit_type:'both',checked_in_at:null},[load],now),null);
 assert.equal(confirmedDeparture({...visit,visit_type:'both',checked_in_at:'2026-11-11T15:00:00Z'},[load],now),null);
 assert(confirmedDeparture({...visit,visit_type:'both'},[load],now));
 assert.equal(confirmedDeparture({...visit,visit_type:'dropoff'},[load],now),null);
});
function fixture({source='sms',error=false,alreadyDeparted=false}={}){
 const writes=[];let departed=false;const active={...visit,user_id:'driver',facility_name:'Prosperity Road',profile_snapshot:{name:'John'}};
 const db={from(table){let patch,filters=[];const q={select(){return q},is(){return q},not(){return q},gte(){return q},order(){return q},eq(k,v){filters.push([k,v]);return q},range(){return Promise.resolve({data:table===(source==='sms'?'shipping_sms_arrival_feed':'driver_arrival_history_feed')&&!departed?[active]:[]})},maybeSingle(){return Promise.resolve({data:alreadyDeparted?null:active})},single(){return Promise.resolve({data:{...active,id:1}})},update(p){patch=p;return q},upsert(p){patch=p;return q},then(resolve){assert(['driver_sms_arrivals','driver_arrivals'].includes(table),'Never change messages or conversations');writes.push({table,patch});departed=true;return Promise.resolve({data:[{id:1}]}).then(resolve)}};return q}};
 const worker=createWorker({db,chart:{},now:()=>now,candidates:async()=>{if(error)throw Error('PS offline');return[load]}});return{worker,writes};
}
for(const source of ['sms','app'])test(`worker records ${source} departure once and preserves conversation/history`,async()=>{
 const f=fixture({source});await f.worker.tick();await f.worker.tick();assert.equal(f.writes.length,1);assert.equal(f.worker.health.totalRemoved,1);
 assert.equal(f.writes[0].patch[source==='sms'?'departed_at':'occurred_at'],'2026-11-11T13:57:00.000Z');
 if(source==='app')assert.equal(f.writes[0].patch.client_event_id,'ps-shipped-1');
});
test('PS lookup failure and concurrent manual checkout never remove a truck',async()=>{
 for(const opts of [{error:true},{alreadyDeparted:true}]){const f=fixture(opts);await f.worker.tick();assert.equal(f.writes.length,0);}
});
