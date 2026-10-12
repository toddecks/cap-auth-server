const {test}=require('node:test'),assert=require('node:assert/strict');
const {processAttempts,lookupRelease,INVALID,ACCEPTED,ENABLED_AT}=require('./release-attempts');
function fixture(){let clock=ENABLED_AT+60000;const messages=[],sent=[],c={id:'test',visit_type:'pickup',facility_id:'csp-toledo-main'};return {messages,sent,c,add(body){clock+=1000;messages.push({id:messages.length+1,body,sent_at:new Date(clock).toISOString(),direction:'driver_to_shipping'});},async run(lookup=async(_,release)=>release==='968751'?'matched':'invalid'){return processAttempts({c,messages,pickupAt:ENABLED_AT,now:()=>clock,lookup,send:async(_,kind,body)=>sent.push({kind,body})});}};}
test('three invalid replies, repeat webhook safety, correct release recovery, and reset after success',async()=>{
 const f=fixture();for(const value of ['1111','2222','3333']){f.add(value);await f.run();await f.run();}
 assert.deepEqual(f.sent.map(m=>m.body),INVALID);
 f.add('968751');assert.equal((await f.run()).state,'matched');assert.equal(f.sent[3].body,ACCEPTED);
 f.add('4444');assert.equal((await f.run()).state,'invalid');assert.equal(f.sent[4].body,INVALID[0]);
});
test('same wrong number in a new message counts as another attempt',async()=>{const f=fixture();f.add('1111');await f.run();f.add('1111');await f.run();assert.deepEqual(f.sent.map(m=>m.body),INVALID.slice(0,2));});
test('fourth invalid stays at assistance and a subsequent valid number remains accepted',async()=>{const f=fixture();for(let i=0;i<4;i++){f.add('1111');await f.run();}assert.equal(f.sent[3].body,INVALID[2]);f.add('968751');assert.equal((await f.run()).state,'matched')});
test('database outage and ambiguous partial matches never count as invalid',async()=>{const f=fixture();f.add('9687');await assert.rejects(f.run(async()=>{throw Error('offline')}));assert.equal(f.sent.length,0);assert.equal((await f.run(async()=> 'ambiguous')).state,'ambiguous');assert.equal(f.sent.length,0);await f.run();assert.equal(f.sent[0].body,INVALID[0]);});
test('ordinary text and release details before Both pickup phase do not trigger invalid responses',async()=>{const f=fixture();f.add('Thank you');f.add('PICK UP');await f.run();assert.equal(f.sent.length,0);});
function chart(rows,fail=false){let calls=0;return {get calls(){return calls},from(){let a=0,b=999;const q={select(){return q},gte(){return q},lte(){return q},order(){return q},range(x,y){a=x;b=y;calls++;return q},then(f){return Promise.resolve(fail?{error:Error('unavailable')}:{data:rows.slice(a,b+1)}).then(f)}};return q}}}
const row={scheduleDate:'2026-10-12',scheduleTime:'12:30:00',poRel:'968751/510',location:'PROSPY',unloadingDoor:'MB'};
test('lookup recognizes slash reference, partial number, punctuation and newest matching date',async()=>{for(const release of ['968751','968-751','968510'])assert.equal(await lookupRelease(chart([{...row,scheduleDate:'2026-10-11',unloadingDoor:'BD'},row]),release,{facility_id:'csp-toledo-main'},ENABLED_AT),'matched');});
test('missing and cancelled releases rejected; ambiguous partial not rejected as invalid',async()=>{
 assert.equal(await lookupRelease(chart([]),'968751',{facility_id:'csp-toledo-main'},ENABLED_AT),'invalid');
 assert.equal(await lookupRelease(chart([{...row,cancelLoad:true}]),'968751',{facility_id:'csp-toledo-main'},ENABLED_AT),'invalid');
 assert.equal(await lookupRelease(chart([row,{...row,poRel:'968799'}]),'9687',{facility_id:'csp-toledo-main'},ENABLED_AT),'ambiguous');
});
test('lookup reads beyond first 1000 records',async()=>{const db=chart([...Array.from({length:1000},(_,i)=>({...row,poRel:'other'+i})),row]);assert.equal(await lookupRelease(db,'968751',{facility_id:'csp-toledo-main'},ENABLED_AT),'matched');assert.equal(db.calls,2)});
