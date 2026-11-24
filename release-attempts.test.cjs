const {test}=require('node:test'),assert=require('node:assert/strict');
const {processAttempts,lookupRelease,INVALID,ACCEPTED,AMBIGUOUS,ENABLED_AT}=require('./release-attempts');
function fixture(){let clock=ENABLED_AT+60000;const messages=[],sent=[],c={id:'test',visit_type:'pickup',facility_id:'csp-toledo-main'};return {messages,sent,c,add(body){clock+=1000;messages.push({id:messages.length+1,body,sent_at:new Date(clock).toISOString(),direction:'driver_to_shipping'});},async run(lookup=async(_,release)=>release==='968751'?'matched':'invalid'){return processAttempts({c,messages,pickupAt:ENABLED_AT,now:()=>clock,lookup,send:async(_,kind,body)=>sent.push({kind,body})});}};}
test('three invalid replies, repeat webhook safety, correct release recovery, and silence after acknowledgement',async()=>{
 const f=fixture();for(const value of ['1111','2222','3333']){f.add(value);await f.run();await f.run();}
 assert.deepEqual(f.sent.map(m=>m.body),INVALID);
 f.add('968751');assert.equal((await f.run()).state,'matched');assert.equal(f.sent[3].body,ACCEPTED);
 f.add('4444');assert.equal((await f.run()).state,'matched');assert.equal(f.sent.length,4);
});
test('same wrong number in a new message counts as another attempt',async()=>{const f=fixture();f.add('1111');await f.run();f.add('1111');await f.run();assert.deepEqual(f.sent.map(m=>m.body),INVALID.slice(0,2));});
test('fourth invalid stays at assistance and a subsequent valid number remains accepted',async()=>{const f=fixture();for(let i=0;i<4;i++){f.add('1111');await f.run();}assert.equal(f.sent[3].body,INVALID[2]);f.add('968751');assert.equal((await f.run()).state,'matched')});
test('database outage and ambiguous partial matches never count as invalid',async()=>{const f=fixture();f.add('9687');await assert.rejects(f.run(async()=>{throw Error('offline')}));assert.equal(f.sent.length,0);assert.equal((await f.run(async()=> 'ambiguous')).state,'ambiguous');assert.deepEqual(f.sent.map(m=>m.body),[]);await f.run();assert.equal(f.sent.length,0);f.add("1111");await f.run();assert.equal(f.sent[0].body,INVALID[0]);});
test('ordinary text and release details before Both pickup phase do not trigger invalid responses',async()=>{const f=fixture();f.add('Thank you');f.add('PICK UP');await f.run();assert.equal(f.sent.length,0);});
function chart(rows,fail=false){let calls=0;return {get calls(){return calls},from(){let a=0,b=999;const q={select(){return q},or(){return q},ilike(){return q},gte(){return q},lte(){return q},order(){return q},range(x,y){a=x;b=y;calls++;return q},then(f){return Promise.resolve(fail?{error:Error('unavailable')}:{data:rows.slice(a,b+1)}).then(f)}};return q}}}
const row={scheduleDate:'2026-10-12',scheduleTime:'12:30:00',poRel:'968751/510',location:'PROSPY',unloadingDoor:'MB'};
test('lookup recognizes slash reference, partial number, punctuation and newest matching date',async()=>{for(const release of ['968751','968-751','968510'])assert.equal(await lookupRelease(chart([{...row,scheduleDate:'2026-10-11',unloadingDoor:'BD'},row]),release,{facility_id:'csp-toledo-main'},ENABLED_AT),'matched');});
test('missing and cancelled releases rejected; ambiguous partial not rejected as invalid',async()=>{
 assert.equal(await lookupRelease(chart([]),'968751',{facility_id:'csp-toledo-main'},ENABLED_AT),'invalid');
 assert.equal(await lookupRelease(chart([{...row,cancelLoad:true}]),'968751',{facility_id:'csp-toledo-main'},ENABLED_AT),'invalid');
 assert.equal(await lookupRelease(chart([row,{...row,poRel:'968799'}]),'9687',{facility_id:'csp-toledo-main'},ENABLED_AT),'ambiguous');
});
test('lookup reads beyond first 1000 records',async()=>{const db=chart([...Array.from({length:1000},(_,i)=>({...row,poRel:'other'+i})),row]);assert.equal(await lookupRelease(db,'968751',{facility_id:'csp-toledo-main'},ENABLED_AT),'matched');assert.equal(db.calls,2)});

test('ambiguous MJ 72147 cannot block later PLS 72207, including retries and restart',async()=>{
 const f=fixture();const db=chart([
 {...row,poRel:'72147-1/32232',scheduleDate:'2026-10-13',scheduleTime:'06:30:00'},
 {...row,poRel:'72147-2/32233',scheduleDate:'2026-10-13',scheduleTime:'12:00:00'},
 {...row,poRel:'72207',scheduleDate:'2026-10-13',scheduleTime:'11:00:00'}
 ]);
 const lookup=(_,release,c,at)=>lookupRelease(db,release,c,at);
 f.add('MJ 72147');assert.equal((await f.run(lookup)).state,'ambiguous');await f.run(lookup);
 assert.deepEqual(f.sent.map(m=>m.body),[]);
 f.add('PLS 72207');assert.equal((await f.run(lookup)).state,'matched');await f.run(lookup);
 assert.deepEqual(f.sent.map(m=>m.body),[ACCEPTED]);
});
test('both ambiguous and corrected messages already queued are processed in one pass',async()=>{
 const f=fixture();f.add('72147');f.add('72207');
 assert.equal((await f.run(async(_,r)=>r==='72147'?'ambiguous':'matched')).state,'matched');
 assert.deepEqual(f.sent.map(m=>m.body),[ACCEPTED]);
});
test('made-up 45678 cannot validate against old 945678; complete reference still matches',async()=>{
 const db=chart([{...row,poRel:'945678',scheduleDate:'2024-04-08'}]);
 assert.equal(await lookupRelease(db,'45678',{facility_id:'csp-toledo-main'},ENABLED_AT),'invalid');
 assert.equal(await lookupRelease(db,'945678',{facility_id:'csp-toledo-main'},ENABLED_AT),'matched');
});

test('labeled compound release reaches lookup intact and matches its actual catalog record',async()=>{
 const {releaseFromMessage}=require('./release-attempts');
 const text='Pickup #: CB7201P1-2/7';
 assert.equal(releaseFromMessage(text),'CB7201P1-2/7');
 const f=fixture();f.add(text);
 const db=chart([{...row,poRel:'CB7201P1-2/7'}]);
 assert.equal((await f.run((_,release,c,at)=>lookupRelease(db,release,c,at))).state,'matched');
 assert.deepEqual(f.sent.map(m=>m.body),[ACCEPTED]);
});
test('full slash references match, but a different suffix or unknown second release stays invalid',async()=>{
 const db=chart([{...row,poRel:'CB7201P1-1-6'}, {...row,poRel:'968751/510'}]);
 assert.equal(await lookupRelease(db,'968751/510',{facility_id:'csp-toledo-main'},ENABLED_AT),'matched');
 assert.equal(await lookupRelease(db,'CB7201P1-2/7',{facility_id:'csp-toledo-main'},ENABLED_AT),'invalid');
 assert.equal(await lookupRelease(db,'P329329',{facility_id:'csp-toledo-main'},ENABLED_AT),'invalid');
});

test('obsolete clerk verification cannot override the catalog or pause correction',async()=>{
 const c={id:'manual',visit_type:'pickup'},messages=[{id:'2',direction:'driver_to_shipping',body:'CB7201P1-2/7',sent_at:'2026-11-06T16:02:00Z'}],sent=[],held=[];
 const run=()=>processAttempts({c,messages,pickupAt:Date.parse('2026-11-06T16:00:00Z'),verifiedThrough:{verified_at:'2026-11-06T16:01:00Z',release_number:'CB7201P1-2/7'},lookup:async()=> 'invalid',send:async(c,kind,body)=>sent.push(body),onUnmatched:async(c,m,r)=>held.push(r)});
 assert.equal((await run()).state,'invalid');assert.deepEqual(sent,[INVALID[0]]);assert.deepEqual(held,[]);
});

test('PS outage mode acknowledges newest release without catalog lookup or invalid escalation',async()=>{
 const f=fixture();f.add('969737');await f.run(async()=> 'invalid');
 const run=()=>processAttempts({c:f.c,messages:f.messages,pickupAt:ENABLED_AT,validationEnabled:false,lookup:async()=>{throw Error('must not query stale feed')},send:async(_,kind,body)=>f.sent.push({kind,body})});
 assert.equal((await run()).state,'manual');assert.equal(f.sent.at(-1).body,ACCEPTED);assert.match(f.sent.at(-1).kind,/^manual-/);
 await run();assert.equal(f.sent.length,2,'restart does not duplicate acknowledgement');
 f.add('969738');assert.equal((await run()).state,'manual');assert.equal(f.sent.at(-1).body,ACCEPTED);
 assert.equal(f.sent.filter(m=>INVALID.includes(m.body)).length,1,'no additional invalid responses while paused');
});

test('multi-release input uses the first provided reference, preserving its order',async()=>{
 const {releaseFromMessage}=require('./release-attempts');
 const {parseDetails}=require('./sms-checkin-details');
 const db=chart([{...row,poRel:'72935',scheduleTime:'08:30:00'},{...row,poRel:'72937',scheduleTime:'14:00:00'},{...row,poRel:'967602/968939/969974',scheduleTime:'10:30:00'}]);
 for(const text of ['Pickup 72935/72937','72935 & 72937','72935\n72937','72935,72937']){
  assert.equal(require('./release-list').primary(releaseFromMessage(text)),'72935',text);
  assert.equal(await lookupRelease(db,releaseFromMessage(text),{facility_id:'csp-toledo-main'},ENABLED_AT),'matched',text);
 }
 assert.equal(require('./release-list').primary(parseDetails('969974\n968939\n967602').last_release_number),'969974');
 assert.equal(await lookupRelease(db,releaseFromMessage('969974\n968939\n967602'),{facility_id:'csp-toledo-main'},ENABLED_AT),'matched');
 assert.equal(await lookupRelease(db,'11111 & 72935',{facility_id:'csp-toledo-main'},ENABLED_AT),'invalid','never fall back to a later valid release');
});
test('resuming validation does not replay responses to outage-acknowledged messages',async()=>{
 const f=fixture();f.add('969737');
 await processAttempts({c:f.c,messages:f.messages,pickupAt:ENABLED_AT,validationEnabled:false,now:()=>Date.parse(f.messages.at(-1).sent_at),send:async(_,kind,body)=>f.sent.push({kind,body})});
 await f.run(async()=>{throw Error('already acknowledged message must not be rechecked')});
 assert.equal(f.sent.length,1);
 f.add('11111');assert.equal((await f.run()).state,'manual');assert.equal(f.sent.length,1);
 assert.equal((await f.run()).state,'manual','restart retains the acknowledgement hold');
});

test('silent review survives restart without a transcript entry and later correction remains matched',async()=>{
 const f=fixture(),saved=[];
 const silentStore={load:async()=>saved,save:async(_,row)=>saved.push({...row})};
 f.add('72147');
 const run=lookup=>processAttempts({c:f.c,messages:f.messages,pickupAt:ENABLED_AT,now:()=>ENABLED_AT+999999,lookup,silentStore,send:async(_,kind,body)=>f.sent.push({kind,body})});
 assert.equal((await run(async()=> 'ambiguous')).state,'ambiguous');
 assert.equal(saved.length,1);assert.equal(f.sent.length,0);
 // Reload only real transcript rows, as a fresh worker does after restart.
 f.messages.splice(0,f.messages.length,...f.messages.filter(m=>m.id));
 assert.equal((await run(async()=>{throw Error('must not revalidate a held attempt')})).state,'ambiguous');
 f.add('968751');assert.equal((await run(async()=> 'matched')).state,'matched');
 assert.deepEqual(f.sent.map(m=>m.body),[ACCEPTED]);
 f.messages.splice(0,f.messages.length,...f.messages.filter(m=>m.id||m.body===ACCEPTED));
 assert.equal((await run(async()=>{throw Error('already handled')})).state,'matched');
 assert.equal(f.sent.length,1);
});

test('two releases queued together stop processing after the first verifying message',async()=>{
 const f=fixture();f.add('968751');f.add('11111');await f.run();await f.run();
 assert.deepEqual(f.sent.map(m=>m.body),[ACCEPTED]);
});
