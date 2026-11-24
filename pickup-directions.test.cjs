const {test}=require('node:test'),assert=require('node:assert/strict');
const {pickupInstructions:routeInstructions,COIL_CHECKIN}=require('./pickup-directions');
const pickupInstructions=(db,c,now,door)=>routeInstructions(db,c,now,door,true);
const {PICKUP_CHECKIN}=require('./driver-visit-flow');
const c={release_number:'968751',facility_id:'csp-toledo-main'};
const row={poRel:'968751/510',scheduleDate:'2026-10-09',scheduleTime:'11:00:00',location:'PROSPY',unloadingDoor:'Coil'};
const db=rows=>({from(){const q={select(){return q},or(){return q},ilike(){return q},order(){return q},range:async(a,b)=>({data:rows.slice(a,b+1)})};return q;}});
test('COIL sends RIGHT, ignoring case',async()=>assert.equal(await pickupInstructions(db([row]),c,new Date('2026-10-09T14:00Z')),COIL_CHECKIN));
test('other doors retain existing directions',async()=>assert.equal(await pickupInstructions(db([{...row,unloadingDoor:'MB'}]),c,new Date('2026-10-09T14:00Z')),PICKUP_CHECKIN));
test('facility and newest date determine door',async()=>assert.equal(await pickupInstructions(db([{...row,scheduleDate:'2026-10-08',unloadingDoor:'MB'},row,{...row,location:'ENTPRS',unloadingDoor:'MB'}]),c,new Date('2026-10-09T14:00Z')),COIL_CHECKIN));
test('conflicting doors do not send guessed routing',async()=>assert.rejects(pickupInstructions(db([row,{...row,unloadingDoor:'MB'}]),c,new Date('2026-10-09T14:00Z')),/could not be determined/));

test('unmatched release never defaults to LEFT',async()=>assert.rejects(pickupInstructions(db([]),{...c,release_number:'001288'}),error=>error.code==='LOADING_DOOR_REQUIRED'));
test('missing door never defaults to LEFT',async()=>assert.rejects(pickupInstructions(db([{...row,unloadingDoor:''}]),c),/could not be determined/));
test('explicit clerk COIL routing works for unmatched release and unavailable catalog',async()=>{for(const chart of [db([]),null])assert.equal(await pickupInstructions(chart,{...c,release_number:'001288'},new Date(),'COIL'),COIL_CHECKIN)});
test('explicit other-door routing uses LEFT and invalid choices are rejected',async()=>{assert.equal(await pickupInstructions(null,c,new Date(),'OTHER'),PICKUP_CHECKIN);await assert.rejects(pickupInstructions(null,c,new Date(),'anything'),/Choose COIL/)});
test('COIL confirms check-in with RIGHT and keeps removed PPE text off',()=>assert.equal(COIL_CHECKIN,'You have been successfully checked in. Please pull around back and stay to the RIGHT.'));

test('confirmed PS COIL cannot be overridden by manual OTHER choice',async()=>assert.equal(await pickupInstructions(db([row]),c,new Date(),'OTHER'),COIL_CHECKIN));
test('confirmed PS BD retains standard directions despite manual COIL choice',async()=>assert.equal(await pickupInstructions(db([{...row,unloadingDoor:'BD'}]),c,new Date(),'COIL'),PICKUP_CHECKIN));

test('outage default never looks up or requires a loading door',async()=>{const text=await routeInstructions({from(){throw Error('must not query PS')}},c);assert.match(text,/successfully checked in/);assert.match(text,/LEFT/);});

test('automatic check-in uses first provided release and never guesses a missing or ambiguous door',async()=>{
 const {automaticPickupInstructions:auto,UNKNOWN_CHECKIN}=require('./pickup-directions');
 assert.equal(await auto(db([row,{...row,poRel:'72950',unloadingDoor:'MB'}]),{...c,release_number:'968751 & 72950'}),COIL_CHECKIN);
 assert.equal(await auto(db([]),c),UNKNOWN_CHECKIN);
 assert.equal(await auto(db([row,{...row,unloadingDoor:'MB'}]),c),UNKNOWN_CHECKIN);
 assert.equal(await auto(null,c),UNKNOWN_CHECKIN);
});

test('Gary release 7524 resolves the API 7524-coil reference to RIGHT',async()=>{
 const text=await require('./pickup-directions').automaticPickupInstructions(db([{...row,poRel:'7524-coil',scheduleDate:'2026-11-23',unloadingDoor:'Coil'}]),{...c,release_number:'7524'},new Date('2026-11-24T14:00:00Z'));
 assert.equal(text,COIL_CHECKIN);
});
