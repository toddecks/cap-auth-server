const {test}=require('node:test'),assert=require('node:assert/strict');
const {pickupInstructions,COIL_CHECKIN}=require('./pickup-directions');
const {PICKUP_CHECKIN}=require('./driver-visit-flow');
const c={release_number:'968751',facility_id:'csp-toledo-main'};
const row={poRel:'968751/510',scheduleDate:'2026-10-09',scheduleTime:'11:00:00',location:'PROSPY',unloadingDoor:'Coil'};
const db=rows=>({from(){const q={select(){return q},ilike(){return q},order(){return q},range:async(a,b)=>({data:rows.slice(a,b+1)})};return q;}});
test('COIL sends RIGHT, ignoring case',async()=>assert.equal(await pickupInstructions(db([row]),c,new Date('2026-10-09T14:00Z')),COIL_CHECKIN));
test('other doors retain existing directions',async()=>assert.equal(await pickupInstructions(db([{...row,unloadingDoor:'MB'}]),c,new Date('2026-10-09T14:00Z')),PICKUP_CHECKIN));
test('facility and newest date determine door',async()=>assert.equal(await pickupInstructions(db([{...row,scheduleDate:'2026-10-08',unloadingDoor:'MB'},row,{...row,location:'ENTPRS',unloadingDoor:'MB'}]),c,new Date('2026-10-09T14:00Z')),COIL_CHECKIN));
test('conflicting doors do not send guessed routing',async()=>assert.rejects(pickupInstructions(db([row,{...row,unloadingDoor:'MB'}]),c,new Date('2026-10-09T14:00Z')),/could not be determined/));

test('unmatched release never defaults to LEFT',async()=>assert.rejects(pickupInstructions(db([]),{...c,release_number:'001288'}),error=>error.code==='LOADING_DOOR_REQUIRED'));
test('missing door never defaults to LEFT',async()=>assert.rejects(pickupInstructions(db([{...row,unloadingDoor:''}]),c),/could not be determined/));
test('explicit clerk COIL routing works for unmatched release and unavailable catalog',async()=>{for(const chart of [db([]),null])assert.equal(await pickupInstructions(chart,{...c,release_number:'001288'},new Date(),'COIL'),COIL_CHECKIN)});
test('explicit other-door routing uses LEFT and invalid choices are rejected',async()=>{assert.equal(await pickupInstructions(null,c,new Date(),'OTHER'),PICKUP_CHECKIN);await assert.rejects(pickupInstructions(null,c,new Date(),'anything'),/Choose COIL/)});
test('COIL first message exactly includes the full requested PPE wording',()=>assert.equal(COIL_CHECKIN,'Please pull around back and stay to the RIGHT. Please have all required PPE ready before entering the facility, including a hard hat, safety glasses, and fully enclosed shoes.'));
