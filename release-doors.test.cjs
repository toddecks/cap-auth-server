const {test}=require('node:test'),assert=require('node:assert/strict');
const {doorMatches}=require('./release-doors');
const row={poRel:'880740',unloadingDoor:'BD',location:'PROSPY',scheduleDate:'2026-10-06',scheduleTime:null};
const lookup=rows=>doorMatches(rows,['880740'],'2026-10-06')['880740:PROSPY'];
test('matches release without appointment time',()=>assert.deepEqual(lookup([row]),{door:'BD',status:'matched',appointment_at:null,match_type:'exact',release_reference:'880740',customer:''}));
test('matches explicit release in list',()=>assert.equal(lookup([{...row,poRel:'123456/880740'}]).door,'BD'));
test('supports shorthand suffixes in explicit release lists',()=>assert.equal(lookup([{...row,poRel:'880739/740'}]).door,'BD'));
test('conflicting doors require confirmation',()=>assert.equal(lookup([row,{...row,unloadingDoor:'MB'}]).status,'ambiguous'));
test('newest date wins over older assignment',()=>assert.equal(lookup([row,{...row,unloadingDoor:'MB',scheduleDate:'2026-10-05'}]).door,'BD'));
test('canceled records excluded',()=>assert.equal(lookup([{...row,cancelLoad:true}]),undefined));
test('facility assignments kept separate',()=>assert.equal(lookup([row,{...row,location:'ENTPRS',unloadingDoor:'1'}]).door,'BD'));
test('missing door never invented',()=>assert.equal(lookup([{...row,unloadingDoor:''}]).status,'unassigned'));

test('appointment and door come from same release and facility',()=>{const match=lookup([{...row,scheduleTime:'10:30:00'},{...row,location:'ENTPRS',scheduleTime:'08:00:00',unloadingDoor:'1'}]);assert.equal(match.door,'BD');assert.equal(match.appointment_at,'2026-10-06T10:30:00');});
test('conflicting appointments do not produce a guessed color',()=>{const match=lookup([{...row,scheduleTime:'10:00:00'},{...row,scheduleTime:'12:00:00'}]);assert.equal(match.status,'ambiguous');assert.equal(match.appointment_at,null);});

test('punctuation and spaces normalize before exact matching',()=>assert.equal(lookup([{...row,poRel:'Release # 880-740'}]).match_type,'exact'));
test('unique partial number never validates or assigns a door',()=>assert.equal(doorMatches([row],['0740'],'2026-10-06')['0740:PROSPY'],undefined));
test('ambiguous partial numbers never assign a door',()=>{const m=doorMatches([row,{...row,poRel:'990740'}],['0740'],'2026-10-06')['0740:PROSPY'];assert.equal(m.status,'ambiguous');assert.equal(m.door,null);});
test('exact reference wins over partial candidates',()=>assert.equal(lookup([row,{...row,poRel:'1880740',unloadingDoor:'MB'}]).door,'BD'));
test('short fragments do not guess',()=>assert.equal(doorMatches([row],['740'],'2026-10-06')['740:PROSPY'],undefined));

test('newest annual release wins regardless of input order or arrival proximity',()=>{
 const old={...row,scheduleDate:'2025-10-06',unloadingDoor:'OLD',customerNo:'OLD'};
 const recent={...row,scheduleDate:'2026-10-07',scheduleTime:'13:00:00',unloadingDoor:'NEW',customerNo:'CENTER'};
 for(const rows of [[old,recent],[recent,old]]){
  const m=doorMatches(rows,['880740'],'2025-10-06')['880740:PROSPY'];
  assert.equal(m.door,'NEW');assert.equal(m.appointment_at,'2026-10-07T13:00:00');assert.equal(m.customer,'Center Steel Sales, Inc.');
 }
});
test('annual duplicates do not validate partial references',()=>assert.equal(doorMatches([row,{...row,scheduleDate:'2025-10-06'}],['0740'],'2026-10-06')['0740:PROSPY'],undefined));
test('newer unrelated full reference cannot resolve an ambiguous fragment',()=>{
 const m=doorMatches([row,{...row,poRel:'990740',scheduleDate:'2026-10-07'}],['0740'],'2026-10-06')['0740:PROSPY'];assert.equal(m.status,'ambiguous');
});
test('newest missing door does not reuse older door',()=>assert.equal(lookup([row,{...row,scheduleDate:'2026-10-07',unloadingDoor:''}]).status,'unassigned'));
test('invalid dated records do not override valid records',()=>assert.equal(lookup([row,{...row,scheduleDate:null,unloadingDoor:'OLD'}]).door,'BD'));
test('cancelled newest record does not override active record',()=>assert.equal(lookup([row,{...row,scheduleDate:'2026-10-07',cancelLoad:true,unloadingDoor:'OLD'}]).door,'BD'));
test('COIL suffix allows complete numeric base only',()=>{
 const rows=[{...row,poRel:'232662-coil',unloadingDoor:'Coil'}];
 for(const release of ['232662','232662-coil'])assert.equal(doorMatches(rows,[release],'2026-10-13')[release.toUpperCase().replace(/[^A-Z0-9]/g,'')+':PROSPY'].door,'Coil');
 for(const release of ['232622','32662','23266'])assert.notEqual(doorMatches(rows,[release],'2026-10-13')[release+':PROSPY']?.status,'matched');
 assert.equal(doorMatches([{...row,poRel:'232662-1'}],['232662'],'2026-10-13')['232662:PROSPY'],undefined);
});
