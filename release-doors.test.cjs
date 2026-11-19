const {test}=require('node:test'),assert=require('node:assert/strict');
const {doorMatches}=require('./release-doors');
const row={poRel:'880740',unloadingDoor:'BD',location:'PROSPY',scheduleDate:'2026-10-06',scheduleTime:null};
const lookup=rows=>doorMatches(rows,['880740'],'2026-10-06')['880740:PROSPY'];
test('matches release without appointment time',()=>assert.deepEqual(lookup([row]),{bol_number:null,bol_ambiguous:false,door:'BD',status:'matched',appointment_at:null,match_type:'exact',release_reference:'880740',customer:'',carrier_name:null}));
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

test('reused annual release uses the requested appointment date',()=>{
 const old={...row,scheduleDate:'2025-10-06',unloadingDoor:'OLD',customerNo:'OLD'};
 const recent={...row,scheduleDate:'2026-10-07',scheduleTime:'13:00:00',unloadingDoor:'NEW',customerNo:'CENTER'};
 for(const rows of [[old,recent],[recent,old]]){
  const m=doorMatches(rows,['880740'],'2025-10-06')['880740:PROSPY'];
  assert.equal(m.door,'OLD');assert.equal(m.appointment_at,null);assert.equal(m.customer,'OLD');
 }
});
test('annual duplicates do not validate partial references',()=>assert.equal(doorMatches([row,{...row,scheduleDate:'2025-10-06'}],['0740'],'2026-10-06')['0740:PROSPY'],undefined));
test('newer unrelated full reference cannot resolve an ambiguous fragment',()=>{
 const m=doorMatches([row,{...row,poRel:'990740',scheduleDate:'2026-10-07'}],['0740'],'2026-10-06')['0740:PROSPY'];assert.equal(m.status,'ambiguous');
});
test('newest missing door does not reuse older door',()=>assert.equal(doorMatches([row,{...row,scheduleDate:'2026-10-07',unloadingDoor:''}],['880740'],'2026-10-07')['880740:PROSPY'].status,'unassigned'));
test('invalid dated records do not override valid records',()=>assert.equal(lookup([row,{...row,scheduleDate:null,unloadingDoor:'OLD'}]).door,'BD'));
test('cancelled newest record does not override active record',()=>assert.equal(lookup([row,{...row,scheduleDate:'2026-10-07',cancelLoad:true,unloadingDoor:'OLD'}]).door,'BD'));
test('COIL suffix allows complete numeric base only',()=>{
 const rows=[{...row,poRel:'232662-coil',unloadingDoor:'Coil'}];
 for(const release of ['232662','232662-coil'])assert.equal(doorMatches(rows,[release],'2026-10-13')[release.toUpperCase().replace(/[^A-Z0-9]/g,'')+':PROSPY'].door,'Coil');
 for(const release of ['232622','32662','23266'])assert.notEqual(doorMatches(rows,[release],'2026-10-13')[release+':PROSPY']?.status,'matched');
 assert.equal(doorMatches([{...row,poRel:'232662-1'}],['232662'],'2026-10-13')['232662:PROSPY'].status,'matched');
});
test('letters and punctuation ignored, all digits still required',()=>{
 for(const poRel of ['ABC-232662','232662-coil','23-26-62 LOAD']){
  const rows=[{...row,poRel}];
  for(const input of ['232662','232662-coil','ABC232662'])assert.equal(doorMatches(rows,[input],'2026-10-13')[input.toUpperCase().replace(/[^A-Z0-9]/g,'')+':PROSPY'].status,'matched');
  assert.equal(doorMatches(rows,['232622'],'2026-10-13')['232622:PROSPY'],undefined);
 }
});
test('BOL comes from the same newest release/facility match',()=>{
 const newer={...row,scheduleDate:'2026-11-11',bolNumber:'94978'};
 const m=doorMatches([{...row,bolNumber:'11111'},newer,{...newer,location:'ENTPRS',bolNumber:'99999'}],['880740'],'2026-11-11')['880740:PROSPY'];
 assert.equal(m.bol_number,'94978');assert.equal(m.bol_ambiguous,false);
});
test('conflicting or incomplete BOLs do not silently choose one',()=>{
 const first={...row,bolNumber:'94978'};
 for(const second of [{...row,bolNumber:'94979'},row]){const m=lookup([first,second]);assert.equal(m.bol_number,null);assert.equal(m.bol_ambiguous,true);}
 assert.equal(lookup([{...row,bolNumber:'0'}]).bol_number,null);
});
test('BOL preserves leading zeroes and respects slash references',()=>{
 const m=doorMatches([{...row,poRel:'968638/896',bolNumber:'0094978'}],['968896'],'2026-10-06')['968896:PROSPY'];
 assert.equal(m.bol_number,'0094978');
});

test('one appointment follows the first listed release, not the earliest appointment',()=>{
 const rows=[{...row,poRel:'72935',scheduleTime:'14:00:00'},{...row,poRel:'72937',scheduleTime:'08:30:00'}];
 for(const input of ['72935/72937','72935 & 72937','72935\n72937']){
  const matches=doorMatches(rows,[input],row.scheduleDate);
  assert.equal(Object.keys(matches).length,1);
  assert.equal(Object.values(matches)[0].appointment_at,row.scheduleDate+'T14:00:00');
 }
 assert.equal(Object.values(doorMatches(rows,['72937/72935'],row.scheduleDate))[0].appointment_at,row.scheduleDate+'T08:30:00');
 assert.deepEqual(doorMatches(rows,['11111/72937'],row.scheduleDate),{});
 assert.equal(require('./release-list').expand([{release_number:'72935 & 72937'}]).length,1);
});

test('alphanumeric suffixes keep JFB appointments distinct',()=>{
 const rows=[{...row,poRel:'JFB1119**',scheduleTime:'06:00:00'},{...row,poRel:'JFB1119A',scheduleTime:'07:00:00'},{...row,poRel:'JFB1119B',scheduleTime:'08:00:00'}];
 for(const [release,time] of [['JFB1119A','07:00:00'],['jfb1119b','08:00:00']]){
  const match=doorMatches(rows,[release],row.scheduleDate)[release.toUpperCase()+':PROSPY'];assert.equal(match.status,'matched');assert.equal(match.appointment_at,row.scheduleDate+'T'+time);
 }
 assert.equal(doorMatches(rows,['JFB119B'],row.scheduleDate)['JFB119B:PROSPY'],undefined);
 assert.equal(doorMatches([rows[1]],['JFB1119C'],row.scheduleDate)['JFB1119C:PROSPY'],undefined);
});
