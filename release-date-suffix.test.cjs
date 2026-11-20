const {test}=require('node:test'),assert=require('node:assert/strict');
const {doorMatches}=require('./release-doors');
const {lookupRelease,processAttempts,ACCEPTED}=require('./release-attempts');
const {confirmedDeparture}=require('./ps-departures');
const row={poRel:'72934-1',scheduleDate:'2026-11-16',scheduleTime:'11:00:00',bolNumber:'95122',masterBolNumber:'0',location:'PROSPY',unloadingDoor:'SkidR',carrierName:'Customer Truck',customerNo:'MIAMIV'};
const tomorrow={...row,poRel:'72934-2',scheduleDate:'2026-11-17',scheduleTime:'15:00:00',bolNumber:'95120',unloadingDoor:'BD'};
const lookup=(rows,input='72934',date='2026-11-16')=>doorMatches(rows,[input],date)[input.toUpperCase().replace(/[^A-Z0-9]/g,'')+':PROSPY'];
test('PS 72934 uses today first, without being blocked by tomorrow or unrelated numeric fragments',()=>{
 for(const rows of [[row,tomorrow],[tomorrow,row]]){
  const m=lookup([...rows,{...row,poRel:'729342',scheduleDate:'2025-04-22',bolNumber:'76389'}]);
  assert.equal(m.status,'matched');assert.equal(m.bol_number,'95122');assert.equal(m.release_reference,'72934-1');assert.equal(m.door,'SkidR');assert.equal(m.appointment_at,'2026-11-16T11:00:00');
  assert.equal(lookup(rows,'72934','2026-11-17').bol_number,'95120');
 }
});
test('omitted punctuation works for complete references and separated numeric suffixes',()=>{
 for(const input of ['729341','72934-1','72934.1','72934_1','72934 # 1'])assert.equal(lookup([row,tomorrow],input).bol_number,'95122');
 for(const poRel of ['72934-1','72934_1','72934 #1','72934–1','72934.1'])assert.equal(lookup([{...row,poRel}]).bol_number,'95122');
 assert.notEqual(lookup([row,tomorrow],'2934')?.status,'matched');
 assert.equal(lookup([{...row,poRel:'729342'}]),undefined);
 assert.equal(lookup([row,tomorrow],'729343'),undefined);
});
test('today suffixed reference wins over reused exact base on an old date',()=>{
 assert.equal(lookup([row,{...tomorrow,poRel:'72934',scheduleDate:'2025-11-16'}]).bol_number,'95122');
});
test('same-day unlinked suffixes or equal-distance dates still require clarification',()=>{
 assert.equal(lookup([row,{...tomorrow,scheduleDate:'2026-11-16'}]).status,'ambiguous');
 assert.equal(lookup([row,{...tomorrow,scheduleDate:'2026-11-18'}],'72934','2026-11-17').status,'ambiguous');
});
function chart(rows){return{from(){const q={select(){return q},or(){return q},ilike(){return q},order(){return q},range:async()=>({data:rows})};return q}}}
test('base release sends the requested verification acknowledgement through SMS',async()=>{
 const sent=[],at='2026-11-16T15:42:00Z',c={id:'suffix-test',visit_type:'pickup',facility_id:'csp-toledo-main'};
 assert.equal(await lookupRelease(chart([row,tomorrow]),'72934',c,at),'matched');
 const result=await processAttempts({chart:chart([row,tomorrow]),c,messages:[{id:'test',direction:'driver_to_shipping',sent_at:at,body:'Pick up number 72934\nFraley transport\nTruck number 418'}],pickupAt:Date.parse(at),send:async(_c,_kind,body)=>sent.push(body)});
 assert.equal(result.state,'matched');assert.deepEqual(sent,[ACCEPTED]);
});
test('base release departure uses same selected BOL and does not close for tomorrow',()=>{
 const visit={release_number:'72934',facility_id:'csp-toledo-main',visit_type:'pickup',entered_at:'2026-11-16T15:43:00Z',checked_in_at:'2026-11-16T15:43:00Z'};
 const shipped={...row,arrivalDate:'2026-11-16',arrivalTime:'10:43:34',shipDate:'2026-11-16',shipTime:'11:56:27'};
 const now=Date.parse('2026-11-16T18:00:00Z');
 assert.deepEqual(confirmedDeparture(visit,[shipped,tomorrow],now),{departedAt:'2026-11-16T16:56:27.000Z',bolNumber:'95122'});
 assert.equal(confirmedDeparture(visit,[{...shipped,shipDate:null,shipTime:null},tomorrow],now),null);
});
