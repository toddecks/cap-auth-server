const {test}=require('node:test'),assert=require('node:assert/strict');
const list=require('./release-list'),{parseDetails,nextCheckin}=require('./sms-checkin-details');
const {lookupRelease,processAttempts,releaseFromMessage,ACCEPTED}=require('./release-attempts');
const {displayRows}=require('./public-arrivals');
const {confirmedDeparture}=require('./ps-departures');
const {pickupInstructions}=require('./pickup-directions');
const rows=['32381','32382'].map((poRel,i)=>({poRel,bolNumber:String(95109+i),scheduleDate:'2026-11-16',scheduleTime:'16:00:00',location:'PROSPY',unloadingDoor:'BD',customerNo:'STOSTE',arrivalDate:'2026-11-16',arrivalTime:'13:58:00',shipDate:'2026-11-16',shipTime:i?'17:05:00':'17:00:00'}));
function chart(data=rows){return{from(){const q={select(){return q},ilike(){return q},order(){return q},range:async()=>({data})};return q}}}
test('vertical messages preserve both loads, carrier, and ignore truck ID as a name',()=>{
 assert.deepEqual(parseDetails('Pick up\n#32381 & 32382\nChase freight\nTruck 122'),{driver_company:'Chase freight',last_release_number:'32381 & 32382'});
 for(const body of ['Shane\n32381&32382','Shane\n32381\n32382','Shane, 32381 & 32382']){
  const p=parseDetails(body);assert.equal(p.full_name,'Shane');assert.equal(p.last_release_number,'32381 & 32382');assert.equal(releaseFromMessage(body),'32381 & 32382');
 }
 const first=nextCheckin({body:'Pick up\n#32381 & 32382\nChase freight\nTruck 122',conversationCreated:true,now:new Date('2026-11-16T18:57:00Z')});
 const second=nextCheckin({body:'Shane\n32381&32382',existing:first.contact,now:new Date('2026-11-16T18:58:00Z')});
 assert.equal(second.contact.full_name,'Shane');assert.equal(second.contact.driver_company,'Chase freight');assert.equal(second.contact.last_release_number,'32381 & 32382');assert.equal(second.contact.onboarding_step,'ready');
});
test('list delimiters and dedup work; slash and numbered suffix remain one reference',()=>{
 for(const text of ['32381 & 32382','32381 and 32382','#32381 + #32382','32381,32382'])assert.deepEqual(list.split(text),['32381','32382']);
 for(const text of ['968751/510','CB7201P1-2/7','72934-1'])assert.deepEqual(list.split(text),[text]);
 assert.deepEqual(list.extract('Shane\n4193887506\n32381').releases,['32381']);
});
test('both releases validated individually, one normal acknowledgement, no duplicate replies',async()=>{
 const at='2026-11-16T18:58:00Z',c={id:'two-loads',visit_type:'pickup',facility_id:'csp-toledo-main'};
 assert.equal(await lookupRelease(chart(),'32381 & 32382',c,at),'matched');
 assert.equal(await lookupRelease(chart(),'32381 & 99999',c,at),'invalid');
 const messages=[{id:'sms1',direction:'driver_to_shipping',body:'Shane\n32381&32382',sent_at:at}],sent=[];
 const args={chart:chart(),c,messages,pickupAt:Date.parse(at),send:async(_c,_kind,body)=>sent.push(body)};
 await processAttempts(args);await processAttempts(args);assert.deepEqual(sent,[ACCEPTED]);
});
test('separate public appointment rows retain each BOL and same driver check-in',()=>{
 const visit={conversation_id:'thread',release_number:'32381 & 32382',facility_id:'csp-toledo-main',visit_type:'pickup',driver_name:'Shane',driver_company:'Chase freight',entered_at:'2026-11-16T18:57:00Z',checked_in_at:'2026-11-16T18:58:00Z'};
 const result=displayRows([visit,visit],rows,Date.parse('2026-11-16T20:00:00Z'));
 assert.equal(result.length,2);assert.deepEqual(new Set(result.map(r=>r.bol_number)),new Set(['95109','95110']));assert.deepEqual(new Set(result.map(r=>r.release_number)),new Set(['32381','32382']));assert(result.every(r=>r.checked_in_at===visit.checked_in_at));
});
test('mixed loading routes pause instructions; same routes send only one instruction sequence',async()=>{
 const c={release_number:'32381 & 32382',facility_id:'csp-toledo-main'};
 const instruction=await pickupInstructions(chart(),c,new Date('2026-11-16T20:00:00Z'));assert(instruction.includes('LEFT'));
 await assert.rejects(pickupInstructions(chart([rows[0],{...rows[1],unloadingDoor:'COIL'}]),c,new Date('2026-11-16T20:00:00Z')),/different loading-door/);
});
test('driver departs only after all separate loads have shipped',()=>{
 const visit={release_number:'32381 & 32382',visit_type:'pickup',facility_id:'csp-toledo-main',entered_at:'2026-11-16T18:57:00Z',checked_in_at:'2026-11-16T18:58:00Z'},now=Date.parse('2026-11-16T23:00:00Z');
 assert.equal(confirmedDeparture(visit,[rows[0],{...rows[1],shipDate:null,shipTime:null}],now),null);
 assert.deepEqual(confirmedDeparture(visit,rows,now),{departedAt:'2026-11-16T22:05:00.000Z',bolNumber:'95109'});
});
