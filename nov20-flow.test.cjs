const {test}=require('node:test'),assert=require('node:assert/strict');
const {parseDetails,nextCheckin}=require('./sms-checkin-details'),flow=require('./driver-visit-flow');
const noon=new Date('2026-11-20T16:00:00Z');
test('Unicode names and requested mixed-order examples',()=>{
 for(const name of ['Tom','José','Jean-Luc',"D’Angelo",'Олексій','محمد','李','सुरेश']){
  const p=parseDetails(`${name} Eastern Express tag 1326631`);assert.equal(p.full_name,name);assert.equal(p.driver_company,'Eastern Express');assert.equal(p.last_release_number,'1326631');
 }
 const p=parseDetails(flow.stripVisitWords('This is Phil with Knowlan Trucking picking up 255711 for Midwest Metals'));
 assert.equal(p.full_name,'Phil');assert.equal(p.driver_company,'Knowlan Trucking');assert.equal(p.last_release_number,'255711');assert.equal(p.customer_name,'Midwest Metals');
});
test('dropoff asks material then routes typo variants, preserving uncertainty',()=>{
 const first=nextCheckin({body:'DROP OFF',conversationCreated:true,now:noon});assert.equal(first.reply,'Dropping off coils or sheets?');
 for(const word of ['coil','coils','coils!','coilz','cois'])assert.match(nextCheckin({existing:first.contact,body:word,now:noon}).reply,/RIGHT/);
 for(const word of ['sheet','sheets','shets','sheetz'])assert.match(nextCheckin({existing:first.contact,body:word,now:noon}).reply,/LEFT/);
 assert.equal(require('./dropoff-material').material('coils and sheets'),null);
 assert.equal(nextCheckin({existing:first.contact,body:'sheets',now:noon}).contact.full_name,'');
});
test('updated office hours use 5:30 AM Eastern, manual button uses distinct call wording',()=>{
 const a=require('./after-hours-reply');assert(a.closedPeriod(Date.parse('2026-11-20T05:29:59-05:00')));assert.equal(a.closedPeriod(Date.parse('2026-11-20T05:30:00-05:00')),null);
 assert.equal(a.CALL_OFFICE,'Please call 419-269-9706, select 5 or stay on the line to be connected. Thank you!');
});
test('abbreviated slash tails match one appointment, conflicting times stay ambiguous',()=>{
 const row={poRel:'968003/682/997',location:'PROSPY',scheduleDate:'2026-11-20',scheduleTime:'09:00:00',unloadingDoor:'BD'};
 const {doorMatches}=require('./release-doors');assert.equal(doorMatches([row],['969682'],'2026-11-20')['969682:PROSPY'].status,'matched');
 assert.equal(doorMatches([row,{...row,poRel:'111003/682',scheduleTime:'10:00:00'}],['969682'],'2026-11-20')['969682:PROSPY'].status,'ambiguous');
});
test('repeated identical valid release sends only one verifying response',async()=>{
 const {processAttempts,ENABLED_AT,ACCEPTED}=require('./release-attempts');const messages=[1,2].map(id=>({id,direction:'driver_to_shipping',body:'969682',sent_at:new Date(ENABLED_AT+id*1000).toISOString()})),sent=[];
 const run=()=>processAttempts({c:{id:'visit'},messages,pickupAt:ENABLED_AT,lookup:async()=> 'matched',send:async(_,kind,body)=>sent.push(body)});
 await run();await run();assert.deepEqual(sent,[ACCEPTED]);assert.equal(ACCEPTED,'Thank you! We’re verifying your release number and will send instructions shortly.');
});
test('returning driver is held until official current check-in',async()=>{
 const c={id:'return',channel:'sms',sms_phone_e164:'+15555550100',created_at:'2026-11-20T12:00:00Z'};
 const db={from(){const q={select(){return q},eq(){return q},not(){return q},lt(){return q},limit:async()=>({data:[{id:1}]})};return q}};
 const {held}=require('./returning-driver');assert.equal(await held(db,c,null),true);assert.equal(await held(db,c,{checked_in_at:'2026-11-20T12:01:00Z'}),false);
});

test('customer detail survives subsequent messages and resets for a new visit',()=>{
 const first=nextCheckin({body:'This is Phil with Knowlan Trucking picking up 255711 for Midwest Metals',conversationCreated:true,now:noon});
 assert.equal(first.contact.customer_name,'Midwest Metals');
 assert.equal(nextCheckin({existing:first.contact,body:'Thanks',now:noon}).contact.customer_name,'Midwest Metals');
 assert.equal(nextCheckin({existing:first.contact,body:'Pick up',conversationCreated:true,now:noon}).contact.customer_name,'');
});
