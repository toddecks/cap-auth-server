const{test}=require('node:test'),assert=require('node:assert/strict'),{visitType,dropoffReply}=require('./driver-visit-flow');
test('recognizes visit choice without confusing ambiguous text',()=>{assert.equal(visitType('PICKUP'),'pickup');assert.equal(visitType('drop-off'),'dropoff');assert.equal(visitType('pickup or dropoff'),null);assert.equal(visitType('here'),null)});
for(const [time,match] of [['2026-09-16T09:59:00Z',/outside/],['2026-09-16T10:00:00Z',/stay to the right/],['2026-09-16T21:44:59Z',/stay to the right/],['2026-09-16T21:45:00Z',/cut-off/],['2026-09-16T21:59:59Z',/cut-off/],['2026-09-16T22:00:00Z',/outside/],['2026-09-19T14:00:00Z',/outside/],['2026-12-16T11:00:00Z',/stay to the right/]])test('receiving hours '+time,()=>assert.match(dropoffReply(time),match));
const{nextCheckin,parseDetails}=require('./sms-checkin-details');
test('first message asks visit type, choice gives one appropriate reply',()=>{
 const first=nextCheckin({body:'HERE',conversationCreated:true,now:'2026-09-16T21:46:00Z'});assert.match(first.reply,/PICKUP or DROPOFF/);
 const chosen=nextCheckin({existing:first.contact,body:'DROPOFF',now:'2026-09-16T22:05:00Z'});assert.match(chosen.reply,/cut-off/);assert.equal(chosen.contact.onboarding_step,'ready');
 assert.equal(nextCheckin({existing:chosen.contact,body:'Thanks'}).reply,'');
});
test('pickup details accept carrier name in any order',()=>{
 const first=nextCheckin({body:'PICKUP',conversationCreated:true});assert.match(first.reply,/carrier name/);
 const second=nextCheckin({existing:first.contact,body:'Carrier name: ABC Trucking; Release: 123456; Name: John Smith'});assert.equal(second.contact.onboarding_step,'ready');assert.equal(second.contact.driver_company,'ABC Trucking');assert.equal(second.reply,'');
});
test('new visit does not reuse previous type',()=>{const r=nextCheckin({existing:{visit_type:'dropoff',onboarding_step:'ready'},body:'HERE',conversationCreated:true});assert.equal(r.visitType,null);assert.match(r.reply,/PICKUP/)});
