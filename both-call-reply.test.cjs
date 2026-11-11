const {test}=require('node:test');
const assert=require('node:assert/strict');
const {bothReply,dropoffReply}=require('./driver-visit-flow');
const {nextCheckin}=require('./sms-checkin-details');
const expected='Please call 419-269-9706 for further instructions.';
test('Both always receives the exact call instructions',()=>{
 for(const now of ['2026-11-11T15:19:00Z','2026-11-11T23:00:00Z','2026-11-14T15:00:00Z']){
 assert.equal(bothReply(new Date(now)),expected);
 assert.equal(nextCheckin({body:'Both',conversationCreated:true,now:new Date(now)}).reply,expected);
 }
});
test('standard dropoff directions and Both pickup transition remain intact',()=>{
 assert.match(dropoffReply(new Date('2026-11-11T15:19:00Z')),/unchain/);
 const first=nextCheckin({body:'Both',conversationCreated:true});
 const pickup=nextCheckin({existing:first.contact,body:'Pick Up'});
 assert.equal(pickup.visitType,'both');assert.equal(pickup.reply,'');
});
