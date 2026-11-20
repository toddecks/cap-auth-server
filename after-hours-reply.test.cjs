const {test}=require('node:test'),assert=require('node:assert/strict');
const {BODY,closedPeriod,plan}=require('./after-hours-reply');
const c={id:'visit',status:'open',created_at:'2026-11-17T12:00:00Z'};
const incoming=at=>({direction:'driver_to_shipping',sent_at:at,body:'Are they still coming out?'});
test('exact requested wording',()=>assert.equal(BODY,'Our shipping office is currently closed. Please call 419-269-9706, select 5 or stay on the line to be connected. Thank you'));
test('6 PM through 5:30 AM Eastern remains the existing closed interval, including DST',()=>{
 for(const date of ['2026-11-17','2026-07-17']){const offset=date.includes('11-')?'-05:00':'-04:00';assert.equal(closedPeriod(Date.parse(date+'T17:59:59'+offset)),null);assert.equal(closedPeriod(Date.parse(date+'T18:00:00'+offset)),date);assert.equal(closedPeriod(Date.parse(date+'T05:29:59'+offset)),date==='2026-11-17'?'2026-11-16':'2026-07-16');assert.equal(closedPeriod(Date.parse(date+'T05:30:00'+offset)),null);}
});
test('an open existing conversation gets the notice after a new after-hours reply',()=>{const reply=plan(c,[incoming('2026-11-17T23:24:00Z')],Date.parse('2026-11-17T23:24:01Z'));assert.equal(reply.body,BODY);assert.equal(reply.send,true);});
test('no unsolicited notice, daytime reply, stale historical reply or ended/closed visit',()=>{
 const now=Date.parse('2026-11-17T23:24:01Z');for(const [visit,messages]of [[c,[]],[c,[incoming('2026-11-17T22:59:00Z')]],[c,[incoming('2026-11-17T23:00:00Z')]],[{...c,status:'closed'},[incoming('2026-11-17T23:24:00Z')]],[{...c,session_ended_at:'2026-11-17T23:00:00Z'},[incoming('2026-11-17T23:24:00Z')]]])assert.equal(plan(visit,messages,now),null);
 assert.equal(plan(c,[incoming('2026-11-17T22:59:00Z')],Date.parse('2026-11-17T22:59:01Z')),null);
});
test('once per closed period across midnight, including preexisting closed-office notices',()=>{
 const messages=[incoming('2026-11-17T23:24:00Z'),{direction:'shipping_to_driver',sent_at:'2026-11-17T23:24:01Z',body:BODY,client_message_id:'release-wait:after-hours-2026-11-17:visit'},incoming('2026-11-18T05:01:00Z')];
 assert.equal(plan(c,messages,Date.parse('2026-11-18T05:01:01Z')).send,false);
 assert.equal(plan(c,messages,Date.parse('2026-11-18T05:20:00Z')).send,false);
 messages.push(incoming('2026-11-18T23:01:00Z'));assert.equal(plan(c,messages,Date.parse('2026-11-18T23:01:01Z')).send,true);
});
