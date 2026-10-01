const {test}=require('node:test'),assert=require('node:assert/strict');
const {startsNewVisit,currentCheckin,key}=require('./visit-session');
const {nextCheckin}=require('./sms-checkin-details');
test('complete text after checkout starts a new visit, ordinary chat and duplicate introductions do not',()=>{
 const c={id:'thread',created_at:'2026-09-30T12:00:00Z'};
 const a={departed_at:'2026-09-30T14:00:00Z'};
 assert(startsNewVisit(c,a,'Todd, CSP, 45678910'));
 assert(!startsNewVisit(c,a,'Thank you'));assert(!startsNewVisit(c,{departed_at:null},'Todd, CSP, 45678910'));
 c.session_started_at='2026-10-01T13:11:00Z';
 assert(!startsNewVisit(c,a,'Todd, CSP, 45678910'));
 assert(!currentCheckin(c,{checked_in_at:'2026-09-30T13:00:00Z'}));
 assert(currentCheckin(c,{checked_in_at:'2026-10-01T13:12:00Z'}));
 assert.equal(key(c),'thread:2026-10-01T13:11:00.000Z');
});
test('complete current introduction overrides stored identity and infers pickup',()=>{
 const r=nextCheckin({existing:{full_name:'Previous Driver',driver_company:'Previous Company'},body:'Todd, CSP, 45678910',conversationCreated:true});
 assert.equal(r.contact.full_name,'Todd');assert.equal(r.contact.driver_company,'CSP');assert.equal(r.contact.last_release_number,'45678910');assert.equal(r.visitType,'pickup');assert.equal(r.reply,'');
});
