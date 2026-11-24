const {test}=require('node:test'),assert=require('node:assert/strict');
const {acknowledgement,held}=require('./verification-hold');
const c={id:'visit',session_started_at:'2026-11-24T12:00:00Z'};
const reply={direction:'shipping_to_driver',sent_at:'2026-11-24T12:01:00Z',delivery_status:'sent',body:'Thank you! We’re verifying your release number and will send instructions shortly.'};
test('verification hold is per visit, durable, and released only by a current official check-in',()=>{
 assert(held(c,[reply]));assert(!held({...c,session_started_at:'2026-11-25T12:00:00Z'},[reply]));
 assert(held(c,[reply],{checked_in_at:'2026-11-23T12:00:00Z'}));
 assert(!held(c,[reply],{checked_in_at:'2026-11-24T12:02:00Z'}));
 for(const delivery_status of ['failed','undelivered'])assert(!acknowledgement(c,[{...reply,delivery_status}]));
 assert(!held(c,[{...reply,direction:'driver_to_shipping'}]));
 assert(held(c,[{...reply,delivery_status:'queued'}]));
});
