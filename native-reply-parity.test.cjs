const {test}=require('node:test');
const assert=require('node:assert/strict');
const flow=require('./driver-visit-flow');
// Captured by executing the live native_dropoff_reply decision branch in a
// rolled-back temporary PostgreSQL function, 2026-10-13. Re-capture if changed.
for(const row of require('./native-reply-parity.json'))test(`native/SMS receiving parity ${row.visit_type} ${row.at_time}`,()=>{
 assert.equal(flow[row.visit_type==='both'?'bothReply':'dropoffReply'](row.at_time),row.body);
});
