const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createReviewWorker, REVIEW_BODY } = require('./driver-reviews');
function fixture(send) {
  const job={arrival_id:1,conversation_id:'conversation',phone:'+15555550101',status:'pending'};
  const messages=[];
  const db={rpc:async()=>({data:job.status==='pending' ? (job.status='processing',[{...job}]) : []}),
    from(table) { return { upsert:async(row)=>{messages.push(row);return{};},
      update(patch){return {eq:async()=>{ if(table==='driver_review_requests') Object.assign(job,patch); return{}; }};} }; }
  };
  let count=0;
  const worker=createReviewWorker({db,twilio:{messages:{create:async args=>{count++;assert.equal(args.body,REVIEW_BODY);return send(args);}}},
    messagingServiceSid:'service',publicBaseUrl:'https://example.com',scheduleStatusSync(){},logger:{error(){}}});
  return {worker,job,messages,count:()=>count};
}
test('one review per closed visit across repeated worker checks, exact review URL in transcript',async()=>{
  const f=fixture(async()=>({sid:'SMtest',status:'queued'}));
  await f.worker.tick(); await f.worker.tick();
  assert.equal(f.count(),1);assert.equal(f.job.status,'sent');
  assert.equal(f.messages.length,1);assert.match(f.messages[0].body,/https:\/\/g.page\/r\/CS0GZ0amOC9REAE\/review/);
});
test('ambiguous provider timeout is held and never automatically resent',async()=>{
  const f=fixture(async()=>{throw new Error('timeout');});
  await f.worker.tick();await f.worker.tick();
  assert.equal(f.count(),1);assert.equal(f.job.status,'uncertain');
});
test('provider opt-out rejection is recorded without retries',async()=>{
  const f=fixture(async()=>{throw Object.assign(new Error('opted out'),{status:400,code:21610});});
  await f.worker.tick();await f.worker.tick();
  assert.equal(f.count(),1);assert.equal(f.job.status,'failed');assert.equal(f.job.error_code,'21610');
});
test('overlapping timer calls cannot send twice',async()=>{
  let release;const gate=new Promise(resolve=>release=resolve);
  const f=fixture(async()=>{await gate;return{sid:'SMtest',status:'sent'};});
  const first=f.worker.tick();await Promise.resolve();const second=f.worker.tick();release();await Promise.all([first,second]);
  assert.equal(f.count(),1);
});
