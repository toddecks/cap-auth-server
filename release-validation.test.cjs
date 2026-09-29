const {test} = require('node:test');
const assert = require('node:assert/strict');
const {createWorker} = require('./release-validation');

test('disabled release replies never query data, claim attempts, or send messages', async () => {
  const fail = () => { throw new Error('Disabled worker must not perform side effects'); };
  const worker = createWorker({db:{from:fail,rpc:fail},chart:{from:fail},twilio:{messages:{create:fail}}});
  worker.start();
  await worker.tick();
  await worker.tick();
  assert.equal(worker.health.state, 'disabled');
  assert.equal(worker.health.mode, 'release-auto-replies-disabled-v1');
  assert.equal(worker.health.lastChecked, null);
});
