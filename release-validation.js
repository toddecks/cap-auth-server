'use strict';

// Release-number correction replies are disabled. Shipping staff handle follow-up.
// Keep this interface so startup and the health endpoint remain compatible.
function createWorker() {
  const health = {
    mode: 'release-auto-replies-disabled-v1',
    state: 'disabled',
    lastChecked: null,
    lastIngest: null,
    error: null
  };
  return { start() {}, async tick() {}, health };
}

module.exports = { createWorker };
