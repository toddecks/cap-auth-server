const {test}=require('node:test'),assert=require('node:assert/strict');
const {releaseCatalog}=require('./release-catalog');
const {lookupRelease}=require('./release-attempts');
test('future valid release and yearly duplicates use newest record without a one-day cutoff',async()=>{
 let calls=0;
 const rows=[{poRel:'72037',scheduleDate:'2025-10-13',location:'PROSPY',unloadingDoor:'MB'},
 {poRel:'72037',scheduleDate:'2026-10-16',location:'PROSPY',unloadingDoor:'BD'}];
 const db={from(){const q={select(){return q},order(){return q},async range(){calls++;return {data:rows}}};return q}};
 assert.equal(await lookupRelease(db,'72037',{facility_id:'csp-toledo-main'},'2026-10-13T15:06:21Z'),'matched');
 assert.equal(await lookupRelease(db,'72039',{facility_id:'csp-toledo-main'},'2026-10-13T15:05:26Z'),'invalid');
 assert.equal(calls,1);
 const match=require('./release-doors').doorMatches(await releaseCatalog(db),['72037'],'2026-10-13')['72037:PROSPY'];
 assert.equal(match.door,'BD');
});
