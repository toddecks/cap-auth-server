const {test}=require('node:test'),assert=require('node:assert/strict');
const {carrierFromRelease}=require('./release-carrier');
const row={poRel:'72967',scheduleDate:'2026-11-19',scheduleTime:'10:00:00',location:'PROSPY',unloadingDoor:'BD',carrierName:'Ace Doran'};
const c={release_number:'72967',facility_id:'csp-toledo-main'};
const at='2026-11-19T13:00:00Z';
function chart(rows){return {from(){const q={select(){return q},or(){return q},ilike(){return q},order(){return q},range:async()=>({data:rows})};return q}};}
test('missing carrier filled from confirmed release and merged driver name repaired',async()=>{
 assert.deepEqual(await carrierFromRelease(chart([row]),c,{full_name:'Josue ace Doran'},at),{driver_company:'Ace Doran',full_name:'Josue'});
 assert.deepEqual(await carrierFromRelease(chart([row]),c,{full_name:'Josue'},at),{driver_company:'Ace Doran'});
});
test('preserves driver supplied carrier without querying PS',async()=>{
 assert.equal(await carrierFromRelease({from(){throw Error('should not query')}},c,{driver_company:'Another Hauler'},at),null);
});
test('multiple releases use first supplied release carrier',async()=>{
 const rows=[row,{...row,poRel:'72968',carrierName:'Other Carrier'}];
 assert.deepEqual(await carrierFromRelease(chart(rows),{...c,release_number:'72967/72968'},{full_name:'Josue'},at),{driver_company:'Ace Doran'});
});
test('does not fill carrier for missing, cancelled, ambiguous or conflicting matches',async()=>{
 for(const rows of [[],[{...row,cancelLoad:true}],[row,{...row,scheduleTime:'14:00:00'}],[row,{...row,carrierName:'Other Carrier'}],[{...row,carrierName:''}],[row,{...row,carrierName:''}],[{...row,location:'ENTPRS'}]]){
  assert.equal(await carrierFromRelease(chart(rows),c,{full_name:'Josue'},at),null);
 }
});
