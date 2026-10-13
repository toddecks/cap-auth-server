const {test}=require('node:test'),assert=require('node:assert/strict');
const {releaseCatalog}=require('./release-catalog');
const {lookupRelease}=require('./release-attempts');
test('future valid release and yearly duplicates use newest record without a one-day cutoff',async()=>{
 let calls=0;
 const rows=[{poRel:'72037',scheduleDate:'2025-10-13',location:'PROSPY',unloadingDoor:'MB'},
 {poRel:'72037',scheduleDate:'2026-10-16',location:'PROSPY',unloadingDoor:'BD'}];
 const db={from(){const q={select(){return q},ilike(){return q},order(){return q},async range(){calls++;return {data:rows}}};return q}};
 assert.equal(await lookupRelease(db,'72037',{facility_id:'csp-toledo-main'},'2026-10-13T15:06:21Z'),'matched');
 assert.equal(await lookupRelease(db,'72039',{facility_id:'csp-toledo-main'},'2026-10-13T15:05:26Z'),'invalid');
 assert.equal(calls,2);
 const match=require('./release-doors').doorMatches(await releaseCatalog(db),['72037'],'2026-10-13')['72037:PROSPY'];
 assert.equal(match.door,'BD');
});
test('catalog pages load concurrently and preserve complete ordered results',async()=>{
 let active=0,peak=0,calls=0;
 const db={from(){const q={select(){return q},ilike(){return q},order(){return q},async range(start,end){
  calls++;active++;peak=Math.max(peak,active);
  await new Promise(resolve=>setImmediate(resolve));active--;
  return {data:Array.from({length:Math.max(0,Math.min(end+1,30822)-start)},(_,i)=>({bolNumber:start+i}))};
 }};return q}};
 const [a,b]=await Promise.all([releaseCatalog(db),releaseCatalog(db)]);
 assert.equal(a,b,'concurrent callers share the refresh');assert.equal(a.length,30822);
 assert.deepEqual(a.map(r=>r.bolNumber),Array.from({length:30822},(_,i)=>i));
 assert.equal(peak,8);assert.equal(calls,33);
});
test('slow in-flight refresh is reused even past TTL and failures are retried',async()=>{
 let resolve,calls=0,fail=true;
 const db={from(){const q={select(){return q},ilike(){return q},order(){return q},range(){calls++;return new Promise(r=>{resolve=r})}};return q}};
 const a=releaseCatalog(db,0),b=releaseCatalog(db,61000);
 assert.equal(calls,1);resolve({error:Error('lookup unavailable')});
 const errors=await Promise.allSettled([a,b]);assert(errors.every(r=>r.status==='rejected'));
 const retry=releaseCatalog(db,62000);assert.equal(calls,2);resolve({data:[{bolNumber:1}]});
 assert.deepEqual(await retry,[{bolNumber:1}]);
});
test('targeted lookup filters on server and preserves shorthand candidates for exact validation',async()=>{
 const rows=[{poRel:'968751/510',location:'PROSPY',scheduleDate:'2026-10-13',unloadingDoor:'COIL'},{poRel:'945678',location:'PROSPY',scheduleDate:'2024-04-08'}];
 let pattern;
 const db={from(){const q={select(){return q},ilike(column,value){assert.equal(column,'poRel');pattern=value;return q},order(){return q},range:async()=>({data:rows.filter(r=>new RegExp('^'+pattern.replaceAll('%','.*')+'$','i').test(r.poRel))})};return q}};
 const {releaseCandidates}=require('./release-catalog');
 const matches=await releaseCandidates(db,'968510');assert.equal(matches.length,1);
 assert.equal(require('./release-doors').doorMatches(matches,['968510'],'2026-10-13')['968510:PROSPY'].door,'COIL');
 assert.equal(await lookupRelease(db,'45678',{facility_id:'csp-toledo-main'},'2026-10-13'),'invalid');
});
test('shipping screen queries only unique requested numbers and merges overlapping candidates',async()=>{
 const patterns=[];const row={poRel:'232662-coil'};
 const db={from(){const q={select(){return q},ilike(_,p){patterns.push(p);return q},order(){return q},range:async()=>({data:[row]})};return q}};
 const rows=await require('./release-catalog').releaseCandidatesForMany(db,['232662','232662-coil','967833']);
 assert.equal(patterns.length,2);assert.deepEqual(rows,[row]);
 assert.deepEqual(await require('./release-catalog').releaseCandidatesForMany(db,[]),[]);
});
