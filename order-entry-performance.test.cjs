const test=require('node:test');
const assert=require('node:assert/strict');
const {handler,dateOnly}=require('./order-entry-performance');
const {normalize,config}=require('./scripts/sync-azure-order-entries.cjs');
function response(){return {statusCode:200,status(n){this.statusCode=n;return this;},set(){return this;},json(body){this.body=body;return this;}};}
function database({run={id:'run-one',finished_at:'2026-11-17T20:00:00Z',source_start_date:'2025-01-01',source_max_date:'2026-11-16'},pages=[[]],error=null}={}){
 const calls=[];
 return {calls,from(table){const chain={};for(const method of ['select','eq','gte','lte','order','limit','range'])chain[method]=(...args)=>{calls.push([table,method,...args]);return chain;};chain.then=(resolve,reject)=>Promise.resolve(table.endsWith('sync_runs')?{data:run?[run]:[],error}:{data:pages.shift()||[],error}).then(resolve,reject);return chain;}};
}
test('report uses latest successful run and loads every page',async()=>{
 const first=Array.from({length:1000},(_,i)=>({setup_number:i+1,entered_by:'Amanda',entered_date:'2026-11-16'}));
 const db=database({pages:[first,[{setup_number:1001,entered_by:'Janet',entered_date:'2026-11-16'}]]});
 const res=response();await handler(db)({query:{start:'2026-11-01',end:'2026-11-17'}},res);
 assert.equal(res.statusCode,200);assert.equal(res.body.rows.length,1001);assert.equal(res.body.sourceThrough,'2026-11-16');
 assert.ok(db.calls.some(c=>c[1]==='eq'&&c[2]==='status'&&c[3]==='complete'));
 assert.equal(db.calls.filter(c=>c[1]==='eq'&&c[2]==='sync_run_id'&&c[3]==='run-one').length,2);
});
test('rejects invalid ranges before querying data',async()=>{
 for(const q of [{start:'2026-02-31',end:'2026-03-03'},{start:'2026-11-17',end:'2026-11-16'},{start:'2024-01-01',end:'2026-11-17'}]){
  const db=database(),res=response();await handler(db)({query:q},res);assert.equal(res.statusCode,400);assert.equal(db.calls.length,0);
 }
 assert.equal(dateOnly('2026-02-31'),false);
});
test('does not display first refresh failure as zero orders',async()=>{
 const res=response();await handler(database({run:null}))({query:{start:'2026-11-01',end:'2026-11-17'}},res);assert.equal(res.statusCode,503);assert.match(res.body.error,/not completed/);
});
test('normalization retains header attribution and rejects duplicate setups',()=>{
 const row={setup_number:73215,entered_date:'2026-11-17',entered_by:' Amanda  Schonter ',source_input_date:82504};
 assert.equal(normalize([row])[0].entered_by,'Amanda Schonter');assert.throws(()=>normalize([row,row]),/Duplicate/);
 assert.throws(()=>normalize([{...row,entered_date:'2026-02-31'}]),/Invalid/);
});
test('Azure connection keeps encryption and certificate verification enabled',()=>{
 const c=config({PSDATA_SQL_USER:'test',PSDATA_SQL_PASSWORD:'test'});
 assert.equal(c.options.encrypt,true);assert.equal(c.options.trustServerCertificate,false);assert.equal(c.options.readOnlyIntent,true);
});
