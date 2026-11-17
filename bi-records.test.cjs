const test=require('node:test');const assert=require('node:assert/strict');
const {handler,allowedMetrics}=require('./bi-records');
const response=()=>({statusCode:200,status(n){this.statusCode=n;return this;},set(){return this;},json(v){this.body=v;return this;}});
test('records use existing report permissions without exposing other departments',()=>{
 assert.deepEqual(allowedMetrics(['production']),['production']);
 assert.deepEqual(allowedMetrics(['shipping_overview']),['shipping']);
 assert.deepEqual(allowedMetrics(['receiving']),['appointments','receiving']);
 assert.equal(allowedMetrics(['admin']).length,5);assert.equal(allowedMetrics(['shipping']).length,0);
});
test('rejects bad filters and unauthorized roles before accessing data',async()=>{
 for(const req of [{query:{period:'week'},authRoles:['admin']},{query:{year:'2026.5'},authRoles:['admin']},{query:{},authRoles:[]}]){
  const res=response();await handler({rpc(){throw new Error('must not query');}})(req,res);assert.ok([400,403].includes(res.statusCode));
 }
});
test('service RPC receives only metrics allowed by trusted roles',async()=>{
 const res=response();await handler({rpc:async(name,args)=>{
  assert.equal(name,'get_bi_record_rankings');assert.deepEqual(args,{p_period:'month',p_scope:'people',p_year:2026,p_metrics:['production']});
  return {data:{cards:[]}};
 }})({query:{period:'month',scope:'people',year:'2026',metrics:'orders'},authRoles:['production']},res);
 assert.equal(res.statusCode,200);assert.deepEqual(res.body.allowedMetrics,['production']);
});
test('failed data reads are errors rather than empty records',async()=>{
 const res=response();await handler({rpc:async()=>({error:{code:'57014'}})})({query:{},authRoles:['admin']},res);assert.equal(res.statusCode,503);
});
