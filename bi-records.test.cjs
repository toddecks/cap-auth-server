const test=require('node:test');const assert=require('node:assert/strict');
const {handler,allowedMetrics,dashboardRoles}=require('./bi-records');
const response=()=>({statusCode:200,status(n){this.statusCode=n;return this;},set(){return this;},json(v){this.body=v;return this;}});
test('Production, Shipping, Receiving and Admin can see all records',()=>{
 for(const role of dashboardRoles)assert.deepEqual(allowedMetrics([role]),['shipping','production','shift_reports','appointments','orders','receiving']);
 for(const role of ['inventory','customer_summary','bonus_report','iso','alarm_logs','website_leads','quote_calculator','work_order_pricing','todd_requests','shipping_app','shipping','employee','pickem'])assert.deepEqual(allowedMetrics([role]),[]);
});
test('rejects bad filters and unauthorized roles before accessing data',async()=>{
 for(const req of [{query:{period:'week'},authRoles:['admin']},{query:{year:'2026.5'},authRoles:['admin']},{query:{},authRoles:[]}]){
  const res=response();await handler({rpc(){throw new Error('must not query');}})(req,res);assert.ok([400,403].includes(res.statusCode));
 }
});
test('server authorizes all record metrics from trusted dashboard roles',async()=>{
 const res=response();await handler({rpc:async(name,args)=>{
  assert.equal(name,'get_bi_record_rankings');assert.deepEqual(args,{p_period:'month',p_scope:'people',p_year:2026,p_metrics:allowedMetrics(['production'])});
  return {data:{cards:[]}};
 }})({query:{period:'month',scope:'people',year:'2026',metrics:'orders',version:'2'},authRoles:['production']},res);
 assert.equal(res.statusCode,200);assert.deepEqual(res.body.allowedMetrics,allowedMetrics(['production']));
});
test('failed data reads are errors rather than empty records',async()=>{
 const res=response();await handler({rpc:async()=>({error:{code:'57014'}})})({query:{},authRoles:['admin']},res);assert.equal(res.statusCode,503);
});
test('older open Records pages keep receiving categories they can render',async()=>{
 const res=response();await handler({rpc:async(name,args)=>{
  assert.ok(!args.p_metrics.includes('shift_reports'));return {data:{cards:[]}};
 }})({query:{},authRoles:['production']},res);
 assert.equal(res.statusCode,200);assert.ok(!res.body.allowedMetrics.includes('shift_reports'));
});
