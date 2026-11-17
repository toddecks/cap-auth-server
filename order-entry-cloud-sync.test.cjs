const test=require('node:test');const assert=require('node:assert/strict');
const {handler,jobKey}=require('./order-entry-cloud-sync');
const response=()=>({statusCode:200,status(n){this.statusCode=n;return this;},json(v){this.body=v;return this;}});
const request=(token,body={sqlUser:'test-user',sqlPassword:'test-password'})=>({get:()=>token,body});
test('rejects unauthorized refreshes before opening a database connection',async()=>{
 let called=false;const res=response();await handler({url:'https://test',key:'secret',run:async()=>{called=true;}})(request('é'.repeat(64)),res);
 assert.equal(res.statusCode,401);assert.equal(called,false);
});
test('scheduled refresh uses the fixed Azure destination and returns counts only',async()=>{
 const res=response();await handler({url:'https://test',key:'secret',run:async env=>{
  assert.equal(env.PSDATA_SQL_SERVER,'psdata-azure.database.windows.net');assert.equal(env.PSDATA_SQL_USER,'test-user');
  return {rows:4,sourceThrough:'2026-11-16'};
 }})(request(jobKey('secret')),res);
 assert.deepEqual(res.body,{ok:true,rows:4,sourceThrough:'2026-11-16'});
});
test('firewall diagnostics never return credentials or provider error text',async()=>{
 const res=response();await handler({url:'https://test',key:'secret',run:async()=>{throw Object.assign(new Error("Client with IP address '1.2.3.4' is not allowed. test-password"),{code:'ELOGIN'});}})(request(jobKey('secret')),res);
 assert.equal(res.statusCode,502);assert.equal(res.body.blockedCloudIp,'1.2.3.4');assert.equal(JSON.stringify(res.body).includes('test-password'),false);
});
