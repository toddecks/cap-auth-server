const {test}=require('node:test');const assert=require('node:assert/strict');
const {parseInput,createHandler}=require('./manual-arrival');
const body={driverName:'Test Driver',driverCompany:'Test Carrier',phone:'419-555-0123',releaseNumber:'REL1'};
function setup(role='shipping',active=false){
 const writes=[];let found=0;
 const db={auth:{getUser:async()=>({data:{user:{id:'staff',app_metadata:{csp_role:role}}}})},from(table){let op='read';const q={select(){return q},eq(){return q},is(){return q},limit(){return Promise.resolve({data:active?[{id:1}]:[]})},upsert(v){op='write';writes.push([table,v]);return q},update(v){op='write';writes.push([table,v]);return q},insert(v){op='write';writes.push([table,v]);return q},single(){return Promise.resolve({data:{id:3}})},then(ok){return Promise.resolve({data:null}).then(ok)}};return q}};
 const handler=createHandler({db,findOrCreateConversation:async()=>{found++;return {id:'conversation'}}});
 const res={code:200,set(){return this},status(n){this.code=n;return this},json(v){this.body=v;return this}};
 return {handler,res,writes,get found(){return found}};
}
test('normalizes phone and rejects missing or oversized details',()=>{assert.equal(parseInput(body).phone,'+14195550123');for(const b of [{...body,phone:'bad'},{...body,driverName:''},{...body,releaseNumber:'x'.repeat(101)}])assert.throws(()=>parseInput(b));});
test('requires a staff session',async()=>{let s=setup();await s.handler({headers:{},body},s.res);assert.equal(s.res.code,401);assert.equal(s.writes.length,0)});
test('denies driver role before database changes',async()=>{let s=setup('driver');await s.handler({headers:{authorization:'Bearer token'},body},s.res);assert.equal(s.res.code,403);assert.equal(s.writes.length,0)});
test('does not overwrite a current arrival',async()=>{let s=setup('shipping',true);await s.handler({headers:{authorization:'Bearer token'},body},s.res);assert.equal(s.res.code,409);assert.equal(s.writes.length,0);assert.equal(s.found,0)});
test('creates arrival with staff attribution without check-in or outgoing message',async()=>{let s=setup();await s.handler({headers:{authorization:'Bearer token'},body},s.res);assert.equal(s.res.code,200);assert.equal(s.res.body.arrivalId,3);assert.deepEqual(s.writes.map(x=>x[0]),['driver_sms_contacts','driver_conversations','driver_sms_arrivals']);assert.equal(s.writes[2][1].added_by,'staff');assert.equal(s.writes[2][1].visit_type,'pickup')});
