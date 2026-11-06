const {test}=require('node:test'),assert=require('node:assert/strict');
const {createHandlers,createStore}=require('./release-verification-hold');
const id='12345678-1234-4234-8234-123456789abc';
function fixture({role='shipping',newer=false,closed=false,stale=false}={}){
 const conversation={id,status:closed?'closed':'open',session_ended_at:null,created_at:'2026-11-06T15:00:00Z',release_number:'CB7201P1-2/7'};
 const hold={conversation_id:id,session_key:stale?'old-session':id,attempt_id:'1',release_number:conversation.release_number,requested_at:'2026-11-06T15:01:00Z',verified_at:null,verified_by:null};
 const tables={driver_conversations:[conversation],shipping_release_verifications:[hold],shipping_staff_checkins:[],driver_messages:[{id:'1',conversation_id:id,direction:'driver_to_shipping',body:conversation.release_number,sent_at:hold.requested_at},...(newer?[{id:'2',conversation_id:id,direction:'driver_to_shipping',body:'1234567',sent_at:'2026-11-06T15:02:00Z'}]:[])]};
 const db={auth:{getUser:async()=>({data:{user:{id:'clerk',app_metadata:{csp_role:role}}}})},from(table){let filters=[],operation='select',value,single=false;const q={select(){return q},order(){return q},limit(){return q},eq(k,v){filters.push(r=>r[k]===v);return q},is(k,v){filters.push(r=>r[k]===v);return q},in(k,values){filters.push(r=>values.includes(r[k]));return q},gte(k,v){filters.push(r=>r[k]>=v);return q},single(){single=true;return q},maybeSingle(){single=true;return q},update(v){operation='update';value=v;return q},then(fn){const rows=tables[table].filter(r=>filters.every(f=>f(r)));if(operation==='update')rows.forEach(r=>Object.assign(r,value));return Promise.resolve({data:single?rows[0]||null:rows}).then(fn)}};return q;}};
 let resumed=0;const handlers=createHandlers({db,onVerified:()=>resumed++});
 async function call(kind,body={conversationId:id,attemptId:'1',verified:true},token='test'){let status=200,payload;await handlers[kind]({headers:{authorization:token?'Bearer '+token:''},body},{set(){},status(s){status=s;return this},json(p){payload=p;return this}});await Promise.resolve();return{status,payload};}
 return{tables,hold,conversation,call,get resumed(){return resumed},db};
}
test('only Shipping/admin may verify a release, with explicit acknowledgement',async()=>{
 const f=fixture({role:'driver'});assert.equal((await f.call('verify')).status,403);assert.equal(f.resumed,0);assert.equal(f.hold.verified_at,null);
 const staff=fixture();assert.equal((await staff.call('verify',{conversationId:id,attemptId:'1'})).status,400);assert.equal((await staff.call('verify',undefined,'')).status,401);
});
test('verification is durable, attributed to clerk, and idempotent',async()=>{
 const f=fixture();const response=await f.call('verify');assert.equal(response.status,200);assert.ok(f.hold.verified_at);assert.equal(f.hold.verified_by,'clerk');assert.equal(f.resumed,1);
 await f.call('verify');assert.equal(f.resumed,1);assert.equal((await createStore(f.db).get(f.conversation)).verified_by,'clerk');
});
for(const options of [{closed:true},{stale:true},{newer:true}])test('refuses stale, closed, or superseded release: '+JSON.stringify(options),async()=>{const f=fixture(options);assert.equal((await f.call('verify')).status,409);assert.equal(f.hold.verified_at,null);assert.equal(f.resumed,0);});
test('list hides old sessions, ended visits, and checked-in drivers',async()=>{
 const f=fixture();assert.equal((await f.call('list')).payload.verifications.length,1);
 f.tables.shipping_staff_checkins.push({conversation_id:id,checked_in_at:'2026-11-06T15:05:00Z'});assert.equal((await f.call('list')).payload.verifications.length,0);
});
