const{test}=require('node:test'),assert=require('node:assert/strict'),{register}=require('./shipping-ownership');
function harness(role,owner){let passed=false,status=200;const user={id:'clerk',app_metadata:{csp_role:role}};const db={auth:{getUser:async()=>({data:{user}})},from(){return{select(){return this},eq(){return this},maybeSingle:async()=>({data:owner?{assigned_to:owner}:null})}}};const {middleware}=register({app:{use(){}},db,getBearerToken:()=> 'token'});return{run:async()=>{await middleware({path:'/send-message',method:'POST',body:{conversationId:'conversation'}},{status(s){status=s;return this},json(){}},()=>{passed=true});return{passed,status}}};}
test('assigned clerk may send',async()=>assert.deepEqual(await harness('shipping','clerk').run(),{passed:true,status:200}));
test('another clerk cannot send',async()=>assert.deepEqual(await harness('shipping','someone-else').run(),{passed:false,status:403}));
test('unassigned conversation must be claimed',async()=>assert.deepEqual(await harness('shipping',null).run(),{passed:false,status:403}));
test('admin may manage every conversation',async()=>assert.deepEqual(await harness('admin','someone-else').run(),{passed:true,status:200}));
test('driver cannot use staff message endpoint',async()=>assert.deepEqual(await harness('driver',null).run(),{passed:false,status:403}));
