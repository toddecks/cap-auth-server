const {test}=require('node:test'),assert=require('node:assert/strict');
const {register}=require('./permanent-conversations');
const id='12345678-1234-1234-1234-123456789abc';
function fixture({role='shipping',status='closed',storageFailure=false,dbFailure=false}={}){
 const calls=[],removed=[];let handler;
 const db={auth:{getUser:async()=>({data:{user:{app_metadata:{csp_role:role}}}})},storage:{from:()=>({remove:async paths=>{removed.push(...paths);return storageFailure?{error:Error('storage offline')}:{}}})},rpc:async(name,args)=>{calls.push([name,args]);return dbFailure?{error:Error('database offline')}:status!=='closed'?{error:{code:'22023',message:'Only closed conversations can be permanently deleted.'}}:{data:{deleted:true,attachmentPaths:[`sms/555/${id}/image.jpg`,'someone-else/shared.jpg']}}},from(table){const q={select(){return q},not(){return q},limit:async()=>({data:[]}),update(v){calls.push([table,v]);return q},eq(){return q},then(f){return Promise.resolve({data:[]}).then(f)}};return q}};
 const feature=register({app:{post(path,fn){handler=fn;}},db});feature.stop();
 async function call(body={conversationId:id},token='test'){const res={code:200,set(){},status(code){this.code=code;return this},json(body){this.body=body;return this}};await handler({headers:{authorization:token?'Bearer '+token:''},body},res);return res;}
 return{call,calls,removed,feature};
}
test('shipping clerk permanently purges a closed thread and its own attachments',async()=>{const f=fixture();const r=await f.call();assert.equal(r.code,200);assert.equal(r.body.ok,true);assert.deepEqual(f.removed,[`sms/555/${id}/image.jpg`]);assert.equal(f.calls[0][0],'shipping_purge_closed_conversation');assert(!f.calls.some(x=>/arrivals|checkins/.test(x[0])));});
test('open conversations are refused by atomic purge operation',async()=>{const f=fixture({status:'open'});assert.equal((await f.call()).code,409);assert.equal(f.removed.length,0);});
test('signed-out, driver role, malformed identifier never purge',async()=>{for(const opts of [{role:'driver'},{}]){const f=fixture(opts);assert.equal((await f.call({conversationId:id},opts.role?'token':'')).code,opts.role?403:401);assert.equal(f.calls.length,0);}const f=fixture();assert.equal((await f.call({conversationId:'invalid'})).code,400);assert.equal(f.calls.length,0);});
test('database failure preserves attachments; storage failure is queued without restoring messages',async()=>{let f=fixture({dbFailure:true});assert.equal((await f.call()).code,500);assert.equal(f.removed.length,0);f=fixture({storageFailure:true});const r=await f.call();assert.equal(r.code,200);assert.match(r.body.warning,/queued/);assert(!f.calls.some(x=>x[1]?.purged_attachment_paths===null));});
