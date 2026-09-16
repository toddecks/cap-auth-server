const {test}=require('node:test');const assert=require('node:assert/strict');const fs=require('node:fs');const vm=require('node:vm');
const src=fs.readFileSync(__dirname+'/server.js','utf8');const route=src.slice(src.indexOf('app.post("/api/shipping/send-message"'),src.indexOf('app.post("/api/shipping/send-photo"'));
async function run({review=true,status='closed',role='shipping',duplicate=false}={}){
 let handler,sends=0,inserted;const db={auth:{getUser:async()=>({data:{user:{id:'staff',app_metadata:{csp_role:role}}}})},from(table){const q={select(){return q},eq(){return q},update(){return q},insert(row){inserted=row;return q},single:async()=>duplicate?{error:{code:'23505'}}:{data:{id:'message'}},maybeSingle:async()=>({data:table==='driver_conversations'?{id:'thread',status,channel:'sms',sms_phone_e164:'mock-phone'}:{id:'existing',delivery_status:'sent'}}),then(resolve){return Promise.resolve({}).then(resolve)}};return q}};
 vm.runInNewContext(route,{app:{post:(path,fn)=>handler=fn},driverSupabase:db,getBearerToken:()=> 'token',require,crypto:require('node:crypto'),twilioClient:{messages:{create:async args=>{sends++;assert.equal(args.body,require('./driver-reviews').REVIEW_BODY);return {sid:'mock',status:'sent'}}}},TWILIO_MESSAGING_SERVICE_SID:'mock',TWILIO_PUBLIC_BASE_URL:'https://example.com',mapTwilioDeliveryStatus:x=>x,scheduleTwilioStatusSync:()=>{},console});
 const res={code:200,set(){},status(n){this.code=n;return this},json(v){this.body=v;return this}};
 await handler({body:{conversationId:'12345678-1234-4123-8123-123456789012',reviewRequest:review,body:'ignored for review'}},res);return {res,sends,inserted};
}
test('manual review sends on closed chat with fixed text',async()=>{const x=await run();assert.equal(x.res.code,200);assert.equal(x.sends,1);assert.match(x.inserted.client_message_id,/^manual-review:/)});
test('ordinary messages still reject closed chat',async()=>{const x=await run({review:false});assert.equal(x.res.code,404);assert.equal(x.sends,0)});
test('duplicate review never sends another SMS',async()=>{const x=await run({duplicate:true});assert.equal(x.sends,0);assert.equal(x.res.body.alreadySent,true)});
test('nonstaff cannot send reviews',async()=>{const x=await run({role:'driver'});assert.equal(x.res.code,403);assert.equal(x.sends,0)});
test('automatic worker stays stopped',()=>assert.ok(!src.includes('driverReviewWorker.start(')));
