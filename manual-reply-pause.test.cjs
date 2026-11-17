const {test}=require('node:test'),assert=require('node:assert/strict');
const pause=require('./manual-reply-pause');
const c={id:'12345678-1234-1234-1234-123456789abc',status:'open',session_started_at:'2026-11-16T12:00:00Z'};
const message=(patch={})=>({sender_user_id:'clerk',direction:'shipping_to_driver',sent_at:'2026-11-16T12:01:00Z',delivery_status:'sent',...patch});
test('sent clerk reply persists the pause across module reload, current visit only',()=>{
 assert(pause.paused(c,[message()]));delete require.cache[require.resolve('./manual-reply-pause')];
 assert(require('./manual-reply-pause').paused(c,[message()]));
 assert(!pause.paused({...c,session_started_at:'2026-11-17T12:00:00Z'},[message()]));
 assert(!pause.paused(c,[message({sender_user_id:null})]));
 assert(!pause.paused(c,[message({client_message_id:'manual-review:123'})]));
});
test('only a current official Check In resumes automation; old check-in and driver command cannot',()=>{
 pause.begin(c);assert(pause.paused(c,[]));
 assert(pause.paused(c,[],{checked_in_at:'2026-11-15T12:00:00Z'}));
 assert(pause.paused(c,[{direction:'driver_to_shipping',body:'Pick up'}]));
 assert(!pause.paused(c,[message()],{checked_in_at:'2026-11-16T12:02:00Z'}));
 pause.clear(c);assert(!pause.paused(c,[]));
});
function db(role='shipping'){
 return{auth:{getUser:async()=>({data:{user:{id:'staff',app_metadata:{csp_role:role}}}})},from(){const q={select(){return q},eq(){return q},maybeSingle:async()=>({data:c})};return q;}};
}
async function request(database,authorization='Bearer staff'){
 let status=200,payload;await pause.handler(database)({headers:{authorization},body:{conversationId:c.id}},{set(){},status(n){status=n;return this},json(p){payload=p;return this}});return{status,payload};
}
test('typing endpoint requires authenticated Shipping access before creating a draft hold',async()=>{
 pause.clear(c);assert.equal((await request(db(),'')).status,401);assert(!pause.paused(c,[]));
 assert.equal((await request(db('driver'))).status,403);assert(!pause.paused(c,[]));
 assert.equal((await request(db())).status,200);assert(pause.paused(c,[]));pause.clear(c);
});
test('the live inbound SMS reply path checks the same hold before calling Twilio',async()=>{
 const fs=require('node:fs'),vm=require('node:vm');const source=fs.readFileSync(require.resolve('./server.js'),'utf8');
 const a=source.indexOf('const sendAutomatedSmsReply = async'),b=source.indexOf('\nconst MMS_MAX_BYTES',a);
 let paused=true;const sends=[],stored=[];
 const ctx={require:name=>{if(name==='./sms-translation')return{forConversation:async(_db,_id,body)=>({body,patch:{}})};assert.equal(name,'./manual-reply-pause');return{load:async()=>paused}},driverSupabase:{from:()=>({insert:async row=>{stored.push(row);return{}}})},twilioClient:{messages:{create:async row=>{sends.push(row);return{sid:'SM-test',status:'sent'}}}},TWILIO_MESSAGING_SERVICE_SID:'test',TWILIO_PUBLIC_BASE_URL:'https://test.invalid',mapTwilioDeliveryStatus:s=>s,scheduleTwilioStatusSync:()=>{}};
 vm.createContext(ctx);vm.runInContext(source.slice(a,b)+';globalThis.reply=sendAutomatedSmsReply;',ctx);
 await ctx.reply({phone:'+15555550100',conversationId:c.id,body:'Please TEXT: Drop Off, Pick Up, or Both'});
 assert.equal(sends.length,0);assert.equal(stored.length,0);
 paused=false;await ctx.reply({phone:'+15555550100',conversationId:c.id,body:'Normal flow'});
 assert.equal(sends.length,1);assert.equal(stored.length,1);
});
