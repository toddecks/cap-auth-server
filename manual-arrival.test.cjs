const {test}=require('node:test');const assert=require('node:assert/strict');
const {parseInput,createHandler}=require('./manual-arrival');
const at='2026-11-10T15:00:00.000Z';
const body={driverName:'Test Driver',driverCompany:'Test Carrier',phone:'419-555-0123',releaseNumber:'968529'};
const load={poRel:'968529/969082/969083-coil',location:'PROSPY',scheduleDate:'2026-11-10',scheduleTime:'11:00:00',unloadingDoor:'COIL',customerNo:'CENTER'};
function setup({role='shipping',active=null,checkin=null,loads=[load],lookupError=false,checkinError=false,ended=false,closed=false,sessionStarted=null,startAt=at,messageFailure=false}={}){
 const tables={driver_sms_arrivals:active?[{phone_e164:'+14195550123',facility_id:'csp-toledo-main',conversation_id:'conversation',departed_at:null,entered_at:at,...active}]:[],driver_conversations:[{id:'conversation',status:closed?'closed':'open',created_at:'2026-11-09T15:00:00Z',updated_at:'2026-11-09T16:00:00Z',session_started_at:sessionStarted,session_ended_at:ended?'2026-11-09T16:00:00Z':null}],driver_sms_contacts:[],shipping_staff_checkins:checkin?[{conversation_id:'conversation',checked_in_at:checkin}]:[]};
 const writes=[],reads=[],notifications=[];let found=0,command;
 const db={auth:{getUser:async()=>({data:{user:{id:'staff',app_metadata:{csp_role:role}}}})},from(table){reads.push(table);let op='read',value,one=false,limit=Infinity,filters=[];
  const q={select(){return q},order(){return q},eq(k,v){filters.push(r=>r[k]===v);return q},is(k,v){filters.push(r=>(r[k]??null)===v);return q},limit(n){limit=n;return q},upsert(v){op='upsert';value=v;return q},update(v){op='update';value=v;return q},insert(v){op='insert';value=v;return q},single(){one=true;return q},maybeSingle(){one=true;return q},then(resolve,reject){return Promise.resolve().then(()=>{
   if(!tables[table])throw Error('Unexpected table '+table);
   if(op!=='read'){
    writes.push([table,op,value]);
    if(table==='shipping_staff_checkins'&&checkinError)return {error:{message:'Check-in write failed'}};
    if(op==='insert')tables[table].push({id:3,...value});
    else if(op==='upsert'){const key=table==='driver_sms_contacts'?'phone_e164':'conversation_id';const row=tables[table].find(r=>r[key]===value[key]);if(row)Object.assign(row,value);else tables[table].push({...value});}
    else tables[table].filter(r=>filters.every(f=>f(r))).forEach(r=>Object.assign(r,value));
   }
   const rows=op==='insert'?[tables[table].at(-1)]:tables[table].filter(r=>filters.every(f=>f(r))).slice(0,limit);return {data:one?rows[0]||null:rows};
  }).then(resolve,reject)}};return q}};
 const chartDb={from(table){assert.equal(table,'psdata_loads_api');const q={select(){return q},ilike(){return q},order(){return q},range:async()=>lookupError?{error:{message:'Lookup offline'}}:{data:loads}};return q}};
 const handler=createHandler({db,chartDb,now:()=>at,sendInstructions:async args=>{assert(tables.shipping_staff_checkins.some(r=>r.conversation_id===args.conversation.id),'Check-in must precede messages');notifications.push(args);if(messageFailure)throw Error('SMS provider unavailable');},findOrCreateConversation:async(phone,body)=>{found++;command=body;const previous=tables.driver_conversations[0];if(previous.status!=='open'){tables.driver_conversations.push({id:'new-conversation',status:'open',created_at:at,session_started_at:at,session_ended_at:null});return {id:'new-conversation'}}if(previous.session_ended_at){previous.session_started_at=startAt;previous.session_ended_at=null;}return {id:'conversation'}}});
 const call=async(input=body,headers={authorization:'Bearer token'})=>{const res={code:200,set(){return this},status(n){this.code=n;return this},json(v){this.body=v;return this}};await handler({headers,body:input},res);return res;};
 return {call,tables,writes,reads,notifications,get found(){return found},get command(){return command}};
}
test('normalizes phone and rejects missing or oversized details',()=>{assert.equal(parseInput(body).phone,'+14195550123');for(const b of [{...body,phone:'bad'},{...body,driverName:''},{...body,releaseNumber:'x'.repeat(101)}])assert.throws(()=>parseInput(b));});
test('requires a staff session',async()=>{const f=setup();assert.equal((await f.call(body,{})).code,401);assert.equal(f.writes.length,0)});
test('denies drivers before database reads or changes',async()=>{const f=setup({role:'driver'});assert.equal((await f.call()).code,403);assert.equal(f.writes.length,0);assert.equal(f.reads.length,0)});
test('manual add creates an officially checked-in arrival then invokes shared instructions',async()=>{
 const f=setup(),r=await f.call();assert.equal(r.code,200);assert.equal(r.body.arrivalId,3);assert.equal(r.body.checkedInAt,at);
 assert.equal(r.body.appointment.door,'COIL');assert.equal(r.body.appointment.customer,'Center Steel Sales, Inc.');assert.equal(r.body.appointment.appointment_at,'2026-11-10T11:00:00');assert.equal(r.body.appointment.release_reference,load.poRel);
 assert.equal(f.tables.driver_sms_arrivals[0].checked_in_at,at);assert.equal(f.tables.shipping_staff_checkins[0].checked_in_at,at);assert.equal(f.tables.shipping_staff_checkins[0].checked_in_by,'staff');assert.equal(f.command,'PICK UP');
 assert.deepEqual(f.writes.map(x=>x[0]),['driver_sms_contacts','driver_sms_arrivals','shipping_staff_checkins','driver_conversations']);
});
test('repairs hidden unchecked arrival without a new arrival or conversation',async()=>{
 const f=setup({active:{id:8,release_number:'111111',checked_in_at:null}}),r=await f.call();assert.equal(r.code,200);assert.equal(r.body.recovered,true);assert.equal(r.body.arrivalId,8);assert.equal(f.found,0);assert.equal(f.tables.driver_sms_arrivals.length,1);assert.equal(f.tables.driver_sms_arrivals[0].release_number,body.releaseNumber);assert.equal(f.tables.shipping_staff_checkins[0].checked_in_at,at);
});
test('retry reuses checked-in arrival without moving its time or duplicating it',async()=>{
 const f=setup();await f.call();const writes=f.writes.length;const r=await f.call();assert.equal(r.code,200);assert.equal(r.body.alreadyAdded,true);assert.equal(f.writes.length,writes);assert.equal(f.tables.driver_sms_arrivals.length,1);assert.equal(f.found,1);
});
test('different release cannot overwrite a checked-in driver',async()=>{
 const f=setup({active:{id:8,release_number:'999999',checked_in_at:at},checkin:at}),r=await f.call();assert.equal(r.code,409);assert.equal(r.body.arrivalId,8);assert.equal(f.writes.length,0);assert.equal(f.found,0);
});
test('release lookup outage makes no writes or empty text conversation',async()=>{const f=setup({lookupError:true}),r=await f.call();assert.equal(r.code,503);assert.equal(f.found,0);assert.equal(f.writes.length,0)});
test('unknown release requires explicit clerk routing before check-in or messages',async()=>{const f=setup({loads:[]});assert.equal((await f.call()).code,409);assert.equal(f.writes.length,0);assert.equal(f.notifications.length,0);const r=await f.call({...body,loadingDoor:'OTHER'});assert.equal(r.code,200);assert.equal(r.body.appointment,null);assert.equal(f.tables.shipping_staff_checkins.length,1);assert.equal(f.notifications[0].preparedInstructions,require('./driver-visit-flow').PICKUP_CHECKIN);});
test('ambiguous release does not assign a guessed loading door',async()=>{const f=setup({loads:[load,{...load,unloadingDoor:'MB'}]}),r=await f.call({...body,loadingDoor:'COIL'});assert.equal(r.code,200);assert.equal(r.body.appointment.status,'ambiguous');assert.equal(r.body.appointment.door,null)});
for(const closed of [false,true])test(`returning driver after ${closed?'closed thread':'ended session'} gets a new arrival`,async()=>{
 const f=setup({active:{id:8,entered_at:'2026-11-09T15:00:00Z',release_number:'999999',checked_in_at:'2026-11-09T15:00:00Z'},ended:true,closed,checkin:'2026-11-09T15:00:00Z'}),r=await f.call();
 assert.equal(r.code,200);assert.equal(f.found,1);assert.equal(r.body.arrivalId,3);assert.equal(f.tables.driver_sms_arrivals.length,2);
 const old=f.tables.driver_sms_arrivals.find(r=>r.id===8);assert.equal(old.release_number,'999999');assert.equal(old.departed_at,'2026-11-09T16:00:00Z');assert.equal(old.checked_in_at,'2026-11-09T15:00:00Z');
 const added=f.tables.driver_sms_arrivals.find(r=>r.id===3);assert.equal(added.release_number,body.releaseNumber);assert.equal(added.departed_at,undefined);assert.equal(added.checked_in_at,at);assert.equal(r.body.appointment.door,'COIL');
 if(closed)assert.equal(r.body.conversationId,'new-conversation');else assert.equal(r.body.conversationId,'conversation');
 const again=await f.call();assert.equal(again.body.alreadyAdded,true);assert.equal(f.tables.driver_sms_arrivals.length,2);
});
test('arrival from an older session cannot be mistaken for a current check-in',async()=>{
 const f=setup({active:{id:8,entered_at:'2026-11-09T15:00:00Z',release_number:'999999'},sessionStarted:at,checkin:'2026-11-09T15:00:00Z'}),r=await f.call();
 assert.equal(r.code,200);assert.equal(f.tables.driver_sms_arrivals.length,2);assert.equal(f.tables.driver_sms_arrivals[0].departed_at,'2026-11-10T14:59:59.999Z');assert.equal(f.tables.shipping_staff_checkins[0].checked_in_at,at);
});
test('return after completed checkout starts fresh without modifying the old arrival',async()=>{
 const f=setup({ended:true});f.tables.driver_sms_arrivals.push({id:8,phone_e164:'+14195550123',facility_id:'csp-toledo-main',conversation_id:'conversation',entered_at:'2026-11-09T15:00:00Z',departed_at:'2026-11-09T16:00:00Z',release_number:'old'});
 const r=await f.call();assert.equal(r.code,200);assert.equal(f.tables.driver_sms_arrivals[0].release_number,'old');assert.equal(f.tables.driver_sms_arrivals.length,2);
});
test('failed official check-in can be retried even if arrival timestamp already saved',async()=>{
 const f=setup({checkinError:true});assert.equal((await f.call()).code,500);assert.equal(f.tables.driver_sms_arrivals.length,1);
 const retry=setup({active:f.tables.driver_sms_arrivals[0]});assert.equal((await retry.call()).code,200);assert.equal(retry.tables.driver_sms_arrivals.length,1);assert.equal(retry.tables.shipping_staff_checkins.length,1);
});

test('new-session database time cannot make a returning manual arrival look stale on retry',async()=>{
 const startAt='2026-11-10T15:00:00.100Z';const f=setup({ended:true,startAt});const r=await f.call();assert.equal(r.code,200);assert.equal(r.body.checkedInAt,startAt);assert.equal(f.tables.driver_sms_arrivals[0].entered_at,startAt);
 const again=await f.call();assert.equal(again.body.alreadyAdded,true);assert.equal(f.tables.driver_sms_arrivals.length,1);
});
test('manual standard pickup sends the exact standard instructions through the shared sender',async()=>{
 const f=setup({loads:[{...load,unloadingDoor:'MB'}]}),r=await f.call();assert.equal(r.code,200);assert.equal(f.notifications.length,1);
 const n=f.notifications[0];assert.equal(n.preparedInstructions,'You have been successfully checked in. Please pull around back and stay to the LEFT.');assert.equal(n.conversation.channel,'sms');assert.equal(n.conversation.sms_phone_e164,'+14195550123');assert.equal(n.arrivalId,3);assert.equal(n.staffId,'staff');
});
test('known COIL manual pickup retains RIGHT-side instructions',async()=>{
 const f=setup();await f.call();assert.equal(f.notifications[0].preparedInstructions,require('./pickup-directions').COIL_CHECKIN);
});
test('message failure keeps the official check-in and reports a warning',async()=>{
 const f=setup({messageFailure:true}),r=await f.call();assert.equal(r.code,200);assert.match(r.body.warning,/automatic instructions/);assert.equal(f.tables.shipping_staff_checkins.length,1);
});
test('manual arrival uses confirmed COIL even when the submitted routing says OTHER',async()=>{
 const f=setup();const r=await f.call({...body,loadingDoor:'OTHER'});assert.equal(r.code,200);assert.equal(f.notifications[0].preparedInstructions,require('./pickup-directions').COIL_CHECKIN);
});
