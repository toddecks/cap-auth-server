const{test}=require('node:test'),assert=require('node:assert/strict'),{sendCheckinInstructions}=require('./checkin-instructions');
function setup(duplicate=false,door='MB') {
 let sent=0; const writes=[];
 const db={from(){
  const query={
   insert(row){writes.push(row);return query;},
   select(){return query;},
   single:async()=>duplicate?{error:{code:'23505'}}:{data:{id:1}},
   update(row){writes.push(row);return query;},
   eq(){return query;},
   in(){return query},or(){return query},not(){return query},neq(){return query},limit:async()=>({data:[{id:"previous-image"}]}),then(resolve,reject){return query.limit().then(resolve,reject)}
  };
  return query;
 }};
 return {input:{db,chartDb:{from(){const q={select(){return q},or(){return q},ilike(){return q},order(){return q},range:async()=>({data:[{poRel:'12345',scheduleDate:'2026-11-11',location:'PROSPY',unloadingDoor:door}]})};return q}},twilio:{messages:{create:async()=>{sent++;return{sid:'mock',status:'sent'};}}},messagingServiceSid:'mock',publicBaseUrl:'https://example.com',scheduleStatusSync(){},conversation:{id:'test',channel:'sms',sms_phone_e164:'mock',facility_id:'csp-toledo-main',release_number:'12345'},arrivalId:1,staffId:'staff',visitType:'pickup'},sent:()=>sent,writes};
}
for(const channel of ['sms','app'])for(const first of [false,true])test(`${channel} ${first?'first-time':'returning'} pickup sends one LEFT confirmation, map only for first visit`,async()=>{
 const x=setup(false,'COIL');x.input.conversation.channel=channel;
 if(first){const from=x.input.db.from;x.input.db.from=(...args)=>{const q=from(...args);q.limit=async()=>({data:[]});return q;};}
 const uploads=[];x.input.db.storage={from:()=>({upload:async path=>{uploads.push(path);return{}},createSignedUrl:async path=>({data:{signedUrl:'https://example.invalid/'+path}})})};
 await sendCheckinInstructions({...x.input,alwaysSendPpe:true,preparedInstructions:require('./pickup-directions').COIL_CHECKIN});
 const messages=x.writes.filter(m=>m.body);assert.equal(messages.length,1);assert.equal(messages[0].body,require('./driver-visit-flow').PICKUP_CHECKIN);
 assert.equal(x.sent(),channel==='sms'?1:0);assert.equal(uploads.length,first?1:0);
 if(first)assert.match(messages[0].attachment_path,/shipping-map.jpg$/);else assert(!messages[0].attachment_path);
 assert(!messages.some(m=>/PPE|hard hat/.test(m.body)||/ppe/.test(m.attachment_path||'')));
});
test('dropoff never receives conflicting left-side instructions',async()=>{const x=setup();await sendCheckinInstructions({...x.input,visitType:'dropoff'});assert.equal(x.sent(),0);assert.equal(x.writes.length,0)});
test('repeat click does not resend instructions',async()=>{const x=setup(true);await sendCheckinInstructions(x.input);assert.equal(x.sent(),0)});
