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
   in(){return query},like(){return query},not(){return query},neq(){return query},limit:async()=>({data:[{id:"previous-image"}]})
  };
  return query;
 }};
 return {input:{db,chartDb:{from(){const q={select(){return q},ilike(){return q},order(){return q},range:async()=>({data:[{poRel:'12345',scheduleDate:'2026-11-11',location:'PROSPY',unloadingDoor:door}]})};return q}},twilio:{messages:{create:async()=>{sent++;return{sid:'mock',status:'sent'};}}},messagingServiceSid:'mock',publicBaseUrl:'https://example.com',scheduleStatusSync(){},conversation:{id:'test',channel:'sms',sms_phone_e164:'mock',facility_id:'csp-toledo-main',release_number:'12345'},arrivalId:1,staffId:'staff',visitType:'pickup'},sent:()=>sent,writes};
}
test('returning driver gets separate confirmation and PPE messages without pictures',async()=>{
 const x=setup();await sendCheckinInstructions(x.input);assert.equal(x.sent(),2);
 const messages=x.writes.filter(m=>m.body);
 assert.deepEqual(messages.map(m=>m.body),[
 'You have been successfully checked in. Please pull around back and stay to the LEFT.',
 'Please have all required PPE ready before entering the facility, including a hard hat, safety glasses, and fully enclosed shoes.'
 ]);assert(messages.every(m=>!m.attachment_path));
});
for(const channel of ['sms','app'])test('first-time '+channel+' gets map on confirmation and PPE image on separate reminder',async()=>{
 const x=setup();x.input.conversation.channel=channel;
 const from=x.input.db.from;x.input.db.from=(...args)=>{const q=from(...args);q.limit=async()=>({data:[]});return q;};
 const uploads=[];x.input.db.storage={from(){return{upload:async(path)=>{uploads.push(path);return{}},createSignedUrl:async(path)=>({data:{signedUrl:'https://example.invalid/'+path}})}}};
 await sendCheckinInstructions(x.input);const messages=x.writes.filter(m=>m.body);
 assert.equal(messages.length,2);assert.match(messages[0].attachment_path,/shipping-map.jpg$/);assert.match(messages[1].attachment_path,/ray-ppe.png$/);
 assert.match(messages[0].body,/successfully checked in/);assert.match(messages[1].body,/hard hat, safety glasses/);
 assert.equal(x.sent(),channel==='sms'?2:0);assert.equal(uploads.length,2);
});
test('dropoff never receives conflicting left-side instructions',async()=>{const x=setup();await sendCheckinInstructions({...x.input,visitType:'dropoff'});assert.equal(x.sent(),0);assert.equal(x.writes.length,0)});
test('repeat click does not resend instructions',async()=>{const x=setup(true);await sendCheckinInstructions(x.input);assert.equal(x.sent(),0)});

for(const channel of ['sms','app'])test(`returning COIL ${channel} gets exact combined first message without duplicate PPE`,async()=>{
 const x=setup(false,'COIL');x.input.conversation.channel=channel;await sendCheckinInstructions(x.input);const messages=x.writes.filter(m=>m.body);assert.equal(messages.length,1);assert.equal(messages[0].body,require('./pickup-directions').COIL_CHECKIN);assert(!messages[0].attachment_path);
});
for(const channel of ['sms','app'])test(`first-time COIL ${channel} gets map on combined first text and safety image second`,async()=>{
 const x=setup(false,'COIL');x.input.conversation.channel=channel;const from=x.input.db.from;x.input.db.from=(...args)=>{const q=from(...args);q.limit=async()=>({data:[]});return q;};
 x.input.db.storage={from:()=>({upload:async()=>({}),createSignedUrl:async path=>({data:{signedUrl:'https://example.invalid/'+path}})})};
 await sendCheckinInstructions(x.input);const messages=x.writes.filter(m=>m.body);assert.equal(messages.length,2);assert.equal(messages[0].body,require('./pickup-directions').COIL_CHECKIN);assert.match(messages[0].attachment_path,/shipping-map.jpg$/);assert.match(messages[1].attachment_path,/ray-ppe.png$/);
});
test('manual COIL check-in still sends a separate PPE message for returning drivers',async()=>{
 const x=setup(false,'COIL');await sendCheckinInstructions({...x.input,alwaysSendPpe:true});
 const messages=x.writes.filter(m=>m.body);assert.equal(messages.length,2);assert.equal(messages[0].body,require('./pickup-directions').COIL_CHECKIN);assert.equal(messages[1].body,require('./driver-visit-flow').PICKUP_PPE);assert.equal(x.sent(),2);
});
