const{test}=require('node:test'),assert=require('node:assert/strict'),{sendCheckinInstructions}=require('./checkin-instructions');
function setup(duplicate=false) {
 let sent=0; const writes=[];
 const db={from(){
  const query={
   insert(row){writes.push(row);return query;},
   select(){return query;},
   single:async()=>duplicate?{error:{code:'23505'}}:{data:{id:1}},
   update(row){writes.push(row);return query;},
   eq:async()=>({}),
   in(){return query},like(){return query},not(){return query},neq(){return query},limit:async()=>({data:[{id:"previous-image"}]})
  };
  return query;
 }};
 return {input:{db,chartDb:{from(){const q={select(){return q},ilike(){return q},order(){return q},range:async()=>({data:[]})};return q}},twilio:{messages:{create:async()=>{sent++;return{sid:'mock',status:'sent'};}}},messagingServiceSid:'mock',publicBaseUrl:'https://example.com',scheduleStatusSync(){},conversation:{id:'test',channel:'sms',sms_phone_e164:'mock',facility_id:'csp-toledo-main',release_number:'12345'},arrivalId:1,staffId:'staff',visitType:'pickup'},sent:()=>sent,writes};
}
test('pickup confirmation contains exact PPE instructions',async()=>{const x=setup();await sendCheckinInstructions(x.input);assert.equal(x.sent(),1);assert.match(x.writes[0].body,/stay to the LEFT.*hard hat, safety glasses, and fully enclosed shoes/)});
test('dropoff never receives conflicting left-side instructions',async()=>{const x=setup();await sendCheckinInstructions({...x.input,visitType:'dropoff'});assert.equal(x.sent(),0);assert.equal(x.writes.length,0)});
test('repeat click does not resend instructions',async()=>{const x=setup(true);await sendCheckinInstructions(x.input);assert.equal(x.sent(),0)});
