const {test}=require('node:test'),assert=require('node:assert/strict');
const {sendCheckinInstructions}=require('./checkin-instructions');
function fixture(prior){let saved=prior,sends=0;const db={from(){let op='read',value,single=false;const q={select(){return q},eq(){return q},in(){return q},or(){return q},not(){return q},neq(){return q},maybeSingle(){single=true;return q},single(){single=true;return q},insert(v){op='insert';value=v;return q},update(v){op='update';value=v;return q},then(resolve){
 let result;
 if(op==='insert')result=saved?{error:{code:'23505'}}:{data:(saved={id:1,...value})};
 else if(op==='update'){Object.assign(saved,value);result={data:null};}
 else result={data:single?saved:[{conversation_id:'old',sent_at:'2026-01-01T00:00:00Z'}]};
 return Promise.resolve(result).then(resolve);
 }};return q;}};
 const deps={db,requireDeliveryConfirmation:true,preparedInstructions:'Checked in, stay RIGHT.',conversation:{id:'visit',channel:'sms',created_at:'2026-11-24T12:00:00Z',sms_phone_e164:'+15555550100'},visitType:'pickup',staffId:'staff',messagingServiceSid:'test',twilio:{messages:{create:async()=>{sends++;return{sid:'SM-test',status:'sent'}}}}};
 return{deps,get sends(){return sends},get saved(){return saved}};
}
test('worker retries cannot duplicate a provider-accepted check-in SMS',async()=>{
 const f=fixture();await sendCheckinInstructions(f.deps);await sendCheckinInstructions(f.deps);assert.equal(f.sends,1);
});
test('unknown prior delivery outcome is flagged for review instead of resent or silently marked complete',async()=>{
 const f=fixture({id:1,delivery_status:'queued'});await assert.rejects(sendCheckinInstructions(f.deps),/no confirmed provider result/);assert.equal(f.sends,0);
});
test('ended visits cannot receive delayed instructions',async()=>{
 const f=fixture();await sendCheckinInstructions({...f.deps,isCurrent:async()=>false});assert.equal(f.sends,0);assert.equal(f.saved,undefined);
});
test('a visit ending during translation cancels the pending SMS',async()=>{
 const f=fixture(),translation=require('./sms-translation'),original=translation.prepareOutgoing;let current=true;
 translation.prepareOutgoing=async()=>{current=false;return{body:'test',patch:{}}};
 try{await sendCheckinInstructions({...f.deps,isCurrent:async()=>current});assert.equal(f.sends,0);assert.equal(f.saved.delivery_status,'failed');}finally{translation.prepareOutgoing=original;}
});
