const {test}=require('node:test'),assert=require('node:assert/strict');
const {BODY,ENABLED_AT,messageId,attachmentForReply,attachAppMap}=require('./dropoff-map');
const {needsImages}=require('./first-visit-images');
const c={id:'current',channel:'sms',sms_phone_e164:'+15555550100',facility_id:'csp-toledo-main',visit_type:'dropoff',session_started_at:new Date(ENABLED_AT+1000).toISOString()};
function fixture(history=[]){const writes=[],conversations=[c,{...c,id:'prior'}];return{writes,db:{from(table){let rows=table==='driver_conversations'?conversations:history;const q={select(){return q},eq(k,v){rows=rows.filter(r=>r[k]===v);return q},in(k,vs){rows=rows.filter(r=>vs.includes(r[k]));return q},or(){rows=rows.filter(r=>/^(staff-pickup-|dropoff-instructions:)/.test(r.client_message_id));return q},not(k){rows=rows.filter(r=>r[k]!=null);return q},neq(k,v){rows=rows.filter(r=>r[k]!==v);return q},is(){return q},update(v){writes.push(v);return q},then(f){return Promise.resolve({data:rows}).then(f)}};return q}}};}
const attachment={fields:{attachment_path:'map.jpg',attachment_name:'Shipping and receiving map.jpg'},mediaUrl:'https://example.test/map.jpg'};
const photo={conversation_id:'prior',client_message_id:'staff-pickup-dropoff-map:prior',attachment_path:'map.jpg',delivery_status:'delivered',sent_at:new Date(ENABLED_AT-1000).toISOString()};
test('first-time SMS dropoff gets existing map attachment on exact direction message',async()=>{
 const f=fixture();assert.deepEqual(await attachmentForReply({db:f.db,conversation:c,body:BODY,prepare:async()=>attachment}),attachment);
 assert.equal(messageId(c),`staff-pickup-dropoff-map:current:${c.session_started_at}`);
});
test('returning phone with prior dropoff map does not get another',async()=>{
 const f=fixture([photo]);assert.equal(await attachmentForReply({db:f.db,conversation:c,body:BODY,prepare:()=>assert.fail('must not upload')}),null);
});
test('closed receiving, cutoff, Both and Enterprise replies do not attach unloading map',async()=>{
 for(const input of [{body:'Receiving is closed.'},{body:'Please call 419-269-9706.'},{conversation:{...c,visit_type:'both'}},{conversation:{...c,facility_id:'csp-toledo-enterprise'}}])assert.equal(await attachmentForReply({db:fixture().db,conversation:c,body:BODY,...input,prepare:()=>assert.fail('must not upload')}),null);
});
test('failed prior delivery retains first-visit eligibility',async()=>assert(await attachmentForReply({db:fixture([{...photo,delivery_status:'failed'}]).db,conversation:c,body:BODY,prepare:async()=>attachment})));
test('native app attaches to original message without changing trigger deduplication key',async()=>{
 const f=fixture();const conversation={...c,channel:'app'};
 const message={id:12,client_message_id:'dropoff-instructions:original',direction:'shipping_to_driver',body:BODY,sent_at:new Date(ENABLED_AT+2000).toISOString(),delivery_status:'sent'};
 await attachAppMap({db:f.db,conversation,messages:[message],prepare:async()=>attachment});assert.deepEqual(f.writes,[attachment.fields]);
 await attachAppMap({db:f.db,conversation,messages:[{...message,attachment_path:'map.jpg'}],prepare:()=>assert.fail('duplicate upload')});assert.equal(f.writes.length,1);
 await attachAppMap({db:f.db,conversation,messages:[{...message,sent_at:new Date(ENABLED_AT-1000).toISOString()}],prepare:()=>assert.fail('retroactive upload')});assert.equal(f.writes.length,1);
});
test('prior native map suppresses future visit, current native map allows PPE phase',async()=>{
 const native={...photo,client_message_id:'dropoff-instructions:prior'};
 assert.equal(await needsImages(fixture([native]).db,c),false);
 assert.equal(await needsImages(fixture([{...native,conversation_id:c.id,sent_at:new Date(ENABLED_AT+2000).toISOString()}]).db,c),true);
});
