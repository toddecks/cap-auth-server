const {test}=require('node:test'),assert=require('node:assert/strict');
const {needsImages}=require('./first-visit-images');
const {dropoffReply}=require('./driver-visit-flow');
function db(conversations,messages){return {from(table){let rows=table==='driver_conversations'?conversations:messages;const q={select(){return q},eq(k,v){rows=rows.filter(r=>r[k]===v);return q},in(k,vs){rows=rows.filter(r=>vs.includes(r[k]));return q},like(k){rows=rows.filter(r=>r[k]?.startsWith('staff-pickup-'));return q},not(k){rows=rows.filter(r=>r[k]!=null);return q},neq(k,v){rows=rows.filter(r=>r[k]!==v);return q},limit(n){rows=rows.slice(0,n);return q},then(f){return Promise.resolve({data:rows}).then(f)}};return q}}}
const old={id:'old',user_id:'driver',sms_phone_e164:'+15555550100'};
const current={...old,id:'new',session_started_at:'2026-10-06T12:00:00Z'};
const photo={id:1,conversation_id:'old',client_message_id:'staff-pickup-ppe:old',attachment_path:'ray-ppe.png',delivery_status:'delivered'};
test('first visit gets images',async()=>assert.equal(await needsImages(db([current],[]),current),true));
test('returning app driver does not get images in new thread',async()=>assert.equal(await needsImages(db([old,current],[photo]),current),false));
test('returning SMS phone is recognized',async()=>assert.equal(await needsImages(db([{...old,user_id:null},{...current,user_id:null}],[photo]),{...current,user_id:null}),false));
test('current visit retries retain image eligibility',async()=>assert.equal(await needsImages(db([current],[{...photo,conversation_id:'new',client_message_id:'staff-pickup-ppe:new:2026-10-06T12:00:00.000Z'}]),current),true));
test('other drivers do not suppress images',async()=>assert.equal(await needsImages(db([{...old,user_id:'other',sms_phone_e164:'+15555550200'},current],[photo]),current),true));
test('failed delivery does not suppress a later attempt',async()=>assert.equal(await needsImages(db([old,current],[{...photo,delivery_status:'failed'}]),current),true));
test('regular-hours dropoff text is exact',()=>assert.equal(dropoffReply('2026-10-06T14:00:00Z'),'Please pull around back and stay to the right. Once stopped in the drop-off line, unchain your load and open the trailer for unloading.'));
