const {test}=require('node:test'),assert=require('node:assert/strict');
const {sendBothImages}=require('./both-visit-images');
function fixture(channel='sms',returning=false){
 const c={id:'both',sms_phone_e164:'+15555550100',facility_id:'csp-toledo-main',channel};
 const rows=[{id:1,conversation_id:'both',direction:'shipping_to_driver',body:'STEP 1 – DROP OFF: instructions',delivery_status:'sent'}];
 if(returning)rows.push({id:2,conversation_id:'both',client_message_id:'staff-pickup-ppe:old-session',attachment_path:'old.png',delivery_status:'sent'});
 const sends=[];
 const db={from(table){let filters=[],op,value;const q={select(){return q},eq(k,v){filters.push(r=>r[k]===v);return q},in(k,v){filters.push(r=>v.includes(r[k]));return q},like(k){filters.push(r=>r[k]?.startsWith('staff-pickup-'));return q},not(k){filters.push(r=>r[k]!=null);return q},neq(k,v){filters.push(r=>r[k]!==v);return q},limit(){return q},single(){q.one=true;return q},insert(v){op='insert';value=v;return q},update(v){op='update';value=v;return q},then(resolve){let data=(table==='driver_conversations'?[c]:rows).filter(r=>filters.every(f=>f(r)));if(op==='insert'){if(rows.some(r=>r.client_message_id===value.client_message_id))return Promise.resolve({error:{code:'23505'}}).then(resolve);const row={...value,id:rows.length+1};rows.push(row);data=[row];}if(op==='update')data.forEach(r=>Object.assign(r,value));return Promise.resolve({data:q.one?data[0]:data}).then(resolve)}};return q}};
 const options={db,conversation:c,messages:rows,twilio:{messages:{async create(v){sends.push(v);return {sid:'SM'+sends.length,status:'sent'}}}},messagingServiceSid:'test',publicBaseUrl:'https://example.test',prepare:[async()=>({fields:{attachment_path:'map.jpg'},mediaUrl:'https://example.test/map.jpg'}),async()=>({fields:{attachment_path:'ppe.png'},mediaUrl:'https://example.test/ppe.png'})]};
 return {rows,sends,options};
}
for(const channel of ['sms','app'])test(channel+' Both gets property and PPE images once on first visit',async()=>{const f=fixture(channel);await sendBothImages(f.options);await sendBothImages(f.options);assert.equal(f.rows.filter(r=>r.attachment_path).length,2);assert.equal(f.sends.length,channel==='sms'?2:0);if(channel==='sms')assert.deepEqual(f.sends.map(r=>r.mediaUrl),[['https://example.test/map.jpg'],['https://example.test/ppe.png']]);});
test('returning Both driver does not receive duplicate first-visit images',async()=>{const f=fixture('sms',true);await sendBothImages(f.options);assert.equal(f.sends.length,0)});
test('closed-hours response does not send unloading images',async()=>{const f=fixture();f.rows[0].body='Receiving is closed.';await sendBothImages(f.options);assert.equal(f.sends.length,0)});
