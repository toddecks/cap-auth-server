const {test}=require('node:test'),assert=require('node:assert/strict');
const translation=require('./sms-translation');
const provider=reply=>({chat:{completions:{create:async request=>({choices:[{finish_reason:'stop',message:{content:typeof reply==='function'?reply(request):reply}}]})}}});
test('clear Spanish SMS is translated into English despite the old English input label',async()=>{
 const patch=await translation.detectIncoming('Necesito ayuda con la orden 73014',provider(req=>{assert.equal(req.store,false);assert.equal(req.response_format.json_schema.strict,true);assert.match(req.messages[1].content,/CSPTOKEN0X/);return JSON.stringify({language:'Spanish',confidence:'high',translation:'I need help with order CSPTOKEN0X'});}));
 assert.deepEqual(patch,{shipping_body:'I need help with order 73014',original_language:'Spanish',translated_language:'English'});
});
test('unclear carrier shorthand is preserved without inventing an English interpretation or language',async()=>{
 const text='Coiñdara.plte4',patch=await translation.detectIncoming(text,provider(JSON.stringify({language:'Spanish',confidence:'low',translation:'invented'})));
 assert.equal(patch.shipping_body,text);assert.equal(patch.original_language,'Unknown');
});
test('names, release-only messages and intake commands do not establish a new language',async()=>{
 for(const body of ['73014','Pick up','Both','Drop Off'])assert.equal((await translation.detectIncoming(body,null)).original_language,'Unknown');
 const patch=await translation.detectIncoming('John Smith',provider(JSON.stringify({language:'Unknown',confidence:'low',translation:'John Smith'})));assert.equal(patch.shipping_body,'John Smith');
});
test('outgoing text preserves compound releases, phone numbers and URLs exactly',async()=>{
 const source='Release CB7201P1-2/7. Please call 419-269-9706 and select option 5. https://csp.example/map';
 const text=await translation.translateOutgoing(source,'Spanish',provider(req=>req.messages[1].content.replace('Please call','Llame al').replace('and select option','y seleccione la opción')));
 for(const token of ['CB7201P1-2/7','419-269-9706','5','https://csp.example/map'])assert(text.includes(token));
});
test('translation that drops references, refuses, fails or is incomplete is never accepted',async()=>{
 await assert.rejects(()=>translation.translateOutgoing('Call 419-269-9706','French',provider('Appelez le bureau')),/omitted/);
 const bad={chat:{completions:{create:async()=>({choices:[{finish_reason:'length',message:{content:'partial'}}]})}}};
 await assert.rejects(()=>translation.detectIncoming('Necesito ayuda',bad),/incomplete/);
});
test('known driver language changes only on clear newer messages; earlier lookup completion cannot overwrite it',async()=>{
 const c={id:'language-order-test',session_started_at:'2026-11-17T12:00:00Z'};
 translation.remember(c,{original_language:'Spanish'},'2026-11-17T12:02:00Z');translation.remember(c,{original_language:'French'},'2026-11-17T12:01:00Z');translation.remember(c,{original_language:'Unknown'},'2026-11-17T12:03:00Z');
 assert.equal(await translation.languageFor({},c),'Spanish');
 translation.remember(c,{original_language:'English'},'2026-11-17T12:04:00Z');assert.equal(await translation.languageFor({},c),'English');
});
test('language lookup survives a restart using processed current-visit incoming messages only',async()=>{
 const rows=[{direction:'shipping_to_driver',original_language:'English',translated_language:'Spanish',sent_at:'2026-11-17T13:00:00Z'},{direction:'driver_to_shipping',original_language:'Unknown',translated_language:'English',sent_at:'2026-11-17T12:59:00Z'},{direction:'driver_to_shipping',original_language:'Spanish',translated_language:'English',sent_at:'2026-11-17T12:55:00Z'}];
 const db={from(){const q={select(){return q},eq(){return q},order(){return q},limit:async()=>({data:rows})};return q;}};
 assert.equal(await translation.languageFor(db,{id:'restart-language',session_started_at:'2026-11-17T12:00:00Z'}),'Spanish');
 assert.equal(await translation.languageFor(db,{id:'new-visit-language',session_started_at:'2026-11-17T14:00:00Z'}),'English');
});
test('outgoing provider failure sends English with an explicit truthful fallback notice',async()=>{
 const c={id:'outgoing-failure'};translation.remember(c,{original_language:'French'});translation.configure(()=>null);
 try{const result=await translation.prepareOutgoing({},c,'Please stay RIGHT.');assert.equal(result.body,'Please stay RIGHT.');assert.equal(result.patch.translated_language,'English');assert.match(result.patch.provider_error_message,/sent in English/);}finally{translation.configure(null);}
});
test('recent SMS copied verbatim by the old translator is translated once without changing the original or resending messages',async()=>{
 const now=Date.parse('2026-11-17T17:00:00Z');let calls=0;
 const tables={driver_messages:[{id:'history-1',conversation_id:'history-c',direction:'driver_to_shipping',body:'Necesito ayuda',original_body:'Necesito ayuda',original_language:'English',shipping_body:'Necesito ayuda',translated_language:null,sent_at:'2026-11-17T16:59:00Z'}],driver_conversations:[{id:'history-c',channel:'sms',session_started_at:'2026-11-17T12:00:00Z'}]};
 const db={from(table){assert(['driver_messages','driver_conversations'].includes(table));let filters=[],patch=null,one=false,limit=Infinity;const q={select(){return q},is(k,v){filters.push(r=>(r[k]??null)===v);return q},eq(k,v){filters.push(r=>r[k]===v);return q},gte(k,v){filters.push(r=>r[k]>=v);return q},order(){return q},limit(n){limit=n;return q},maybeSingle(){one=true;return q},update(value){patch=value;return q},then(resolve){const rows=tables[table].filter(r=>filters.every(f=>f(r))).slice(0,limit);if(patch)rows.forEach(r=>Object.assign(r,patch));return Promise.resolve({data:one?rows[0]:rows}).then(resolve)}};return q;}};
 const client=provider(()=>{calls++;return JSON.stringify({language:'Spanish',confidence:'high',translation:'I need help'});});
 const worker=require('./driver-translation').createWorker({db,getClient:()=>client,now:()=>now});
 await worker.tick();await worker.tick();assert.equal(worker.health.error,null);assert.equal(calls,1);assert.equal(tables.driver_messages[0].body,'Necesito ayuda');assert.equal(tables.driver_messages[0].shipping_body,'I need help');assert.equal(tables.driver_messages[0].original_language,'Spanish');assert.equal(tables.driver_messages.length,1);
});
