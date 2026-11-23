const {test}=require('node:test'),assert=require('node:assert/strict');
const {createStore}=require('./release-review-store');
test('private review storage round trips without using driver_messages',async()=>{
 const rows=[];const db={from(table){assert.equal(table,'driver_release_reviews');return {select(){return {eq:async(_,id)=>({data:rows.filter(r=>r.conversation_id===id)})}},upsert:async(row)=>{rows.push(row);return {error:null}}}}};
 const store=createStore(db);await store.save('visit',{client_message_id:'release-wait:ambiguous-1:visit',sent_at:'2026-11-23T17:00:00Z',body:'must not persist'});
 assert.equal(rows[0].body,undefined);assert.equal((await store.load('visit'))[0].body,'');assert.deepEqual(await store.load('other'),[]);
});
test('storage failures stop processing instead of sending an alternative reply',async()=>{
 const error=Error('unavailable');const store=createStore({from(){return {select(){return {eq:async()=>({error})}},upsert:async()=>({error})}}});
 await assert.rejects(store.load('visit'),error);await assert.rejects(store.save('visit',{}),error);
});
