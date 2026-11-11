const {test}=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const source=fs.readFileSync(require.resolve('./server.js'),'utf8');
const start=source.indexOf('const sendAutomatedSmsReply ='),end=source.indexOf('const MMS_MAX_BYTES',start);
function harness(){const sent=[],stored=[];const context={twilioClient:{messages:{async create(v){sent.push(v);return{sid:`SM${sent.length}`,status:'queued'}}}},TWILIO_MESSAGING_SERVICE_SID:'test',TWILIO_PUBLIC_BASE_URL:'https://example.test',driverSupabase:{from:()=>({insert:async v=>{stored.push(v);return{}}})},mapTwilioDeliveryStatus:s=>s,scheduleTwilioStatusSync(){}};vm.createContext(context);vm.runInContext(source.slice(start,end)+'globalThis.send=sendAutomatedSmsReply;',context);return{context,sent,stored};}
test('drop-off MMS contains facility map in the same text and stores attachment for clerk view',async()=>{
 const f=harness();for(let i=0;i<2;i++)await f.context.send({phone:'+15555550100',conversationId:'c',body:'Directions',attachment:{mediaUrl:'https://example.test/map.jpg',fields:{attachment_path:'map.jpg'}},clientMessageId:'staff-pickup-dropoff-map:c'});
 assert.equal(f.sent[0].mediaUrl[0],'https://example.test/map.jpg');assert.equal(f.sent[0].body,'Directions');assert.equal(f.stored[0].attachment_path,'map.jpg');assert.notEqual(f.stored[0].client_message_id,f.stored[1].client_message_id);
});
test('returning driver text keeps regular SMS payload without media',async()=>{const f=harness();await f.context.send({phone:'+15555550100',conversationId:'c',body:'Directions'});assert.equal(f.sent[0].mediaUrl,undefined);assert.equal(f.stored[0].client_message_id,'twilio:auto:SM1');});
