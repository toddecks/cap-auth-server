const {test}=require('node:test'),assert=require('node:assert/strict');
const {parseDetails,nextCheckin}=require('./sms-checkin-details');
const {releaseFromMessage}=require('./release-attempts');
const {missingPrompt}=require('./pickup-detail-prompt');
const flow=require('./driver-visit-flow');
const examples=[['This is John with JLS here to pick up for Midwest metals. 255489.','John','JLS','255489','pickup'],['Is this Sean with center express?My release number is 968463','Sean','center express','968463',null],['Bob with center 969425','Bob','center','969425',null]];
for(const [body,name,carrier,release,type] of examples)test('recognizes complete natural introduction: '+name,()=>{
 const first=nextCheckin({body,conversationCreated:true});assert.equal(first.contact.full_name,name);assert.equal(first.contact.driver_company,carrier);assert.equal(first.contact.last_release_number,release);assert.equal(first.visitType,type);assert.equal(releaseFromMessage(body),release);
 if(!type){assert.equal(first.reply,flow.TYPE_PROMPT);const second=nextCheckin({existing:first.contact,body:'Pick Up'});assert.equal(second.contact.full_name,name);assert.equal(second.contact.driver_company,carrier);assert.equal(second.contact.last_release_number,release);assert.equal(second.contact.onboarding_step,'ready');assert.equal(second.reply,'');}
 else {assert.equal(first.contact.onboarding_step,'ready');assert.equal(first.reply,'');}
});
test('only missing pickup details are requested',()=>{
 assert.equal(missingPrompt({full_name:'John',last_release_number:'255489'}),'Please send your carrier name for verification and further instructions.');
 assert.equal(missingPrompt({driver_company:'JLS'}),'Please send your name and your release number for verification and further instructions.');
 assert.equal(missingPrompt({full_name:'John',driver_company:'JLS',last_release_number:'255489'}),null);
 assert.equal(missingPrompt(null),flow.PICKUP_PROMPT);
});
test('ordinary messages and customer/weight commentary do not invent introductions',()=>{
 for(const body of ['I have 40000 pounds','Thanks with center express','Where is the office?','Copy'])assert(!parseDetails(body).full_name,body);
});
test('closed reply includes option 5 verbatim',()=>assert.equal(require('./release-validation').CLOSED,'Our shipping office is currently closed. Please call 419-269-9706 and select option 5 for assistance, or wait in your truck and a member of our shipping/receiving team will be with you shortly.'));
