const {test}=require('node:test');
const assert=require('node:assert/strict');
const {driverName,cleanIdentity}=require('./driver-identity');
const {parseDetails,nextCheckin}=require('./sms-checkin-details');
test('extracts names from introductions while preserving full names',()=>{
 for(const [text,name] of [["This is John","John"],["Hi, this is John O’Neil","John O’Neil"],["My name is José García","José García"],["I’m Anne-Marie Smith.","Anne-Marie Smith"],["I am Sean","Sean"],["John Smith","John Smith"]])assert.equal(driverName(text),name);
});
test('introduction plus carrier and release becomes separate identity fields',()=>{
 assert.deepEqual(parseDetails('This is John with JLS, 255479'),{full_name:'John',driver_company:'JLS',last_release_number:'255479'});
 assert.deepEqual(parseDetails('This is Sean from Center Express, 968310'),{full_name:'Sean',driver_company:'Center Express',last_release_number:'968310'});
 assert.equal(parseDetails('Name: This is John; Carrier: JLS; Release: 255479').full_name,'John');
});
test('unknown placeholders cannot complete onboarding and allow real name later',()=>{
 for(const value of ['?','???','—','N/A','unknown'])assert.equal(cleanIdentity(value),'');
 const existing={full_name:'?',driver_company:'?',visit_type:'pickup',last_release_number:'001288'};
 const missing=nextCheckin({existing,body:'001288'});
 assert.equal(missing.contact.onboarding_step,'awaiting_details');
 assert.equal(missing.contact.full_name,'');
 const fixed=nextCheckin({existing:missing.contact,body:'This is John with JLS'});
 assert.equal(fixed.contact.full_name,'John');assert.equal(fixed.contact.driver_company,'JLS');assert.equal(fixed.contact.onboarding_step,'ready');
});
test('legacy introductions normalize on the next inbound message',()=>{
 const result=nextCheckin({existing:{full_name:'This is John',driver_company:'JLS',visit_type:'pickup',last_release_number:'255479'},body:'255479'});
 assert.equal(result.contact.full_name,'John');
});
