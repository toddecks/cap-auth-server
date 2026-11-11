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
test('existing-record repair changes only clear introductions, never guesses from sentences',()=>{
 const {repairedName}=require('./repair-driver-names');
 assert.equal(repairedName('This is John'),'John');
 assert.equal(repairedName('This is Sean'),'Sean');
 assert.equal(repairedName('This is John with JLS'),null);
 assert.equal(repairedName('?'),null);
 assert.equal(repairedName('John Smith'),null);
});
test('saved-record repair verifies writes and preserves concurrent clerk edits',async()=>{
 const {createRepair}=require('./repair-driver-names');
 const tables={driver_sms_contacts:[{phone_e164:'p1',full_name:'This is John'},{phone_e164:'p2',full_name:'?'}],driver_sms_arrivals:[{id:'a1',driver_name:'This is John'},{id:'a2',driver_name:'My name is Sean'}]};
 let changedDuringRepair=false;
 const db={from(table){let filters=[],patch,cols='';const q={select(c){cols=c;return q},order(){return q},eq(k,v){filters.push([k,v]);return q},update(p){patch=p;return q},range(){return Promise.resolve({data:tables[table].map(r=>({...r}))})},single(){return Promise.resolve({data:tables[table].find(r=>filters.every(([k,v])=>r[k]===v))})},then(resolve){if(table==='driver_sms_arrivals'&&!changedDuringRepair){tables[table][1].driver_name='Sean Smith';changedDuringRepair=true;}const rows=tables[table].filter(r=>filters.every(([k,v])=>r[k]===v));rows.forEach(r=>Object.assign(r,patch));return Promise.resolve({data:rows.map(r=>({...r}))}).then(resolve)}};return q;}};
 const repair=createRepair(db);await repair.run();
 assert.equal(repair.health.state,'complete');assert.equal(repair.health.updated,2);assert.equal(repair.health.verified,2);
 assert.equal(tables.driver_sms_contacts[0].full_name,'John');assert.equal(tables.driver_sms_contacts[1].full_name,'?');
 assert.equal(tables.driver_sms_arrivals[0].driver_name,'John');assert.equal(tables.driver_sms_arrivals[1].driver_name,'Sean Smith');
});
test('acknowledgements never become names or replace a known driver',()=>{
 const {repairedName}=require('./repair-driver-names');
 for(const body of ['Copy','copy that','Copied.','Got it','Understood','Roger that','10-4','Okay','Thank you']){
  assert.equal(driverName(body),'',body);
  assert.equal(parseDetails(body).full_name,undefined,body);
  assert.equal(repairedName(body),'',body);
  const existing={full_name:'John',driver_company:'JLS',visit_type:'dropoff'};
  assert.equal(nextCheckin({existing,body}).contact.full_name,'John',body);
 }
 assert.equal(driverName('Roger'),'Roger');
 assert.equal(nextCheckin({existing:{full_name:'Copy',driver_company:'JLS',visit_type:'dropoff'},body:'Copy'}).contact.full_name,'');
});
