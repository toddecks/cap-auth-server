const test=require('node:test'),assert=require('node:assert/strict');const {nextCheckin,parseDetails}=require('./sms-checkin-details');
const fields=['Kyle Huston','7666373','Huston Trucking'];const permutations=a=>a.length?a.flatMap((x,i)=>permutations(a.filter((_,j)=>i!==j)).map(p=>[x,...p])):[[]];
for(const order of permutations(fields)){
 test('any order: '+order.join(', '),()=>{const r=nextCheckin({body:'PICKUP; '+order.join(', '),conversationCreated:true});assert.equal(r.contact.full_name,'Kyle Huston');assert.equal(r.contact.driver_company,'Huston Trucking');assert.equal(r.contact.last_release_number,'7666373');assert.equal(r.contact.onboarding_step,'ready');});
 test('separate texts: '+order.join(', '),()=>{let existing=nextCheckin({body:'PICKUP',conversationCreated:true}).contact;for(const body of order)existing=nextCheckin({existing,body,conversationCreated:!existing}).contact;assert.equal(existing.full_name,'Kyle Huston');assert.equal(existing.driver_company,'Huston Trucking');assert.equal(existing.onboarding_step,'ready');});
}
test('labels work in arbitrary order without commas',()=>{const p=parseDetails('Company: Swift Release: AB1234 Name: Mike');assert.equal(p.driver_company,'Swift');assert.equal(p.full_name,'Mike');assert.equal(p.last_release_number,'AB1234');});
test('wrong release can be corrected on an already ready contact',()=>{const existing={full_name:'Kyle Huston',driver_company:'Huston Trucking',last_release_number:'1234567',onboarding_step:'ready'};for(const body of ['Release: 7666373','7666373','Sorry 7666373']){const r=nextCheckin({existing,body});assert.equal(r.releaseNumber,'7666373');assert.equal(r.contact.full_name,existing.full_name);assert.equal(r.reply,'');}});
test('normal conversation cannot replace release or identity',()=>{const existing={full_name:'Kyle Huston',driver_company:'Huston Trucking',last_release_number:'7666373',onboarding_step:'ready'};for(const body of ['Thanks','What door do I go to?','I have 40000 pounds','My phone is 4195551234']){const r=nextCheckin({existing,body});assert.equal(r.releaseNumber,'');assert.equal(r.reply,'');}});
test('new visit does not reuse prior release; asks only missing detail',()=>{const existing={full_name:'Kyle Huston',driver_company:'Huston Trucking',last_release_number:'old',onboarding_step:'ready'};const r=nextCheckin({existing,body:'Hi',conversationCreated:true});assert.equal(r.contact.last_release_number,'');assert.match(r.reply,/PICKUP or DROPOFF/);});
test('unclear message asks for clarification instead of accepting a release',()=>{const r=nextCheckin({body:'I am here'});assert.equal(r.contact.onboarding_step,'awaiting_details');assert(!r.releaseNumber);});

test('visit-type prompt is sent once, without repeated correction instructions',()=>{const first=nextCheckin({body:'Hi',conversationCreated:true});assert.match(first.reply,/PICKUP or DROPOFF/);assert.equal(nextCheckin({existing:first.contact,body:'Thanks'}).reply,'');});

test('complete first text infers pickup from the release details',()=>{const r=nextCheckin({body:'Kyle Huston, 7666373, Huston Trucking',conversationCreated:true});assert.equal(r.reply,'');assert.equal(r.visitType,'pickup');assert.equal(nextCheckin({existing:r.contact,body:'Release: 9999999'}).reply,'');});

test('initial punctuated details are retained after pickup choice',()=>{
 const first=nextCheckin({body:'Dave..center express. Release 1321851',conversationCreated:true});
 assert.equal(first.contact.full_name,'Dave');assert.equal(first.contact.driver_company,'center express');assert.equal(first.contact.last_release_number,'1321851');
 const second=nextCheckin({existing:first.contact,body:'Pick up'});
 assert.equal(second.contact.onboarding_step,'ready');assert.equal(second.reply,'');
});
test('complete pickup first message never asks to repeat details',()=>{
 const r=nextCheckin({body:'Pickup; Dave, Center Express, Release 1321851',conversationCreated:true});
 assert.equal(r.contact.onboarding_step,'ready');assert.equal(r.reply,'');
});
test('pickup selection asks only missing fields',()=>{
 for(const [body,prompt] of [['Name: Dave; Release: 1321851','Please reply with your carrier name.'],['Carrier: Center Express; Release: 1321851','Please reply with your name.'],['Name: Dave; Carrier: Center Express','Please reply with your release number.']]){
 const first=nextCheckin({body,conversationCreated:true});
 assert.equal(nextCheckin({existing:first.contact,body:'Pick up'}).reply,prompt);
 }
});
