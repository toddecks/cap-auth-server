const {test}=require('node:test');
const assert=require('node:assert/strict');
const {visitType}=require('./driver-visit-flow');
const {nextCheckin}=require('./sms-checkin-details');
const pickups=['pickup','pick up','pick-up','pick','picking up','pikup','pik up','picup','pckup','pcik up','pikcup','pickpu','pickupp','pick ip','pickuo','pickng up','piking up'];
const dropoffs=['dropoff','drop off','drop-off','dropping off','dropof','drop of','dropp off','droppof','drpo off','dorp off','drpoff','drooff','dropofff','drop oft','droping off','dropping of'];
for(const [type,variants] of [['pickup',pickups],['dropoff',dropoffs]]) {
 test(type+' spelling variants use arrival flow without becoming names',()=>{
  for(const body of variants){
   assert.equal(visitType(body.toUpperCase()+'!'),type,body);
   const r=nextCheckin({body,conversationCreated:true,now:new Date('2026-10-06T14:00:00Z')});
   assert.equal(r.visitType,type,body);assert.equal(r.contact.full_name,'',body);
   assert.match(r.reply,type==='dropoff'?/unchain your load/:/name, release number, and carrier name/,body);
  }
 });
}
test('typos work alongside complete driver information',()=>{
 const r=nextCheckin({body:'Lindsey, Center Express, 676767, pikup',conversationCreated:true});
 assert.equal(r.contact.full_name,'Lindsey');assert.equal(r.contact.driver_company,'Center Express');assert.equal(r.releaseNumber,'676767');assert.equal(r.contact.onboarding_step,'ready');assert.equal(r.reply,'');
});
test('unrelated words do not become arrival types',()=>{
 for(const s of ['Dick','Rick','photo','backup','drop','off','group','truck','stop','Pico','I will call the office','thanks'])assert.equal(visitType(s),null,s);
});
test('mixed arrival types remain ambiguous and release intent is retained',()=>{
 assert.equal(visitType('pikup or dropp off'),null);
 assert.equal(visitType('release 12345'),'pickup');assert.equal(visitType('12345'),'pickup');
});

test('Both selection, combined intent, and subsequent details preserve a single Both visit',()=>{
 for(const body of ['BOTH','both, Lindsey, MLM, 967433','dropoff and pickup','drop off & pick up','pikup and dropp off']){
  const r=nextCheckin({body,conversationCreated:true,now:new Date('2026-10-09T14:00:00Z')});
  assert.equal(r.visitType,'both',body);assert.match(r.reply,/drop off first/);
  assert(!['and','both'].includes(r.contact.full_name.toLowerCase()));
  const details=nextCheckin({existing:r.contact,body:'Lindsey, MLM, 967433'});
  assert.equal(details.visitType,'both');assert.equal(details.contact.onboarding_step,'ready');assert.equal(details.reply,'');
  assert.equal(nextCheckin({existing:details.contact,body:'967434'}).visitType,'both');
  assert.equal(nextCheckin({existing:details.contact,body:'Both'}).reply,'');
  assert.equal(nextCheckin({existing:details.contact,body:'dropoff'}).reply,'');
 }
 const closed=nextCheckin({body:'Both',conversationCreated:true,now:new Date('2026-10-09T22:02:00Z')});
 assert.match(closed.reply,/Receiving is closed/);assert(!closed.reply.includes('unchain'));
 const fresh=nextCheckin({existing:{visit_type:'both'},body:'pickup',conversationCreated:true});assert.equal(fresh.visitType,'pickup');
});

test('combined visit phrase does not become the driver name',()=>{
 for(const intent of ['drop off and pick up','pick up & drop off','drop off then pick up']){
  const r=nextCheckin({body:intent+', Lindsey, MLM, 967433',conversationCreated:true,now:new Date('2026-10-09T14:00:00Z')});
  assert.equal(r.visitType,'both');assert.equal(r.contact.full_name,'Lindsey');assert.equal(r.contact.driver_company,'MLM');assert.equal(r.contact.last_release_number,'967433');
 }
});
