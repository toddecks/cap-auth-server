const {test}=require('node:test'), assert=require('node:assert/strict');
const {visitType}=require('./driver-visit-flow');
const {nextCheckin}=require('./sms-checkin-details');
const {startsNewVisit}=require('./visit-session');
const signWords={Spanish:{dropoff:'entrega',pickup:'recogida',both:'ambas'},Russian:{dropoff:'доставка',pickup:'получение',both:'и то и другое'},Ukrainian:{dropoff:'доставка',pickup:'отримання',both:'і те, й інше'},French:{dropoff:'dépôt',pickup:'retrait',both:'les deux'}};
for(const [language,words] of Object.entries(signWords))for(const [type,word] of Object.entries(words)){
 test(language+' sign: '+word+' → '+type,()=>{
  for(const body of [word,word.toUpperCase(),'  «'+word+'»!  ',word.normalize('NFD')]){
   assert.equal(visitType(body),type,body);
   const r=nextCheckin({body,conversationCreated:true,now:new Date('2026-10-09T14:00:00Z')});
   assert.equal(r.visitType,type);assert.equal(r.contact.full_name,'');assert.equal(r.contact.driver_company,'');
   assert.match(r.reply,type==='pickup'?/name, release number, and carrier name/:type==='both'?/unchain your load/:/unchain your load/);
   assert.equal(startsNewVisit({session_ended_at:'2026-10-08T14:00:00Z'},null,body),true);
  }
  const full=nextCheckin({body:word+', José, MLM, 967433',conversationCreated:true,now:new Date('2026-10-09T14:00:00Z')});
  assert.equal(full.contact.full_name,'José');assert.equal(full.contact.driver_company,'MLM');assert.equal(full.contact.last_release_number,'967433');assert.equal(full.contact.onboarding_step,'ready');
  if(type==='both')assert.equal(nextCheckin({existing:full.contact,body:'967434'}).visitType,'both');
 });
}
test('native app labels and unaccented French are accepted',()=>{
 for(const word of ['ambos','обидва','оба'])assert.equal(visitType(word),'both');
 for(const word of ['entregar','доставити','доставить','depot','livraison'])assert.equal(visitType(word),'dropoff');
 for(const word of ['recoger','забрати','забрать','enlèvement','enlevement'])assert.equal(visitType(word),'pickup');
});
test('combined foreign-language commands retain the driver details',()=>{
 for(const body of ['entrega y recogida','доставка и получение','доставка й отримання','dépôt et retrait']){
  const r=nextCheckin({body:body+', José, MLM, 967433',conversationCreated:true});
  assert.equal(r.visitType,'both');assert.equal(r.contact.full_name,'José');assert.equal(r.contact.driver_company,'MLM');
 }
 for(const body of ['entrega o recogida','доставка или получение','доставка або отримання','dépôt ou retrait'])assert.equal(visitType(body),null);
});
test('translated dropoff and Both obey receiving closed hours',()=>{
 for(const words of Object.values(signWords))for(const body of [words.dropoff,words.both]){
  const r=nextCheckin({body,conversationCreated:true,now:new Date('2026-10-09T22:02:00Z')});
  assert.match(r.reply,/Receiving is closed/);assert(!r.reply.includes('unchain'));
 }
});
test('commands do not match inside unrelated words or Cyrillic names',()=>{
 for(const text of ['Cambridge','Retraite','Ambassade','Доставкам','Получением','Отриманням','Обама','André','Dépôts','recogidas'])assert.equal(visitType(text),null,text);
});

test('Both initial response is exactly Drop-off, with no pickup details requested',()=>{
 for(const now of ['2026-10-09T14:00:00Z','2026-10-09T21:45:00Z','2026-10-09T22:02:00Z','2026-10-10T14:00:00Z']){
  const expected=require('./driver-visit-flow').dropoffReply(now);
  for(const body of ['Both','ambas','и то и другое','і те, й інше','les deux']){
   for(const existing of [undefined,{full_name:'Lindsey',driver_company:'MLM'}]){
    const result=nextCheckin({body,existing,conversationCreated:true,now:new Date(now)});
    assert.equal(result.reply,expected);assert.equal(result.visitType,'both');
    assert(!result.reply.includes('release number'));assert(!result.reply.includes('After unloading'));
   }
  }
 }
});
