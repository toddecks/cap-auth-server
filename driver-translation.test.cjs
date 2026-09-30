const {test}=require('node:test');
const assert=require('node:assert/strict');
const {translateMessage}=require('./driver-translation');
function provider(text,inspect=()=>{}){return {chat:{completions:{async create(request){inspect(request);return {choices:[{message:{content:text},finish_reason:'stop'}]}}}}};}
test('French incoming translated for Shipping without changing driver text',async()=>{
 const m={direction:'driver_to_shipping',body:'Je suis ici pour récupérer une commande.',original_language:'Français'};
 const r=await translateMessage(m,'Français',provider('I am here to pick up an order.',req=>{assert.match(req.messages[0].content,/French into English/);assert.equal(req.store,false)}));
 assert.deepEqual(r,{shipping_body:'I am here to pick up an order.'});assert.equal(m.body,'Je suis ici pour récupérer une commande.');
});
test('automatic and manual outgoing messages translated for driver, English kept for Shipping',async()=>{
 const m={direction:'shipping_to_driver',body:'Please stay on the left side.'};
 const r=await translateMessage(m,'Français',provider('Veuillez rester du côté gauche.',req=>assert.match(req.messages[0].content,/English into French/)));
 assert.equal(r.shipping_body,m.body);assert.equal(r.translated_language,'Français');assert.equal(r.translated_body,'Veuillez rester du côté gauche.');
});
test('English and structured release messages do not need the provider',async()=>{
 assert.deepEqual(await translateMessage({direction:'driver_to_shipping',body:'Release number: TESTING'},'Français',null),{shipping_body:'Release number: TESTING'});
 assert.equal((await translateMessage({direction:'shipping_to_driver',body:'Hello'},'English',null)).translated_body,'Hello');
});
test('provider failures do not silently mark untranslated content complete',async()=>{
 await assert.rejects(()=>translateMessage({direction:'shipping_to_driver',body:'Hello'},'Français',null),/not configured/);
 await assert.rejects(()=>translateMessage({direction:'shipping_to_driver',body:'Hello'},'Français',provider('')),/empty/);
});
