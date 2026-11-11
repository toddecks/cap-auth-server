const {test}=require('node:test'),assert=require('node:assert/strict');
const {resolveCombo}=require('./combo-loads');
const {doorMatches}=require('./release-doors');
const {lookupRelease,processAttempts,ACCEPTED,ENABLED_AT}=require('./release-attempts');
const {confirmedDeparture}=require('./ps-departures');
const rows=[94971,95018,95019,95020,95021,95022].map((bol,i)=>({poRel:`1327575-${i+1}`,bolNumber:String(bol),masterBolNumber:'2006826',scheduleDate:'2026-11-11',scheduleTime:'16:00:00',arrivalDate:'2026-11-11',arrivalTime:'13:14:00',shipDate:null,shipTime:null,location:'PROSPY',unloadingDoor:'BD',customerNo:'LANEST',carrierName:'SHERI-RAE LLC'}));
const match=(r,input='1327575')=>doorMatches(r,[input],'2026-11-11')[input.replace(/[^A-Z0-9]/gi,'')+':PROSPY'];
test('Lane Steel base release selects first child and BOL independently of source order',()=>{
 for(const r of [rows,[...rows].reverse()]){const m=match(r);assert.equal(m.status,'matched');assert.equal(m.match_type,'exact');assert.equal(m.release_reference,'1327575-1');assert.equal(m.bol_number,'94971');assert.equal(m.master_bol_number,'2006826');assert.equal(m.appointment_at,'2026-11-11T16:00:00');assert.equal(m.door,'BD');assert.equal(m.combo_bol_numbers.length,6);}
 assert.equal(match(rows,'1327575-2').bol_number,'95018');
});
for(const field of ['masterBolNumber','customerNo','carrierName','location','unloadingDoor'])test(`conflicting ${field} does not resolve a combo`,()=>assert.equal(resolveCombo([rows[0],{...rows[1],[field]:'OTHER'}],'1327575'),null));
test('missing or zero master BOL, absent first child and duplicate child BOL conflicts remain unresolved',()=>{
 for(const master of [null,'','0'])assert.equal(resolveCombo(rows.map(r=>({...r,masterBolNumber:master})),'1327575'),null);
 assert.equal(resolveCombo(rows.slice(1),'1327575'),null);
 assert.equal(resolveCombo([...rows,{...rows[0],bolNumber:'99999'}],'1327575'),null);
 assert.equal(resolveCombo(rows,'132757'),null);
 assert.equal(match(rows.map(r=>({...r,masterBolNumber:null}))).status,'ambiguous');
});
function chart(r){return{from(){const q={select(){return q},ilike(){return q},order(){return q},range(){return Promise.resolve({data:r})}};return q}}}
test('SMS accepts driver details with combo release instead of sending ambiguity message',async()=>{
 const c={id:'combo-test',visit_type:'pickup',facility_id:'csp-toledo-main'},sent=[];
 assert.equal(await lookupRelease(chart(rows),'1327575',c,ENABLED_AT+60000),'matched');
 const r=await processAttempts({c,messages:[{id:1,body:'Gary. 1327575. Sheri Rae',sent_at:new Date(ENABLED_AT+60000).toISOString(),direction:'driver_to_shipping'}],pickupAt:ENABLED_AT,now:()=>ENABLED_AT+61000,lookup:(_,release)=>lookupRelease(chart(rows),release,c,ENABLED_AT),send:async(_,kind,body)=>sent.push(body)});
 assert.equal(r.state,'matched');assert.deepEqual(sent,[ACCEPTED]);
});
test('combo stays on property until every child has shipped, then uses final departure',()=>{
 const visit={release_number:'1327575',facility_id:'csp-toledo-main',visit_type:'pickup',entered_at:'2026-11-11T18:14:00Z',checked_in_at:'2026-11-11T18:14:00Z'};
 const now=Date.parse('2026-11-11T22:00:00Z');
 const shipped=rows.map((r,i)=>({...r,shipDate:'2026-11-11',shipTime:`15:0${i}:00`}));
 assert.equal(confirmedDeparture(visit,rows,now),null);
 assert.equal(confirmedDeparture(visit,[...shipped.slice(0,-1),rows.at(-1)],now),null);
 assert.deepEqual(confirmedDeparture(visit,shipped,now),{departedAt:'2026-11-11T20:05:00.000Z',bolNumber:'94971'});
 assert.equal(confirmedDeparture(visit,shipped.map((r,i)=>({...r,masterBolNumber:i?'other':'2006826'})),now),null);
});
