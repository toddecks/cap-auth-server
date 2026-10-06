'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {retrieve}=require('./sad-knowledge');
const {validate,makeContext,fingerprint}=require('./sad-diagnosis');
const docs=[{id:'maintenance',filename:'M1055 Maintenance.pdf',page_count:5,searchable_pages:2,pages:[{page:1,text:'Coil car . . . . . . page 20\nCoil car . . . . . . page 21\nCoil car . . . . . . page 22\nCoil car . . . . . . page 23\n'+'. '.repeat(100)},{page:2,text:'Coil stage and coil car troubleshooting. Check recorded position readings before deciding what to inspect. '.repeat(5)}]}];
const group={id:'entry',name:'Entry coil car',patterns:[{alarm_type:'AL',message:'Height signal out of range',plc_tag:'ENTRY',current:10,previous:2}],reports:[{submission_id:3,report_date:'2026-10-05',shift:'1',text:'Coil car observation',operator:'Private Name'}]};
const review={days:7,start:'2026-09-29',end:'2026-10-06',latest:'2026-10-06',coverage:{partial:false}};
test('retrieval excludes dotted contents and retains troubleshooting evidence',()=>{const hits=retrieve(docs,group);assert.equal(hits.length,1);assert.equal(hits[0].page,2);});
test('report context omits operator identity',()=>{const context=makeContext(review,group,docs);assert.ok(!JSON.stringify(context).includes('Private Name'));assert.ok(context.sources.some(s=>s.kind==='shift_report'));});
const context={sources:[{id:'A1',kind:'alarm'},{id:'M1',kind:'manual',excerpt:'Coil car troubleshooting says contact the manufacturer.'}]};
const valid=()=>({confidence:'low',causes:[{sourceIds:['A1','M1'],manualEvidence:[{sourceId:'M1',quote:'contact the manufacturer.'}]}],checks:[],watchFor:[],gaps:[]});
test('unknown citations and fabricated manual quotations are rejected',()=>{let v=valid();v.causes[0].sourceIds.push('M999');assert.throws(()=>validate(v,context),/unsupported citation/);v=valid();v.causes[0].manualEvidence[0].quote='The height sensor has failed.';assert.throws(()=>validate(v,context),/quotation/);});
test('a cause needs both observed alarms and manual support',()=>{const v=valid();v.causes[0].sourceIds=['M1'];assert.throws(()=>validate(v,context),/alarm and manual/);});
test('insufficient evidence with no causes is a valid result',()=>assert.doesNotThrow(()=>validate({confidence:'low',causes:[],checks:[],watchFor:[],gaps:['Exact alarm meaning unavailable']},context)));
test('cache separates restricted report contexts and changes after feedback',()=>{const c=makeContext(review,group,docs);assert.notEqual(fingerprint({...c,reportsIncluded:true}),fingerprint({...c,reportsIncluded:false}));assert.notEqual(fingerprint(c),fingerprint(makeContext(review,group,docs,[{note:'Wiring inspected',outcome:'no_issue_found'}])));});

test('reads structured output when the SDK convenience field is absent',async()=>{
 const {generate}=require('./sad-diagnosis');
 const answer={headline:'Evidence needed',summary:'Current evidence is insufficient.',priority:'insufficient_evidence',confidence:'low',causes:[],checks:[],watchFor:[],gaps:['No observations']};
 const client={responses:{create:async()=>({status:'completed',output:[{type:'message',content:[{type:'output_text',text:JSON.stringify(answer)}]}]})}};
 assert.deepEqual(await generate(client,{sources:[]}),answer);
});

test('retries source validation once and never returns an unverified draft',async()=>{
 const {generate}=require('./sad-diagnosis');let calls=0;
 const answer={headline:'Evidence needed',summary:'Current evidence is insufficient.',priority:'insufficient_evidence',confidence:'low',causes:[],checks:[],watchFor:[],gaps:[]};
 const invalid={...answer,checks:[{check:'Unsupported',purpose:'',expectedFinding:'',owner:'operator_observation',sourceIds:['fake']}]};
 const client={responses:{create:async()=>({status:'completed',output_text:JSON.stringify(++calls===1?invalid:answer)})}};
 assert.deepEqual(await generate(client,{sources:[]}),answer);assert.equal(calls,2);
 calls=0;client.responses.create=async()=>{calls++;return{status:'completed',output_text:JSON.stringify(invalid)}};
 await assert.rejects(()=>generate(client,{sources:[]}),/unsupported citation/);assert.equal(calls,2);
});
