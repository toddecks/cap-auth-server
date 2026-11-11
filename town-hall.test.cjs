const {test}=require('node:test'),assert=require('node:assert/strict');
const {validate,emailHtml,register,RECIPIENTS}=require('./town-hall');
const token='d32842ee-4bfc-4a13-83a1-6ccf60ba1081';
const input={submissionToken:token,questions:'What training will we receive?'};
const escape=s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
test('anonymous submissions need no name, email, phone, or login',()=>{
 const row=validate(input);assert.equal(row.employee_name,null);assert.equal(row.questions,input.questions);assert.equal(row.payload_hash.length,64);assert.equal('email' in row,false);
 for(const key of ['questions','concerns','leadershipNotes'])assert.doesNotThrow(()=>validate({submissionToken:token,[key]:'A response'}));
});
test('validation rejects empty forms, bad tokens, oversized answers, and structured inputs',()=>{
 for(const body of [{submissionToken:token},{...input,submissionToken:'bad'},{...input,questions:'x'.repeat(6001)},{...input,concerns:{}},{...input,employeeName:'x'.repeat(161)}])assert.throws(()=>validate(body));
 assert.equal(validate({...input,employeeName:'  Kim  '}).employee_name,'Kim');
});
test('notification escapes content, labels anonymous submissions and sends only to Kim',()=>{
 const html=emailHtml(validate({...input,questions:'<script>bad()</script>\nA question'}),escape);
 assert(html.includes('&lt;script&gt;'));assert(!html.includes('<script>'));assert(html.includes('Anonymous'));assert(html.includes('white-space:pre-wrap'));assert.deepEqual(RECIPIENTS,['kim@coilsteelprocessing.com']);
});
function fixture({failEmail=false}={}){
 let handler,stored=null,sends=0;const sent=[];
 const db={from(table){assert.equal(table,'town_hall_submissions');let action='select',payload;const q={select(){return q},eq(){return q},maybeSingle:async()=>({data:stored}),insert(row){action='insert';payload=row;return q},single:async()=>{stored={...payload};return{data:stored}},update(row){action='update';payload=row;return q},then(resolve){if(action==='update')Object.assign(stored,payload);return Promise.resolve({data:stored}).then(resolve)}};return q;}};
 register({post(path,fn){assert.equal(path,'/api/town-hall/submissions');handler=fn;}},{db,consumeAttempt:()=>true,escapeHtml:escape,sendEmail:async msg=>{sends++;sent.push(msg);if(failEmail){failEmail=false;throw Error('Resend unavailable');}return 'email-accepted';}});
 const submit=async body=>{const res={code:200,set(){return this},status(c){this.code=c;return this},json(payload){this.payload=payload;return this}};await handler({body},res);return res;};
 return{submit,get:()=>({stored,sends,sent})};
}
test('request saves responses and emails once across successful retries',async()=>{
 const f=fixture();assert.equal((await f.submit(input)).code,200);assert.equal((await f.submit(input)).code,200);assert.equal(f.get().sends,1);assert.equal(f.get().stored.employee_name,null);assert(f.get().stored.email_sent_at);assert.deepEqual(f.get().sent[0].to,RECIPIENTS);
 assert.equal((await f.submit({...input,questions:'Changed'})).code,409);assert.equal(f.get().stored.questions,input.questions);
});
test('Resend failures preserve the response; retry uses the same idempotency key',async()=>{
 const f=fixture({failEmail:true});assert.equal((await f.submit(input)).code,503);assert(f.get().stored);assert.equal((await f.submit(input)).code,200);assert.equal(f.get().sent[0].idempotencyKey,f.get().sent[1].idempotencyKey);
});
test('honeypot and invalid requests do not send notifications',async()=>{
 const f=fixture();assert.equal((await f.submit({...input,website:'spam'})).code,200);assert.equal((await f.submit({})).code,400);assert.equal(f.get().sends,0);
});
