const{test}=require('node:test'),assert=require('node:assert/strict'),{validate,register}=require('./fall-festival');
const valid={employeeName:'Test Employee',familyCount:'2',attendance:'yes',chili:'maybe',submissionToken:'b5d86fe9-303d-4b28-bd47-80d478367c64'};
test('validates counts and choices',()=>{assert.equal(validate(valid).family_count,2);for(const changes of [{familyCount:''},{familyCount:'-1'},{familyCount:'2.5'},{attendance:'bad'},{employeeName:''}])assert.throws(()=>validate({...valid,...changes}));assert.equal(validate({...valid,familyCount:'0',attendance:'no'}).family_count,0)});
test('retry saves and emails once; changed payload is rejected',async()=>{
 let handler,row=null,emails=0;
 const db={from(){let operation='read',value;const q={select(){return q},eq(){return q},maybeSingle:async()=>({data:row}),insert(v){operation='insert';value=v;return q},single:async()=>({data:row={...value}}),update(v){operation='update';value=v;return q},then(resolve){if(operation==='update')row={...row,...value};return Promise.resolve({error:null}).then(resolve)}};return q}};
 register({post(path,fn){handler=fn}},{db,sendEmail:async args=>{assert.equal(args.to,'lindsey@coilsteelprocessing.com');emails++;return 'test'},escapeHtml:x=>x,consumeAttempt:()=>true});
 const run=async(body)=>{const res={code:200,set(){},status(n){this.code=n;return this},json(v){this.value=v;return this}};await handler({body},res);return res};
 assert.equal((await run(valid)).code,200);assert.equal((await run(valid)).code,200);assert.equal(emails,1);assert.equal((await run({...valid,familyCount:'3'})).code,409);assert.equal(row.family_count,2);
});
