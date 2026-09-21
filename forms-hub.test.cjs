const{test}=require('node:test'),assert=require('node:assert/strict'),{allowed,csv,register}=require('./forms-hub');
test('access is admin or exact configured recipient',()=>{const f={recipients:['lindsey@coilsteelprocessing.com']};assert(allowed(f,{email:'Lindsey@coilsteelprocessing.com'},[]));assert(allowed(f,{email:'someone@x.com'},['admin']));assert(!allowed(f,{email:'someone@x.com'},['hr_admin']));assert(!allowed(f,{email:'lindsey@coilsteelprocessing.com.evil'},[]))});
test('CSV quotes text, preserves nested values, and neutralizes formulas',()=>{const result=csv([{name:'A,"B"',comment:'=1+1',payload:{answer:'line1\nline2'}}],['name']);assert(result.startsWith('\uFEFF'));assert(result.includes('"A,""B"""'));assert(result.includes("'"+'=1+1'));assert(result.includes('payload.answer'));assert(result.includes('line1\nline2'))});
test('authorized export reads beyond first 1000 rows, unauthorized export denied',async()=>{
 const routes={};let queries=0;const db={from(){queries++;let start=0,end=0;const q={select(){return q},eq(){return q},lte(){return q},order(){return q},range(a,b){start=a;end=b;return q},then(resolve){return Promise.resolve({data:Array.from({length:Math.max(0,Math.min(end+1,1201)-start)},(_,i)=>({employee_name:'Person '+(start+i)}))}).then(resolve)}};return q}};
 register({get(path,...handlers){routes[path]=handlers}},{auth:db,chart:db,roleRows:async()=>[],roleNames:x=>x,recipients:{fantasy:[],shift:[],forklift:[],crane:[],pro:[],expansion:[],maintenance:[]}});
 const handler=routes['/api/forms-hub/:id.csv'].at(-1);const res={code:200,set(){},status(n){this.code=n;return this},json(v){this.value=v},send(v){this.value=v}};
 await handler({params:{id:'fall-festival'},formsUser:{email:'unrelated@csp.com'},formsRoles:[]},res);assert.equal(res.code,403);assert.equal(queries,0);
 res.code=200;await handler({params:{id:'fall-festival'},formsUser:{email:'lindsey@coilsteelprocessing.com'},formsRoles:[]},res);assert.equal(res.code,200);assert(res.value.includes('Person 1200'));assert.equal(queries,3);
});
test('Kim can list and export Fall Festival without access to other forms',async()=>{
 const routes={},queried=[];
 const db={from(table){queried.push(table);const q={select(){return q},eq(){return q},lte(){return q},order(){return q},range(){return q},then(resolve){return Promise.resolve({data:[{employee_name:'Test attendee'}],count:1}).then(resolve)}};return q}};
 register({get(path,...handlers){routes[path]=handlers}},{auth:db,chart:db,roleRows:async()=>[],roleNames:x=>x,recipients:{fantasy:[],shift:[],forklift:[],crane:[],pro:[],expansion:[],maintenance:[]}});
 const req={formsUser:{email:'Kim@coilsteelprocessing.com'},formsRoles:[]};
 const res={code:200,set(){},status(n){this.code=n;return this},json(v){this.value=v},send(v){this.value=v}};
 await routes['/api/forms-hub'].at(-1)(req,res);
 assert.deepEqual(res.value.forms.map(f=>f.id),['fall-festival']);
 await routes['/api/forms-hub/:id.csv'].at(-1)({...req,params:{id:'fall-festival'}},res);
 assert.equal(res.code,200);assert(res.value.includes('Test attendee'));
 const readCount=queried.length;
 await routes['/api/forms-hub/:id.csv'].at(-1)({...req,params:{id:'todd-requests'}},res);
 assert.equal(res.code,403);assert.equal(queried.length,readCount);
 await routes['/api/forms-hub/:id.csv'].at(-1)({...req,formsUser:{email:'kim@coilsteelprocessing.com.evil'},params:{id:'fall-festival'}},res);
 assert.equal(res.code,403);assert.equal(queried.length,readCount);
});
