const {test}=require('node:test'),assert=require('node:assert/strict');
const {normalizePhone,lookupDriver,createHandler}=require('./manual-driver-lookup');
function fixture({contact=null,profile=null,arrivals=[],role='shipping',failure=false}={}){
 const reads=[];const tables={driver_sms_contacts:contact?[{phone_e164:'+14195550123',...contact}]:[],driver_profiles:profile?[{phone_e164:'+14195550123',...profile}]:[],driver_sms_arrivals:arrivals};
 const db={auth:{getUser:async()=>({data:{user:{id:'clerk',app_metadata:{csp_role:role}}}})},from(table){reads.push(table);let filters=[],one=false;const q={select(){return q},eq(k,v){filters.push(r=>r[k]===v);return q},order(){return q},limit(){return q},maybeSingle(){one=true;return q},then(resolve){const rows=tables[table].filter(r=>filters.every(f=>f(r)));return Promise.resolve(failure?{error:{message:'offline'}}:{data:one?rows[0]||null:rows}).then(resolve)}};return q;}};
 return{db,reads};
}
test('phone formats share the manual check-in normalization',()=>{for(const x of ['4195550123','(419) 555-0123','1-419-555-0123','+14195550123'])assert.equal(normalizePhone(x),'+14195550123');assert.equal(normalizePhone('419555'),'');});
test('saved SMS contact fills identity without exposing the old release or querying unrelated history',async()=>{
 const f=fixture({contact:{full_name:'This is John',driver_company:'JLS',last_release_number:'OLD-123'}});
 assert.deepEqual(await lookupDriver(f.db,'+14195550123'),{found:true,driverName:'John',driverCompany:'JLS'});assert.deepEqual(f.reads,['driver_sms_contacts']);
});
test('app profile and arrival history fill missing fields while ignoring placeholders and acknowledgements',async()=>{
 const f=fixture({contact:{full_name:'Copy',driver_company:'?'},profile:{full_name:'Alex',driver_company:''},arrivals:[{phone_e164:'+14195550123',driver_name:'?',driver_company:'Not provided'},{phone_e164:'+14195550123',driver_name:'Alex',driver_company:'Drew Trucking',release_number:'OLD'}]});
 assert.deepEqual(await lookupDriver(f.db,'+14195550123'),{found:true,driverName:'Alex',driverCompany:'Drew Trucking'});
});
test('a different phone cannot retrieve someone else’s driver record',async()=>{const f=fixture({contact:{full_name:'John',driver_company:'JLS'}});assert.deepEqual(await lookupDriver(f.db,'+14195559999'),{found:false,driverName:'',driverCompany:''});});
async function request(f,phone='4195550123',authorization='Bearer staff'){let code=200,body;await createHandler(f)({headers:{authorization},body:{phone}},{set(){},status(n){code=n;return this},json(x){body=x;return this}});return{code,body};}
test('only signed-in shipping/admin clerks can look up a complete phone number',async()=>{
 for(const role of ['driver','employee']){const f=fixture({role});assert.equal((await request(f)).code,403);assert.equal(f.reads.length,0);}
 const f=fixture();assert.equal((await request(f,'4195550123','')).code,401);assert.equal((await request(f,'419')).code,400);assert.equal(f.reads.length,0);
 for(const role of ['shipping','admin'])assert.equal((await request(fixture({role}))).code,200);
});
test('lookup failure permits manual entry with a clear error, rather than claiming no match',async()=>{assert.equal((await request(fixture({failure:true}))).code,503);});
