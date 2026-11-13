const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { employeeConfirmationUrl } = require('./employee-confirmation');
test('signup confirmation stays on confirmation-only page', () => {
 const url = new URL(employeeConfirmationUrl({properties:{hashed_token:'sample/hash',action_link:'https://example.com/bi'}},'https://hr.coilsteelprocessing.com'));
 assert.equal(url.pathname,'/confirm.html');
 assert.equal(url.searchParams.get('token_hash'),'sample/hash');
 assert.equal(url.searchParams.get('type'),'email');
 assert.equal(url.searchParams.has('redirect_to'),false);
 assert.throws(()=>employeeConfirmationUrl({properties:{action_link:'https://example.com'}},'https://hr.coilsteelprocessing.com'));
});
test('Shipping app and Pickem permissions are retained by role resolver',async()=>{
 const source=fs.readFileSync(__dirname+'/server.js','utf8');
 const ctx={supabase:{from:()=>({upsert:async()=>({error:null})})}};
 vm.createContext(ctx);
 vm.runInContext(source.slice(source.indexOf('const ROLE_DEFINITIONS'),source.indexOf('const getBearerToken'))+';globalThis.resolve=resolveRoleIds;',ctx);
 assert.deepEqual(Array.from(await ctx.resolve([20,25])),[20,25]);
 assert.deepEqual(Array.from(await ctx.resolve(['pickem','website_leads'])),[24,19]);
 assert.deepEqual(Array.from(await ctx.resolve([999])),[]);
});
