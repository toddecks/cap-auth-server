import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const source=readFileSync(new URL('../scripts/sync-psdata-inventory.mjs',import.meta.url),'utf8').replace('import process from "node:process";','').split('main().catch')[0];
function harness(fetch){
 const delays=[],warnings=[];
 const context=vm.createContext({process:{env:{PSDSTEEL_API_USERNAME:'test',PSDSTEEL_API_PASSWORD:'test',SUPABASE_URL:'https://db.test',SUPABASE_SERVICE_ROLE_KEY:'test'}},fetch,AbortController,console:{log(){},warn(s){warnings.push(s)}},setTimeout,clearTimeout});
 vm.runInContext(source.replace('const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));','const sleep = async (milliseconds) => delays.push(milliseconds);'),Object.assign(context,{delays}));
 return {run:s=>vm.runInContext(s,context),delays,warnings};
}
test('temporary database failures back off and recover with an exact HEAD count',async()=>{
 let calls=0;const h=harness(async (url,opts)=>{assert.equal(opts.method,'HEAD');calls++;return calls<3?new Response('',{status:503}):new Response(null,{headers:{'content-range':'0-0/4299'}})});
 assert.equal(await h.run('getCurrentActiveCount()'),4299);assert.deepEqual(h.delays,[15000,30000]);assert.equal(h.warnings.length,2);
});
test('persistent outage stops before downloading or writing inventory',async()=>{
 let calls=0;const h=harness(async(url,opts)=>{calls++;assert.equal(opts.method,'HEAD');assert.match(url,/psdata_cust_inv/);return new Response('',{status:522})});
 await assert.rejects(h.run('main()'),/Supabase active-count check.*HTTP 522.*attempt 5\/5/);assert.equal(calls,5);assert.deepEqual(h.delays,[15000,30000,60000,120000]);
});
test('authorization failures are not retried',async()=>{
 const h=harness(async()=>new Response('',{status:401}));await assert.rejects(h.run('getCurrentActiveCount()'),/HTTP 401.*attempt 1\/5/);assert.equal(h.delays.length,0);
});
test('aborted requests identify stage and timeout',async()=>{
 const h=harness(async(url,opts)=>{await new Promise((resolve,reject)=>{opts.signal.addEventListener('abort',()=>reject(new Error('This operation was aborted')))});});
 h.run('config.supabaseRequestTimeoutMs=5;config.supabaseRequestAttempts=1');
 await assert.rejects(h.run('getCurrentActiveCount()'),/Supabase active-count check: timed out after 0.005s/);
});
test('incomplete inventory still cannot pass the safety checks',()=>{
 const h=harness(()=>{throw Error('Unexpected network')});assert.throws(()=>h.run('validateSnapshot(200,4299)'),/Safety stop/);assert.throws(()=>h.run('validateSnapshot(1100,4299)'),/Safety stop/);
});
