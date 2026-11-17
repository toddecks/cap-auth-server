'use strict';
const sql = require('mssql');
const {randomUUID} = require('node:crypto');

function config(env=process.env) {
  for (const name of ['PSDATA_SQL_USER','PSDATA_SQL_PASSWORD']) if (!env[name]) throw new Error(`Missing ${name}`);
  return {server:env.PSDATA_SQL_SERVER || 'psdata-azure.database.windows.net',
    database:env.PSDATA_SQL_DATABASE || 'psdata-azure',user:env.PSDATA_SQL_USER,password:env.PSDATA_SQL_PASSWORD,
    port:1433,connectionTimeout:60000,requestTimeout:120000,
    options:{encrypt:true,trustServerCertificate:false,readOnlyIntent:true},pool:{min:0,max:1}};
}
function normalize(rows) {
  const ids=new Set();
  return rows.map(row=>{
    const id=Number(row.setup_number), day=String(row.entered_date || '');
    if(!Number.isSafeInteger(id)||id<1||!/^\d{4}-\d{2}-\d{2}$/.test(day)||new Date(day+'T00:00:00Z').toISOString().slice(0,10)!==day) throw new Error('Invalid Azure order-entry row');
    if(ids.has(id))throw new Error('Duplicate Azure setup number');
    ids.add(id);
    return {setup_number:id,entered_date:day,entered_by:String(row.entered_by||'').trim().replace(/\s+/g,' ')||null,
      source_input_date:Number(row.source_input_date),source_status:String(row.source_status||'').trim()||null,
      source_order_type:String(row.source_order_type||'').trim()||null};
  });
}
async function readAzure(env=process.env) {
  const start=env.PSDATA_ORDER_ENTRY_START_DATE || '2025-01-01';
  if(!/^\d{4}-\d{2}-\d{2}$/.test(start))throw new Error('Invalid order-entry start date');
  const pool=await new sql.ConnectionPool(config(env)).connect();
  try {
    const result=await pool.request().input('startDate',sql.Date,start).query(`
      SELECT SETUPNUMBER AS setup_number, LTRIM(RTRIM(ENTEREDBY)) AS entered_by,
        CONVERT(varchar(10),DATEADD(day,INPUTDATE,CONVERT(date,'18001228',112)),23) AS entered_date,
        INPUTDATE AS source_input_date,STATUS AS source_status,WORKORDERSETUPFLAG AS source_order_type
      FROM dbo.SetupHdr
      WHERE INPUTDATE >= DATEDIFF(day,CONVERT(date,'18001228',112),@startDate)
        AND INPUTDATE BETWEEN 1 AND 200000
      ORDER BY SETUPNUMBER;`);
    return {rows:normalize(result.recordset),startDate:start};
  } finally {await pool.close();}
}
async function sync(env=process.env) {
  // Never log configuration, SQL credentials, request headers, or provider error messages.
  const snapshot=await readAzure(env);
  if(!snapshot.rows.length)throw new Error('Empty Azure snapshot; previous data retained');
  const max=snapshot.rows.reduce((last,r)=>r.entered_date>last?r.entered_date:last,'');
  if(env.PSDATA_ORDER_ENTRY_INSPECT==='true'){
    const people={};for(const r of snapshot.rows)people[r.entered_by||'Not recorded']=(people[r.entered_by||'Not recorded']||0)+1;
    console.log(JSON.stringify({rows:snapshot.rows.length,startDate:snapshot.startDate,sourceMaxDate:max,employees:people}));return;
  }
  const url=String(env.SUPABASE_URL||'').replace(/\/$/,'');
  const key=env.SUPABASE_SERVICE_ROLE_KEY;
  if(!url||!key)throw new Error('Supabase sync credentials missing');
  const database=async(path,method,body)=>{
    const res=await fetch(`${url}/rest/v1/${path}`,{method,headers:{apikey:key,Authorization:`Bearer ${key}`,'Content-Type':'application/json',Prefer:'return=minimal'},body:JSON.stringify(body),signal:AbortSignal.timeout(60000)});
    if(!res.ok)throw new Error(`Order-entry storage HTTP ${res.status}`);
  };
  const id=randomUUID();
  await database('psdata_order_entry_sync_runs','POST',{id,status:'running',source_start_date:snapshot.startDate,source_max_date:max});
  try {
    for(let i=0;i<snapshot.rows.length;i+=500)await database('psdata_order_entries','POST',snapshot.rows.slice(i,i+500).map(row=>({...row,sync_run_id:id})));
    await database('rpc/finalize_psdata_order_entry_sync','POST',{run_id:id,expected_rows:snapshot.rows.length});
    console.log(`Order-entry sync complete: ${snapshot.rows.length} orders; source through ${max}.`);
    return {rows:snapshot.rows.length,sourceThrough:max};
  }catch(error){
    await database(`psdata_order_entry_sync_runs?id=eq.${id}`,'PATCH',{status:'failed',finished_at:new Date().toISOString(),error_code:error.code||'SYNC_FAILED'}).catch(()=>{});
    throw error;
  }
}
module.exports={config,normalize,readAzure,sync};
if(require.main===module)sync().catch(error=>{
  let detail=String(error.message||error.name||'Unknown error');
  for(const key of ['PSDATA_SQL_PASSWORD','SUPABASE_SERVICE_ROLE_KEY'])if(process.env[key])detail=detail.split(process.env[key]).join('[redacted]');
  console.error('Order-entry sync failed:',error.code||'SYNC_FAILED',detail.slice(0,400));process.exitCode=1;
});
