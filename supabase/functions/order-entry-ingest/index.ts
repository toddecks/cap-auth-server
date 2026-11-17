const EXPECTED='ef1df6a194893fd0da9f65b7f06025896c61ed91d312e3df98915e1ce02217f8';
Deno.serve(async(req)=>{
 const reply=(status,data)=>Response.json(data,{status});
 if(req.method!=='POST')return reply(405,{error:'Method not allowed'});
 const supplied=req.headers.get('x-csp-ingest-key')||'';
 const hash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(supplied)))).map(x=>x.toString(16).padStart(2,'0')).join('');
 let mismatch=0;for(let i=0;i<64;i++)mismatch|=hash.charCodeAt(i)^EXPECTED.charCodeAt(i);
 if(mismatch)return reply(401,{error:'Authentication required'});
 let runId;
 const url=Deno.env.get('SUPABASE_URL'),key=Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
 async function db(path,method,body){const r=await fetch(url+'/rest/v1/'+path,{method,headers:{apikey:key,Authorization:'Bearer '+key,'Content-Type':'application/json',Prefer:'return=minimal'},body:JSON.stringify(body)});if(!r.ok)throw new Error('STORAGE_'+r.status);}
 try {
 const payload=await req.json(),rows=payload.rows;
 if(!Array.isArray(rows)||rows.length<1||rows.length>100000||payload.startDate!=='2025-01-01')return reply(400,{error:'Invalid snapshot'});
 const ids=new Set();const clean=rows.map(r=>{
  if(!Number.isSafeInteger(r.setup_number)||r.setup_number<1||ids.has(r.setup_number)||!/^\d{4}-\d{2}-\d{2}$/.test(r.entered_date)||!Number.isInteger(r.source_input_date))throw new Error('INVALID_ROW');
  if(new Date(r.entered_date+'T00:00:00Z').toISOString().slice(0,10)!==r.entered_date)throw new Error('INVALID_DATE');
  ids.add(r.setup_number);
  return {setup_number:r.setup_number,entered_date:r.entered_date,entered_by:String(r.entered_by||'').slice(0,200)||null,source_input_date:r.source_input_date,source_status:String(r.source_status||'').slice(0,20),source_order_type:String(r.source_order_type||'').slice(0,20)};
 });
 runId=crypto.randomUUID();const max=clean.reduce((a,r)=>r.entered_date>a?r.entered_date:a,'');
 await db('psdata_order_entry_sync_runs','POST',{id:runId,status:'running',source_start_date:payload.startDate,source_max_date:max});
 for(let i=0;i<clean.length;i+=500)await db('psdata_order_entries','POST',clean.slice(i,i+500).map(r=>({...r,sync_run_id:runId})));
 await db('rpc/finalize_psdata_order_entry_sync','POST',{run_id:runId,expected_rows:clean.length});
 return reply(200,{ok:true,rows:clean.length,sourceThrough:max});
 }catch(e){if(runId)await db('psdata_order_entry_sync_runs?id=eq.'+runId,'PATCH',{status:'failed',finished_at:new Date().toISOString(),error_code:'INGEST_FAILED'}).catch(()=>{});return reply(500,{error:'Snapshot import failed'});}
});