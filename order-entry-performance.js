'use strict';
const dateOnly=s=>typeof s==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(s)&&Number.isFinite(Date.parse(s+'T00:00:00Z'))&&new Date(s+'T00:00:00Z').toISOString().slice(0,10)===s;
function handler(db){return async(req,res)=>{
  const {start,end}=req.query;
  if(!dateOnly(start)||!dateOnly(end)||start>end||(Date.parse(end)-Date.parse(start))/86400000>370)return res.status(400).json({error:'Choose a reporting period of up to one year.'});
  if(!db)return res.status(503).json({error:'Order-entry storage is unavailable.'});
  try{
    const {data:runs,error:runError}=await db.from('psdata_order_entry_sync_runs').select('id,finished_at,source_start_date,source_max_date').eq('status','complete').order('finished_at',{ascending:false}).limit(1);
    if(runError)throw runError;
    const run=runs?.[0];
    if(!run)return res.status(503).json({error:'The first order-entry refresh has not completed yet.'});
    if(start<run.source_start_date)return res.status(400).json({error:`Order-entry history starts ${run.source_start_date}.`});
    const rows=[];
    for(let offset=0;;offset+=1000){
      const {data,error}=await db.from('psdata_order_entries').select('setup_number,entered_by,entered_date').eq('sync_run_id',run.id).gte('entered_date',start).lte('entered_date',end).order('setup_number').range(offset,offset+999);
      if(error)throw error;
      rows.push(...data);if(data.length<1000)break;
    }
    res.set('Cache-Control','private, no-store');
    return res.json({rows,lastSyncedAt:run.finished_at,sourceThrough:run.source_max_date,historyStarts:run.source_start_date});
  }catch(error){console.error('Order-entry report failed:',error.code||'STORAGE_ERROR');return res.status(503).json({error:'Unable to load order entries. Please try again shortly.'});}
};}
module.exports={handler,dateOnly};
