'use strict';
const dashboardRoles=['admin','production','shipping_overview','shipping_performance','receiving','inventory','customer_summary','bonus_report','iso','alarm_logs','website_leads','quote_calculator','work_order_pricing','todd_requests'];
const metrics=['shipping','production','appointments','orders','receiving'];
function allowedMetrics(roles){return dashboardRoles.some(role=>roles.includes(role))?[...metrics]:[];}
function handler(db){return async(req,res)=>{
 const period=req.query.period||'day',scope=req.query.scope||'company',rawYear=req.query.year;
 const year=rawYear==null||rawYear==='all'?null:Number(rawYear);
 if(!['day','month'].includes(period)||!['company','people'].includes(scope)||(year!==null&&(!/^\d{4}$/.test(String(rawYear))||year<2000||year>2100)))return res.status(400).json({error:'Choose a valid period and year.'});
 const metrics=allowedMetrics(req.authRoles||[]);if(!metrics.length)return res.status(403).json({error:'BI report access is required.'});
 if(!db)return res.status(503).json({error:'Records are temporarily unavailable.'});
 try{
  const {data,error}=await db.rpc('get_bi_record_rankings',{p_period:period,p_scope:scope,p_year:year,p_metrics:metrics});if(error)throw error;
  res.set('Cache-Control','private, no-store');return res.json({...data,allowedMetrics:metrics});
 }catch(e){console.error('BI records failed:',e.code||'REPORT_FAILED');return res.status(503).json({error:'Unable to load records. Please try again.'});}
};}
module.exports={handler,allowedMetrics,dashboardRoles};
