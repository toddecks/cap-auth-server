'use strict';
const {driverName,cleanIdentity}=require('./driver-identity');
function normalizePhone(value){
 const text=String(value||'').trim(),digits=text.replace(/\D/g,'');
 return digits.length===10?`+1${digits}`:digits.length===11&&digits[0]==='1'?`+${digits}`:/^\+[1-9]\d{7,14}$/.test(text)?text:'';
}
async function lookupDriver(db,phone){
 const read=async query=>{const r=await query;if(r.error)throw r.error;return r.data;};
 const contact=await read(db.from('driver_sms_contacts').select('full_name,driver_company').eq('phone_e164',phone).maybeSingle());
 let name=driverName(contact?.full_name),company=cleanIdentity(contact?.driver_company);
 if(!name||!company){
  const profile=await read(db.from('driver_profiles').select('full_name,driver_company').eq('phone_e164',phone).maybeSingle());
  name=name||driverName(profile?.full_name);company=company||cleanIdentity(profile?.driver_company);
 }
 if(!name||!company){
  const arrivals=await read(db.from('driver_sms_arrivals').select('driver_name,driver_company').eq('phone_e164',phone).order('entered_at',{ascending:false}).limit(20));
  for(const row of arrivals||[]){name=name||driverName(row.driver_name);company=company||cleanIdentity(row.driver_company);if(name&&company)break;}
 }
 // Deliberately exclude previous releases, appointments and check-in state.
 return{found:Boolean(name||company),driverName:name.slice(0,120),driverCompany:company.slice(0,160)};
}
function createHandler({db}){return async(req,res)=>{
 res.set('Cache-Control','no-store');
 if(!db)return res.status(503).json({error:'Driver lookup is temporarily unavailable.'});
 const token=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');
 if(!token)return res.status(401).json({error:'Sign in to Shipping.'});
 try{
  const {data,error}=await db.auth.getUser(token);
  if(error||!data?.user)return res.status(401).json({error:'Your session expired. Sign in again.'});
  if(!['shipping','admin'].includes(data.user.app_metadata?.csp_role))return res.status(403).json({error:'Shipping access is required.'});
  const phone=normalizePhone(req.body?.phone);
  if(!phone)return res.status(400).json({error:'Enter a complete phone number.'});
  return res.json(await lookupDriver(db,phone));
 }catch(error){console.error('Manual driver lookup:',error.message);return res.status(503).json({error:'Driver lookup is unavailable. Enter the name and company manually.'});}
};}
module.exports={normalizePhone,lookupDriver,createHandler};
