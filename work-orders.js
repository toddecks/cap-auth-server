'use strict';
function parseLookup(body={}){
 const setup=String(body.setup||'').trim(),customer=String(body.customer||'').trim();
 const tags=body.tags||[];
 if(!/^\d{1,12}$/.test(setup)||customer&&!/^[A-Za-z0-9_-]{1,40}$/.test(customer)||!Array.isArray(tags)||tags.length>100||tags.some(t=>typeof t!=='string'||!/^\d{1,20}$/.test(t)))throw Error('Scan a valid work-order code.');
 return {setup,customer,tags:[...new Set(tags)]};
}
async function lookup(db,input){
 let q=db.from('psdata_production_tags').select('setup_number,customer_number,tag_number,setup_type,machine_id,tag_lbs,tag_length,setup_input_date,setup_close_date,processing_start_date,processing_end_date,last_updated_date').eq('setup_number',input.setup);
 if(input.customer)q=q.eq('customer_number',input.customer);
 const tasks=[q.order('last_updated_date',{ascending:false}).limit(201)];
 if(input.tags.length){let inv=db.from('psdata_cust_inv').select('tag_number,vendor_number,status,prod_class,tag_type,location,length,tons,input_date,last_seen_at,closed_at').in('tag_number',input.tags);if(input.customer)inv=inv.eq('vendor_number',input.customer);tasks.push(inv.limit(201));}
 const detailsIndex=tasks.length;tasks.push(db.from('psdata_setup_details').select('details,fetched_at').eq('setup_number',input.setup).maybeSingle());
 const correctionIndex=tasks.length;tasks.push(db.from('psdata_setup_instruction_corrections').select('tag_number,additional_instructions,source').eq('setup_number',input.setup));
 const results=await Promise.all(tasks);for(const r of results)if(r.error)throw r.error;
 const production=results[0].data||[],inventory=input.tags.length?(results[1]?.data||[]):[];
 const corrections=new Map((results[correctionIndex].data||[]).map(r=>[String(r.tag_number),r]));
 const cached=results[detailsIndex].data;const details=(cached?.details||[]).map(r=>{const correction=corrections.get(String(r.tagNumber));return correction&&!Object.hasOwn(r,'additionalInstructions')?{...r,additionalInstructions:correction.additional_instructions,additionalInstructionsSource:correction.source}:r;}).filter(r=>(!input.customer||r.customerNumber===input.customer)&&(!input.tags.length||input.tags.includes(String(r.tagNumber))));
 return {details,detailsFetchedAt:details.length?cached.fetched_at:null,setup:input.setup,customer:input.customer,requestedTags:input.tags,inventory:inventory.slice(0,200),production:production.slice(0,200),truncated:production.length>200||inventory.length>200,missingTags:input.tags.filter(t=>!inventory.some(r=>String(r.tag_number)===t)),setupFound:details.length>0||production.length>0};
}
function register(app,{db,requireAccess}){app.post('/api/work-orders/lookup',requireAccess,async(req,res)=>{res.set('Cache-Control','no-store');let input;try{input=parseLookup(req.body);}catch(e){return res.status(400).json({error:e.message});}if(!db)return res.status(503).json({error:'PSDATA is temporarily unavailable.'});try{res.json(await lookup(db,input));}catch(e){console.error('Work-order lookup:',e.message);res.status(503).json({error:'Unable to read PSDATA. Please try again.'});}});}
module.exports={register,parseLookup,lookup};
