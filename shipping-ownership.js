'use strict';
const names=[['kayleigh@coilsteelprocessing.com','Kayleigh'],['nicole@coilsteelprocessing.com','Nicole'],['jplace@coilsteelprocessing.com','Jplace']];
function register({app,db,auth,findUser,issueSession,getBearerToken}) {
 const health={mode:'claimed-inbox-v1',accounts:'pending',error:null};
 async function provision(){try{for(const[email,name]of names){const{data:done,error:doneError}=await db.from('shipping_staff_provisioning').select('email').eq('email',email).maybeSingle();if(doneError)throw doneError;if(done)continue;
 let user=await findUser(auth,email);if(!user){const{data,error}=await auth.auth.admin.createUser({email,email_confirm:true,user_metadata:{full_name:name}});if(error)throw error;user=data.user;}
 const{error:profileError}=await auth.from('users').upsert({id:user.id,email,first_name:name},{onConflict:'id',ignoreDuplicates:true});if(profileError)throw profileError;
 const{error:roleError}=await auth.from('user_roles').upsert({user_id:user.id,role_id:25},{onConflict:'user_id,role_id'});if(roleError)throw roleError;
 // The existing bridge creates the driver-project identity without resetting its password.
 await issueSession(user,'shipping');
 const{error}=await db.from('shipping_staff_provisioning').insert({email});if(error)throw error;
 }health.accounts='ready';health.error=null;}catch(e){health.accounts='error';health.error=e.message;console.error('Shipping clerk setup:',e.message);}}
 async function middleware(req,res,next){const actions=new Set(['/send-message','/send-photo','/add-sms-arrival','/edit-arrival','/close-stale-visit']);if(!actions.has(req.path)||req.method!=='POST')return next();
 try{const token=getBearerToken(req);const{data,error}=await db.auth.getUser(token||'');const user=data?.user;if(error||!user)return res.status(401).json({error:'Sign in to Shipping.'});const role=user.app_metadata?.csp_role;if(role==='admin')return next();if(role!=='shipping')return res.status(403).json({error:'Shipping access required.'});
 let ids=[];if(req.body?.conversationId)ids.push(req.body.conversationId);
 if(req.body?.arrivalId&&['/edit-arrival','/close-stale-visit'].includes(req.path)){
  if(req.body.arrivalSource==='sms'){const{data:a,error}=await db.from('driver_sms_arrivals').select('conversation_id').eq('id',req.body.arrivalId).maybeSingle();if(error)throw error;if(a?.conversation_id)ids.push(a.conversation_id);}
  else{const{data:rows,error}=await db.from('driver_conversations').select('id').eq('arrival_id',req.body.arrivalId).eq('status','open');if(error)throw error;ids.push(...(rows||[]).map(c=>c.id));}
 }
 for(const id of new Set(ids)){const{data:a,error}=await db.from('shipping_conversation_assignments').select('assigned_to').eq('conversation_id',id).maybeSingle();if(error)throw error;if(a?.assigned_to!==user.id)return res.status(403).json({error:'Claim this conversation before making changes. Only its assigned clerk or an administrator can manage it.'});}
 return next();}catch(e){console.error('Shipping ownership:',e.message);return res.status(503).json({error:'Could not verify conversation ownership. Try again.'});}}
 app.use('/api/shipping',middleware);
 return{health,provision,middleware};
}
module.exports={register};
