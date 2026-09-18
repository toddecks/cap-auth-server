'use strict';
const names=[['kayleigh@coilsteelprocessing.com','Kayleigh'],['nicole@coilsteelprocessing.com','Nicole'],['jplace@coilsteelprocessing.com','Jplace']];
function register({app,db,auth,findUser,issueSession,getBearerToken}) {
 const health={mode:'shared-inbox-v1',accounts:'pending',error:null};
 async function provision(){try{for(const[email,name]of names){const{data:done,error:doneError}=await db.from('shipping_staff_provisioning').select('email').eq('email',email).maybeSingle();if(doneError)throw doneError;if(done)continue;
 let user=await findUser(auth,email);if(!user){const{data,error}=await auth.auth.admin.createUser({email,email_confirm:true,user_metadata:{full_name:name}});if(error)throw error;user=data.user;}
 const{error:profileError}=await auth.from('users').upsert({id:user.id,email},{onConflict:'id',ignoreDuplicates:true});if(profileError)throw profileError;
 const{error:roleError}=await auth.from('user_roles').upsert({user_id:user.id,role_id:25},{onConflict:'user_id,role_id'});if(roleError)throw roleError;
 // The existing bridge creates the driver-project identity without resetting its password.
 await issueSession(user,'shipping');
 const{error}=await db.from('shipping_staff_provisioning').insert({email});if(error)throw error;
 }health.accounts='ready';health.error=null;}catch(e){health.accounts='error';health.error=e.message;console.error('Shipping clerk setup:',e.message);}}
 return{health,provision};
}
module.exports={register};
