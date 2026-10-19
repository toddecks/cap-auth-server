'use strict';
const OWNER='todd@coilsteelprocessing.com';
const normalize=value=>String(value||'').normalize('NFKD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9@.]+/g,' ').trim();
function recipientsFor(visiting,users=[]){
 const query=normalize(visiting), recipients=new Set([OWNER]);
 if(!query)return [...recipients];
 if(['hr','h r','human resources','human resources hr','kim'].includes(query)){recipients.add('kim@coilsteelprocessing.com');return [...recipients];}
 const staff=users.filter(u=>u.email_confirmed_at&&/^[^@]+@coilsteelprocessing\.com$/i.test(u.email||'')&&!u.deleted_at);
 const matches=staff.filter(u=>{
 const email=u.email.toLowerCase();const meta=u.user_metadata||{};
 const names=[meta.full_name,meta.fullName,meta.display_name,meta.name,[meta.first_name,meta.last_name].filter(Boolean).join(' ')].filter(Boolean).map(normalize);
 return [email,normalize(email.split('@')[0]),...names,...names.map(n=>n.split(' ')[0])].includes(query);
 });
 const emails=[...new Set(matches.map(u=>u.email.toLowerCase()))];
 if(emails.length===1)recipients.add(emails[0]);
 return [...recipients];
}
function createGuestRecipientResolver(listUsers){
 let cache=[],expires=0,pending;
 return async visiting=>{
 if(!normalize(visiting)||['hr','h r','human resources','human resources hr','kim'].includes(normalize(visiting)))return recipientsFor(visiting);
 if(Date.now()>=expires){pending ||= Promise.resolve().then(listUsers).then(users=>{cache=users;expires=Date.now()+300000;}).finally(()=>{pending=null;});await pending;}
 return recipientsFor(visiting,cache);
 };
}
module.exports={recipientsFor,createGuestRecipientResolver};
