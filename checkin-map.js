'use strict';
const fs=require('node:fs');
const path=require('node:path');
async function prepareMap(db,conversation) {
 // This map is for Prosperity Road, not the separate Enterprise property.
 if(conversation.facility_id!=='csp-toledo-main')return null;
 const bytes=fs.readFileSync(path.join(__dirname,'assets','shipping-map.jpg'));
 const owner=conversation.user_id || `sms/${String(conversation.sms_phone_e164||'').replace(/\D/g,'')}`;
 const attachmentPath=`${owner}/${conversation.id}/shipping-map.jpg`;
 const bucket=db.storage.from('driver-paperwork');
 const {error}=await bucket.upload(attachmentPath,bytes,{contentType:'image/jpeg',upsert:true,cacheControl:'3600'});
 if(error)throw error;
 let mediaUrl;
 if(conversation.channel==='sms') {
  const signed=await bucket.createSignedUrl(attachmentPath,3600);
  if(signed.error || !signed.data?.signedUrl)throw signed.error || Error('Could not prepare the shipping map.');
  mediaUrl=signed.data.signedUrl;
 }
 return {fields:{attachment_path:attachmentPath,attachment_name:'Shipping and receiving map.jpg',attachment_content_type:'image/jpeg',attachment_size_bytes:bytes.length},mediaUrl};
}
module.exports={prepareMap};
