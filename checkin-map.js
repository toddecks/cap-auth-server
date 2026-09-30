'use strict';
const fs=require('node:fs');
const path=require('node:path');
async function prepareAttachment(db,conversation,fileName,displayName,contentType) {
 const bytes=fs.readFileSync(path.join(__dirname,'assets',fileName));
 const owner=conversation.user_id || `sms/${String(conversation.sms_phone_e164||'').replace(/\D/g,'')}`;
 const attachmentPath=`${owner}/${conversation.id}/${fileName}`;
 const bucket=db.storage.from('driver-paperwork');
 const {error}=await bucket.upload(attachmentPath,bytes,{contentType,upsert:true,cacheControl:'3600'});
 if(error)throw error;
 let mediaUrl;
 if(conversation.channel==='sms') {
  const signed=await bucket.createSignedUrl(attachmentPath,3600);
  if(signed.error || !signed.data?.signedUrl)throw signed.error || Error('Could not prepare the check-in image.');
  mediaUrl=signed.data.signedUrl;
 }
 return {fields:{attachment_path:attachmentPath,attachment_name:displayName,attachment_content_type:contentType,attachment_size_bytes:bytes.length},mediaUrl};
}
async function prepareMap(db,conversation) {
 // This map is for Prosperity Road, not the separate Enterprise property.
 if(conversation.facility_id!=='csp-toledo-main')return null;
 return prepareAttachment(db,conversation,'shipping-map.jpg','Shipping and receiving map.jpg','image/jpeg');
}
async function preparePpe(db,conversation) {
 return prepareAttachment(db,conversation,'ray-ppe.png','Ray - PPE requirements.png','image/png');
}
module.exports={prepareMap,preparePpe};
