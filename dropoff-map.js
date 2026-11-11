'use strict';
const session=require('./visit-session');
const {needsImages}=require('./first-visit-images');
const {prepareMap}=require('./checkin-map');
const BODY='Please pull around back and stay to the right. Once stopped in the drop-off line, unchain your load and open the trailer for unloading.';
const ENABLED_AT=Date.parse('2026-11-11T20:13:11Z');
const messageId=c=>`staff-pickup-dropoff-map:${session.key(c)}`;
async function attachmentForReply({db,conversation:c,body,prepare=prepareMap}){
 if(body!==BODY||c.visit_type!=='dropoff'||c.facility_id!=='csp-toledo-main')return null;
 if(!await needsImages(db,c))return null;
 return prepare(db,c);
}
async function attachAppMap({db,conversation:c,messages,prepare=prepareMap}){
 if(c.channel!=='app'||c.visit_type!=='dropoff')return;
 const message=messages.find(m=>m.direction==='shipping_to_driver'&&m.body===BODY&&!m.attachment_path&&Date.parse(m.sent_at)>=ENABLED_AT&&!['failed','undelivered'].includes(m.delivery_status));
 if(!message)return;
 const attachment=await attachmentForReply({db,conversation:c,body:message.body,prepare});
 if(!attachment)return;
 const {error}=await db.from('driver_messages').update(attachment.fields).eq('id',message.id).is('attachment_path',null);
 if(error)throw error;
}
module.exports={BODY,ENABLED_AT,messageId,attachmentForReply,attachAppMap};
