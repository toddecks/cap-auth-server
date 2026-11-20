'use strict';
const session=require('./visit-session');
async function returning(db,c){
 if(c.channel!=='sms'||!c.sms_phone_e164)return false;
 const {data,error}=await db.from('driver_sms_arrivals').select('id').eq('phone_e164',c.sms_phone_e164).not('checked_in_at','is',null).lt('checked_in_at',new Date(session.start(c)).toISOString()).limit(1);
 if(error)throw error;return Boolean(data?.length);
}
async function held(db,c,checked){
 if(c.channel!=='sms'||session.currentCheckin(c,checked))return false;
 return returning(db,c);
}
async function holdIncoming(db,id){
 const {data:c,error}=await db.from('driver_conversations').select('*').eq('id',id).maybeSingle();if(error)throw error;if(!c)return false;
 const {data:checked,error:e}=await db.from('shipping_staff_checkins').select('checked_in_at').eq('conversation_id',id).maybeSingle();if(e)throw e;
 return held(db,c,checked);
}
module.exports={returning,held,holdIncoming};
