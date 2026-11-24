'use strict';
const session=require('./visit-session');
function acknowledgement(c,messages=[]){
 return messages.find(m=>m.direction==='shipping_to_driver'
  &&(!c.session_started_at||Date.parse(m.sent_at)>=session.start(c))
  &&!['failed','undelivered'].includes(m.delivery_status)
  &&(/^release-wait:(?:matched|manual)-/.test(m.client_message_id||'')
   ||/verifying your release number/i.test(m.body||'')));
}
function held(c,messages,checked){return !session.currentCheckin(c,checked)&&Boolean(acknowledgement(c,messages));}
module.exports={acknowledgement,held};
