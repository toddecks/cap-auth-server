'use strict';
const session=require('./visit-session');
const CALL_OFFICE='Please call 419-269-9706, select 5 or stay on the line to be connected. Thank you!';
const BODY='Our shipping office is currently closed. Please call 419-269-9706, select 5 or stay on the line to be connected. Thank you';
const formatter=new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
function parts(timestamp){return Object.fromEntries(formatter.formatToParts(new Date(timestamp)).map(p=>[p.type,p.value]));}
function closedPeriod(timestamp,override){
 if(!Number.isFinite(Number(timestamp)))return null;
 const p=parts(timestamp),hour=Number(p.hour),minute=Number(p.minute);if(hour*60+minute>=330&&hour<18&&!require('./office-closure').active(override,timestamp))return null;
 const day=hour*60+minute<330?parts(timestamp-12*3600000):p;
 return`${day.year}-${day.month}-${day.day}`;
}
function hasNotice(c,messages){
 return messages.some(m=>m.direction==='shipping_to_driver'&&Date.parse(m.sent_at)>=session.start(c)
  &&(/^release-wait:(?:after-hours-|closed:)/.test(m.client_message_id||'')||m.body===CALL_OFFICE||/^Our shipping office is (?:currently|now) closed\./.test(m.body||'')));
}
function plan(c,messages,now,override){
 const period=closedPeriod(now,override);
 if(!period||c.status!=='open'||c.session_ended_at)return null;
 const inbound=messages.filter(m=>m.direction==='driver_to_shipping'&&Number.isFinite(Date.parse(m.sent_at))&&(!c.session_started_at||Date.parse(m.sent_at)>=session.start(c))).sort((a,b)=>Date.parse(b.sent_at)-Date.parse(a.sent_at))[0];
 if(!inbound||now-Date.parse(inbound.sent_at)<0||closedPeriod(Date.parse(inbound.sent_at),override)!==period)return null;
 const kind=`after-hours-${period}`;
 const alreadySent=messages.some(m=>m.direction==='shipping_to_driver'&&(!c.session_started_at||Date.parse(m.sent_at)>=session.start(c))&&closedPeriod(Date.parse(m.sent_at),override)===period
  &&(m.client_message_id===`release-wait:${kind}:${session.key(c)}`||m.body===CALL_OFFICE||/^Our shipping office is (?:currently|now) closed\./.test(m.body||'')));
 if(!alreadySent&&now-Date.parse(inbound.sent_at)>5*60000)return null;
 return{kind,body:require('./office-closure').active(override,now)?CALL_OFFICE:BODY,send:!alreadySent};
}
module.exports={BODY,CALL_OFFICE,closedPeriod,plan,hasNotice};
