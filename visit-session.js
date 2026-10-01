'use strict';
const key=c=>c.session_started_at?`${c.id}:${new Date(c.session_started_at).toISOString()}`:c.id;
const start=c=>Date.parse(c.session_started_at||c.created_at);
const currentCheckin=(c,checked)=>checked && (!c.session_started_at||Date.parse(checked.checked_in_at)>=start(c));
function startsNewVisit(c,arrival,body,now=Date.now()) {
 const details=require('./sms-checkin-details').parseDetails(body,{});
 if(!details.full_name||!details.driver_company||!details.last_release_number)return false;
 // A complete new introduction after departure begins another visit in the same thread.
 if(arrival)return Boolean(arrival.departed_at&&Date.parse(arrival.departed_at)>=start(c));
 return now-start(c)>24*60*60*1000;
}
module.exports={key,start,currentCheckin,startsNewVisit};
