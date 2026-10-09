'use strict';

// This worker acknowledges receipt only. Release verification belongs to Shipping.
const session=require('./visit-session');
const VERIFY = 'Thank you. Our Shipping Office is verifying your release number. We’ll send you further instructions shortly.';
const ASSIST = 'Please call 419-269-9706 for assistance.';
const ENABLED_AT = '2026-09-29T16:03:02Z';
const WAIT_MS = 3 * 60 * 1000;
const ASSIST_MS = 5 * 60 * 1000;
const CHECKIN_FOLLOWUP_MS = 60 * 1000;
const AUTOMATION_ENABLED_AT = '2026-09-30T18:05:07Z';
const SOON = 'Our shipping team will be with you shortly.';
const CLOSED = 'Our shipping office is currently closed. Please call 419-269-9706 for assistance, or wait in your truck and a member of our shipping/receiving team will be with you shortly.';
const REMAIN = 'Please remain in your truck. A shipping team member will assist you shortly.';
const officeClosed = timestamp => {
  const hour = Number(new Intl.DateTimeFormat('en-US', {timeZone:'America/New_York',hour:'2-digit',hourCycle:'h23'}).format(new Date(timestamp)));
  return hour >= 18 || hour < 6;
};
const validPickup = c => c.status === 'open' && ['pickup','both'].includes(c.visit_type) &&
  Boolean(String(c.release_number || '').trim()) && !/^(TEXT\s*\d*|drop[ -]?off|pick[ -]?up)$/i.test(String(c.release_number).trim());

function createWorker({db, twilio, messagingServiceSid, publicBaseUrl, scheduleStatusSync = () => {}, now = () => Date.now()}) {
  const health = {mode:'pickup-followups-v7-manual-checkout',state:'idle',lastChecked:null,lastIngest:null,error:null};
  let running = false, timer;
  async function result(query) { const r = await query; if(r.error) throw r.error; return r.data; }
  async function send(c, kind, body) {
    if(c.channel === 'sms' && (!twilio || !messagingServiceSid || !c.sms_phone_e164)) throw Error('Text messaging is not configured.');
    const r = await db.from('driver_messages').insert({
      client_message_id:`release-wait:${kind}:${session.key(c)}`, conversation_id:c.id,
      sender_user_id:null, direction:'shipping_to_driver', driver_phone:c.sms_phone_e164,
      body, original_body:body, original_language:'English', sent_at:new Date(now()).toISOString(),
      delivery_status:c.channel === 'sms' ? 'queued' : 'sent'
    }).select('id').single();
    if(r.error?.code === '23505') return; // Survives restarts and overlapping instances.
    if(r.error) throw r.error;
    if(c.channel !== 'sms') return;
    try {
      const sent = await twilio.messages.create({to:c.sms_phone_e164,body,messagingServiceSid,statusCallback:`${publicBaseUrl}/api/twilio/message-status`});
      await result(db.from('driver_messages').update({provider_message_id:sent.sid,provider_status:sent.status,
        delivery_status:sent.status === 'undelivered' ? 'failed' : (['queued','sent','delivered','failed'].includes(sent.status) ? sent.status : 'queued'),
        provider_status_updated_at:new Date(now()).toISOString()}).eq('id',r.data.id));
      scheduleStatusSync(sent.sid);
    } catch(error) {
      await db.from('driver_messages').update({delivery_status:'failed',provider_error_message:String(error.message || error).slice(0,500)}).eq('id',r.data.id);
      throw error;
    }
  }
  async function processConversation(id) {
    // Re-read state just before dispatch so staff activity cancels pending replies.
    const c = await result(db.from('driver_conversations').select('*').eq('id',id).maybeSingle());
    if(!c || c.status!=='open' || c.session_ended_at) return;
    const checked = await result(db.from('shipping_staff_checkins').select('conversation_id,checked_in_at').eq('conversation_id',id).maybeSingle());
    if(!validPickup(c))return;
    const allMessages = await result(db.from('driver_messages').select('client_message_id,sender_user_id,direction,sent_at,delivery_status').eq('conversation_id',id));
    const messages=allMessages.filter(m=>!c.session_started_at||Date.parse(m.sent_at)>=session.start(c));
    if(session.currentCheckin(c,checked)) {
      // Only new staff check-ins receive this automation; no retroactive messages.
      if(Date.parse(checked.checked_in_at) < Date.parse(AUTOMATION_ENABLED_AT)) return;
      const instructions = messages.find(m => m.client_message_id === `staff-pickup-checkin:${session.key(c)}`);
      if(instructions && !['failed','undelivered'].includes(instructions.delivery_status)
        && now() - Date.parse(checked.checked_in_at) >= CHECKIN_FOLLOWUP_MS
        && !messages.some(m => m.client_message_id === `release-wait:remain:${session.key(c)}`)) {
        await send(c,'remain',REMAIN);
      }
      return;
    }
    // Both drivers unload first; pickup automation starts only after staff check-in.
    if(c.visit_type==='both')return;
    if(session.start(c) < Math.max(Date.parse(ENABLED_AT),now()-24*60*60*1000)) return;
    if(messages.some(m => m.direction === 'shipping_to_driver' && m.sender_user_id && !['failed','undelivered'].includes(m.delivery_status))) return;
    const initial = messages.find(m => m.client_message_id === `release-wait:verify:${session.key(c)}`);
    const closed = messages.find(m => m.client_message_id === `release-wait:closed:${session.key(c)}`);
    if(closed) return;
    if(!initial) return officeClosed(session.start(c)) ? send(c,'closed',CLOSED) : send(c,'verify',VERIFY);
    if(['failed','undelivered'].includes(initial.delivery_status)) return;
    if(now() - Date.parse(initial.sent_at) >= ASSIST_MS && !messages.some(m => m.client_message_id === `release-wait:assist:${session.key(c)}`)) {
      await send(c,'assist',ASSIST);
    } else if(now() - Date.parse(initial.sent_at) >= WAIT_MS && now() - Date.parse(initial.sent_at) < ASSIST_MS
      && !messages.some(m => m.client_message_id === `release-wait:soon:${session.key(c)}`)) {
      await send(c,'soon',SOON);
    }
  }
  async function tick() {
    if(running || !db) return;
    running = true;
    health.state = 'running'; health.error = null;
    try {
      // Do not send retroactive replies to old visits when this feature is deployed.
      for(let offset=0;;offset+=100) {
        const rows = await result(db.from('driver_conversations').select('id').eq('status','open').order('created_at').range(offset,offset+99));
        for(const row of rows) {
          try { await processConversation(row.id); }
          catch(error) {health.error=String(error.message || error); console.error('Release wait reply failed:',health.error);}
        }
        if(rows.length < 100) break;
      }
      health.state = health.error ? 'error' : 'idle';
    } catch(error) {health.state='error';health.error=String(error.message || error);console.error('Release wait worker failed:',health.error);}
    finally {health.lastChecked=new Date(now()).toISOString();running=false;}
  }
  return {health,tick,start(){if(timer)return;void tick();timer=setInterval(()=>void tick(),5000);timer.unref?.();},stop(){clearInterval(timer);timer=null;}};
}
module.exports = {createWorker,VERIFY,ASSIST,WAIT_MS,ASSIST_MS,SOON,CLOSED,REMAIN,officeClosed,AUTOMATION_ENABLED_AT,validPickup};
