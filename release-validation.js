'use strict';

// This worker acknowledges receipt only. Release verification belongs to Shipping.
const VERIFY = 'Thank you! Our Shipping Office is verifying your release number. We’ll send you further instructions shortly.';
const ASSIST = 'Please call 419-269-9706 for assistance.';
const ENABLED_AT = '2026-09-29T16:03:02Z';
const WAIT_MS = 3 * 60 * 1000;
const validPickup = c => c.status === 'open' && c.visit_type === 'pickup' &&
  Boolean(String(c.release_number || '').trim()) && !/^(TEXT\s*\d*|drop[ -]?off|pick[ -]?up)$/i.test(String(c.release_number).trim());

function createWorker({db, twilio, messagingServiceSid, publicBaseUrl, scheduleStatusSync = () => {}, now = () => Date.now()}) {
  const health = {mode:'release-verification-wait-v2',state:'idle',lastChecked:null,lastIngest:null,error:null};
  let running = false, timer;
  async function result(query) { const r = await query; if(r.error) throw r.error; return r.data; }
  async function send(c, kind, body) {
    if(c.channel === 'sms' && (!twilio || !messagingServiceSid || !c.sms_phone_e164)) throw Error('Text messaging is not configured.');
    const r = await db.from('driver_messages').insert({
      client_message_id:`release-wait:${kind}:${c.id}`, conversation_id:c.id,
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
    if(!c || !validPickup(c)) return;
    const checked = await result(db.from('shipping_staff_checkins').select('conversation_id').eq('conversation_id',id).maybeSingle());
    if(checked) return;
    const messages = await result(db.from('driver_messages').select('client_message_id,sender_user_id,direction,sent_at,delivery_status').eq('conversation_id',id));
    if(messages.some(m => m.direction === 'shipping_to_driver' && m.sender_user_id && !['failed','undelivered'].includes(m.delivery_status))) return;
    const initial = messages.find(m => m.client_message_id === `release-wait:verify:${id}`);
    if(!initial) return send(c,'verify',VERIFY);
    if(['failed','undelivered'].includes(initial.delivery_status)) return;
    if(now() - Date.parse(initial.sent_at) >= WAIT_MS && !messages.some(m => m.client_message_id === `release-wait:assist:${id}`)) {
      await send(c,'assist',ASSIST);
    }
  }
  async function tick() {
    if(running || !db) return;
    running = true;
    health.state = 'running'; health.error = null;
    try {
      // Do not send retroactive replies to old visits when this feature is deployed.
      const since = new Date(Math.max(Date.parse(ENABLED_AT),now()-24*60*60*1000)).toISOString();
      for(let offset=0;;offset+=100) {
        const rows = await result(db.from('driver_conversations').select('id').eq('status','open').eq('visit_type','pickup').gte('created_at',since).order('created_at').range(offset,offset+99));
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
module.exports = {createWorker,VERIFY,ASSIST,WAIT_MS,validPickup};
