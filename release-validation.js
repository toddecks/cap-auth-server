'use strict';

// Validate pickup releases against the shipping feed; staff still controls check-in.
const session=require('./visit-session');
const RELEASE_VALIDATION_ENABLED = true;
const VERIFY = require('./driver-visit-flow').PICKUP_PROMPT;
const ASSIST = 'Please call 419-269-9706 for assistance.';
const ENABLED_AT = '2026-09-29T16:03:02Z';
const WAIT_MS = 3 * 60 * 1000;
const ASSIST_MS = 5 * 60 * 1000;
const CHECKIN_FOLLOWUP_MS = 60 * 1000;
const AUTOMATION_ENABLED_AT = '2026-09-30T18:05:07Z';
const SOON = 'Our shipping team will be with you shortly.';
const CLOSED = 'Our shipping office is currently closed. Please call 419-269-9706 and select option 5 for assistance, or wait in your truck and a member of our shipping/receiving team will be with you shortly.';
const REMAIN = 'Please remain in your truck. A shipping team member will be with you shortly.';
const COIL_REMAIN = 'Please remain in your truck. A shipping team member will assist you shortly.';
const officeClosed = timestamp => {
  const hour = Number(new Intl.DateTimeFormat('en-US', {timeZone:'America/New_York',hour:'2-digit',hourCycle:'h23'}).format(new Date(timestamp)));
  return hour >= 18 || hour < 6;
};
const validPickup = c => c.status === 'open' && ['pickup','both'].includes(c.visit_type) &&
  Boolean(String(c.release_number || '').trim()) && !/^(TEXT\s*\d*|drop[ -]?off|pick[ -]?up)$/i.test(String(c.release_number).trim());

function createWorker({getOfficeOverride=async()=>null, db, chart, twilio, messagingServiceSid, publicBaseUrl, scheduleStatusSync = () => {}, now = () => Date.now(), releaseValidationEnabled = RELEASE_VALIDATION_ENABLED, sendBothImages = require('./both-visit-images').sendBothImages, attachAppMap = require('./dropoff-map').attachAppMap}) {
  const health = {mode:'pickup-followups-v28-office-notice-stops-flow',releaseValidationEnabled,pollIntervalMs:5000,state:'idle',lastChecked:null,lastIngest:null,error:null};
  let running = false, timer;
  const mediaJobs=new Map();
  async function result(query) { const r = await query; if(r.error) throw r.error; return r.data; }
  async function send(c, kind, body, afterHours=false) {
    if(!afterHours&&(require('./after-hours-reply').closedPeriod(now(),await getOfficeOverride())||await require('./manual-reply-pause').load(db,c.id)))return;
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
      const delivery=await require('./sms-translation').prepareOutgoing(db,c,body);
      if(!afterHours&&(require('./after-hours-reply').closedPeriod(now(),await getOfficeOverride())||await require('./manual-reply-pause').load(db,c.id))){
        await result(db.from('driver_messages').update({delivery_status:'failed',provider_error_message:'Automatic reply paused because a clerk took over.'}).eq('id',r.data.id));
        return;
      }
      const sent = await twilio.messages.create({to:c.sms_phone_e164,body:delivery.body,messagingServiceSid,statusCallback:`${publicBaseUrl}/api/twilio/message-status`});
      await result(db.from('driver_messages').update({provider_message_id:sent.sid,provider_status:sent.status,
        delivery_status:sent.status === 'undelivered' ? 'failed' : (['queued','sent','delivered','failed'].includes(sent.status) ? sent.status : 'queued'),
        provider_status_updated_at:new Date(now()).toISOString(),...delivery.patch}).eq('id',r.data.id));
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
    const [checked,allMessages] = await Promise.all([
      result(db.from('shipping_staff_checkins').select('conversation_id,checked_in_at').eq('conversation_id',id).maybeSingle()),
      result(db.from('driver_messages').select('id,client_message_id,sender_user_id,direction,sent_at,delivery_status,body,attachment_path').eq('conversation_id',id))
    ]);
    const messages=allMessages.filter(m=>!c.session_started_at||Date.parse(m.sent_at)>=session.start(c));
    const override=await getOfficeOverride();
    const afterHours=require('./after-hours-reply').plan(c,messages,now(),override);
    if(afterHours){
      if(afterHours.send)await send(c,afterHours.kind,afterHours.body,true);
      return;
    }
    if(require('./after-hours-reply').hasNotice(c,messages))return;
    if(require('./after-hours-reply').closedPeriod(now(),override)){
      // Preserve the ordinary initial night-arrival notice; an early override
      // must not broadcast to visits without a new incoming message.
      if(!require('./office-closure').active(override,now())&&!messages.length&&validPickup(c)&&!session.currentCheckin(c,checked)&&session.start(c)>=Date.parse(ENABLED_AT)&&now()-session.start(c)<24*3600000)
        await send(c,'closed',CLOSED,true);
      return;
    }
    if(require('./manual-reply-pause').paused(c,messages,checked))return;
    if(c.visit_type==='both' && session.start(c)>=Date.parse('2026-10-12T16:13:00Z') && now()-session.start(c)<24*60*60*1000) {
      const mediaKey=session.key(c);
      if(!mediaJobs.has(mediaKey)){
        const job=Promise.resolve().then(()=>sendBothImages({db,twilio,messagingServiceSid,publicBaseUrl,scheduleStatusSync,conversation:c,messages}))
          .catch(error=>{health.mediaError=String(error.message||error);console.error('First-visit images failed:',health.mediaError);})
          .finally(()=>mediaJobs.delete(mediaKey));
        mediaJobs.set(mediaKey,job);
      }
    }
    if(c.channel==='app'&&c.visit_type==='dropoff'){
      const mediaKey=`dropoff:${session.key(c)}`;
      if(!mediaJobs.has(mediaKey)){
        const job=Promise.resolve().then(()=>attachAppMap({db,conversation:c,messages}))
          .catch(error=>{health.mediaError=String(error.message||error);console.error('Drop-off map failed:',health.mediaError);})
          .finally(()=>mediaJobs.delete(mediaKey));
        mediaJobs.set(mediaKey,job);
      }
    }
    const requestedAt=require('./both-pickup-phase').pickupRequestedAt(messages);
    if(!['pickup','both'].includes(c.visit_type)||(!validPickup(c)&&!requestedAt))return;
    if(session.currentCheckin(c,checked)) {
      // Only new staff check-ins receive this automation; no retroactive messages.
      if(Date.parse(checked.checked_in_at) < Date.parse(AUTOMATION_ENABLED_AT)) return;
      const instructions = messages.find(m => m.client_message_id === `staff-pickup-checkin:${session.key(c)}`);
      if(instructions && !['failed','undelivered'].includes(instructions.delivery_status)
        && now() - Date.parse(checked.checked_in_at) >= CHECKIN_FOLLOWUP_MS
        && !messages.some(m => m.client_message_id === `release-wait:remain:${session.key(c)}`)) {
        const coil= /stay to the RIGHT\b/.test(instructions.body);
        await send(c,'remain',coil ? COIL_REMAIN : REMAIN);
      }
      return;
    }
    // The explicit pickup command starts the second leg without ending the visit.
    // Native arrival messages carry the phone's arrival timestamp, which precedes
    // the conversation insert. They belong to this arrival even before created_at.
    const nativeInitial=c.channel==='app'&&!c.session_started_at
      ? messages.find(m=>m.direction==='driver_to_shipping'&&m.client_message_id?.endsWith('-release')):null;
    const pickupAt=c.visit_type==='both'?requestedAt:(requestedAt||Math.min(session.start(c),nativeInitial?Date.parse(nativeInitial.sent_at):Infinity));
    if(!pickupAt)return;
    if(pickupAt < Math.max(Date.parse(ENABLED_AT),now()-24*60*60*1000)) return;
    const verificationAt=c.channel==='sms'&&c.visit_type==='pickup'?session.start(c):pickupAt;
    const verification=await require('./release-attempts').processAttempts({chart,c,messages,pickupAt:verificationAt,send,now,validationEnabled:releaseValidationEnabled});
    if(c.channel==='app'&&['matched','manual'].includes(verification.state)) {
      const accepted=messages.filter(m=>['matched','manual'].some(kind=>m.client_message_id?.startsWith(`release-wait:${kind}-`))).sort((a,b)=>Date.parse(a.sent_at)-Date.parse(b.sent_at)).at(-1);
      const attempt=accepted&&messages.find(m=>['matched','manual'].some(kind=>accepted.client_message_id===`release-wait:${kind}-${m.id}:${session.key(c)}`));
      const release=attempt&&require('./release-attempts').releaseFromMessage(attempt.body);
      if(release&&c.release_number!==release) {
        await result(db.from('driver_conversations').update({release_number:release}).eq('id',c.id).eq('status','open'));
        if(c.arrival_id)await result(db.from('driver_arrivals').update({release_number:release}).eq('id',c.arrival_id));
      }
    }
    if(verification.state==='invalid'||verification.state==='ambiguous')return;
    if(c.channel==='sms'&&['matched','manual'].includes(verification.state)){
      let contact=await result(db.from('driver_sms_contacts').select('full_name,driver_company,last_release_number').eq('phone_e164',c.sms_phone_e164).maybeSingle());
      if(contact&&verification.state==='matched'&&!require('./driver-identity').cleanIdentity(contact.driver_company)){
        const carrier=await require('./release-carrier').carrierFromRelease(chart,c,contact,now());
        if(carrier){
          const updatedAt=new Date(now()).toISOString();
          await result(db.from('driver_sms_contacts').update({...carrier,updated_at:updatedAt}).eq('phone_e164',c.sms_phone_e164));
          await result(db.from('driver_sms_arrivals').update({driver_company:carrier.driver_company,...(carrier.full_name?{driver_name:carrier.full_name}:{}),updated_at:updatedAt}).eq('conversation_id',c.id).is('departed_at',null));
          contact={...contact,...carrier};
        }
      }
      const missing=contact&&require('./pickup-detail-prompt').missingPrompt(contact);
      if(missing){
        const kind='details-'+require('crypto').createHash('sha256').update(missing).digest('hex').slice(0,12);
        if(!messages.some(m=>m.client_message_id===`release-wait:${kind}:${session.key(c)}`))await send(c,kind,missing);
        return;
      }
    }
    const initial = verification.initial || messages.find(m => m.client_message_id === `release-wait:verify:${session.key(c)}`);
    const closed = messages.find(m => m.client_message_id === `release-wait:closed:${session.key(c)}`);
    if(closed) return;
    if(!initial){
      if(officeClosed(pickupAt))return send(c,'closed',CLOSED,true);
      let prompt=VERIFY;
      if(c.channel==='sms'){
        const contact=await result(db.from('driver_sms_contacts').select('full_name,driver_company,last_release_number').eq('phone_e164',c.sms_phone_e164).maybeSingle());
        prompt=require('./pickup-detail-prompt').missingPrompt(contact)||VERIFY;
      }
      return send(c,'verify',prompt);
    }
    if(['failed','undelivered'].includes(initial.delivery_status)) return;
    if(now() - Date.parse(initial.sent_at) >= ASSIST_MS && !messages.some(m => m.client_message_id === `release-wait:assist:${session.key(c)}`)) {
      await send(c,'assist',ASSIST);
    } else if(now() - Date.parse(initial.sent_at) >= WAIT_MS && now() - Date.parse(initial.sent_at) < ASSIST_MS
      && !messages.some(m => m.client_message_id === `release-wait:soon:${session.key(c)}`)) {
      await send(c,'soon',SOON);
    }
  }
  async function respondAfterHours(id){
    const c=await result(db.from('driver_conversations').select('*').eq('id',id).maybeSingle());
    if(!c||c.status!=='open'||c.session_ended_at)return false;
    const messages=await result(db.from('driver_messages').select('client_message_id,direction,sent_at,body').eq('conversation_id',id));
    if(require('./after-hours-reply').hasNotice(c,messages))return true;
    const reply=require('./after-hours-reply').plan(c,messages,now(),await getOfficeOverride());
    if(!reply)return false;
    if(reply.send)await send(c,reply.kind,reply.body,true);
    return true;
  }
  async function tick() {
    if(running || !db) return;
    running = true;
    health.state = 'running'; health.error = null;
    try {
      // Do not send retroactive replies to old visits when this feature is deployed.
      for(let offset=0;;offset+=100) {
        const rows = await result(db.from('driver_conversations').select('id').eq('status','open').order('created_at').range(offset,offset+99));
        // Bounded parallelism keeps one slow driver lookup from delaying others.
        let next=0;
        await Promise.all(Array.from({length:Math.min(4,rows.length)},async()=>{
          while(next<rows.length){const row=rows[next++];
            try {await processConversation(row.id);}
            catch(error){health.error=String(error.message||error);console.error('Release wait reply failed:',health.error);}
          }
        }));
        if(rows.length < 100) break;
      }
      health.state = health.error ? 'error' : 'idle';
    } catch(error) {health.state='error';health.error=String(error.message || error);console.error('Release wait worker failed:',health.error);}
    finally {health.lastChecked=new Date(now()).toISOString();running=false;}
  }
  return {health,tick,respondAfterHours,start(){if(timer)return;void tick();timer=setInterval(()=>void tick(),5000);timer.unref?.();},stop(){clearInterval(timer);timer=null;}};
}
module.exports = {createWorker,VERIFY,ASSIST,WAIT_MS,ASSIST_MS,SOON,CLOSED,REMAIN,COIL_REMAIN,officeClosed,AUTOMATION_ENABLED_AT,validPickup};
