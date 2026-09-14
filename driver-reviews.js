"use strict";
const REVIEW_BODY = "Thank you for visiting Coil Steel Processing! Have a safe trip. When you're safely parked, please leave us a Google review: https://g.page/r/CS0GZ0amOC9REAE/review";

function createReviewWorker({ db, twilio, messagingServiceSid, publicBaseUrl, scheduleStatusSync, logger = console }) {
  let running = false;
  const health = { configured: Boolean(db && twilio && messagingServiceSid), lastCheckedAt: null, lastError: null };
  const checked = async (query) => { const result = await query; if (result.error) throw result.error; return result.data; };
  async function tick() {
    if (!health.configured || running) return;
    running = true;
    try {
      for (let i = 0; i < 20; i++) {
        const rows = await checked(db.rpc('claim_driver_review_request'));
        health.lastCheckedAt = new Date().toISOString();
        health.lastError = null;
        const job = rows?.[0];
        if (!job) break;
        const clientId = `departure-review:sms:${job.arrival_id}`;
        // Save the transcript before contacting Twilio. The claim is durable and
        // is never automatically reclaimed after an ambiguous network response.
        try {
          await checked(db.from('driver_messages').upsert({
            client_message_id: clientId, conversation_id: job.conversation_id,
            sender_user_id: null, direction: 'shipping_to_driver', driver_phone: job.phone,
            body: REVIEW_BODY, original_body: REVIEW_BODY, original_language: 'English',
            sent_at: new Date().toISOString(), delivery_status: 'queued'
          }, { onConflict: 'client_message_id', ignoreDuplicates: true }));
        } catch (error) {
          await checked(db.from('driver_review_requests').update({ status: 'pending', claimed_at: null }).eq('arrival_id',job.arrival_id));
          throw error;
        }
        let sent;
        try {
          sent = await twilio.messages.create({ to: job.phone, body: REVIEW_BODY,
            messagingServiceSid, statusCallback: `${publicBaseUrl}/api/twilio/message-status` });
        } catch (error) {
          const rejected = error.status >= 400 && error.status < 500;
          await checked(db.from('driver_review_requests').update({ status: rejected ? 'failed' : 'uncertain',
            error_code: String(error.code || 'provider_response_unknown') }).eq('arrival_id',job.arrival_id));
          await checked(db.from('driver_messages').update({ delivery_status: 'failed',
            provider_error_code: String(error.code || 'provider_response_unknown'),
            provider_error_message: rejected ? 'Review text rejected by provider.' : 'Delivery uncertain; not resent to prevent duplicate texts.'
          }).eq('client_message_id',clientId));
          continue;
        }
        // A failure after provider acceptance leaves the claim held, never resent.
        await checked(db.from('driver_review_requests').update({ status:'sent',sent_at:new Date().toISOString(),
          provider_message_id:sent.sid }).eq('arrival_id',job.arrival_id));
        await checked(db.from('driver_messages').update({ provider_message_id:sent.sid,
          provider_status:sent.status, delivery_status: ['delivered','sent'].includes(sent.status) ? sent.status : 'queued',
          provider_status_updated_at:new Date().toISOString()
        }).eq('client_message_id',clientId));
        scheduleStatusSync(sent.sid);
      }
    } catch (error) {
      health.lastError = 'Review worker could not finish its last check.';
      logger.error('Driver review worker failed:', error?.code || 'database_or_provider_error');
    } finally { running = false; }
  }
  function start() { void tick(); const timer=setInterval(() => void tick(),30000); timer.unref?.(); return timer; }
  return { tick, start, health };
}
module.exports = { createReviewWorker, REVIEW_BODY };
