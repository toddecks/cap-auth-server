'use strict';
const BODY=url=>'Reminder: Please complete the Coil Steel Processing Check-In Survey.\n\nYour feedback helps us improve the check-in process. Complete the survey to be entered in our November 30 drawing for a $50 Visa gift card.\n\nPLEASE DO NOT reply to this message. Submit your survey using the link below:\n\n'+url;
const DRAFT={sendAt:'2026-11-25T15:00:00.000Z',timeZone:'America/New_York',status:'awaiting_approval',audience:'completed_checkins_without_survey',body:BODY('[personal survey link]')};
function unanswered(eligible,completed){const responded=new Set(completed.filter(row=>!row.is_test).map(row=>row.phone_e164));return eligible.filter(row=>!responded.has(row.phone_e164));}
module.exports={BODY,DRAFT,unanswered};
