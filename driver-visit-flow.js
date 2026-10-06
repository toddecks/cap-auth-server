'use strict';
const TYPE_PROMPT='Are you here for a pick-up or drop-off? Please reply PICKUP or DROPOFF.';
const PICKUP_PROMPT='Please reply with your name, release number, and carrier name.';
const PICKUP_CHECKIN='Please pull around back and stay on the left side. Please have all required PPE ready before entering the facility, including a hard hat, safety glasses, and fully enclosed shoes.';
function visitType(text){
 const pickup=/\b(pick(?:[ -]?up)?|picking up|release)\b/i.test(text),dropoff=/\b(drop[ -]?off|dropping off|delivery|delivering)\b/i.test(text);
 if(dropoff)return pickup?null:'dropoff';
 return pickup||/^\s*#?\d+\s*$/.test(text)?'pickup':null;
}
function dropoffReply(at=new Date()){
 const parts=Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',weekday:'short',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date(at)).map(p=>[p.type,p.value]));
 const minutes=Number(parts.hour)*60+Number(parts.minute);
 if(['Sat','Sun'].includes(parts.weekday)||minutes<360||minutes>=1080)return 'Receiving is closed. Hours are Monday–Friday, 6:00 AM–6:00 PM. Please return during those hours. No overnight parking. Thank you.';
 if(minutes>=1050)return 'We are approaching the cut-off time for drop-offs. Please call the office at 419-269-9706 for further instructions.';
 return 'Please pull around back and stay to the right. Once stopped in the drop-off line, unchain your load and open the trailer for unloading.';
}
module.exports={TYPE_PROMPT,PICKUP_PROMPT,PICKUP_CHECKIN,visitType,dropoffReply};
