'use strict';
const TYPE_PROMPT='Are you here for a pick-up or drop-off? Please reply PICKUP or DROPOFF.';
const PICKUP_PROMPT='Please reply with your name, release number, and carrier name.';
const PICKUP_CHECKIN='Please pull around back and stay on the left side. Please have all required PPE ready before entering the facility, including a hard hat, safety glasses, and fully enclosed shoes.';
// Explicit common typos avoid treating unrelated short words as arrival intent.
const PICKUP_WORDS = String.raw`(?:pick[\s-]*up|picking[\s-]*up|pik[\s-]*up|pic[\s-]*up|pck[\s-]*up|pcik[\s-]*up|pikc[\s-]*up|pick[\s-]*pu|pick[\s-]*upp|pick[\s-]*ip|pick[\s-]*uo|pickng[\s-]*up|piking[\s-]*up|pick)`;
const DROPOFF_WORDS = String.raw`(?:drop[\s-]*off|dropping[\s-]*off|drop[\s-]*of|dropp[\s-]*off|dropp[\s-]*of|drpo[\s-]*off|dorp[\s-]*off|drp[\s-]*off|dro[\s-]*off|drop[\s-]*offf|drop[\s-]*oft|droping[\s-]*off|dropping[\s-]*of|delivery|delivering)`;
function visitType(text){
 const value=String(text||'');
 const pickup=new RegExp(String.raw`\b(?:${PICKUP_WORDS}|release)\b`,'i').test(value);
 const dropoff=new RegExp(String.raw`\b${DROPOFF_WORDS}\b`,'i').test(value);
 if(dropoff)return pickup?null:'dropoff';
 return pickup||/^\s*#?\d+\s*$/.test(value)?'pickup':null;
}
function stripVisitWords(text){
 return String(text||'').replace(new RegExp(String.raw`\b(?:${PICKUP_WORDS}|${DROPOFF_WORDS})\b`,'gi'),'');
}
function dropoffReply(at=new Date()){
 const parts=Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',weekday:'short',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date(at)).map(p=>[p.type,p.value]));
 const minutes=Number(parts.hour)*60+Number(parts.minute);
 if(['Sat','Sun'].includes(parts.weekday)||minutes<360||minutes>=1080)return 'Receiving is closed. Hours are Monday–Friday, 6:00 AM–6:00 PM. Please return during those hours. No overnight parking. Thank you.';
 if(minutes>=1050)return 'We are approaching the cut-off time for drop-offs. Please call the office at 419-269-9706 for further instructions.';
 return 'Please pull around back and stay to the right. Once stopped in the drop-off line, unchain your load and open the trailer for unloading.';
}
module.exports={TYPE_PROMPT,PICKUP_PROMPT,PICKUP_CHECKIN,visitType,stripVisitWords,dropoffReply};
