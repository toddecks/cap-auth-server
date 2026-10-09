'use strict';
const TYPE_PROMPT='Are you here for a pick-up, drop-off, or both? Please reply PICKUP, DROPOFF, or BOTH.';
const BOTH_SEQUENCE='For both drop-off and pick-up, drop off first. After unloading, Shipping will check you in for pickup and send your pickup instructions.';
const PICKUP_PROMPT='Please reply with your name, release number, and carrier name.';
const PICKUP_CHECKIN='Please pull around back and stay on the left side. Please have all required PPE ready before entering the facility, including a hard hat, safety glasses, and fully enclosed shoes.';
// Explicit common typos avoid treating unrelated short words as arrival intent.
const PICKUP_WORDS = String.raw`(?:pick[\s-]*up|picking[\s-]*up|pik[\s-]*up|pic[\s-]*up|pck[\s-]*up|pcik[\s-]*up|pikc[\s-]*up|pick[\s-]*pu|pick[\s-]*upp|pick[\s-]*ip|pick[\s-]*uo|pickng[\s-]*up|piking[\s-]*up|pick)`;
const DROPOFF_WORDS = String.raw`(?:drop[\s-]*off|dropping[\s-]*off|drop[\s-]*of|dropp[\s-]*off|dropp[\s-]*of|drpo[\s-]*off|dorp[\s-]*off|drp[\s-]*off|dro[\s-]*off|drop[\s-]*offf|drop[\s-]*oft|droping[\s-]*off|dropping[\s-]*of|delivery|delivering)`;
// Exact sign wording plus the corresponding native-app labels. Unicode word
// boundaries keep Cyrillic commands out of names and longer unrelated words.
const TRANSLATED_VISITS = [
 ['both', ['ambas','ambos','les deux','и то и другое','і те й інше','і те і інше','обидва','оба']],
 ['dropoff', ['entrega','entregar','доставка','доставити','доставить','dépôt','depot','livraison']],
 ['pickup', ['recogida','recoger','получение','отримання','забрати','забрать','retrait','enlèvement','enlevement']]
];
function translatedCommands(text){
 let value=String(text||'').normalize('NFC');
 for(const [type,words] of TRANSLATED_VISITS){
  const alternatives=words.map(word=>word.split(' ').join(String.raw`[\s,]+`)).join('|');
  value=value.replace(new RegExp(String.raw`(?<![\p{L}\p{N}_])(?:${alternatives})(?![\p{L}\p{N}_])`,'giu'),type);
 }
 return value;
}
function visitType(text){
 const value=translatedCommands(text);
 if(/^[\s"'«»“”‘’¡¿]*both\b/i.test(value))return 'both';
 const pickup=new RegExp(String.raw`\b(?:${PICKUP_WORDS}|release)\b`,'i').test(value);
 const dropoff=new RegExp(String.raw`\b${DROPOFF_WORDS}\b`,'i').test(value);
 if(dropoff)return pickup?(/(?<!\p{L})(?:or|o|ou|или|або)(?!\p{L})/iu.test(value)?null:'both'):'dropoff';
 return pickup||/^\s*#?\d+\s*$/.test(value)?'pickup':null;
}
function stripVisitWords(text){
 return translatedCommands(text).replace(new RegExp(String.raw`\b(?:${PICKUP_WORDS}|${DROPOFF_WORDS})\s*(?:and|&|\+|then|y|et|и|і|й)\s*(?:${PICKUP_WORDS}|${DROPOFF_WORDS})\b`,'gi'),'').replace(new RegExp(String.raw`\b(?:${PICKUP_WORDS}|${DROPOFF_WORDS}|both)\b`,'gi'),'').replace(/^\s*(?:and|&)\s*$/i,'');
}
function dropoffReply(at=new Date()){
 const parts=Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',weekday:'short',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date(at)).map(p=>[p.type,p.value]));
 const minutes=Number(parts.hour)*60+Number(parts.minute);
 if(['Sat','Sun'].includes(parts.weekday)||minutes<360||minutes>=1080)return 'Receiving is closed. Hours are Monday–Friday, 6:00 AM–6:00 PM. Please return during those hours. No overnight parking. Thank you.';
 if(minutes>=1050)return 'We are approaching the cut-off time for drop-offs. Please call the office at 419-269-9706 for further instructions.';
 return 'Please pull around back and stay to the right. Once stopped in the drop-off line, unchain your load and open the trailer for unloading.';
}
function bothReply(at=new Date()){
 const reply=dropoffReply(at);
 return reply.startsWith('Please pull around')?`${reply} ${BOTH_SEQUENCE}`:reply;
}
module.exports={TYPE_PROMPT,PICKUP_PROMPT,PICKUP_CHECKIN,BOTH_SEQUENCE,visitType,stripVisitWords,dropoffReply,bothReply};
