'use strict';
// A driver's introduction is not part of their name. Never manufacture an identity.
function cleanIdentity(value) {
 const text=String(value||'').trim();
 return !/[\p{L}\p{N}]/u.test(text)||/^(?:unknown|not provided|name not provided|company not provided|text message|drop-off driver|n\/?a|none)$/i.test(text)?'':text;
}
function isAcknowledgement(value) {
 return /^(?:copy(?: that)?|copied|roger that|got it|understood|acknowledged|thanks?(?: you)?|thank you|ok(?:ay)?|yes|no|10[ -]?4)[.!\s]*$/i.test(String(value||'').trim());
}
function driverName(value) {
 if(isAcknowledgement(value))return '';
 const name=cleanIdentity(cleanIdentity(value).replace(/^(?:(?:hello|hi|hey)[,!]?\s+)?(?:this\s+is|my\s+name\s+is|i\s+am|i['’]m|it['’]s)\s+/i,'').replace(/[.!]+$/,'').trim());
 return isAcknowledgement(name)?'':name;
}
module.exports={cleanIdentity,driverName,isAcknowledgement};
