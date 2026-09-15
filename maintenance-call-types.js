'use strict';
const types=[['Phone','Maintenance Calls (Phone Support):'],['Onsite','Maintenance Call-Ins (On-Site Response):']];
const selected=value=>value===true||value==='Yes'||value==='on';
function maintenanceTypeDetails(payload){return types.filter(([kind])=>selected(payload['maintenance'+kind])).map(([kind,label])=>`${label} ${String(payload['maintenance'+kind+'Details']||'').trim()}`);}
function validateMaintenanceTypes(payload){for(const [kind,label] of types){if(selected(payload['maintenance'+kind])&&!String(payload['maintenance'+kind+'Details']||'').trim())return `Add details for ${label}`;}return null;}
function maintenanceEmailRows(payload){return types.map(([kind,label])=>({label,value:selected(payload['maintenance'+kind])?String(payload['maintenance'+kind+'Details']||'').trim():'None reported'}));}
module.exports={maintenanceTypeDetails,validateMaintenanceTypes,maintenanceEmailRows};
