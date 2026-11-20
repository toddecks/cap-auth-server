'use strict';
const PROMPT='Dropping off coils or sheets?';
const distance=(a,b)=>{let v=Array.from({length:b.length+1},(_,i)=>i);for(let i=0;i<a.length;i++){let next=[i+1];for(let j=0;j<b.length;j++)next.push(Math.min(next[j]+1,v[j+1]+1,v[j]+(a[i]!==b[j])));v=next;}return v[b.length]};
function material(text){
 const words=String(text||'').normalize('NFKD').replace(/\p{M}/gu,'').toLowerCase().match(/[\p{L}]+/gu)||[];
 const kinds=new Set();
 for(const word of words){for(const [kind,aliases] of [['coils',['coil','coils','roll','rolls','bobinas','bobine','рулоны','рулони']],['sheets',['sheet','sheets','skid','skids','laminas','feuilles','листы','листи']]])if(aliases.includes(word)||(word.length>=4&&aliases.slice(0,2).some(alias=>distance(word,alias)<=1)))kinds.add(kind);}
 return kinds.size===1?[...kinds][0]:null;
}
const directions=kind=>`Please pull around back and stay to the ${kind==='coils'?'RIGHT':'LEFT'}. Once stopped in the drop-off line, unchain your load and open the trailer for unloading.`;
function reply(text,awaiting=false){const kind=material(text);return kind?directions(kind):PROMPT;}
module.exports={PROMPT,material,directions,reply};
