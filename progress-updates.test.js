const {test} = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const jwt = require('jsonwebtoken');
const createRouter = require('./progress-updates');

test('progress subscriptions confirm ownership, handle failures, and preserve unsubscribe', async () => {
  const calls = [];
  let unsubscribed = false;
  let fail = false;
  const env = {RESEND_API_KEY:'test', RESEND_PROGRESS_SEGMENT_ID:'progress', PROGRESS_CONFIRM_SECRET:'x'.repeat(40), PRO_FORMS_FROM_EMAIL:'test@example.com'};
  const app = express();
  app.use(express.json()); app.use(express.urlencoded({extended:false}));
  app.use('/api/progress-updates',createRouter({env,request:async (url, options) => {
    calls.push({url,...options});
    return {ok:!fail,status:fail?500:200,json:async()=>url.includes('/contacts/')?{id:'contact',unsubscribed}:{id:'email'}};
  }}));
  const server = app.listen(0,'127.0.0.1');
  await new Promise(resolve=>server.once('listening',resolve));
  const base = 'http://127.0.0.1:'+server.address().port+'/api/progress-updates';
  const post = (path,body) => fetch(base+path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  try {
    assert.equal((await post('/subscribe',{name:'Test',email:'test@example.com'})).status,400);
    assert.equal((await post('/subscribe',{website:'bot'})).status,200);
    assert.equal(calls.length,0);
    assert.equal((await post('/subscribe',{name:'Test',email:'test@example.com',consent:true})).status,200);
    assert.equal(calls.length,1);
    const email = JSON.parse(calls[0].body);
    const token = decodeURIComponent(email.text.match(/token=([^\s]+)/)[1]);
    assert.equal((await fetch(base+'/confirm?token='+token)).status,200);
    assert.equal(calls.length,1,'GET confirmation must not subscribe');
    assert.equal((await post('/confirm',{token:'invalid'})).status,400);
    const expired = jwt.sign({email:'test@example.com',name:'Test'},env.PROGRESS_CONFIRM_SECRET,{expiresIn:-1,audience:'csp-progress-updates',issuer:'csp'});
    assert.equal((await post('/confirm',{token:expired})).status,400);
    assert.equal((await post('/confirm',{token})).status,200);
    assert.ok(calls.at(-1).url.endsWith('/segments/progress'));
    unsubscribed = true;
    const before = calls.length;
    assert.equal((await post('/confirm',{token})).status,200);
    assert.equal(calls.length,before+1,'unsubscribed contact must only be read');
    fail = true;
    assert.equal((await post('/subscribe',{name:'Test',email:'other@example.com',consent:true})).status,502);
  } finally { await new Promise(resolve=>server.close(resolve)); }
});

test('unconfigured service fails closed', async () => {
  const app=express();app.use(createRouter({env:{}}));
  const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));
  try {assert.equal((await fetch('http://127.0.0.1:'+server.address().port+'/subscribe',{method:'POST'})).status,503);}
  finally {await new Promise(resolve=>server.close(resolve));}
});

test('existing email configuration supports signed confirmations and creates the right contact', async () => {
  const calls=[];
  const app=express();app.use(express.json());app.use(express.urlencoded({extended:false}));
  const router=createRouter({env:{RESEND_API_KEY:'test-existing-server-key'},request:async(url,options)=>{
    calls.push({url,...options});
    const missing=options.method==='GET' && url.includes('/contacts/');
    return {ok:!missing,status:missing?404:200,json:async()=>missing?{}:{id:'test-result'}};
  }});app.use('/api/progress-updates',router);
  const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));
  const base='http://127.0.0.1:'+server.address().port+'/api/progress-updates';
  const post=(path,body)=>fetch(base+path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  try{
    assert.equal((await fetch(base+'/status')).status,200);
    assert.equal((await post('/subscribe',{name:'Test Employee',email:'test@example.com',consent:true})).status,200);
    const mail=JSON.parse(calls[0].body);
    assert.equal(mail.from,'CSP Updates <updates@pro.coilsteelprocessing.com>');
    const token=decodeURIComponent(mail.text.match(/token=([^\s]+)/)[1]);
    assert.equal((await fetch(base+'/confirm?token='+token)).status,200);
    assert.equal(calls.length,1,'link scanners cannot create contacts');
    assert.equal((await post('/confirm',{token})).status,200);
    const contact=JSON.parse(calls.find(c=>c.url.endsWith('/contacts')&&c.method==='POST').body);
    assert.equal(contact.email,'test@example.com');assert.equal(contact.unsubscribed,false);
    assert.ok(calls.at(-1).url.endsWith('/segments/f79254b7-9583-458d-a981-ff76849308db'));
  }finally{await new Promise(resolve=>server.close(resolve));}
});
