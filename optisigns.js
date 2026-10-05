const crypto = require('node:crypto');
const PLAYLIST = '623LvMmXfAYwkqrZW';
const ENDPOINT = 'https://graphql-gateway.optisigns.com/graphql';
const TABLE = 'optisigns_revisions';
const hash = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
async function readPlaylist(token, fetcher = fetch) {
  const query = `query { playlists(query:{_id:"${PLAYLIST}"},first:2) { page { pageInfo { hasNextPage } edges { node { _id name assets { _id filename type fileType AWSS3ID webLink thumbnail isHide status } } } } } }`;
  const response = await fetcher(ENDPOINT, { method:'POST', headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'}, body:JSON.stringify({query}), signal:AbortSignal.timeout(30000) });
  if (!response.ok) throw new Error('OptiSigns connection failed. Check the API credential.');
  const result = await response.json();
  if (result.errors?.length) throw new Error('OptiSigns could not return the complete playlist. Check the API credential.');
  const page = result.data?.playlists?.page;
  const playlist = page?.edges?.find(e => e.node._id === PLAYLIST)?.node;
  if (!playlist || page.pageInfo?.hasNextPage || !Array.isArray(playlist.assets)) throw new Error('Main Playlist could not be read completely. Saved content has been retained.');
  return playlist.assets.map((a,position) => {
    if (!a._id || !a.filename || !a.type) throw new Error('An incomplete playlist item was returned. Saved content has been retained.');
    const metadata={title:a.filename,type:a.type,file_type:a.fileType||null,source_key:a.AWSS3ID||null,web_url:a.webLink||null,hidden:!!a.isHide,status:a.status||null};
    return {asset_id:a._id,revision_hash:hash(metadata),metadata,position};
  });
}
function registerOptisigns(app, {db, requireAdminAccess, encryptionSecret}) {
  const key = crypto.createHash('sha256').update('csp-optisigns:'+encryptionSecret).digest();
  const seal = token => {const iv=crypto.randomBytes(12), c=crypto.createCipheriv('aes-256-gcm',key,iv);return Buffer.concat([iv,c.update(token),c.final(),c.getAuthTag()]).toString('base64');};
  const unseal = text => {const b=Buffer.from(text,'base64'),d=crypto.createDecipheriv('aes-256-gcm',key,b.subarray(0,12));d.setAuthTag(b.subarray(-16));return Buffer.concat([d.update(b.subarray(12,-16)),d.final()]).toString();};
  let pending=null;
  const checked = async q => {const {data,error}=await q;if(error)throw error;return data;};
  async function sync() {
    if(pending)return pending;
    pending=(async()=>{
      const settings=await checked(db.from('optisigns_connection').select('*').eq('id',1).single());
      if(!settings.token_ciphertext)throw new Error('Connect an OptiSigns API credential to refresh Main Playlist.');
      try {
        const items=await readPlaylist(unseal(settings.token_ciphertext));
        // Apply the entire successful snapshot in one database transaction.
        await checked(db.rpc('apply_optisigns_snapshot',{items}));
        return {ok:true,count:items.length};
      } catch(e) {
        await db.from('optisigns_connection').update({last_error:e.message?.startsWith('OptiSigns')||e.message?.startsWith('Main Playlist')?e.message:'Sync failed. Saved content has been retained.'}).eq('id',1);
        throw e;
      }
    })().finally(()=>pending=null);
    return pending;
  }
  app.get('/api/optisigns/status',requireAdminAccess,async(req,res)=>{
    try {const s=await checked(db.from('optisigns_connection').select('last_synced_at,last_error,token_expires_at').eq('id',1).single());res.set('Cache-Control','no-store').json(s);}catch{res.status(500).json({error:'Unable to read connection status.'});}
  });
  app.post('/api/optisigns/connect',requireAdminAccess,async(req,res)=>{
    try{
      const token=String(req.body?.token||'').trim();if(token.length<30||token.length>20000)return res.status(400).json({error:'Enter an OptiSigns API key.'});
      await readPlaylist(token);
      let expiry=null;try{const p=JSON.parse(Buffer.from(token.split('.')[1],'base64url'));if(p.exp)expiry=new Date(p.exp*1000).toISOString();}catch{}
      await checked(db.from('optisigns_connection').update({token_ciphertext:seal(token),token_expires_at:expiry,last_error:null}).eq('id',1));
      res.json(await sync());
    }catch{res.status(400).json({error:'Unable to connect. Check that this API key can read Main Playlist.'});}
  });
  app.post('/api/optisigns/sync',requireAdminAccess,async(req,res)=>{try{res.json(await sync());}catch{res.status(502).json({error:'Refresh failed. Saved content has been retained. Check the OptiSigns API key.'});}});
  const timer=setInterval(()=>sync().catch(()=>{}),5*60*1000);timer.unref();
  return {sync};
}
module.exports={registerOptisigns,readPlaylist,hash};
