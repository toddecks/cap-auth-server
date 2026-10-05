const {test}=require('node:test');const assert=require('node:assert/strict');const {readPlaylist,hash}=require('./optisigns');
const node={_id:'623LvMmXfAYwkqrZW',assets:[{_id:'a',filename:'Notice.png',type:'file',fileType:'image',AWSS3ID:'original'}]};
const fetcher=body=>async(url,opts)=>{assert.equal(url,'https://graphql-gateway.optisigns.com/graphql');assert.ok(JSON.parse(opts.body).query.startsWith('query'));return{ok:true,json:async()=>body};};
const result=(node,more=false)=>({data:{playlists:{page:{pageInfo:{hasNextPage:more},edges:[{node}]}}}});
test('reads only Main Playlist and generates stable revision fingerprint',async()=>{const a=await readPlaylist('test',fetcher(result(node)));assert.equal(a.length,1);assert.equal(a[0].metadata.title,'Notice.png');assert.equal(a[0].revision_hash,hash(a[0].metadata));});
test('fails closed on partial GraphQL data',async()=>{await assert.rejects(readPlaylist('test',fetcher({...result(node),errors:[{message:'denied'}]})));});
test('rejects truncated results and missing playlist',async()=>{await assert.rejects(readPlaylist('test',fetcher(result(node,true))));await assert.rejects(readPlaylist('test',fetcher(result({...node,_id:'other'}))));});
test('accepts genuinely empty playlist and rejects malformed assets',async()=>{assert.deepEqual(await readPlaylist('test',fetcher(result({...node,assets:[]}))),[]);await assert.rejects(readPlaylist('test',fetcher(result({...node,assets:[{_id:'a'}]}))));});
