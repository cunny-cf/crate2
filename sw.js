// Streams chunked Discord files to <video>/<audio>/<img> with Range (seek) support.
// Holds no secrets: the page tells it file sizes and gives it fresh links on request.
self.addEventListener('install',()=>self.skipWaiting());
self.addEventListener('activate',e=>e.waitUntil(self.clients.claim()));

async function ask(msg){
  const cs=await self.clients.matchAll({type:'window'});
  if(!cs.length)throw new Error('Drive page is not open');
  return new Promise((res,rej)=>{
    const ch=new MessageChannel(),t=setTimeout(()=>rej(new Error('Page did not answer')),30000);
    ch.port1.onmessage=e=>{clearTimeout(t);e.data&&e.data.error?rej(new Error(e.data.error)):res(e.data)};
    cs[0].postMessage(msg,[ch.port2]);
  });
}

self.addEventListener('fetch',e=>{
  const u=new URL(e.request.url),i=u.pathname.indexOf('/_drive/');
  if(i<0)return;
  e.respondWith(serve(e.request,decodeURIComponent(u.pathname.slice(i+8))).catch(err=>new Response(String(err.message),{status:502})));
});

async function serve(req,fid){
  const m=await ask({t:'meta',fid});               // {size, cs, mime}
  let a=0,b=m.size-1,partial=false;
  const h=req.headers.get('range'),r=h&&/bytes=(\d*)-(\d*)/.exec(h);
  if(r){
    partial=true;
    if(r[1]==='')a=Math.max(0,m.size-Number(r[2]));
    else{a=Number(r[1]);if(r[2])b=Math.min(Number(r[2]),m.size-1)}
  }
  if(a>b||a>=m.size)return new Response(null,{status:416,headers:{'Content-Range':'bytes */'+m.size}});

  // pull one piece at a time so only the part being played is downloaded
  let pos=a,rd=null,skip=0,left=0;
  const body=new ReadableStream({
    async pull(c){
      for(;;){
        if(pos>b){c.close();return}
        if(!rd){
          const i=Math.floor(pos/m.cs),off=pos-i*m.cs,end=Math.min((i+1)*m.cs-1,b)-i*m.cs;
          const {url}=await ask({t:'url',fid,i});
          const res=await fetch(url,{headers:{Range:'bytes='+off+'-'+end}});
          if(!res.ok)throw new Error('Chunk '+(i+1)+' failed: '+res.status);
          rd=res.body.getReader();
          skip=res.status===206?0:off;               // server ignored Range: skip ahead ourselves
          left=end-off+1;
        }
        const {done,value}=await rd.read();
        if(done){rd=null;if(left>0)throw new Error('Chunk ended early');continue}
        let v=value;
        if(skip>=v.length){skip-=v.length;continue}
        if(skip){v=v.subarray(skip);skip=0}
        if(v.length>left)v=v.subarray(0,left);
        left-=v.length;pos+=v.length;
        c.enqueue(v);
        if(left<=0){rd.cancel();rd=null}
        return;
      }
    },
    cancel(){if(rd)rd.cancel()}
  });
  return new Response(body,{status:partial?206:200,headers:{
    'Content-Type':m.mime,'Accept-Ranges':'bytes','Content-Length':String(b-a+1),
    ...(partial?{'Content-Range':'bytes '+a+'-'+b+'/'+m.size}:{})
  }});
}
