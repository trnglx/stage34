import {test} from 'node:test';
import assert from 'node:assert/strict';
import {LocalWorker} from '../dist/worker.js';
test('worker reserves its slot before asynchronous SDK startup and releases after failure',async()=>{
 let release; let calls=0; let closed=false;
 const startup=new Promise(resolve=>{release=resolve;});
 const runtime={async start(){calls++; await startup; throw new Error('startup_failed');}};
 const worker=new LocalWorker(runtime,{close(){closed=true;}});
 const first=worker.start({},'prompt');
 const second=worker.start({},'second');
 const result=await Promise.race([second.then(()=> 'started',e=>e.message),new Promise(r=>setTimeout(()=>r('not_refused'),25))]);
 if(result !== 'worker_busy') { release(); await Promise.allSettled([first,second]); }
 assert.equal(result,'worker_busy');
 assert.throws(()=>worker.close(),/worker_busy/); assert.equal(calls,1);
 release(); await assert.rejects(first,/startup_failed/);
 worker.close(); assert.equal(closed,true);
 await assert.rejects(worker.start({},'closed'),/worker_closed/);
});
test('cancel during asynchronous startup reaches eventual runtime handle',async()=>{
 let release; let cancelled=false; let settled=false;
 const startup=new Promise(resolve=>{release=resolve;});
 const handle={inspect(){return {settled};},cancel(){cancelled=true;},async events(){settled=true;}};
 const worker=new LocalWorker({async start(){await startup;return handle;}},{close(){}});
 const run=worker.start({},'prompt'); worker.cancel(); release(); await run;
 assert.equal(cancelled,true); worker.close();
});
