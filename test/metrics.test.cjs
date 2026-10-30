const {test}=require('node:test');
const assert=require('node:assert/strict');
const {headersFor,readMetrics,latestReading,compareWindows}=require('../lib/railway-metrics.cjs');
const env={RAILWAY_API_TOKEN:'test-account',RAILWAY_TOKEN:'test-project'};
test('account token preferred and project header omitted',()=>{const h=headersFor(env);assert.equal(h.authorization,'Bearer test-account');assert.equal(h['Project-Access-Token'],undefined)});
test('project-only header and explicit account mode supported',()=>{assert.equal(headersFor({RAILWAY_TOKEN:'test'})['Project-Access-Token'],'test');assert.equal(headersFor({RAILWAY_TOKEN:'test',RAILWAY_METRICS_TOKEN_TYPE:'account'}).authorization,'Bearer test')});
test('missing token fails safely',()=>assert.throws(()=>headersFor({}),/CONFIG_ERROR/));
test('empty and stale metrics cannot report normal',()=>{assert.throws(()=>latestReading({ram:[],cpu:[]},Date.now()),/NO_DATA/);assert.throws(()=>latestReading({ram:[{time:0,value:0}],cpu:[{time:0,value:0}]},200000),/STALE_DATA/)});
test('valid zero is retained',()=>assert.equal(latestReading({ram:[{time:1000,value:0}],cpu:[{time:1000,value:0}]},1000).memoryGB,0));
test('HTTP auth failure is sanitized',async()=>{await assert.rejects(readMetrics({env,start:0,end:60000,fetchImpl:async()=>({ok:false,status:403})}),/METRICS_AUTH_ERROR/)});
test('GraphQL auth failure is sanitized',async()=>{await assert.rejects(readMetrics({env,start:0,end:60000,fetchImpl:async()=>({ok:true,json:async()=>({errors:[{message:'Not Authorized secret-value'}]})})}),e=>e.message.includes('AUTH_ERROR')&&!e.message.includes('secret-value'))});
test('samples sorted, invalid values removed and timestamps normalized',async()=>{const data=await readMetrics({env,start:0,end:120000,fetchImpl:async()=>({ok:true,json:async()=>({data:{metrics:[{measurement:'MEMORY_USAGE_GB',values:[{ts:120,value:1},{ts:60,value:.5},{ts:60,value:.5},{ts:90,value:null},{ts:80,value:-1}]}]}})})});assert.deepEqual(data.ram,[{time:60000,value:.5},{time:120000,value:1}])});
function series(){const ram=[],cpu=[];for(let i=0;i<20;i++){ram.push({time:i*60000,value:i<10?1:.5});cpu.push({time:i*60000,value:i<10?.2:.1})}return {ram,cpu}}
test('equal windows compare observed mean and peak',()=>{const r=compareWindows(series(),600000,0,1200000);assert.equal(r.status,'COMPARABLE');assert.equal(r.delta.ram.percent,-50);assert.equal(r.before.ram.peak,1);assert.equal(r.after.ram.samples,10)});
test('insufficient data does not fabricate improvement',()=>{const r=compareWindows({ram:[],cpu:[]},600000,0,1200000);assert.equal(r.status,'INSUFFICIENT_DATA');assert.equal(r.delta,null)});
test('unequal periods rejected',()=>assert.throws(()=>compareWindows(series(),600000,0,1800000),/equal windows/));
