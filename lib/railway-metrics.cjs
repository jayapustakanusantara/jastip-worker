'use strict';
const QUERY = `query BmaMetrics($projectId:String!,$environmentId:String!,$serviceId:String!,$startDate:DateTime!,$endDate:DateTime!,$sampleRateSeconds:Int!,$measurements:[MetricMeasurement!]!){metrics(projectId:$projectId,environmentId:$environmentId,serviceId:$serviceId,startDate:$startDate,endDate:$endDate,sampleRateSeconds:$sampleRateSeconds,measurements:$measurements){measurement values{ts value}}}`;
function headersFor(env) {
 const account = env.RAILWAY_API_TOKEN;
 const project = env.RAILWAY_TOKEN;
 const mode = env.RAILWAY_METRICS_TOKEN_TYPE || (account ? 'account' : 'project');
 const headers = {'content-type':'application/json'};
 if (mode === 'account' && (account || project)) headers.authorization = `Bearer ${account || project}`;
 else if (mode === 'project' && project) headers['Project-Access-Token'] = project;
 else throw new Error('METRICS_CONFIG_ERROR: token missing or invalid token type');
 return headers;
}
function timestamp(ts) {
 if (typeof ts === 'number') return ts < 1e12 ? ts * 1000 : ts;
 if (typeof ts === 'string' && /^\d+(\.\d+)?$/.test(ts)) return timestamp(Number(ts));
 return Date.parse(ts);
}
function samples(rows, measurement, start, end) {
 const row = rows.find(r => r.measurement === measurement);
 const result = (row?.values || []).map(v => ({time:timestamp(v.ts),value:v.value}))
 .filter(v => Number.isFinite(v.time) && typeof v.value === 'number' && Number.isFinite(v.value) && v.value >= 0 && v.time >= start && v.time <= end)
 .sort((a,b)=>a.time-b.time);
 return [...new Map(result.map(v=>[v.time,v])).values()];
}
async function readMetrics({env=process.env,fetchImpl=fetch,start,end,sampleRateSeconds=60}) {
 const controller = new AbortController();
 const timeout = setTimeout(()=>controller.abort(),10000);
 try {
  const response = await fetchImpl(env.RAILWAY_GRAPHQL_URL || 'https://backboard.railway.com/graphql/v2',{
   method:'POST',headers:headersFor(env),signal:controller.signal,
   body:JSON.stringify({query:QUERY,variables:{projectId:env.RAILWAY_PROJECT_ID,environmentId:env.RAILWAY_ENVIRONMENT_ID,serviceId:env.RAILWAY_EVOLUTION_SERVICE_ID,startDate:new Date(start).toISOString(),endDate:new Date(end).toISOString(),sampleRateSeconds,measurements:['CPU_USAGE','MEMORY_USAGE_GB']}})
  });
  if (response.status===401 || response.status===403) throw new Error('METRICS_AUTH_ERROR: verify token type and project/environment access');
  if (!response.ok) throw new Error(`METRICS_HTTP_ERROR: ${response.status}`);
  const payload=await response.json();
  if (payload.errors?.length) {
   const auth=payload.errors.some(e=>/not authorized|unauthorized|forbidden/i.test(e.message || ''));
   throw new Error(auth?'METRICS_AUTH_ERROR: verify token type and project/environment access':'METRICS_QUERY_ERROR: request rejected');
  }
  if (!Array.isArray(payload.data?.metrics)) throw new Error('METRICS_NO_DATA');
  return {ram:samples(payload.data.metrics,'MEMORY_USAGE_GB',start,end),cpu:samples(payload.data.metrics,'CPU_USAGE',start,end)};
 } catch(e) {
  if (e.name==='AbortError') throw new Error('METRICS_TIMEOUT');
  // Never include arbitrary HTTP bodies or credentials in diagnostics.
  if (/^METRICS_/.test(e.message)) throw e;
  throw new Error('METRICS_FETCH_ERROR');
 } finally {clearTimeout(timeout)}
}
function latestReading(series, now) {
 const ram=series.ram.at(-1),cpu=series.cpu.at(-1);
 if (!ram || !cpu) throw new Error('METRICS_NO_DATA');
 if (now-ram.time>180000 || now-cpu.time>180000) throw new Error('METRICS_STALE_DATA');
 return {memoryGB:ram.value,cpu:cpu.value,checkedAt:new Date(Math.min(ram.time,cpu.time)).toISOString()};
}
function summary(values) {
 if (!values.length) return null;
 const sorted=values.map(v=>v.value).sort((a,b)=>a-b);
 return {samples:sorted.length,mean:sorted.reduce((a,b)=>a+b,0)/sorted.length,peak:sorted.at(-1),p95:sorted[Math.ceil(sorted.length*.95)-1]};
}
function compareWindows(series,split,start,end,sampleMs=60000) {
 if (!(start<split && split<end) || split-start!==end-split) throw new Error('METRICS_COMPARISON_ERROR: equal windows required');
 const one=values=>({ram:summary(values.ram),cpu:summary(values.cpu)});
 const before=one(Object.fromEntries(Object.entries(series).map(([k,v])=>[k,v.filter(x=>x.time>=start && x.time<split)])));
 const after=one(Object.fromEntries(Object.entries(series).map(([k,v])=>[k,v.filter(x=>x.time>=split && x.time<end)])));
 const expected=(split-start)/sampleMs;
 const sufficient=[before.ram,before.cpu,after.ram,after.cpu].every(x=>x && x.samples>=Math.max(2,expected*.8));
 const delta=(a,b)=>a&&b?{absolute:b.mean-a.mean,percent:a.mean===0?null:(b.mean-a.mean)/a.mean*100}:null;
 return {status:sufficient?'COMPARABLE':'INSUFFICIENT_DATA',before,after,delta:sufficient?{ram:delta(before.ram,after.ram),cpu:delta(before.cpu,after.cpu)}:null,limitation:'Observed change only; customer traffic must also be comparable before attributing a change to code.'};
}
module.exports={headersFor,readMetrics,latestReading,compareWindows};
