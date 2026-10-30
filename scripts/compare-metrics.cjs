'use strict';
// Read-only API query. ISO dates must include Z or an explicit UTC offset.
const {readMetrics,compareWindows}=require('../lib/railway-metrics.cjs');
(async()=>{
 const [from,change,to]=process.argv.slice(2);
 if (![from,change,to].every(x=>typeof x==='string' && /(?:Z|[+-]\d{2}:\d{2})$/.test(x))) throw new Error('Use three ISO dates with explicit timezone: start change end');
 const [start,split,end]=[from,change,to].map(Date.parse);
 if (![start,split,end].every(Number.isFinite) || !(start<split&&split<end) || split-start!==end-split) throw new Error('Use valid equal windows');
 if (end-start>48*3600000) throw new Error('Maximum total period is 48 hours');
 const series=await readMetrics({start,end});
 console.log(JSON.stringify({startUTC:new Date(start).toISOString(),changeUTC:new Date(split).toISOString(),endUTC:new Date(end).toISOString(),...compareWindows(series,split,start,end)},null,2));
})().catch(err=>{console.error(err.message);process.exitCode=1});
