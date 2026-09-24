// Search API benchmark.
//
//   k6 run benchmarks/search/search.js                                  baseline
//   k6 run -e LOADTEST_SCENARIO=staged benchmarks/search/search.js      rising load
//   k6 run -e LOADTEST_SCENARIO=limits benchmarks/search/search.js      page size
//   k6 run -e LOADTEST_SCENARIO=rps benchmarks/search/search.js         fixed arrival rate
//   k6 run -e LOADTEST_SCENARIO=validation benchmarks/search/search.js  bad requests
//
// Every scenario starts with a warm-up phase whose requests are tagged
// phase:warmup and excluded from the thresholds and from the reported
// percentiles. See benchmarks/README.md for what each one answers.

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { config, queries, pickQuery, searchURL, thresholds, warmupScenario } from './scenarios.js';

// Results per response: a page that comes back empty is not a fast search, it
// is a broken corpus, and the two look identical in a latency chart.
const resultsReturned = new Trend('search_results_returned');
const emptyResponses = new Counter('search_empty_responses');

const querySet = queries();

export const options = buildOptions();

function buildOptions() {
  const scenarios = { warmup: warmupScenario() };
  const startAfterWarmup = config.warmup;

  switch (config.scenario) {
    case 'staged':
      // Load rises in steps so the point where latency starts to climb is
      // visible, instead of only the end state.
      scenarios.staged = {
        executor: 'ramping-vus',
        startVUs: 1,
        startTime: startAfterWarmup,
        gracefulRampDown: '10s',
        tags: { phase: 'measure' },
        exec: 'search',
        stages: [
          { duration: '30s', target: Math.max(1, Math.round(config.maxVUs * 0.2)) },
          { duration: '30s', target: Math.max(1, Math.round(config.maxVUs * 0.5)) },
          { duration: '60s', target: config.maxVUs },
          { duration: '30s', target: 0 },
        ],
      };
      break;

    case 'limits':
      // One scenario per page size, run one after another so their numbers are
      // comparable rather than mixed together.
      config.limits.forEach((limit, i) => {
        scenarios[`limit_${limit}`] = {
          executor: 'constant-vus',
          vus: config.vus,
          duration: config.duration,
          startTime: addSeconds(startAfterWarmup, i * durationSeconds(config.duration)),
          tags: { phase: 'measure', limit: String(limit) },
          env: { LIMIT: String(limit) },
          exec: 'search',
        };
      });
      break;

    case 'rps':
      // A fixed arrival rate, not a fixed number of users: if the system
      // cannot keep up, latency and the queue grow instead of the load
      // quietly shrinking to match.
      scenarios.rps = {
        executor: 'constant-arrival-rate',
        rate: config.targetRPS,
        timeUnit: '1s',
        duration: config.duration,
        startTime: startAfterWarmup,
        preAllocatedVUs: Math.max(config.vus, Math.ceil(config.targetRPS / 5)),
        maxVUs: Math.max(config.maxVUs, config.targetRPS),
        tags: { phase: 'measure' },
        exec: 'search',
      };
      break;

    case 'validation':
      // Correctness of the API's rejections, kept away from the performance
      // numbers: these requests are meant to fail, and mixing them into a
      // throughput figure would make the error rate meaningless.
      scenarios.validation = {
        executor: 'per-vu-iterations',
        vus: 1,
        iterations: 1,
        startTime: startAfterWarmup,
        tags: { phase: 'validation' },
        exec: 'validation',
      };
      break;

    default: // baseline
      scenarios.baseline = {
        executor: 'constant-vus',
        vus: config.vus,
        duration: config.duration,
        startTime: startAfterWarmup,
        tags: { phase: 'measure' },
        exec: 'search',
      };
  }

  return {
    scenarios,
    thresholds: config.scenario === 'validation' ? { checks: ['rate>0.99'] } : thresholds(),
    // Percentiles are the point of the exercise, so ask for the ones the
    // report quotes.
    summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
    discardResponseBodies: false,
  };
}

// search sends one valid request, which is what the measured traffic is made
// of.
export function search() {
  const limit = parseInt(__ENV.LIMIT || String(config.limit), 10);
  const query = pickQuery(querySet, __VU, __ITER);
  const response = http.get(searchURL(query, limit), {
    tags: { name: 'GET /api/v1/search' },
  });

  const ok = check(response, {
    'status is 200': (r) => r.status === 200,
    'body is a search response': (r) => {
      if (r.status !== 200) {
        return false;
      }
      const body = safeJSON(r);
      return body !== null && Array.isArray(body.results);
    },
  });

  if (ok && response.status === 200) {
    const body = safeJSON(response);
    const count = body && Array.isArray(body.results) ? body.results.length : 0;
    resultsReturned.add(count);
    if (count === 0) {
      emptyResponses.add(1);
    }
  }
}

// validation checks how the API answers requests that should be refused. It
// asserts status codes, not speed.
export function validation() {
  const cases = [
    { name: 'empty query', url: `${config.baseURL}/api/v1/search?q=`, want: 400 },
    { name: 'missing query', url: `${config.baseURL}/api/v1/search`, want: 400 },
    { name: 'negative limit', url: `${config.baseURL}/api/v1/search?q=kafka&limit=-1`, want: 400 },
    { name: 'limit above the maximum', url: `${config.baseURL}/api/v1/search?q=kafka&limit=100000`, want: 400 },
    { name: 'limit is not a number', url: `${config.baseURL}/api/v1/search?q=kafka&limit=ten`, want: 400 },
    { name: 'unknown path', url: `${config.baseURL}/api/v1/nothing`, want: 404 },
    { name: 'health', url: `${config.baseURL}/health`, want: 200 },
    { name: 'ready', url: `${config.baseURL}/ready`, want: 200 },
  ];

  cases.forEach((testCase) => {
    const response = http.get(testCase.url, { tags: { name: testCase.name } });
    check(response, {
      [`${testCase.name} answers ${testCase.want}`]: (r) => r.status === testCase.want,
      [`${testCase.name} answers JSON`]: (r) => (r.headers['Content-Type'] || '').includes('application/json'),
    });
  });
}

function safeJSON(response) {
  try {
    return response.json();
  } catch (err) {
    return null;
  }
}

function durationSeconds(duration) {
  const match = /^(\d+)(s|m)$/.exec(duration);
  if (!match) {
    return 30;
  }
  return match[2] === 'm' ? parseInt(match[1], 10) * 60 : parseInt(match[1], 10);
}

function addSeconds(duration, seconds) {
  return `${durationSeconds(duration) + seconds}s`;
}

// handleSummary writes the run to benchmarks/results and prints the few
// numbers a report needs. k6's own summary stays on screen; this is the part
// that gets pasted into results/README.md.
export function handleSummary(data) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const name = `${config.scenario}-${stamp}`;

  const out = {};
  out[`benchmarks/results/${name}.json`] = JSON.stringify(data, null, 2);
  out.stdout = report(data);
  return out;
}

function report(data) {
  const metrics = data.metrics;
  const duration = metrics.http_req_duration || {};
  const requests = metrics.http_reqs || {};
  const failed = metrics.http_req_failed || {};
  const results = metrics.search_results_returned || {};

  const lines = [
    '',
    '='.repeat(64),
    `Search benchmark: ${config.scenario}`,
    '='.repeat(64),
    `Target:        ${config.baseURL}`,
    `Query set:     ${config.querySet} (${querySet.length} queries)`,
    `Warm-up:       ${config.warmup} at ${config.warmupVUs} VUs (excluded below)`,
    `Measured:      ${describeLoad()}`,
    '',
    `Requests:      ${count(requests)}`,
    `Throughput:    ${rate(requests)} req/s`,
    `Error rate:    ${percent(failed)}`,
    `Empty results: ${count(metrics.search_empty_responses || {})}`,
    `Results/page:  avg ${value(results, 'avg')}`,
    '',
    'Latency (all phases, including warm-up; read the per-phase numbers',
    'from the JSON in benchmarks/results for the measured phase alone):',
    `  avg ${value(duration, 'avg')} ms   p50 ${value(duration, 'med')} ms   p90 ${value(duration, 'p(90)')} ms`,
    `  p95 ${value(duration, 'p(95)')} ms   p99 ${value(duration, 'p(99)')} ms   max ${value(duration, 'max')} ms`,
    '',
    'Copy these into benchmarks/results/README.md together with the',
    'environment they were measured on. A number without its environment',
    'cannot be compared with anything.',
    '='.repeat(64),
    '',
  ];
  return lines.join('\n');
}

function describeLoad() {
  switch (config.scenario) {
    case 'staged':
      return `ramping to ${config.maxVUs} VUs`;
    case 'rps':
      return `${config.targetRPS} req/s for ${config.duration}`;
    case 'limits':
      return `${config.vus} VUs for ${config.duration} per limit (${config.limits.join(', ')})`;
    case 'validation':
      return 'one pass over the invalid requests';
    default:
      return `${config.vus} VUs for ${config.duration}, limit=${config.limit}`;
  }
}

function value(metric, key) {
  const values = metric.values || {};
  const n = values[key];
  return n === undefined ? '-' : Number(n).toFixed(1);
}

function count(metric) {
  const values = metric.values || {};
  return values.count === undefined ? '-' : String(Math.round(Number(values.count)));
}

function rate(metric) {
  const values = metric.values || {};
  return values.rate === undefined ? '-' : Number(values.rate).toFixed(1);
}

function percent(metric) {
  const values = metric.values || {};
  return values.rate === undefined ? '-' : `${(Number(values.rate) * 100).toFixed(2)}%`;
}
