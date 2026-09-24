// Shared configuration for the search benchmarks.
//
// Everything here is deterministic: the query set is fixed, and each iteration
// picks its query by counting rather than at random, so two runs of the same
// scenario send the same requests in the same order. A benchmark that varies
// its own input cannot be compared with itself.

// env reads a LOADTEST_* variable, falling back to a default that suits a
// developer machine. Nothing here defaults to a production-sized load.
function env(name, fallback) {
  const value = __ENV[name];
  return value === undefined || value === '' ? fallback : value;
}

function envInt(name, fallback) {
  const parsed = parseInt(env(name, ''), 10);
  return Number.isNaN(parsed) ? fallback : parsed;
}

export const config = {
  baseURL: env('LOADTEST_BASE_URL', 'http://localhost:8080'),
  scenario: env('LOADTEST_SCENARIO', 'baseline'),

  vus: envInt('LOADTEST_VUS', 10),
  duration: env('LOADTEST_DURATION', '30s'),
  // A warm-up at low load, so that connection pools, OpenSearch caches and the
  // JVM's JIT have settled before anything is recorded. Its requests are
  // tagged separately and excluded from every threshold.
  warmup: env('LOADTEST_WARMUP', '15s'),
  warmupVUs: envInt('LOADTEST_WARMUP_VUS', 3),

  limit: envInt('LOADTEST_LIMIT', 10),
  // The limits compared by the "limits" scenario. Nothing larger: the API
  // caps a request at SEARCH_MAX_LIMIT, and a benchmark should measure
  // realistic pages.
  limits: env('LOADTEST_LIMITS', '5,10,20,50').split(',').map((n) => parseInt(n, 10)),

  // Used by the "rps" scenario, which holds a fixed arrival rate rather than
  // a fixed number of users: that is how a sustainable throughput is found.
  targetRPS: envInt('LOADTEST_TARGET_RPS', 50),
  // The highest step of the staged scenario. Raise it deliberately, after the
  // lower steps have stayed healthy.
  maxVUs: envInt('LOADTEST_MAX_VUS', 50),

  querySet: env('LOADTEST_QUERY_SET', 'topics'),
  // Set by the indexing benchmark: a word that appears only in the documents
  // one run generated, so a query can be made to match them and nothing else.
  marker: env('LOADTEST_MARKER', ''),
};

// topicQueries are what the generated corpus is about, so every one of them
// matches real documents. Measuring queries that match nothing measures the
// cost of an empty result, which is not the question.
const topicQueries = [
  'distributed systems',
  'kafka consumer',
  'database indexing',
  'machine learning',
  'vector search',
  'microservices',
  'golang backend',
  'event driven architecture',
];

// phraseQueries are longer, more natural questions. They exist because a
// multi-word query does more work in both retrievers than a single term, and
// the difference belongs in the results.
const phraseQueries = [
  'how do distributed systems handle partial failure',
  'what does a kafka consumer group do with offsets',
  'when should a database add an index',
  'how are embeddings used for search',
  'what makes a service event driven',
];

// querySets are selected with LOADTEST_QUERY_SET.
export function queries() {
  switch (config.querySet) {
    case 'phrases':
      return phraseQueries;
    case 'mixed':
      return topicQueries.concat(phraseQueries);
    case 'marker':
      if (!config.marker) {
        throw new Error('LOADTEST_QUERY_SET=marker needs LOADTEST_MARKER, printed by the indexing benchmark');
      }
      // One term that matches only the documents of one indexing run.
      return [config.marker];
    default:
      return topicQueries;
  }
}

// pickQuery returns the query for an iteration. Counting through the set keeps
// runs reproducible and spreads the load evenly over the corpus, which a
// random pick does neither of.
export function pickQuery(set, vu, iteration) {
  return set[(vu * 7 + iteration) % set.length];
}

// searchURL builds the request the API answers.
export function searchURL(query, limit) {
  return `${config.baseURL}/api/v1/search?q=${encodeURIComponent(query)}&limit=${limit}`;
}

// thresholds are what "healthy" means for a run. They are not a promise about
// the system: they are the line at which a run should be looked at rather than
// read as a number. Only measured traffic counts, never the warm-up.
export function thresholds(extra) {
  return Object.assign(
    {
      'http_req_failed{phase:measure}': ['rate<0.01'],
      'http_req_duration{phase:measure}': ['p(95)<1000', 'p(99)<2000'],
      checks: ['rate>0.99'],
    },
    extra || {}
  );
}

// warmupScenario runs first at low load and is tagged so that thresholds and
// per-phase metrics can ignore it.
export function warmupScenario() {
  return {
    executor: 'constant-vus',
    vus: config.warmupVUs,
    duration: config.warmup,
    startTime: '0s',
    tags: { phase: 'warmup' },
    exec: 'search',
  };
}
