export function createRequestGate(initialQuery = "") {
  let currentQuery = initialQuery;
  let generation = 0;
  let sequence = 0;

  return {
    setQuery(query) {
      if (query !== currentQuery) {
        currentQuery = query;
        generation += 1;
      }
    },
    begin(query) {
      if (query !== currentQuery) {
        return { generation, query, sequence: null };
      }
      const request = { generation, query, sequence: ++sequence };
      return request;
    },
    isCurrent(request) {
      return request.sequence !== null
        && request.generation === generation
        && request.query === currentQuery
        && request.sequence === sequence;
    },
  };
}
