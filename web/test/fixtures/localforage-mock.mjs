export const state = {
  values: new Map(),
  events: [],
  failures: new Map(),
};

export function reset(values = {}) {
  state.values = new Map(Object.entries(values));
  state.events = [];
  state.failures = new Map();
}

function maybeFail(operation, key) {
  const error = state.failures.get(`${operation}:${key}`) || state.failures.get(operation);
  if (error) {
    throw error;
  }
}

const storage = {
  async getItem(key) {
    state.events.push(`get:${key}`);
    maybeFail("get", key);
    return state.values.get(key);
  },
  async setItem(key, value) {
    state.events.push(`set:${key}`);
    maybeFail("set", key);
    state.values.set(key, value);
    return value;
  },
  async removeItem(key) {
    state.events.push(`remove:${key}`);
    maybeFail("remove", key);
    state.values.delete(key);
  },
};

export default {
  createInstance() {
    return storage;
  },
};
