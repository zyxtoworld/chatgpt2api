export const state = {
  calls: [],
  responses: [],
};

export function reset() {
  state.calls = [];
  state.responses = [];
}

export function queueLoginResponse() {
  let resolve;
  const promise = new Promise((nextResolve) => {
    resolve = nextResolve;
  });
  state.responses.push(promise);
  return { resolve };
}

export async function login(key) {
  state.calls.push(key);
  const response = state.responses.shift();
  return response || { role: "user", subject_id: "fresh", name: "Fresh" };
}
