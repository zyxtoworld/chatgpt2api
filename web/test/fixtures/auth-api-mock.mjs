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
  let reject;
  const promise = new Promise((nextResolve, nextReject) => {
    resolve = nextResolve;
    reject = nextReject;
  });
  state.responses.push(promise);
  return { resolve, reject };
}

export async function login(key) {
  state.calls.push(key);
  const response = state.responses.shift();
  return response || { role: "user", subject_id: "fresh", name: "Fresh" };
}
