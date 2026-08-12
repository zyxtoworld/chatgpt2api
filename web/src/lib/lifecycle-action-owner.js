export function createLifecycleActionOwner() {
  let epoch = 0;
  let nextToken = 0;
  let active = true;

  return {
    begin() {
      return { epoch, token: ++nextToken };
    },

    accepts(action) {
      return Boolean(active && action && action.epoch === epoch);
    },

    invalidate() {
      epoch += 1;
    },

    cancel() {
      active = false;
      epoch += 1;
    },

    activate() {
      active = true;
      epoch += 1;
    },
  };
}
