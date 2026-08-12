export function createLatestActionOwner() {
  let generation = 0;
  let active = true;

  return {
    begin(identity) {
      return { generation: ++generation, identity };
    },

    accepts(action, identity = action?.identity) {
      return Boolean(
        active
          && action
          && action.generation === generation
          && Object.is(action.identity, identity),
      );
    },

    invalidate() {
      generation += 1;
    },

    cancel() {
      active = false;
      generation += 1;
    },

    activate() {
      active = true;
      generation += 1;
    },
  };
}
