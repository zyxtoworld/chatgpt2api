export function createMutationRequestGate() {
  const queryGenerations = new Map([["default", 0]]);
  let mutationEpoch = 0;
  let mutationActive = false;

  const getDomain = (domain) => domain || "default";
  const nextQueryGeneration = (domain) => {
    const key = getDomain(domain);
    const generation = (queryGenerations.get(key) || 0) + 1;
    queryGenerations.set(key, generation);
    return { key, generation };
  };

  const acceptsMutation = (owner) => Boolean(
    mutationActive && owner?.epoch === mutationEpoch,
  );

  return {
    beginQuery(domain = "default") {
      const { key, generation } = nextQueryGeneration(domain);
      const owner = {
        domain: key,
        generation,
        mutationEpoch,
        allowed: !mutationActive,
      };
      return owner;
    },

    acceptsQuery(owner) {
      return Boolean(
        owner?.allowed
          && !mutationActive
          && owner.mutationEpoch === mutationEpoch
          && owner.generation === queryGenerations.get(getDomain(owner.domain)),
      );
    },

    beginMutation() {
      if (mutationActive) {
        return { accepted: false, epoch: mutationEpoch };
      }
      mutationEpoch += 1;
      mutationActive = true;
      return { accepted: true, epoch: mutationEpoch };
    },

    acceptsMutation(owner) {
      return Boolean(owner?.accepted && acceptsMutation(owner));
    },

    finishMutation(owner) {
      if (!owner?.accepted || !acceptsMutation(owner)) return false;
      mutationActive = false;
      return true;
    },

    invalidateQueries(domain = "default") {
      nextQueryGeneration(domain);
    },

    cancel() {
      mutationEpoch += 1;
      mutationActive = false;
      for (const domain of queryGenerations.keys()) {
        nextQueryGeneration(domain);
      }
    },

    isMutationActive() {
      return mutationActive;
    },
  };
}
