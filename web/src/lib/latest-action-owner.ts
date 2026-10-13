// @ts-nocheck
export interface LatestAction {
  generation: number;
  identity: unknown;
}

export interface LatestActionOwner {
  begin(identity?: unknown): LatestAction;
  accepts(action: LatestAction | null | undefined, identity?: unknown): boolean;
  invalidate(): void;
  cancel(): void;
  activate(): void;
}

export function createLatestActionOwner(): LatestActionOwner {
  let generation = 0;
  let active = true;

  return {
    begin(identity?: unknown) {
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
