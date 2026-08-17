export function createDownloadAbortRegistry() {
  let active = true;
  const controllers = new Set();

  return {
    activate() {
      active = true;
    },

    begin() {
      const controller = new AbortController();
      if (active) controllers.add(controller);
      else controller.abort();
      return controller;
    },

    finish(controller) {
      controllers.delete(controller);
    },

    cancel() {
      active = false;
      for (const controller of controllers) controller.abort();
      controllers.clear();
    },

    activeCount() {
      return controllers.size;
    },
  };
}
