export function createEditableFileHistoryWriteCoordinator() {
  let tail = Promise.resolve();

  return {
    enqueue(operation) {
      const result = tail.then(operation);
      tail = result.then(
        () => undefined,
        () => undefined,
      );
      return result;
    },
  };
}
