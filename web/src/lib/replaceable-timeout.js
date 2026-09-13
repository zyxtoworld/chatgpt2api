export function createReplaceableTimeout(setTimer = setTimeout, clearTimer = clearTimeout) {
  let timer = null;

  return {
    schedule(callback, delay) {
      if (timer !== null) clearTimer(timer);
      timer = setTimer(() => {
        timer = null;
        callback();
      }, delay);
    },
    cancel() {
      if (timer !== null) {
        clearTimer(timer);
        timer = null;
      }
    },
  };
}
