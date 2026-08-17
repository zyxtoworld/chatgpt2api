export function observeViewTransition(transition, { onReady, onSettled }) {
  let settled = false;
  const settle = () => {
    if (settled) return;
    settled = true;
    onSettled();
  };

  const ready = transition?.ready;
  if (ready && typeof ready.then === "function") {
    void Promise.resolve(ready)
      .then(() => {
        if (!settled) onReady();
      })
      .catch(settle);
  }

  const finished = transition?.finished;
  if (finished && typeof finished.then === "function") {
    void Promise.resolve(finished).then(settle, settle);
  } else {
    settle();
  }
}

export function startObservedViewTransition(startTransition, applyTheme, { onReady, onSettled }) {
  let transition;
  try {
    transition = startTransition();
  } catch {
    onSettled();
    applyTheme();
    return false;
  }
  observeViewTransition(transition, { onReady, onSettled });
  return true;
}
