export function createAuthStorageCoordinator({ keyName, sessionName, getItem, setItem, removeItem }) {
  let mutationEpoch = 0;
  let tail = Promise.resolve();

  /**
   * @template T
   * @param {() => Promise<T>} operation
   * @returns {Promise<T>}
   */
  function enqueue(operation) {
    const result = tail.then(operation);
    tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  function isCurrent(lease) {
    return Boolean(lease && lease.epoch === mutationEpoch);
  }

  function beginValidation() {
    return { epoch: mutationEpoch };
  }

  function beginMutation() {
    mutationEpoch += 1;
    return { epoch: mutationEpoch };
  }

  function setSessionIfCurrent(session, lease) {
    return enqueue(async () => {
      if (!isCurrent(lease)) {
        return false;
      }

      try {
        await setItem(keyName, session.key);
        if (!isCurrent(lease)) {
          await removeBoth();
          return false;
        }
        await setItem(sessionName, session);
        if (!isCurrent(lease)) {
          await removeBoth();
          return false;
        }
      } catch (error) {
        await removeBoth();
        throw error;
      }
      return true;
    });
  }

  /** @returns {Promise<{key: unknown, session: unknown} | null>} */
  function readPairIfCurrent(lease) {
    return enqueue(async () => {
      if (!isCurrent(lease)) {
        return null;
      }

      const [key, session] = await Promise.all([
        getItem(keyName),
        getItem(sessionName),
      ]);
      return isCurrent(lease) ? { key, session } : null;
    });
  }

  async function removeBoth() {
    let firstError = null;
    try {
      await removeItem(sessionName);
    } catch (error) {
      firstError = error;
    }
    try {
      await removeItem(keyName);
    } catch (error) {
      firstError ||= error;
    }
    if (firstError) {
      throw firstError;
    }
  }

  function clearSession() {
    beginMutation();
    return enqueue(async () => {
      await removeBoth();
      return true;
    });
  }

  function clearSessionIfCurrent(lease) {
    if (!isCurrent(lease)) {
      return Promise.resolve(false);
    }

    const clearLease = beginMutation();
    return enqueue(async () => {
      if (!isCurrent(clearLease)) {
        return false;
      }

      await removeBoth();
      return true;
    });
  }

  return {
    beginMutation,
    beginValidation,
    clearSession,
    clearSessionIfCurrent,
    enqueue,
    isCurrent,
    readPairIfCurrent,
    setSessionIfCurrent,
  };
}
