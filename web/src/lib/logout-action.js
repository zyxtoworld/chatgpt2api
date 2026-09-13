export async function runLogoutAfterClear({ clearSession, onSuccess, onFailure }) {
  try {
    await clearSession();
  } catch {
    onFailure();
    return false;
  }

  onSuccess();
  return true;
}
