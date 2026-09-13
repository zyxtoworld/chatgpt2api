export async function runBackupMutation(invalidateDetail, mutation) {
  invalidateDetail();
  return mutation();
}
