export function formatAvailableImageQuota(accounts) {
  if (!Array.isArray(accounts)) return "0";
  const total = accounts.reduce((sum, account) => {
    if (
      !account
      || account.status !== "正常"
      || !Number.isSafeInteger(account.quota)
      || account.quota <= 0
    ) {
      return sum;
    }
    return sum + account.quota;
  }, 0);
  return String(total);
}
