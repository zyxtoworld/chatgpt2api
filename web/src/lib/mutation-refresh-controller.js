/**
 * @param {{
 *   gate: any,
 *   owner: any,
 *   onBusyChange: (busy: boolean) => void,
 *   reloadList: () => unknown,
 *   refreshSecondary?: (() => unknown) | null,
 * }} options
 */
export function finishMutationAndRefresh({
  gate,
  owner,
  onBusyChange,
  reloadList,
  refreshSecondary = null,
}) {
  if (!gate.finishMutation(owner)) return false;
  onBusyChange(false);
  if (reloadList) void reloadList();
  if (refreshSecondary) void refreshSecondary();
  return true;
}
