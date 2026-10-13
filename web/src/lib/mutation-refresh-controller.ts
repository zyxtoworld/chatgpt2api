// @ts-nocheck
type MutationRefreshOptions = {
  gate: { finishMutation(owner: unknown): boolean };
  owner: unknown;
  onBusyChange: (busy: boolean) => void;
  reloadList?: () => unknown;
  refreshSecondary?: (() => unknown) | null;
};

export function finishMutationAndRefresh(options: MutationRefreshOptions) {
  const { gate, owner, onBusyChange, reloadList, refreshSecondary = null } = options;
  if (!gate.finishMutation(owner)) return false;
  onBusyChange(false);
  if (reloadList) void reloadList();
  if (refreshSecondary) void refreshSecondary();
  return true;
}
