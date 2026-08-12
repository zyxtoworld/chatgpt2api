"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";

import webConfig from "@/constants/common-env";
import { createLatestActionOwner } from "@/lib/latest-action-owner";
import { parseChangelog, type ReleaseInfo } from "@/lib/release";

const latestVersionUrl =
  "https://raw.githubusercontent.com/basketikun/chatgpt2api/main/VERSION";
const latestChangelogUrl =
  "https://raw.githubusercontent.com/basketikun/chatgpt2api/main/CHANGELOG.md";

function readLocalReleases(): ReleaseInfo[] {
  return JSON.parse(process.env.NEXT_PUBLIC_APP_RELEASES || "[]");
}

function toVersionParts(version: string) {
  const match = version.trim().match(/^v?(\d+)\.(\d+)\.(\d+)/);
  return match ? match.slice(1).map(Number) : null;
}

function isNewerVersion(latestVersion: string, currentVersion: string) {
  const latest = toVersionParts(latestVersion);
  const current = toVersionParts(currentVersion);
  if (!latest || !current) return false;
  return latest.some(
    (value, index) =>
      value > current[index] &&
      latest.slice(0, index).every((part, prevIndex) => part === current[prevIndex]),
  );
}

export function useVersionCheck() {
  const currentVersion = webConfig.appVersion;
  const localReleases = useMemo(() => readLocalReleases(), []);
  const [latestVersion, setLatestVersion] = useState(currentVersion);
  const [releases, setReleases] = useState<ReleaseInfo[]>(localReleases);
  const [checking, setChecking] = useState(false);
  const [open, setOpen] = useState(false);
  const releaseCheckOwnerRef = useRef(createLatestActionOwner());
  const hasNewVersion = isNewerVersion(latestVersion, currentVersion);

  useEffect(() => {
    const releaseCheckOwner = releaseCheckOwnerRef.current;
    releaseCheckOwner.activate();
    return () => releaseCheckOwner.cancel();
  }, []);

  const checkLatestRelease = useCallback(
    async (showMessage = false) => {
      const releaseCheckOwner = releaseCheckOwnerRef.current;
      const requestOwner = releaseCheckOwner.begin();
      setChecking(true);
      try {
        const [versionResponse, changelogResponse] = await Promise.all([
          fetch(latestVersionUrl),
          fetch(latestChangelogUrl),
        ]);
        if (!versionResponse.ok || !changelogResponse.ok) throw new Error();
        const [version, changelog] = await Promise.all([
          versionResponse.text(),
          changelogResponse.text(),
        ]);
        if (releaseCheckOwner.accepts(requestOwner)) {
          setLatestVersion(version.trim() || currentVersion);
          if (changelog.trim()) setReleases(parseChangelog(changelog));
          if (showMessage) toast.success("已获取最新版本信息");
        }
      } catch {
        if (releaseCheckOwner.accepts(requestOwner)) {
          setLatestVersion(currentVersion);
          setReleases(localReleases);
          if (showMessage) toast.error("获取最新版本信息失败");
        }
      } finally {
        if (releaseCheckOwner.accepts(requestOwner)) {
          setChecking(false);
        }
      }
    },
    [currentVersion, localReleases],
  );

  const openReleaseModal = () => {
    setOpen(true);
    void checkLatestRelease();
  };

  return {
    open,
    setOpen,
    openReleaseModal,
    latestVersion,
    releases,
    checking,
    hasNewVersion,
    checkLatestRelease,
  };
}
