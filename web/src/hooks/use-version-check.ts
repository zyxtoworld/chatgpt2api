"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";

import webConfig from "@/constants/common-env";
import { createLatestActionOwner } from "@/lib/latest-action-owner";
import { parseChangelog, type ReleaseInfo } from "@/lib/release";
import { fetchReleaseText } from "@/lib/version-release-fetch";

const latestVersionUrl =
  "https://raw.githubusercontent.com/basketikun/chatgpt2api/main/VERSION";
const latestChangelogUrl =
  "https://raw.githubusercontent.com/basketikun/chatgpt2api/main/CHANGELOG.md";
const VERSION_MAX_BYTES = 16 * 1024;
const CHANGELOG_MAX_BYTES = 2 * 1024 * 1024;

function readLocalReleases(): ReleaseInfo[] {
  try {
    const parsed: unknown = JSON.parse(process.env.NEXT_PUBLIC_APP_RELEASES || "[]");
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.filter((release) => {
      if (!release || typeof release !== "object" || Array.isArray(release)) {
        return false;
      }
      const items = release.items;
      return (
        typeof release.version === "string"
        && typeof release.date === "string"
        && Array.isArray(items)
        && items.every(
          (item) => item
            && typeof item === "object"
            && !Array.isArray(item)
            && typeof item.type === "string"
            && typeof item.content === "string",
        )
      );
    }) as ReleaseInfo[];
  } catch {
    return [];
  }
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
  const releaseAbortControllerRef = useRef<AbortController | null>(null);
  const hasNewVersion = isNewerVersion(latestVersion, currentVersion);

  useEffect(() => {
    const releaseCheckOwner = releaseCheckOwnerRef.current;
    releaseCheckOwner.activate();
    return () => {
      releaseCheckOwner.cancel();
      releaseAbortControllerRef.current?.abort();
      releaseAbortControllerRef.current = null;
    };
  }, []);

  const checkLatestRelease = useCallback(
    async (showMessage = false) => {
      const releaseCheckOwner = releaseCheckOwnerRef.current;
      const requestOwner = releaseCheckOwner.begin();
      releaseAbortControllerRef.current?.abort();
      const abortController = new AbortController();
      releaseAbortControllerRef.current = abortController;
      setChecking(true);
      try {
        const [versionResponse, changelogResponse] = await Promise.all([
          fetchReleaseText(latestVersionUrl, { maxBytes: VERSION_MAX_BYTES, signal: abortController.signal }),
          fetchReleaseText(latestChangelogUrl, { maxBytes: CHANGELOG_MAX_BYTES, signal: abortController.signal }),
        ]);
        if (releaseCheckOwner.accepts(requestOwner)) {
          setLatestVersion(versionResponse.trim() || currentVersion);
          if (changelogResponse.trim()) setReleases(parseChangelog(changelogResponse));
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
          if (releaseAbortControllerRef.current === abortController) {
            releaseAbortControllerRef.current = null;
          }
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
