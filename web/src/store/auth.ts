"use client";

import localforage from "localforage";
import { createAuthStorageCoordinator } from "@/lib/auth-storage-coordinator";
import { normalizeStoredAuthSession } from "@/lib/auth-storage-record";

export type AuthRole = "admin" | "user";

export type StoredAuthSession = {
  key: string;
  role: AuthRole;
  subjectId: string;
  name: string;
};

export const AUTH_KEY_STORAGE_KEY = "chatgpt2api_auth_key";
export const AUTH_SESSION_STORAGE_KEY = "chatgpt2api_auth_session";

const authStorage = localforage.createInstance({
  name: "chatgpt2api",
  storeName: "auth",
});

const authStorageCoordinator = createAuthStorageCoordinator({
  keyName: AUTH_KEY_STORAGE_KEY,
  sessionName: AUTH_SESSION_STORAGE_KEY,
  getItem: (key: string) => authStorage.getItem(key),
  setItem: (key: string, value: unknown) => authStorage.setItem(key, value),
  removeItem: (key: string) => authStorage.removeItem(key),
});

export type AuthStorageValidationLease = { epoch: number };

export function beginStoredAuthValidation(): AuthStorageValidationLease {
  return authStorageCoordinator.beginValidation();
}

export function beginStoredAuthMutation(): AuthStorageValidationLease {
  return authStorageCoordinator.beginMutation();
}

function normalizeSession(value: unknown, fallbackKey = ""): StoredAuthSession | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  const candidate = value as Partial<StoredAuthSession>;
  const key = String(candidate.key || fallbackKey || "").trim();
  const role = candidate.role === "admin" || candidate.role === "user" ? candidate.role : null;
  if (!key || !role) {
    return null;
  }

  return {
    key,
    role,
    subjectId: String(candidate.subjectId || "").trim(),
    name: String(candidate.name || "").trim(),
  };
}

export function getDefaultRouteForRole(role: AuthRole) {
  return role === "admin" ? "/accounts" : "/image";
}

async function readStoredAuthSnapshot(validationLease?: AuthStorageValidationLease) {
  if (typeof window === "undefined") {
    return null;
  }

  const readLease = validationLease || beginStoredAuthValidation();
  const snapshot = await authStorageCoordinator.readPairIfCurrent(readLease);
  if (!snapshot) {
    return null;
  }

  const normalizedSession = normalizeStoredAuthSession(snapshot.key, snapshot.session);
  if (normalizedSession) {
    return { lease: readLease, session: normalizedSession };
  }

  if (snapshot.key || snapshot.session) {
    await clearStoredAuthSessionIfCurrent(readLease);
  }
  return { lease: readLease, session: null };
}

export async function getStoredAuthKey() {
  const snapshot = await readStoredAuthSnapshot();
  return snapshot?.session?.key || "";
}

export async function getStoredAuthSession(validationLease?: AuthStorageValidationLease) {
  const snapshot = await readStoredAuthSnapshot(validationLease);
  return snapshot?.session || null;
}

export async function setStoredAuthSession(session: StoredAuthSession) {
  const mutationLease = beginStoredAuthMutation();
  await setStoredAuthSessionIfCurrent(session, mutationLease);
}

export async function setStoredAuthSessionIfCurrent(
  session: StoredAuthSession,
  validationLease: AuthStorageValidationLease,
) {
  const normalizedSession = normalizeSession(session);
  if (!normalizedSession) {
    await clearStoredAuthSessionIfCurrent(validationLease);
    return false;
  }

  return authStorageCoordinator.setSessionIfCurrent(normalizedSession, validationLease);
}

export async function clearStoredAuthSession() {
  if (typeof window === "undefined") {
    return;
  }
  await authStorageCoordinator.clearSession();
}

export async function clearStoredAuthSessionIfCurrent(validationLease: AuthStorageValidationLease) {
  if (typeof window === "undefined") {
    return false;
  }
  return authStorageCoordinator.clearSessionIfCurrent(validationLease);
}
