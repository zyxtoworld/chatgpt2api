"use client";

import { login } from "@/lib/api";
import {
  beginStoredAuthValidation,
  clearStoredAuthSessionIfCurrent,
  getStoredAuthSession,
  setStoredAuthSessionIfCurrent,
  type StoredAuthSession,
} from "@/store/auth";

export async function getValidatedAuthSession(): Promise<StoredAuthSession | null> {
  const validationLease = beginStoredAuthValidation();
  try {
    const storedSession = await getStoredAuthSession(validationLease);
    if (!storedSession) {
      return null;
    }

    const data = await login(storedSession.key);
    const nextSession: StoredAuthSession = {
      key: storedSession.key,
      role: data.role,
      subjectId: data.subject_id,
      name: data.name,
    };
    const committed = await setStoredAuthSessionIfCurrent(nextSession, validationLease);
    return committed ? nextSession : null;
  } catch {
    try {
      await clearStoredAuthSessionIfCurrent(validationLease);
    } catch {
      // The coordinator already invalidated this persisted pair for the current runtime.
    }
    return null;
  }
}
