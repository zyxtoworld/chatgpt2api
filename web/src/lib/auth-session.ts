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
  const storedSession = await getStoredAuthSession(validationLease);
  if (!storedSession) {
    return null;
  }

  try {
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
    await clearStoredAuthSessionIfCurrent(validationLease);
    return null;
  }
}
