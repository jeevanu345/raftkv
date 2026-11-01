const SESSION_KEY = 'raftkv-operator-v1';

function normalizeProfile(profile) {
  if (!profile || typeof profile.name !== 'string' || typeof profile.email !== 'string') return null;
  const name = profile.name.trim();
  const email = profile.email.trim();
  if (!name || name.length > 80 || /[\u0000-\u001f\u007f]/.test(name)) return null;
  if (!email || email.length > 254 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return null;
  return { name, email };
}

export function loadSession() {
  try {
    return normalizeProfile(JSON.parse(globalThis.sessionStorage.getItem(SESSION_KEY)));
  } catch {
    return null;
  }
}

export function saveSession(profile) {
  const normalized = normalizeProfile(profile);
  if (!normalized) return null;
  try {
    globalThis.sessionStorage.setItem(SESSION_KEY, JSON.stringify(normalized));
  } catch {
    // The workspace remains usable in memory when browser storage is unavailable.
  }
  return normalized;
}

export function clearSession() {
  try {
    globalThis.sessionStorage.removeItem(SESSION_KEY);
  } catch {
    // A blocked storage area should not prevent leaving the workspace.
  }
}
