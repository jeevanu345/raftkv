import test from 'node:test';
import assert from 'node:assert/strict';
import { loadSession, saveSession, clearSession } from './session.js';

const key = 'raftkv-operator-v1';
function withStorage(storage, run) {
  const previous = Object.getOwnPropertyDescriptor(globalThis, 'sessionStorage');
  Object.defineProperty(globalThis, 'sessionStorage', { configurable: true, value: storage });
  try { run(); } finally {
    if (previous) Object.defineProperty(globalThis, 'sessionStorage', previous);
    else delete globalThis.sessionStorage;
  }
}

test('local profiles round trip normalized fields and clear on sign out', () => {
  const values = new Map();
  withStorage({ getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value), removeItem: key => values.delete(key) }, () => {
    assert.equal(loadSession(), null);
    assert.deepEqual(saveSession({ name: ' Alex ', email: ' alex@example.com ', password: 'discard' }), { name: 'Alex', email: 'alex@example.com' });
    assert.deepEqual(loadSession(), { name: 'Alex', email: 'alex@example.com' });
    assert.equal(JSON.parse(values.get(key)).password, undefined);
    clearSession();
    assert.equal(loadSession(), null);
  });
});

test('corrupt or unbounded profiles do not become operator sessions', () => {
  for (const value of ['{bad', 'null', '{}', '{"name":9,"email":"a@example.com"}', JSON.stringify({ name: 'a'.repeat(81), email: 'a@example.com' }), JSON.stringify({ name: 'Alex\nRoot', email: 'a@example.com' }), JSON.stringify({ name: 'Alex', email: 'invalid' })]) {
    withStorage({ getItem: () => value }, () => assert.equal(loadSession(), null));
  }
  assert.equal(saveSession({ name: ' ', email: 'a@example.com' }), null);
  assert.equal(saveSession({ name: 'Alex', email: `${'a'.repeat(250)}@example.com` }), null);
});

test('unavailable storage preserves in-memory profile flow without throwing', () => {
  const blocked = () => { throw new Error('Storage blocked'); };
  withStorage({ getItem: blocked, setItem: blocked, removeItem: blocked }, () => {
    assert.equal(loadSession(), null);
    assert.deepEqual(saveSession({ name: 'Alex', email: 'alex@example.com' }), { name: 'Alex', email: 'alex@example.com' });
    assert.doesNotThrow(clearSession);
  });
});
