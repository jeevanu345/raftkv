import { test, expect } from '@playwright/test';

const profile = { name: 'Alex Morgan', email: 'alex@example.com' };
const panes = [['home', 'Home'], ['overview', 'Cluster overview'], ['nodes', 'Nodes & replication'], ['keys', 'Keyspace explorer'], ['telemetry', 'Telemetry & events'], ['report', 'Architecture report'], ['settings', 'Connections & runbook']];

async function operator(page) {
  await page.addInitScript(profile => sessionStorage.setItem('raftkv-operator-v1', JSON.stringify(profile)), profile);
}

async function clusterApi(page, offline = false) {
  const writes = [];
  const keys = new Map([['greeting', 'hello raft']]);
  const peers = [1, 2, 3].map(id => ({ id, voter: true, address: `127.0.0.1:${7000 + id}` }));
  await page.route('**/nodes/*/api/**', async route => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    const id = Number(path.match(/\/nodes\/(\d+)/)[1]) - 8079;
    if (offline) return route.fulfill({ status: 502, json: { error: 'Node API unavailable. Check the local port.' } });
    if (path.endsWith('/status')) return route.fulfill({ json: {
      node_id: id, role: id === 1 ? 'leader' : 'follower', term: 4, leader_id: 1,
      commit_index: 12, applied_index: 12, last_log_index: 12, state_hash: 'fixture-hash', keys: keys.size,
      uptime_ms: 60000, membership: { voters: [1, 2, 3], quorum_size: 2, joint: false }, nodes: peers,
      health: { state: 'healthy' }, log: { first_index: 1, last_term: 4, uncommitted_entries: 0, unapplied_entries: 0 },
      queues: { pending_proposals: 0, pending_reads: 0 }, counters: { proposals_received: 12, proposals_committed: 12 },
      config: { heartbeat_ms: 100, election_timeout_ms: 1000, tick_ms: 10, pre_vote: true },
      endpoints: { raft: `127.0.0.1:${7000 + id}`, api: `127.0.0.1:${8079 + id}` },
    } });
    if (path.endsWith('/log')) return route.fulfill({ json: { entries: [] } });
    if (path.endsWith('/keys')) return route.fulfill({ json: { node_id: id, applied_index: 12, keys: [...keys.keys()].map(key => [...new TextEncoder().encode(key)]) } });
    const key = decodeURIComponent(path.split('/keys/')[1] || '');
    if (request.method() === 'PUT') { writes.push({ id, key, value: request.postDataJSON().value }); keys.set(key, request.postDataJSON().value); return route.fulfill({ json: { ok: true } }); }
    if (request.method() === 'DELETE') return route.fulfill({ json: { removed: keys.delete(key) } });
    return route.fulfill({ json: { value: keys.get(key), encoding: 'utf-8', node_id: id, applied_index: 12 } });
  });
  return writes;
}

async function noOverflow(page) {
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
}

test('local sign-in validates, survives reload, and clears on sign-out', async ({ page }) => {
  await clusterApi(page, true);
  await page.goto('/');
  await expect(page).toHaveTitle('Welcome · RaftKV');
  await page.getByRole('button', { name: 'Enter workspace' }).click();
  await expect(page.getByRole('textbox', { name: 'Your name' })).toBeFocused();
  await page.getByRole('textbox', { name: 'Your name' }).fill(profile.name);
  await page.getByRole('textbox', { name: 'Email address' }).fill(profile.email);
  await page.getByRole('button', { name: 'Enter workspace' }).click();
  await expect(page.getByRole('heading', { name: 'Welcome, Alex.' })).toBeVisible();
  await page.reload();
  await expect(page.getByRole('heading', { name: 'Welcome, Alex.' })).toBeVisible();
  await page.getByRole('button', { name: 'Sign out', exact: true }).click();
  await expect(page).toHaveTitle('Welcome · RaftKV');
  await page.reload();
  await expect(page.getByRole('button', { name: 'Enter workspace' })).toBeVisible();
});

test('keyboard command menu searches, navigates, and restores focus', async ({ page }) => {
  await operator(page); await clusterApi(page); await page.goto('/');
  const trigger = page.locator('.workspace-search');
  await trigger.click();
  await expect(page.getByRole('searchbox')).toBeFocused();
  await page.getByRole('searchbox').fill('no such tool');
  await expect(page.getByText(/No tools match/)).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(trigger).toBeFocused();
  await page.keyboard.press('Control+k');
  await page.getByRole('searchbox').fill('keyspace');
  await page.keyboard.press('ArrowDown');
  await page.keyboard.press('Enter');
  await expect(page).toHaveURL(/#keys$/);
  await expect(page.getByRole('heading', { name: 'Keyspace explorer', exact: true })).toBeFocused();
  await expect(page.getByRole('dialog')).not.toBeVisible();
});

test('deep links and browser history keep navigation in sync', async ({ page }) => {
  await operator(page); await clusterApi(page); await page.goto('/#nodes');
  await expect(page.getByRole('heading', { name: 'Nodes & replication', exact: true })).toBeVisible();
  await page.getByRole('link', { name: 'Keyspace explorer', exact: true }).click();
  await expect(page).toHaveURL(/#keys$/);
  await page.getByRole('link', { name: 'Skip to dashboard' }).focus();
  await page.keyboard.press('Enter');
  await expect(page.locator('#content')).toBeFocused();
  await expect(page).toHaveURL(/#keys$/);
  await page.goBack();
  await expect(page.getByRole('link', { name: 'Nodes & replication', exact: true })).toHaveAttribute('aria-current', 'page');
  await page.goto('/#unknown');
  await expect(page.getByRole('heading', { name: 'Home', exact: true })).toBeVisible();
});

test('home reflects observations and selects the chosen node', async ({ page }) => {
  await operator(page); await clusterApi(page); await page.goto('/');
  await expect(page.getByText('Node APIs connected', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: /Node 3.*127.0.0.1:8082.*follower/ })).toBeVisible();
  await page.getByRole('button', { name: /Node 3.*127.0.0.1:8082.*follower/ }).click();
  await expect(page.getByLabel('Inspect runtime')).toHaveValue('3');
});

test('offline guidance, paused polling, and connection updates remain usable', async ({ page }) => {
  await operator(page); await clusterApi(page, true); await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Bring your cluster online' })).toBeVisible();
  await page.getByRole('button', { name: 'Pause polling' }).click();
  await expect(page.getByText('Polling paused', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Check connections', exact: true }).click();
  await page.getByLabel('Node ID', { exact: true }).fill('4');
  await expect(page.getByLabel('Local API port')).toHaveValue('8083');
  await page.getByRole('button', { name: 'Connect / update' }).click();
  await expect(page.getByText('Monitoring node 4 through localhost:8083.', { exact: false })).toBeVisible();
  await expect.poll(() => page.evaluate(() => JSON.parse(localStorage.getItem('raftkv-monitors-v1')).some(node => node.id === 4))).toBe(true);
});

test('key creation continues to target the observed leader', async ({ page }) => {
  await operator(page); const writes = await clusterApi(page); await page.goto('/#keys');
  await page.getByRole('textbox', { name: 'Key', exact: true }).fill('ui:test');
  await page.getByRole('textbox', { name: 'Value', exact: true }).fill('readable console');
  await expect(page.getByRole('button', { name: 'Commit SET', exact: true })).toBeEnabled();
  await page.getByRole('button', { name: 'Commit SET', exact: true }).click();
  await expect(page.getByText('SET committed through node 1: ui:test', { exact: true })).toBeVisible();
  expect(writes).toEqual([{ id: 1, key: 'ui:test', value: 'readable console' }]);
  await page.getByRole('textbox', { name: 'Filter keys' }).fill('ui:test');
  await expect(page.getByRole('cell', { name: 'ui:test', exact: true })).toBeVisible();
});

test('theme persists and reduced motion disables page transitions', async ({ page }) => {
  await operator(page); await clusterApi(page); await page.emulateMedia({ reducedMotion: 'reduce' }); await page.goto('/');
  await page.getByRole('button', { name: 'Light appearance' }).click();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
  await page.reload();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
  await expect(page.locator('.pane-content')).toHaveCSS('animation-name', 'none');
});

test('narrow offline home handles long operator names and readable polling status', async ({ page }) => {
  await page.setViewportSize({ width: 320, height: 844 });
  await page.addInitScript(() => sessionStorage.setItem('raftkv-operator-v1', JSON.stringify({ name: 'A'.repeat(80), email: 'operator@example.com' })));
  await clusterApi(page, true); await page.goto('/');
  await expect(page.getByText('No reachable node APIs', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Pause polling' }).click();
  await expect(page.getByText('Polling paused', { exact: true })).toBeVisible();
  await noOverflow(page);
  const status = await page.locator('.live-state').boundingBox();
  const pause = await page.getByRole('button', { name: 'Resume polling' }).boundingBox();
  expect(pause.y).toBeGreaterThanOrEqual(status.y + status.height);
});

for (const width of [320, 390, 768, 1440]) {
  test(`all pages fit a ${width}px viewport without horizontal overflow or runtime errors`, async ({ page }) => {
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.setViewportSize({ width, height: 900 });
    await clusterApi(page);
    await page.goto('/');
    await noOverflow(page);
    await page.getByRole('textbox', { name: 'Your name' }).fill(profile.name);
    await page.getByRole('textbox', { name: 'Email address' }).fill(profile.email);
    await page.getByRole('button', { name: 'Enter workspace' }).click();
    for (const [pane, title] of panes) {
      if (width <= 760) await page.getByRole('button', { name: 'Open navigation', exact: true }).click();
      await page.getByRole('link', { name: title, exact: true }).click();
      await expect(page.getByRole('heading', { name: title, exact: true })).toBeVisible();
      await noOverflow(page);
      if (width <= 760) await expect(page.getByRole('button', { name: 'Open navigation', exact: true })).toHaveAttribute('aria-expanded', 'false');
    }
    expect(errors).toEqual([]);
  });
}
