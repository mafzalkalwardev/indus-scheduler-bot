import { expect, it } from 'vitest';
import { backoffMs, canStart, dueJobs, followingRun, reconcile, type SchedJob } from '../src/engine';

const job = (over: Partial<SchedJob> = {}): SchedJob => ({
  id: 1, botId: 'monitor', key: 'check', task: 'check', cron: '*/10 * * * *', everyMs: null,
  groupName: 'default', nextRun: '2026-01-01T00:00:00.000Z', status: 'scheduled', retries: 0,
  maxRetries: 3, kind: 'cron', cronOverride: false, payload: {}, ...over,
});

it('backs off and refuses to overlap CPU work', () => {
  expect(backoffMs(0)).toBe(30_000);
  expect(backoffMs(5)).toBe(600_000);
  expect(canStart(job({ groupName: 'llm' }), [{ botId: 'seo', groupName: 'video' }], false)).toBe(false);
  expect(canStart(job(), [], true)).toBe(false);
  expect(canStart(job(), [], false)).toBe(true);
});

it('catches up once and keeps a manual cron edit', () => {
  const now = new Date('2026-11-10T12:00:30.000Z');
  const next = followingRun(job(), now);
  expect(next && next > now.toISOString()).toBe(true);
  const plan = reconcile([job({ cronOverride: true, cron: '5 * * * *', nextRun: '2099-01-01T00:00:00.000Z' })], [{ key: 'check', task: 'check', cron: '*/10 * * * *' }], now);
  expect(plan.upserts[0].cron).toBe('5 * * * *');
  expect(plan.upserts[0].nextRun).toBe('2099-01-01T00:00:00.000Z');
  expect(dueJobs([job({ nextRun: '2099-01-01T00:00:00.000Z' })], now)).toHaveLength(0);
  expect(dueJobs([job({ nextRun: '2000-01-01T00:00:00.000Z' })], now)).toHaveLength(1);
});
