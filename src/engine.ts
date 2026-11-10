import { nextCron } from '@indus/bot-core';

export type JobKind = 'cron' | 'interval' | 'once';

export type SchedJob = {
  id: number;
  botId: string;
  key: string;
  task: string;
  cron: string | null;
  everyMs: number | null;
  groupName: string;
  nextRun: string | null;
  status: string;
  retries: number;
  maxRetries: number;
  kind: JobKind;
  cronOverride: boolean;
  payload: Record<string, unknown>;
};

export function backoffMs(retries: number): number {
  return [30_000, 120_000, 600_000][Math.min(Math.max(retries, 0), 2)];
}

export function poolOf(groupName: string): 'cpu' | 'default' {
  return groupName === 'llm' || groupName === 'video' ? 'cpu' : 'default';
}

const LIMITS = { cpu: 1, default: 2 };

export function canStart(job: Pick<SchedJob, 'botId' | 'groupName'>, running: { botId: string; groupName: string }[], paused: boolean): boolean {
  if (paused) return false;
  if (running.some((item) => item.botId === job.botId)) return false;
  const pool = poolOf(job.groupName);
  const used = running.filter((item) => poolOf(item.groupName) === pool).length;
  return used < LIMITS[pool];
}

export function followingRun(job: Pick<SchedJob, 'cron' | 'everyMs' | 'kind'>, now: Date): string | null {
  if (job.kind === 'once') return null;
  if (job.everyMs) return new Date(now.getTime() + job.everyMs).toISOString();
  if (job.cron) return nextCron(job.cron, now).toISOString();
  return null;
}

export function dueJobs(jobs: SchedJob[], now: Date): SchedJob[] {
  return jobs
    .filter((job) => job.status === 'scheduled' && job.nextRun && job.nextRun <= now.toISOString())
    .sort((a, b) => String(a.nextRun).localeCompare(String(b.nextRun)));
}

export type DesiredInput = {
  key: string;
  task: string;
  cron?: string;
  everyMs?: number;
  group?: string;
};

export function reconcile(existing: SchedJob[], desired: DesiredInput[], now: Date): {
  upserts: Array<DesiredInput & { nextRun: string }>;
  disableKeys: string[];
} {
  const wanted = new Set(desired.map((job) => job.key));
  const disableKeys = existing
    .filter((job) => job.kind !== 'once' && !wanted.has(job.key))
    .map((job) => job.key);
  const upserts = desired.map((job) => {
    const previous = existing.find((item) => item.key === job.key);
    const cron = previous?.cronOverride ? previous.cron || job.cron : job.cron;
    const nextRun = previous?.nextRun && previous.nextRun > now.toISOString()
      ? previous.nextRun
      : followingRun({ cron: cron || null, everyMs: job.everyMs || null, kind: job.everyMs ? 'interval' : 'cron' }, now) || now.toISOString();
    return { ...job, cron, nextRun };
  });
  return { upserts, disableKeys };
}
