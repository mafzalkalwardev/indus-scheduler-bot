import path from 'node:path';
import { createBotService, type BotDeps } from '@indus/bot-core';
import { backoffMs, canStart, dueJobs, followingRun, reconcile, type SchedJob } from './engine';

type BotTarget = { botId: string; baseUrl: string };

const svc = createBotService({
  id: 'scheduler',
  name: 'Scheduler',
  description: 'The only clock. Cron, intervals, one-off jobs, retries, and a pause that stops every bot.',
  accent: '#d7c4a3',
  defaultPort: 41748,
  capabilities: ['schedule', 'pause'],
  defaultSettings: { maxRetries: 3 },
  fields: [{ key: 'maxRetries', label: 'Retries before the next cron slot', type: 'number' }],
  autoTask: 'tick',
  runTask: async (ctx) => {
    if (ctx.task === 'instruct') return { summary: 'The scheduler takes times, not prose. Add a job from the timeline.' };
    const ran = await tick(ctx.db, ctx.settings, process.env, ctx.log);
    return { summary: ran ? `Dispatched ${ran}.` : 'Nothing was due.' };
  },
  publicRoutes: (app, deps) => installInternal(app, deps),
  routes: (app, deps) => {
    app.get('/api/board', (_req, res) => res.json(board(deps)));
    app.post('/api/pause', (_req, res) => { setPaused(deps.db, true); res.json({ paused: true }); });
    app.post('/api/resume', (_req, res) => { setPaused(deps.db, false); res.json({ paused: false }); });
    app.post('/api/jobs/:id', (req, res) => {
      deps.db.prepare(`UPDATE sched_jobs SET cron = ?, cron_override = 1 WHERE id = ?`).run(String(req.body?.cron || ''), Number(req.params.id));
      res.json({ ok: true });
    });
  },
  page: () => page(),
});

ensureTables(svc.db);
const running = new Set<number>();

function ensureTables(db: BotDeps['db']) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS sched_jobs (
      id INTEGER PRIMARY KEY,
      bot_id TEXT NOT NULL,
      key TEXT NOT NULL,
      task TEXT NOT NULL,
      cron TEXT,
      every_ms INTEGER,
      group_name TEXT NOT NULL,
      next_run TEXT,
      status TEXT NOT NULL,
      retries INTEGER NOT NULL DEFAULT 0,
      max_retries INTEGER NOT NULL DEFAULT 3,
      kind TEXT NOT NULL,
      cron_override INTEGER NOT NULL DEFAULT 0,
      payload_json TEXT NOT NULL DEFAULT '{}',
      last_error TEXT,
      UNIQUE (bot_id, key)
    );
    CREATE TABLE IF NOT EXISTS sched_bots (
      bot_id TEXT PRIMARY KEY,
      base_url TEXT NOT NULL,
      seen_at TEXT NOT NULL
    );
  `);
}

function rowToJob(row: Record<string, unknown>): SchedJob {
  return {
    id: Number(row.id),
    botId: String(row.bot_id),
    key: String(row.key),
    task: String(row.task),
    cron: row.cron ? String(row.cron) : null,
    everyMs: row.every_ms ? Number(row.every_ms) : null,
    groupName: String(row.group_name),
    nextRun: row.next_run ? String(row.next_run) : null,
    status: String(row.status),
    retries: Number(row.retries || 0),
    maxRetries: Number(row.max_retries || 3),
    kind: row.kind as SchedJob['kind'],
    cronOverride: Number(row.cron_override) === 1,
    payload: JSON.parse(String(row.payload_json || '{}')),
  };
}

function listJobs(db: BotDeps['db']): SchedJob[] {
  return (db.prepare(`SELECT * FROM sched_jobs`).all() as Record<string, unknown>[]).map(rowToJob);
}

function paused(db: BotDeps['db']): boolean {
  const row = db.prepare(`SELECT value_json FROM kv WHERE key = 'paused'`).get() as { value_json: string } | undefined;
  return row ? JSON.parse(row.value_json) === true : false;
}

function setPaused(db: BotDeps['db'], value: boolean) {
  db.prepare(`INSERT INTO kv (key, value_json) VALUES ('paused', ?) ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json`).run(JSON.stringify(value));
}

function board(deps: BotDeps) {
  const jobs = listJobs(deps.db).map((job) => ({
    ...job,
    when: job.nextRun,
  }));
  return { paused: paused(deps.db), running: running.size, jobs };
}

async function tick(db: BotDeps['db'], settings: Record<string, unknown>, env: NodeJS.ProcessEnv, log: (level: 'info' | 'warn' | 'error', message: string) => void) {
  const now = new Date();
  const jobs = dueJobs(listJobs(db), now);
  const live = listJobs(db).filter((job) => running.has(job.id)).map((job) => ({ botId: job.botId, groupName: job.groupName }));
  const hold = paused(db);
  let dispatched = 0;
  for (const job of jobs) {
    if (!canStart(job, live, hold)) continue;
    const target = db.prepare(`SELECT base_url FROM sched_bots WHERE bot_id = ?`).get(job.botId) as { base_url: string } | undefined;
    if (!target) {
      log('warn', `No address for ${job.botId}.`);
      continue;
    }
    running.add(job.id);
    live.push({ botId: job.botId, groupName: job.groupName });
    db.prepare(`UPDATE sched_jobs SET status = 'running' WHERE id = ?`).run(job.id);
    dispatched += 1;
    void runJob(db, job, target.base_url, env, Number(settings.maxRetries || 3), log);
  }
  return dispatched ? `${dispatched} job${dispatched === 1 ? '' : 's'}` : '';
}

async function runJob(db: BotDeps['db'], job: SchedJob, baseUrl: string, env: NodeJS.ProcessEnv, maxRetries: number, log: (level: 'info' | 'warn' | 'error', message: string) => void) {
  const token = env.INDUS_BOT_TOKEN || 'dev-bot-token';
  try {
    const response = await fetch(new URL('/api/internal/tasks', baseUrl), {
      method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ task: job.task, payload: job.payload, dryRun: false }),
      signal: AbortSignal.timeout(Number(env.LLM_TIMEOUT_MS || 240_000) + 20_000),
    });
    const body = await response.json().catch(() => ({})) as { error?: string; summary?: string };
    if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
    const next = followingRun(job, new Date());
    db.prepare(`UPDATE sched_jobs SET status = ?, retries = 0, next_run = ?, last_error = NULL WHERE id = ?`)
      .run(job.kind === 'once' ? 'done' : 'scheduled', next, job.id);
    log('info', `${job.botId} ${job.task} finished. ${body.summary || ''}`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const retries = job.retries + 1;
    if (retries > maxRetries) {
      const next = followingRun(job, new Date());
      db.prepare(`UPDATE sched_jobs SET status = ?, retries = 0, next_run = ?, last_error = ? WHERE id = ?`)
        .run(job.kind === 'once' ? 'error' : 'scheduled', next, message, job.id);
      log('error', `${job.botId} ${job.task} gave up: ${message}`);
    } else {
      const next = new Date(Date.now() + backoffMs(retries - 1)).toISOString();
      db.prepare(`UPDATE sched_jobs SET status = 'scheduled', retries = ?, next_run = ?, last_error = ? WHERE id = ?`)
        .run(retries, next, message, job.id);
      log('warn', `${job.botId} ${job.task} retry ${retries}: ${message}`);
    }
  } finally {
    running.delete(job.id);
  }
}

function installInternal(app: import('express').Express, deps: BotDeps) {
  const db = deps.db;
  const token = deps.env.INDUS_BOT_TOKEN || 'dev-bot-token';
  app.put('/api/internal/desired', (req, res, next) => guard(req, res, token, next), (req, res) => {
    const botId = String(req.body?.botId || '');
    const baseUrl = String(req.body?.baseUrl || '');
    if (!botId || !baseUrl) {
      res.status(400).json({ error: 'botId and baseUrl are required.' });
      return;
    }
    db.prepare(`INSERT INTO sched_bots (bot_id, base_url, seen_at) VALUES (?, ?, ?)
      ON CONFLICT(bot_id) DO UPDATE SET base_url = excluded.base_url, seen_at = excluded.seen_at`)
      .run(botId, baseUrl, new Date().toISOString());
    const existing = listJobs(db).filter((job) => job.botId === botId);
    const desired = Array.isArray(req.body?.jobs) ? req.body.jobs : [];
    const plan = reconcile(existing, desired, new Date());
    for (const key of plan.disableKeys) db.prepare(`UPDATE sched_jobs SET status = 'disabled' WHERE bot_id = ? AND key = ?`).run(botId, key);
    for (const job of plan.upserts) {
      db.prepare(`INSERT INTO sched_jobs (bot_id, key, task, cron, every_ms, group_name, next_run, status, kind, max_retries, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'scheduled', ?, ?, '{}')
        ON CONFLICT(bot_id, key) DO UPDATE SET
          task = excluded.task,
          cron = CASE WHEN sched_jobs.cron_override = 1 THEN sched_jobs.cron ELSE excluded.cron END,
          every_ms = excluded.every_ms,
          group_name = excluded.group_name,
          status = CASE WHEN sched_jobs.status = 'disabled' THEN 'scheduled' ELSE sched_jobs.status END`)
        .run(botId, job.key, job.task, job.cron || null, job.everyMs || null, job.group || 'default', job.nextRun, job.everyMs ? 'interval' : 'cron', 3);
    }
    res.json({ ok: true });
  });

  app.post('/api/internal/jobs', (req, res, next) => guard(req, res, token, next), (req, res) => {
    const botId = String(req.body?.botId || '');
    const task = String(req.body?.task || '');
    const known = db.prepare(`SELECT base_url FROM sched_bots WHERE bot_id = ?`).get(botId);
    if (!known) {
      res.status(404).json({ error: `${botId} has not registered.` });
      return;
    }
    const key = `once-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
    const when = String(req.body?.runAt || new Date().toISOString());
    const id = Number(db.prepare(`INSERT INTO sched_jobs (bot_id, key, task, group_name, next_run, status, kind, max_retries, payload_json)
      VALUES (?, ?, ?, ?, ?, 'scheduled', 'once', 3, ?)`).run(
      botId,
      key,
      task,
      String(req.body?.group || 'default'),
      when,
      JSON.stringify(req.body?.payload || {}),
    ).lastInsertRowid);
    res.status(201).json({ ok: true, id });
  });

  app.get('/api/internal/state', (req, res, next) => guard(req, res, token, next), (_req, res) => {
    res.json({ paused: paused(db), jobs: listJobs(db) });
  });
  app.post('/api/internal/pause', (req, res, next) => guard(req, res, token, next), (_req, res) => {
    setPaused(db, true);
    res.json({ paused: true });
  });
  app.post('/api/internal/resume', (req, res, next) => guard(req, res, token, next), (_req, res) => {
    setPaused(db, false);
    res.json({ paused: false });
  });
}

function guard(req: { get(name: string): string | undefined }, res: { status(code: number): { json(body: unknown): void } }, token: string, next: () => void) {
  const header = req.get('authorization') || '';
  const got = header.toLowerCase().startsWith('bearer ') ? header.slice(7) : '';
  if (got !== token) {
    res.status(401).json({ error: 'Missing bot token.' });
    return;
  }
  next();
}

function page(): string {
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Scheduler · INDUS</title>
<style>
  :root { color-scheme: dark; }
  body { margin:0; background:#14120f; color:#f3ecdf; font:15px/1.45 "Avenir Next", "Segoe UI", sans-serif; }
  header, main { padding: 22px; }
  h1 { font-family: Georgia, serif; font-weight: 500; margin: 0 0 8px; }
  .row { display:flex; gap:8px; align-items:center; }
  button { background:#e0b15c; color:#221a0c; border:0; border-radius:999px; padding:8px 14px; }
  .lane { display:grid; gap:8px; margin-top:16px; }
  .job { display:grid; grid-template-columns: 140px 1fr auto; gap:10px; padding:10px 12px; border:1px solid #3a342c; border-radius:12px; background:#1d1a16; }
  .when { color:#d7c4a3; font-variant-numeric: tabular-nums; }
  @media (max-width:700px) { .job { grid-template-columns: 1fr; } }
</style></head>
<body>
<header><div class="row"><div><p style="letter-spacing:.14em;color:#e0b15c;font-size:12px">INDUS · TIME</p><h1>Scheduler</h1></div></div>
<div class="row"><button id="pause" type="button">Pause all</button><span id="state"></span></div></header>
<main><div class="lane" id="lane"></div></main>
<script>
  function headers(){const h={'content-type':'application/json'}; if(window.__INDUS_CSRF__) h['x-csrf-token']=window.__INDUS_CSRF__; return h;}
  async function load(){
    const res = await fetch('api/board', {headers: headers()});
    const data = await res.json();
    document.getElementById('state').textContent = data.paused ? 'Paused. Nothing will start.' : 'Running.';
    document.getElementById('pause').textContent = data.paused ? 'Resume' : 'Pause all';
    document.getElementById('pause').dataset.paused = data.paused ? '1' : '0';
    const jobs = (data.jobs||[]).filter(j=>j.status!=='disabled').sort((a,b)=>String(a.nextRun||'').localeCompare(String(b.nextRun||'')));
    document.getElementById('lane').innerHTML = jobs.map(j => '<div class="job"><div class="when">'+(j.nextRun?new Date(j.nextRun).toLocaleString():j.status)+'</div><div><b>'+j.botId+'</b> · '+j.task+'<div style="color:#b7ab9a">'+j.groupName+' · '+j.kind+(j.cron?' · '+j.cron:'')+'</div></div><div>'+j.status+'</div></div>').join('') || '<p>No jobs registered yet. Bots check in every few seconds.</p>';
  }
  document.getElementById('pause').onclick = async (event) => {
    const paused = event.target.dataset.paused === '1';
    await fetch(paused ? 'api/resume' : 'api/pause', { method:'POST', headers: headers() });
    await load();
  };
  load(); setInterval(load, 4000);
</script>
</body></html>`;
}

setInterval(() => {
  void tick(svc.db, svc.readSettings(), process.env, () => undefined);
}, 5000).unref?.();

svc.start().catch((error) => {
  console.error(error);
  process.exit(1);
});
