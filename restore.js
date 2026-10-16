// שחזור מקובץ גיבוי (שהורדת מלוח הניהול).
// שימוש:  node restore.js olam-backup-2026-01-01.json
// כדי לשחזר למסד הנתונים באינטרנט, מגדירים קודם DATABASE_URL (ראי הסבר בהודעה).
const fs = require('fs');
const path = require('path');

const file = process.argv[2];
if (!file) { console.log('חסר שם קובץ. דוגמה: node restore.js olam-backup.json'); process.exit(1); }
const b = JSON.parse(fs.readFileSync(file, 'utf8'));
const users = b.users || {}, dms = b.dms || {}, reports = b.reports || [];

(async () => {
  if (process.env.DATABASE_URL) {
    const { Pool } = require('pg');
    const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 2 });
    for (const t of ['users', 'dms'])
      await pool.query('CREATE TABLE IF NOT EXISTS ' + t + ' (key text PRIMARY KEY, data jsonb NOT NULL)');
    await pool.query('CREATE TABLE IF NOT EXISTS reports (id serial PRIMARY KEY, created timestamptz DEFAULT now(), data jsonb NOT NULL)');
    const up = (t, obj) => pool.query('INSERT INTO ' + t + '(key, data) SELECT key, value FROM jsonb_each($1::jsonb) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data', [JSON.stringify(obj)]);
    await up('users', users); await up('dms', dms);
    const have = (await pool.query('SELECT count(*) FROM reports')).rows[0].count;
    if (Number(have) === 0) for (const r of reports) await pool.query('INSERT INTO reports(data) VALUES ($1::jsonb)', [JSON.stringify(r.data || r)]);
    await pool.end();
    console.log('שוחזרו למסד הנתונים:', Object.keys(users).length, 'חשבונות');
  } else {
    fs.writeFileSync(path.join(__dirname, 'users.json'), JSON.stringify(users));
    fs.writeFileSync(path.join(__dirname, 'dms.json'), JSON.stringify(dms));
    fs.writeFileSync(path.join(__dirname, 'reports.json'), JSON.stringify(reports.map((r) => r.data || r), null, 2));
    console.log('שוחזרו לקבצים:', Object.keys(users).length, 'חשבונות');
  }
})().catch((e) => { console.error('השחזור נכשל:', e.message); process.exit(1); });
