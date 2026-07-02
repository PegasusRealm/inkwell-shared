/**
 * One-off: backfill ALL existing InkWell users into ActiveCampaign (2026-07-02).
 *
 * Pulls every user from Firebase AUTH (source of truth for emails — catches
 * users with missing/emailless Firestore docs), then for each:
 *   contact/sync → subscribe to "Master Contact List" → tags:
 *   ['InkWell Web', 'Audience', 'InkWell Backfill']
 * The Backfill tag marks this cohort so it can be identified/cleaned in AC later.
 * contact/sync is idempotent — users already in AC just get updated/tagged.
 *
 * SAFE BY DEFAULT: dry run (prints what it would sync). Pass --apply to write.
 *
 * Run from shared/functions/:
 *   cd ~/Documents/Pegasus_Realm/15_App_Projects/inkwell-monorepo/shared/functions
 *   export AC_API_KEY='paste-your-ac-key-here'
 *   node backfill-emails-to-ac.js            # dry run
 *   node backfill-emails-to-ac.js --apply    # execute
 *   unset AC_API_KEY                         # tidy up after
 *
 * Requires gcloud application-default credentials (same as convert-connect-to-plus.js).
 * Rate-limited to ~3 contacts/sec (AC allows 5/sec).
 */

const admin = require('firebase-admin');
const fetch = require('node-fetch');

admin.initializeApp({ projectId: 'inkwell-alpha' });

const APPLY = process.argv.includes('--apply');
const AC_URL = 'https://pegasusrealm.api-us1.com';
const LIST_NAME = 'Master Contact List';
const TAG_NAMES = ['InkWell Web', 'Audience', 'InkWell Backfill'];
const AC_KEY = process.env.AC_API_KEY;

if (!AC_KEY) {
  console.error('Missing AC_API_KEY. Run:  export AC_API_KEY=\'your-key\'  first.');
  process.exit(1);
}

const headers = { 'Api-Token': AC_KEY, 'Content-Type': 'application/json' };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function resolveListId() {
  // Brackets MUST be percent-encoded — literal [ ] in the query gets an empty
  // response from AC's edge (found live 2026-07-02).
  const res = await fetch(`${AC_URL}/api/3/lists?filters%5Bname%5D=${encodeURIComponent(LIST_NAME)}`, { headers });
  const text = await res.text();
  if (!res.ok || !text) throw new Error(`lists lookup ${res.status}: ${text.slice(0, 200) || '(empty body)'}`);
  const id = JSON.parse(text).lists?.[0]?.id;
  if (!id) throw new Error(`List "${LIST_NAME}" not found in AC`);
  return id;
}

async function resolveTagIds() {
  const ids = {};
  for (const name of TAG_NAMES) {
    const q = await fetch(`${AC_URL}/api/3/tags?search=${encodeURIComponent(name)}`, { headers });
    const found = ((await q.json()).tags || []).find((t) => t.tag === name);
    if (found) {
      ids[name] = found.id;
    } else if (APPLY) {
      const created = await fetch(`${AC_URL}/api/3/tags`, {
        method: 'POST',
        headers,
        body: JSON.stringify({ tag: { tag: name, tagType: 'contact', description: 'Auto-created by InkWell backfill' } })
      });
      ids[name] = (await created.json()).tag?.id;
      console.log(`➕ Created AC tag "${name}" (id ${ids[name]})`);
    } else {
      ids[name] = '(would create)';
    }
  }
  return ids;
}

async function syncOne(email, listId, tagIds) {
  const syncRes = await fetch(`${AC_URL}/api/3/contact/sync`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ contact: { email } })
  });
  if (!syncRes.ok) throw new Error(`contact/sync ${syncRes.status}`);
  const contactId = (await syncRes.json()).contact?.id;
  if (!contactId) throw new Error('no contact id returned');

  await fetch(`${AC_URL}/api/3/contactLists`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ contactList: { list: listId, contact: contactId, status: 1 } })
  });

  for (const name of TAG_NAMES) {
    if (!tagIds[name] || tagIds[name] === '(would create)') continue;
    await fetch(`${AC_URL}/api/3/contactTags`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ contactTag: { contact: contactId, tag: tagIds[name] } })
    });
  }
  return contactId;
}

async function main() {
  console.log(`Mode: ${APPLY ? 'APPLY (writing to ActiveCampaign)' : 'DRY RUN (no writes)'}\n`);

  // Collect every auth user with an email
  const emails = [];
  let pageToken;
  do {
    const page = await admin.auth().listUsers(1000, pageToken);
    page.users.forEach((u) => { if (u.email) emails.push(u.email.toLowerCase()); });
    pageToken = page.pageToken;
  } while (pageToken);

  const unique = [...new Set(emails)];
  console.log(`Found ${emails.length} auth users, ${unique.length} unique emails.\n`);

  if (!APPLY) {
    unique.forEach((e) => console.log(`  would sync: ${e}`));
    console.log(`\nDry run only. Tags: ${TAG_NAMES.join(', ')} → list "${LIST_NAME}".`);
    console.log('Re-run with --apply to execute.');
    return;
  }

  const listId = await resolveListId();
  const tagIds = await resolveTagIds();
  console.log(`List "${LIST_NAME}" = id ${listId}. Tags resolved. Syncing...\n`);

  let ok = 0, failed = 0;
  for (const email of unique) {
    try {
      const id = await syncOne(email, listId, tagIds);
      ok++;
      console.log(`  ✅ ${email} (contact ${id})`);
    } catch (e) {
      failed++;
      console.error(`  ❌ ${email}: ${e.message}`);
    }
    await sleep(350); // ~3/sec, under AC's 5/sec limit
  }

  console.log(`\nDone. Synced: ${ok}, failed: ${failed}, total: ${unique.length}.`);
}

main()
  .then(() => process.exit(0))
  .catch((e) => { console.error('Fatal:', e); process.exit(1); });
