/**
 * One-off: convert all Connect-tier users to Plus (Connect tier retirement, v2).
 *
 * SAFE BY DEFAULT: runs as a dry run (prints what it would change, changes nothing).
 * Pass --apply to actually write.
 *
 * Run from shared/functions/ (firebase-admin already installed here):
 *   cd ~/Documents/Pegasus_Realm/15_App_Projects/inkwell-monorepo/shared/functions
 *   node convert-connect-to-plus.js            # dry run
 *   node convert-connect-to-plus.js --apply    # execute
 *
 * Requires gcloud application-default credentials (same as restore-tier.js).
 * Project is pinned to inkwell-alpha per the cross-deploy lesson.
 */

const admin = require('firebase-admin');

admin.initializeApp({
  projectId: 'inkwell-alpha',
});

const db = admin.firestore();
const APPLY = process.argv.includes('--apply');

async function convertConnectToPlus() {
  console.log(`Mode: ${APPLY ? 'APPLY (writing changes)' : 'DRY RUN (no writes)'}`);
  console.log('Querying users where subscriptionTier == "connect"...\n');

  const snapshot = await db
    .collection('users')
    .where('subscriptionTier', '==', 'connect')
    .get();

  if (snapshot.empty) {
    console.log('No Connect-tier users found. Nothing to do.');
    return;
  }

  console.log(`Found ${snapshot.size} Connect-tier user(s):\n`);

  let converted = 0;
  let errors = 0;

  for (const doc of snapshot.docs) {
    const data = doc.data();
    console.log(`- uid: ${doc.id}`);
    console.log(`    email: ${data.email || '(none)'}`);
    console.log(`    subscriptionTier: ${data.subscriptionTier}`);
    console.log(`    subscriptionStatus: ${data.subscriptionStatus || '(none)'}`);
    console.log(`    stripeSubscriptionId: ${data.stripeSubscriptionId || '(none — not a paying subscriber)'}`);

    if (data.stripeSubscriptionId) {
      console.log('    ⚠️  WARNING: this user has a Stripe subscription ID. Verify in the');
      console.log('        Stripe dashboard that it is not an active paid Connect sub before');
      console.log('        converting. Skipping this user — handle manually.');
      continue;
    }

    if (APPLY) {
      try {
        await doc.ref.update({ subscriptionTier: 'plus' });
        converted++;
        console.log('    ✅ converted to plus');
      } catch (e) {
        errors++;
        console.error(`    ❌ update failed: ${e.message}`);
      }
    } else {
      console.log('    → would set subscriptionTier: "plus" (dry run)');
    }
  }

  console.log(`\nDone. ${APPLY ? `Converted: ${converted}, errors: ${errors}` : 'Dry run only — re-run with --apply to execute.'}`);
}

convertConnectToPlus()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error('Fatal:', e);
    process.exit(1);
  });
