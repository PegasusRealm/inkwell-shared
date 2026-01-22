const admin = require('firebase-admin');

// Initialize - this script must be run from a directory with firebase-admin installed
// and with proper gcloud auth application-default credentials
admin.initializeApp();

const db = admin.firestore();

async function upgradeAllUsersToPlus() {
  console.log('🚀 Starting bulk upgrade to Plus tier...');
  
  const usersSnapshot = await db.collection('users').get();
  
  if (usersSnapshot.empty) {
    console.log('No users found.');
    return;
  }
  
  console.log(`Found ${usersSnapshot.size} users to upgrade`);
  
  let successCount = 0;
  let errorCount = 0;
  
  for (const userDoc of usersSnapshot.docs) {
    try {
      await userDoc.ref.update({
        subscriptionTier: 'plus',
        subscriptionStatus: 'active',
        'betaProgress.tierOverride': {
          tier: 'plus',
          setAt: admin.firestore.FieldValue.serverTimestamp(),
          setBy: 'admin-migration-script'
        }
      });
      successCount++;
      console.log(`✅ Upgraded: ${userDoc.id}`);
    } catch (err) {
      errorCount++;
      console.error(`❌ Failed: ${userDoc.id} - ${err.message}`);
    }
  }
  
  console.log('');
  console.log('=== COMPLETE ===');
  console.log(`Success: ${successCount}`);
  console.log(`Errors: ${errorCount}`);
  
  process.exit(0);
}

upgradeAllUsersToPlus().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
