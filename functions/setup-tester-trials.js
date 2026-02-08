/**
 * InkWell Alpha/Beta Tester Free Trial Setup
 * 
 * Run this script to set up free trial periods for all alpha/beta testers:
 * - Alpha testers: 6 months free Plus
 * - Beta testers: 3 months free Plus
 * 
 * Usage: node setup-tester-trials.js
 * 
 * Prerequisites:
 * - Firebase Admin SDK credentials (GOOGLE_APPLICATION_CREDENTIALS env var)
 * - Or run from Cloud Functions environment
 */

const admin = require('firebase-admin');

// Initialize Firebase Admin with project ID
if (!admin.apps.length) {
  admin.initializeApp({
    projectId: 'inkwell-alpha'
  });
}

const db = admin.firestore();

// Hollis Verdant coach UID (founder's pen name)
const HOLLIS_VERDANT_UID = 'ZiNM7YK1jnRgIkAKiCaO1lC6DGx2';

async function setupTesterTrials() {
  console.log('🚀 Starting Alpha/Beta Tester Free Trial Setup...');
  console.log('📅 Current date:', new Date().toISOString());
  
  const now = new Date();
  
  // Calculate trial end dates
  const alphaTrial = new Date(now);
  alphaTrial.setMonth(alphaTrial.getMonth() + 6); // 6 months from now
  
  const betaTrial = new Date(now);
  betaTrial.setMonth(betaTrial.getMonth() + 3); // 3 months from now
  
  console.log('📆 Alpha trial ends:', alphaTrial.toISOString());
  console.log('📆 Beta trial ends:', betaTrial.toISOString());
  
  // Query all users with special_code
  const usersRef = db.collection('users');
  
  // Get alpha testers
  const alphaSnapshot = await usersRef.where('special_code', '==', 'alpha').get();
  console.log(`\n👑 Found ${alphaSnapshot.size} Alpha testers`);
  
  // Get beta testers
  const betaSnapshot = await usersRef.where('special_code', '==', 'beta').get();
  console.log(`🧪 Found ${betaSnapshot.size} Beta testers`);
  
  let alphaUpdated = 0;
  let betaUpdated = 0;
  let errors = [];
  
  // Update alpha testers
  for (const doc of alphaSnapshot.docs) {
    try {
      const userData = doc.data();
      const updates = {
        freeTrialEnds: admin.firestore.Timestamp.fromDate(alphaTrial),
        subscriptionTier: 'plus', // Grant Plus during trial
        connectedCoach: userData.connectedCoach || HOLLIS_VERDANT_UID, // Default to Hollis
        testerTierSetAt: admin.firestore.FieldValue.serverTimestamp(),
        testerPlan: {
          type: 'alpha',
          trialMonths: 6,
          plusDiscount: 80,
          connectDiscount: 20,
          hollisIncluded: true,
        },
      };
      
      await doc.ref.update(updates);
      console.log(`  ✅ Alpha: ${userData.email || doc.id}`);
      alphaUpdated++;
    } catch (err) {
      console.error(`  ❌ Failed for ${doc.id}:`, err.message);
      errors.push({ id: doc.id, error: err.message });
    }
  }
  
  // Update beta testers
  for (const doc of betaSnapshot.docs) {
    try {
      const userData = doc.data();
      const updates = {
        freeTrialEnds: admin.firestore.Timestamp.fromDate(betaTrial),
        subscriptionTier: 'plus', // Grant Plus during trial
        connectedCoach: userData.connectedCoach || HOLLIS_VERDANT_UID, // Default to Hollis
        testerTierSetAt: admin.firestore.FieldValue.serverTimestamp(),
        testerPlan: {
          type: 'beta',
          trialMonths: 3,
          plusDiscount: 50,
          connectDiscount: 10,
          hollisIncluded: true,
        },
      };
      
      await doc.ref.update(updates);
      console.log(`  ✅ Beta: ${userData.email || doc.id}`);
      betaUpdated++;
    } catch (err) {
      console.error(`  ❌ Failed for ${doc.id}:`, err.message);
      errors.push({ id: doc.id, error: err.message });
    }
  }
  
  console.log('\n📊 Summary:');
  console.log(`  Alpha testers updated: ${alphaUpdated}/${alphaSnapshot.size}`);
  console.log(`  Beta testers updated: ${betaUpdated}/${betaSnapshot.size}`);
  console.log(`  Errors: ${errors.length}`);
  
  if (errors.length > 0) {
    console.log('\n❌ Errors:');
    errors.forEach(e => console.log(`  - ${e.id}: ${e.error}`));
  }
  
  console.log('\n✨ Setup complete!');
  return { alphaUpdated, betaUpdated, errors };
}

// Run if called directly
if (require.main === module) {
  setupTesterTrials()
    .then(() => process.exit(0))
    .catch(err => {
      console.error('Fatal error:', err);
      process.exit(1);
    });
}

module.exports = { setupTesterTrials };
