const admin = require('firebase-admin');
admin.initializeApp({
  projectId: 'inkwell-alpha'
});
const db = admin.firestore();

async function restoreUser() {
  const uid = 'RZUEDeAWHjVf2uX5LkKsAQxythm1';
  const userDoc = await db.collection('users').doc(uid).get();
  
  if (!userDoc.exists) {
    console.log('User not found');
    return;
  }
  
  console.log('Current user data:');
  const data = userDoc.data();
  console.log('subscriptionTier:', data.subscriptionTier);
  console.log('stripeCustomerId:', data.stripeCustomerId);
  console.log('stripeSubscriptionId:', data.stripeSubscriptionId);
  console.log('subscriptionPeriodEnd:', data.subscriptionPeriodEnd);
  
  // Restore to Connect with cancel at period end flag
  await db.collection('users').doc(uid).update({
    subscriptionTier: 'connect',
    subscriptionCancelAtPeriodEnd: true
  });
  
  console.log('\n✅ Restored subscriptionTier to connect with cancelAtPeriodEnd flag');
  
  // Verify
  const updated = await db.collection('users').doc(uid).get();
  console.log('\nVerified new tier:', updated.data().subscriptionTier);
}

restoreUser().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
