const admin = require('firebase-admin');
const serviceAccount = require('./inkwell-alpha-firebase-adminsdk-fbsvc-c85e629af6.json');
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

async function checkCoaches() {
  const usersRef = db.collection('users');
  
  // Check userRole = coach
  const coachQuery = await usersRef.where('userRole', '==', 'coach').get();
  console.log('Coaches with userRole=coach:', coachQuery.size);
  coachQuery.forEach(doc => {
    const d = doc.data();
    console.log('- ID:', doc.id);
    console.log('  Name:', d.displayName || d.signupUsername);
    console.log('  Email:', d.email);
    console.log('  userRole:', d.userRole);
    console.log('  freeAgentOptIn:', d.freeAgentOptIn);
    console.log('  accountType:', d.accountType);
    console.log('');
  });
  
  // Also check accountType = coach
  const accountTypeQuery = await usersRef.where('accountType', '==', 'coach').get();
  console.log('\nUsers with accountType=coach:', accountTypeQuery.size);
  accountTypeQuery.forEach(doc => {
    const d = doc.data();
    console.log('- ID:', doc.id);
    console.log('  Name:', d.displayName || d.signupUsername);
    console.log('  userRole:', d.userRole);
    console.log('  freeAgentOptIn:', d.freeAgentOptIn);
  });
  
  process.exit(0);
}
checkCoaches();
