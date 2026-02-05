const admin = require('firebase-admin');
const serviceAccount = require('/Users/Grimm/Downloads/inkwell-alpha-50741270b7ab.json');

admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

async function run() {
  // Get both coach profiles
  const coach1 = await db.collection('users').doc('ZiNM7YK1jnRgIkAKiCaO1lC6DGx2').get();
  const coach2 = await db.collection('users').doc('14QhSBZSxyOmk0bdWvuCNPQnRgZ2').get();
  
  console.log('=== HOLLIS VERDANT (ZiNM7YK1jnRgIkAKiCaO1lC6DGx2) ===');
  console.log(JSON.stringify(coach1.data(), null, 2));
  
  console.log('\n=== ADAM GRIMM (14QhSBZSxyOmk0bdWvuCNPQnRgZ2) ===');
  console.log(JSON.stringify(coach2.data(), null, 2));
  
  // Fix admin account - restore to admin, remove coach fields I incorrectly added
  await db.collection('users').doc('4FeEdZPE5AOM7jQpii3y4LYnC3I2').update({
    userRole: 'admin',
    freeAgentOptIn: admin.firestore.FieldValue.delete(),
    isPractitioner: admin.firestore.FieldValue.delete(),
    practitionerVerified: admin.firestore.FieldValue.delete()
  });
  console.log('\n✅ ADMIN FIXED: tfershi@pm.me restored to userRole=admin');
  
  process.exit(0);
}

run().catch(e => { console.error(e); process.exit(1); });
