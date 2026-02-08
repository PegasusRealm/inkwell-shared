const admin = require('firebase-admin');
const serviceAccount = require('../serviceAccountKey.json');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

db.collection('journalEntries')
  .where('userId', '==', '4FeEdZPE5AOM7jQpii3y4LYnC3I2')
  .orderBy('createdAt', 'desc')
  .limit(5)
  .get()
  .then(snapshot => {
    console.log('\n=== RECENT JOURNAL ENTRIES FIELDS ===\n');
    snapshot.docs.forEach((doc, i) => {
      const data = doc.data();
      console.log('--- Entry', i+1, '(' + doc.id + ') ---');
      console.log('Fields:', Object.keys(data).join(', '));
      if (data.manifestData) console.log('manifestData:', JSON.stringify(data.manifestData));
      if (data.contextManifest) console.log('contextManifest:', data.contextManifest);
      if (data.linkedManifestId) console.log('linkedManifestId:', data.linkedManifestId);
      if (data.manifestId) console.log('manifestId:', data.manifestId);
      if (data.tags) console.log('tags:', data.tags);
      console.log('');
    });
    process.exit(0);
  })
  .catch(err => {
    console.error('Error:', err);
    process.exit(1);
  });
