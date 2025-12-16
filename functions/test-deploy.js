const { onCall, onRequest } = require("firebase-functions/v2/https");

// Simple test function to verify deployment works
exports.testDeploy = onCall(async (data, context) => {
  return { message: "Deployment test successful", timestamp: Date.now() };
});

// Test the problematic functions one by one
exports.testVerifyRecaptcha = onCall(async (data, context) => {
  return { message: "verifyRecaptcha test - deployment works" };
});

exports.testSendPractitionerInvitation = onRequest(async (req, res) => {
  res.json({ message: "sendPractitionerInvitation test - deployment works" });
});

exports.testEmbedAndStoreEntry = onRequest(async (req, res) => {
  res.json({ message: "embedAndStoreEntry test - deployment works" });
});
