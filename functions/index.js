const { defineSecret } = require("firebase-functions/params"); 
const { getApps } = require("firebase-admin/app");
const { onCall, onRequest, HttpsError } = require("firebase-functions/v2/https");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const admin = require("firebase-admin");
const sgMail = require("@sendgrid/mail");
const fetch = require("node-fetch");
const SENDGRID_API_KEY = defineSecret("SENDGRID_API_KEY");
const RECAPTCHA_SECRET_KEY = defineSecret("RECAPTCHA_SECRET_KEY");
const ANTHROPIC_API_KEY = defineSecret("ANTHROPIC_API_KEY");
const OPENAI_API_KEY = defineSecret("OPENAI_API_KEY");

// ═══════════════════════════════════════════════════════════════════════════
// MODEL ROUTING — route by ROLE, never inline model strings (2026-07-01).
// A model retirement or swap = edit THIS BLOCK ONLY. History: the app ran on
// claude-3-haiku-20240307 inline in 12 places; Anthropic retired it
// 2026-04-19 and every AI feature 404'd silently for 10 weeks.
// NO silent fallback chains by design: PRIME (reflection/crisis) must never
// auto-route to an unvalidated model (safety gate, decision log 2026-06-13).
// A retired model now fails LOUD (see MODEL RETIRED log in retry helper).
const MODELS = {
  FAST: 'claude-haiku-4-5-20251001',   // utility: prompts, cleanup, subtraction, letters, insights
  PRIME: 'claude-haiku-4-5-20251001',  // Sophy reflection/crisis — re-pass the suicide-entry test before changing
};
// PROVIDER FAILOVER (2026-07-02, Adam-approved): if Anthropic is unavailable
// (outage-class failures ONLY — 5xx/429-exhausted/timeouts/MODEL_RETIRED, never
// content refusals), roles fail over to the provider below. Failover is LOUD
// (🚨 FAILOVER log). Context: Anthropic model retirement took all AI features
// down for 10 weeks (Apr-Jul 2026); July 2026 export-control actions proved
// models can be yanked with days of notice.
const MODEL_FALLBACKS = {
  FAST: { provider: 'openai', model: 'gpt-4o-mini' },
  PRIME: null, // SAFETY GATE: no crisis-path fallback until the fallback model passes the suicide-entry test
};
// ═══════════════════════════════════════════════════════════════════════════
const MAILCHIMP_API_KEY = defineSecret("MAILCHIMP_API_KEY"); // dead — subscription cancelled; remove in Connect sweep
const MAILCHIMP_LIST_ID = defineSecret("MAILCHIMP_LIST_ID"); // dead — subscription cancelled; remove in Connect sweep
const AC_API_KEY = defineSecret("AC_API_KEY"); // ActiveCampaign (pegasusrealm.api-us1.com), 2026-07-02
const TWILIO_ACCOUNT_SID = defineSecret("TWILIO_ACCOUNT_SID");
const TWILIO_AUTH_TOKEN = defineSecret("TWILIO_AUTH_TOKEN");
const TWILIO_PHONE_NUMBER = defineSecret("TWILIO_PHONE_NUMBER");
const STRIPE_SECRET_KEY = defineSecret("STRIPE_SECRET_KEY");
const STRIPE_WEBHOOK_SECRET = defineSecret("STRIPE_WEBHOOK_SECRET");
const { onDocumentCreated } = require("firebase-functions/v2/firestore");

// Simple Firebase Admin initialization - let it auto-detect credentials
if (!getApps().length) {
  admin.initializeApp();
}

// Helper: Create user profile in Firestore if not exists
async function createUserProfileIfNotExists(uid, email) {
  const userDocRef = admin.firestore().collection("users").doc(uid);
  const userDoc = await userDocRef.get();
  if (!userDoc.exists) {
    await userDocRef.set({
      userId: uid,
      email: email,
      displayName: email.split('@')[0], // Default to email prefix if no username
      signupUsername: email.split('@')[0],
      userRole: "journaler",
      // Beta period ended Feb 2026 - no auto-tagging
      // special_code only set manually for loyalty rewards now
      avatar: "",
      // Subscription fields (default to free tier)
      subscriptionTier: "free",
      subscriptionStatus: "active",
      stripeCustomerId: null,
      stripeSubscriptionId: null,
      interactionsThisMonth: 0,
      interactionsLimit: 0,
      extraInteractionsPurchased: 0,
      giftedBy: null,
      // Default insight preferences for new users (opt-in by default)
      insightsPreferences: {
        weeklyEnabled: true,
        monthlyEnabled: true,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      },
      // Progressive onboarding state tracking
      onboardingState: {
        hasCompletedVoiceEntry: false,
        hasSeenWishTab: false,
        hasCreatedWish: false,
        hasUsedSophy: false,
        totalEntries: 0,
        currentMilestone: "new_user",
        milestones: {
          firstEntry: null,
          firstVoiceEntry: null,
          firstWish: null,
          firstSophy: null,
          tenEntries: null,
          monthlyUser: null
        },
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        lastMilestoneAt: admin.firestore.FieldValue.serverTimestamp()
      },
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    });
    return { created: true };
  }
  return { created: false };
}
const cors = require("cors");
const corsHandler = cors({ origin: true });

// Hardened CORS configuration
const ALLOWED_ORIGINS = [
  'http://localhost:5002', 
  'http://localhost:5000',  // Firebase hosting emulator default
  'http://127.0.0.1:5002',
  'http://127.0.0.1:5000',
  'https://inkwelljournal.io',      // Production domain
  'https://www.inkwelljournal.io'   // Production domain with www
];

function setupHardenedCORS(req, res) {
  const origin = req.headers.origin;
  
  // Always set Vary: Origin for proper caching behavior
  res.set('Vary', 'Origin');
  
  // Allow requests with no origin (mobile apps, server-to-server)
  // These are authenticated via Bearer token anyway
  if (!origin) {
    return true;
  }
  
  // Check if origin is allowed
  if (!ALLOWED_ORIGINS.includes(origin)) {
    // Bail early on non-allowed origins
    return false;
  }
  
  // Set CORS headers for allowed origins
  res.set('Access-Control-Allow-Origin', origin);
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  
  return true;
}

function sendSecureErrorResponse(res, statusCode, userMessage, internalError = null) {
  if (internalError) {
    console.error("Internal error:", internalError);
  }
  
  // Don't leak internal details to client
  const safeMessage = typeof userMessage === 'string' ? userMessage : 'An error occurred';
  res.status(statusCode).json({ error: safeMessage });
}

// Helper: Generate unique request ID for tracking
function generateRequestId() {
  return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
}

// Helper: Sleep function for retry delays
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Helper: Send push notification via FCM
// Returns true if sent successfully, false otherwise
// Optional options: { badge: number } - set badge count on app icon
async function sendPushNotification(fcmToken, title, body, data = {}, options = {}) {
  if (!fcmToken) {
    console.log('📱 No FCM token provided, skipping push');
    return false;
  }
  
  try {
    const apsPayload = {
      sound: 'default'
    };
    
    // Only add badge if explicitly requested
    if (options.badge !== undefined) {
      apsPayload.badge = options.badge;
    }
    
    const message = {
      token: fcmToken,
      notification: {
        title: title,
        body: body
      },
      data: data,
      apns: {
        payload: {
          aps: apsPayload
        }
      },
      android: {
        priority: 'high',
        notification: {
          sound: 'default',
          priority: 'high',
          channelId: 'default'
        }
      }
    };
    
    const response = await admin.messaging().send(message);
    console.log(`✅ Push notification sent successfully: ${response}`);
    return true;
  } catch (error) {
    console.error(`❌ Failed to send push notification:`, error.code, error.message);
    
    // Handle invalid token - should remove from user document
    if (error.code === 'messaging/invalid-registration-token' ||
        error.code === 'messaging/registration-token-not-registered') {
      console.log('🗑️ Token is invalid, should be removed from user');
      // Could optionally remove the token here
    }
    
    return false;
  }
}

// Helper: Safely update user onboarding state (non-blocking)
async function updateOnboardingState(userId, updates) {
  try {
    const userDocRef = admin.firestore().collection("users").doc(userId);
    
    // Prepare the update object with nested onboardingState fields
    const updateData = {};
    for (const [key, value] of Object.entries(updates)) {
      updateData[`onboardingState.${key}`] = value;
    }
    
    // Always update the milestone timestamp when any onboarding state changes
    updateData['onboardingState.lastMilestoneAt'] = admin.firestore.FieldValue.serverTimestamp();
    updateData['updatedAt'] = admin.firestore.FieldValue.serverTimestamp();
    
    await userDocRef.update(updateData);
    console.log(`✅ Updated onboarding state for user ${userId}:`, updates);
  } catch (error) {
    // Log error but don't throw - this should be non-blocking
    console.warn(`⚠️ Failed to update onboarding state for user ${userId}:`, error.message);
  }
}

// Helper: Map technical errors to user-friendly messages
function mapErrorToUserMessage(error, functionContext = 'system') {
  const errorMessage = error?.message || '';
  const errorLower = errorMessage.toLowerCase();
  
  // OpenAI API specific errors
  if (errorLower.includes('timeout') || error.name === 'AbortError') {
    return {
      code: 'TIMEOUT',
      message: 'The request is taking longer than expected. Please try again.',
      retryable: true
    };
  }
  
  if (errorLower.includes('429') || errorLower.includes('rate limit')) {
    return {
      code: 'RATE_LIMITED',
      message: 'The service is currently busy. Please wait a moment and try again.',
      retryable: true
    };
  }
  
  if (errorLower.includes('401') || errorLower.includes('unauthorized')) {
    return {
      code: 'UNAUTHORIZED',
      message: 'Authentication failed. Please refresh the page and try again.',
      retryable: false
    };
  }
  
  if (errorLower.includes('403') || errorLower.includes('forbidden')) {
    return {
      code: 'FORBIDDEN',
      message: 'Access denied. Please check your permissions.',
      retryable: false
    };
  }
  
  if (errorLower.includes('400') || errorLower.includes('bad request')) {
    return {
      code: 'INVALID_REQUEST',
      message: 'Invalid request. Please check your input and try again.',
      retryable: false
    };
  }
  
  if (errorLower.includes('500') || errorLower.includes('502') || errorLower.includes('503') || errorLower.includes('504')) {
    return {
      code: 'SERVER_ERROR',
      message: 'The service is temporarily unavailable. Please try again in a few moments.',
      retryable: true
    };
  }
  
  // Network errors
  if (errorLower.includes('network') || errorLower.includes('fetch') || errorLower.includes('connection')) {
    return {
      code: 'NETWORK_ERROR',
      message: 'Connection issue detected. Please check your internet and try again.',
      retryable: true
    };
  }
  
  // Context-specific fallbacks
  const contextMessages = {
    'askSophy': "Sophy couldn't provide a reflection right now. Please try again later.",
    'generatePrompt': "Unable to generate a writing prompt at the moment. Please try again.",
    'refineManifest': "Unable to refine your manifest statement right now. Please try again.",
    'cleanVoiceTranscript': "Unable to clean the voice transcript right now. Please try again.",
    'processVoiceWithEmotion': "Unable to process voice with emotional analysis right now. Please try again.",
    'embedAndStoreEntry': "Unable to save your journal entry right now. Please try again."
  };
  
  return {
    code: 'UNKNOWN_ERROR',
    message: contextMessages[functionContext] || 'Something went wrong. Please try again later.',
    retryable: true
  };
}

// Helper: Robust OpenAI API call with timeout, retries, and proper logging
// Provider-failover wrapper (2026-07-02). Call sites keep this name; pass
// options.role ('FAST'|'PRIME') to enable failover per MODEL_FALLBACKS.
async function callAnthropicWithRetry(options, functionName, requestId) {
  const role = options.role;
  if (role) delete options.role; // never send unknown fields to the API
  try {
    return await callAnthropicCore(options, functionName, requestId);
  } catch (err) {
    const fb = role ? MODEL_FALLBACKS[role] : null;
    if (fb && err.outage) {
      console.error(`[${requestId}] 🚨 FAILOVER: Anthropic unavailable for ${functionName} (${err.message}) — serving via ${fb.provider}:${fb.model}`);
      return await callOpenAIFallback(fb.model, options, functionName, requestId);
    }
    throw err;
  }
}

// OpenAI fallback — returns an Anthropic-shaped response so call sites need no changes
async function callOpenAIFallback(fbModel, options, functionName, requestId) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 20000);
  try {
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${OPENAI_API_KEY.value()}`
      },
      body: JSON.stringify({
        model: fbModel,
        max_tokens: options.max_tokens || 400,
        messages: options.messages
      }),
      signal: controller.signal
    });
    clearTimeout(timeoutId);
    if (!response.ok) {
      const t = await response.text();
      throw new Error(`OpenAI fallback error ${response.status}: ${t.slice(0, 200)}`);
    }
    const data = await response.json();
    const text = data.choices?.[0]?.message?.content || '';
    if (!text) throw new Error('OpenAI fallback returned empty content');
    console.log(`[${requestId}] ✅ ${functionName} served via FAILOVER ${fbModel} - usage: ${JSON.stringify(data.usage || {})}`);
    return { content: [{ text }], usage: data.usage || {} };
  } catch (e) {
    clearTimeout(timeoutId);
    throw e;
  }
}

async function callAnthropicCore(options, functionName, requestId) {
  const maxRetries = 3;
  let attempt = 0;
  
  while (attempt < maxRetries) {
    attempt++;
    
    // Create AbortController with 20s timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 20000);
    
    try {
      console.log(`[${requestId}] ${functionName} attempt ${attempt}/${maxRetries} - calling Anthropic`);
      
      const response = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': ANTHROPIC_API_KEY.value(),
          'anthropic-version': '2023-06-01'
        },
        body: JSON.stringify(options),
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      // Log response details (safe for production)
      console.log(`[${requestId}] Anthropic response - status: ${response.status}, model: ${options.model}`);
      
      if (response.ok) {
        const data = await response.json();
        console.log(`[${requestId}] Anthropic success - usage: ${JSON.stringify(data.usage || {})}`);
        return data;
      }
      
      // Model retired/unknown — fail LOUD and immediately, never retry.
      // This is the failure mode that hid for 10 weeks (claude-3-haiku retirement).
      if (response.status === 404) {
        console.error(`[${requestId}] 🚨 MODEL RETIRED OR UNKNOWN: "${options.model}" returned 404 from Anthropic. Update the MODELS routing block at the top of index.js. ALL features using this role are down.`);
        const retiredErr = new Error(`MODEL_RETIRED: ${options.model}`);
        retiredErr.outage = true; // failover-eligible
        throw retiredErr;
      }

      // Handle specific error codes
      if (response.status === 429 || response.status >= 500) {
        const errorText = await response.text();
        console.warn(`[${requestId}] Retryable error ${response.status}: ${response.statusText}`);
        
        if (attempt < maxRetries) {
          // Exponential backoff: 1s, 2s, 4s
          const delay = Math.pow(2, attempt - 1) * 1000;
          console.log(`[${requestId}] Retrying in ${delay}ms...`);
          await sleep(delay);
          continue;
        }
      }
      
      // Non-retryable error or final attempt
      const errorText = await response.text();
      console.error(`[${requestId}] Anthropic API error ${response.status}: ${response.statusText}`);
      
      // Create a technical error for mapping
      const technicalError = new Error(`Anthropic API error: ${response.status} ${response.statusText}`);
      const userError = mapErrorToUserMessage(technicalError, functionName);
      const finalErr = new Error(userError.message);
      finalErr.outage = (response.status === 429 || response.status >= 500); // 4xx = our bug, not an outage
      throw finalErr;

    } catch (error) {
      clearTimeout(timeoutId);
      
      if (error.name === 'AbortError') {
        console.error(`[${requestId}] Anthropic request timeout (20s) on attempt ${attempt}`);
        if (attempt < maxRetries) {
          const delay = Math.pow(2, attempt - 1) * 1000;
          console.log(`[${requestId}] Retrying after timeout in ${delay}ms...`);
          await sleep(delay);
          continue;
        }
        const userError = mapErrorToUserMessage(error, functionName);
        const timeoutErr = new Error(userError.message);
        timeoutErr.outage = true; // timeouts = failover-eligible
        throw timeoutErr;
      }

      // Network or other errors
      console.error(`[${requestId}] Anthropic network error on attempt ${attempt}:`, error.message);
      if (error.outage !== undefined) throw error; // already-classified errors (MODEL_RETIRED, 4xx, exhausted 5xx) pass straight up
      if (attempt < maxRetries) {
        const delay = Math.pow(2, attempt - 1) * 1000;
        await sleep(delay);
        continue;
      }

      const userError = mapErrorToUserMessage(error, functionName);
      const netErr = new Error(userError.message);
      netErr.outage = true; // exhausted network retries = failover-eligible
      throw netErr;
    }
  }
}

exports.generatePrompt = onRequest({ secrets: [ANTHROPIC_API_KEY, OPENAI_API_KEY] }, async (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(204).send('');

  try {
    const { topic } = req.body;
    const requestId = generateRequestId();
    
    // Safe logging - no full prompts in production
    console.log(`[${requestId}] generatePrompt - topic provided: ${!!topic}`);
    
    // Check if ANTHROPIC_API_KEY is available
    const hasApiKey = ANTHROPIC_API_KEY && ANTHROPIC_API_KEY.value();
    console.log(`[${requestId}] generatePrompt - API key available: ${!!hasApiKey}`);
    
    if (!hasApiKey) {
      console.log(`[${requestId}] generatePrompt - Anthropic API key not configured`);
      res.status(500).json({ 
        error: "Sophy is taking a brief rest right now. Please try again in a moment, and if this keeps happening, drop us a note — we'd love to help you get back to journaling.",
        code: "AI_TEMPORARILY_UNAVAILABLE",
        retryable: true 
      });
      return;
    }

    const promptContent = topic
      ? `Give me a journaling prompt about: ${topic}`
      : "Give me a creative journaling prompt to help reflect on today.";

    const systemPrompt = `You are Sophy, a supportive journaling assistant. Generate thoughtful, engaging journaling prompts. Always respond with just the prompt text directly - never wrap your response in quotation marks or say things like "Here's a prompt:" or similar prefixes. Just provide the actual prompt text.`;

    const data = await callAnthropicWithRetry(
      {
        model: MODELS.FAST,
        role: 'FAST',
        max_tokens: 200,
        messages: [
          { role: "user", content: `${systemPrompt}\n\n${promptContent}` }
        ]
      },
      "generatePrompt",
      requestId
    );

    console.log(`[${requestId}] generatePrompt success`);
    
    // Clean up the response by removing any quotes and unnecessary formatting
    let cleanPrompt = data.content[0].text.trim();
    
    // Remove surrounding quotes if they exist
    if ((cleanPrompt.startsWith('"') && cleanPrompt.endsWith('"')) ||
        (cleanPrompt.startsWith("'") && cleanPrompt.endsWith("'"))) {
      cleanPrompt = cleanPrompt.slice(1, -1);
    }
    
    // Remove any prefixes like "Here's a prompt:" or similar
    cleanPrompt = cleanPrompt.replace(/^(Here's a prompt:|Here's your prompt:|Prompt:|Journal prompt:)\s*/i, '');
    
    res.status(200).json({ prompt: cleanPrompt });
  } catch (error) {
    console.error(`[${requestId || 'unknown'}] Prompt generation failed:`, {
      message: error.message,
      stack: error.stack,
      name: error.name
    });
    const userError = mapErrorToUserMessage(error, 'generatePrompt');
    res.set('Access-Control-Allow-Origin', '*');
    res.status(500).json({ 
      error: userError.message,
      code: userError.code,
      retryable: userError.retryable 
    });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
// GRATITUDE ENGINE (v2 Phase 2b, 2026-07-01)
// One function, three actions — auth REQUIRED for all:
//  - personalSubtraction (Plus): mental-subtraction prompt personalized from
//    the user's own gratitude history (Koo, Algoe, Wilson & Gilbert 2008).
//  - letterAssist (Plus): Sophy drafts a gratitude letter from rough notes.
//  - emailLetter (any signed-in user): sends the letter to the USER'S OWN
//    email only — recipient is never a request parameter.
// ═══════════════════════════════════════════════════════════════════════════
exports.gratitudeEngine = onRequest({ secrets: [ANTHROPIC_API_KEY, OPENAI_API_KEY, SENDGRID_API_KEY] }, async (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(204).send('');

  const requestId = generateRequestId();
  try {
    const authHeader = req.headers.authorization?.replace('Bearer ', '');
    if (!authHeader) return res.status(401).json({ error: 'Sign in required.' });
    const decoded = await admin.auth().verifyIdToken(authHeader);
    const userId = decoded.uid;
    const { action } = req.body;

    // Shared Plus gate (server-side, authoritative)
    const requirePlus = async () => {
      const userDoc = await admin.firestore().collection('users').doc(userId).get();
      const tier = userDoc.data()?.subscriptionTier || 'free';
      return ['plus', 'connect'].includes(tier);
    };

    if (action === 'personalSubtraction') {
      if (!(await requirePlus())) {
        return res.status(403).json({ error: 'InkWell Plus unlocks personalized practice.', code: 'UPGRADE_REQUIRED' });
      }
      const snap = await admin.firestore().collection('journalEntries')
        .where('userId', '==', userId)
        .orderBy('createdAt', 'desc')
        .limit(60)
        .get();
      const gratitudes = [];
      snap.forEach(d => {
        const e = d.data();
        if (e.entryMode === 'gratitude' || (e.tags || []).includes('gratitude')) {
          if (Array.isArray(e.rawGratitudes)) gratitudes.push(...e.rawGratitudes);
          else if (e.text) gratitudes.push(String(e.text).slice(0, 300));
        }
      });
      if (gratitudes.length < 3) {
        return res.status(200).json({
          prompt: null,
          code: 'NOT_ENOUGH_HISTORY',
          message: 'Write a few more gratitude entries first — then Sophy can personalize this practice from your own journal.'
        });
      }
      const material = gratitudes.slice(0, 40).join('\n- ');
      const sys = `You write ONE mental-subtraction gratitude prompt (based on Koo, Algoe, Wilson & Gilbert, 2008): the user imagines a specific good thing from THEIR OWN life having never happened. Rules: second person; reference ONE concrete thing drawn from their gratitude history below; under 55 words; end with a question asking what would be missing; warm, plain language; NEVER use em dashes, en dashes, or semicolons (commas and periods only); no advice, no clinical terms, no preamble; output only the prompt text.`;
      const data = await callAnthropicWithRetry({
        model: MODELS.FAST,
        role: 'FAST',
        max_tokens: 160,
        messages: [{ role: 'user', content: `${sys}\n\nTheir recent gratitudes:\n- ${material}` }]
      }, 'gratitudeEngine.personalSubtraction', requestId);
      const prompt = data.content[0].text.trim()
        .replace(/^["']|["']$/g, '')
        .replace(/\s*[—–]\s*/g, ', ')
        .replace(/;\s*/g, '. ');
      console.log(`[${requestId}] personalSubtraction success`);
      return res.status(200).json({ prompt });
    }

    if (action === 'letterAssist') {
      if (!(await requirePlus())) {
        return res.status(403).json({ error: 'InkWell Plus unlocks Sophy letter drafting.', code: 'UPGRADE_REQUIRED' });
      }
      const notes = String(req.body.notes || '').trim().slice(0, 4000);
      const recipientName = String(req.body.recipientName || '').trim().slice(0, 80);
      if (notes.length < 5) {
        return res.status(400).json({ error: 'Jot a few rough notes first — who they are, what they did, what it meant.' });
      }
      const sys = `You help someone draft a gratitude letter${recipientName ? ` to ${recipientName}` : ''} (the gratitude-visit exercise, Seligman et al. 2005). Write 120-180 words in the writer's plain first-person voice using their notes. Be specific and concrete about what the person did, what it cost them, and what it changed.

VOICE RULES (strict — this must read like a person, not an assistant):
- NEVER use em dashes or en dashes. Use commas, periods, or start a new sentence.
- No semicolons. Short, plain sentences. Contractions are good.
- No flowery cliches, no "words cannot express," no "truly," no "journey," no "grateful beyond measure."
- Keep the writer's own words and phrasing from their notes wherever possible, including their imperfections.
- No advice, no lists, no summary line.

Output ONLY the letter body, starting with "Dear ${recipientName || '___'}," and ending with a simple sign-off line without a name.`;
      const data = await callAnthropicWithRetry({
        model: MODELS.FAST,
        role: 'FAST',
        max_tokens: 400,
        messages: [{ role: 'user', content: `${sys}\n\nTheir rough notes:\n${notes}` }]
      }, 'gratitudeEngine.letterAssist', requestId);
      // Belt-and-suspenders: strip AI-tell punctuation even if the model slips
      let draft = data.content[0].text.trim()
        .replace(/\s*[—–]\s*/g, ', ')
        .replace(/;\s*/g, '. ');
      // Crisis backstop (uniform with askSophy/plannerAssist): screen the user's notes
      try {
        const crisisPattern = /suicid|kill (myself|me)|end (my|it) (life|all)|don'?t want to (be here|live|exist|wake up)|do not want to (be here|live|exist)|better off without me|no reason to (live|go on)|want (to die|it to end)|wanna die|hurt (myself|me on purpose)|self.?harm|not worth living|take my (own )?life|can'?t go on|ready to give up on (life|everything)/i;
        if (crisisPattern.test(String(notes).replace(/[\u2018\u2019]/g, "'")) && !/988/.test(draft)) {
          console.warn(`[${requestId}] 🚨 CRISIS BACKSTOP FIRED in letterAssist`);
          draft += "\n\nOne more thing, and it matters: if any part of you is thinking about not being here, please reach out to a real person right now. Call or text 988 (Suicide & Crisis Lifeline), or text HOME to 741741 (Crisis Text Line). Veterans can call 988 and press 1. InkWell is a wellness tool, not crisis support.";
        }
      } catch (e) { console.error(`[${requestId}] letterAssist backstop failed (non-blocking):`, e.message); }
      console.log(`[${requestId}] letterAssist success`);
      return res.status(200).json({ draft });
    }

    if (action === 'emailLetter') {
      const letterText = String(req.body.letterText || '').trim();
      if (letterText.length < 10) {
        return res.status(400).json({ error: 'Write your letter first.' });
      }
      const email = decoded.email;
      if (!email) return res.status(400).json({ error: 'No email address on your account.' });
      // Light abuse guard: 1 send per minute per user
      const guardRef = admin.firestore().collection('users').doc(userId);
      const g = (await guardRef.get()).data() || {};
      const last = g.lastLetterEmailAt?.toMillis ? g.lastLetterEmailAt.toMillis() : 0;
      if (Date.now() - last < 60000) {
        return res.status(429).json({ error: 'One email a minute — try again shortly.' });
      }
      const safeName = String(req.body.recipientName || '').trim().slice(0, 80);
      sgMail.setApiKey(SENDGRID_API_KEY.value());
      await sgMail.send({
        to: email, // own email ONLY — never a third party
        from: 'support@inkwelljournal.io',
        subject: safeName ? `Your gratitude letter to ${safeName}` : 'Your gratitude letter',
        text: `${letterText.slice(0, 10000)}\n\n—\nWritten in InkWell. Sending it to them is optional; writing it is where the effect lives.`
      });
      await guardRef.set({ lastLetterEmailAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
      console.log(`[${requestId}] emailLetter sent to self`);
      return res.status(200).json({ sent: true });
    }

    return res.status(400).json({ error: 'Unknown action.' });
  } catch (error) {
    console.error(`[${requestId}] gratitudeEngine failed:`, error.message);
    res.set('Access-Control-Allow-Origin', '*');
    return res.status(500).json({ error: 'Something hiccuped. Please try again.' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
// PRACTICE SUMMARY (2026-07-02)
// Client-generated usage summary a user can forward to their practitioner.
// LAWS (product decisions, do not drift):
//  - FREE tier. Never gate this behind Plus. Practitioner referrals depend
//    on the free app being fully sufficient for the client.
//  - Sent to the USER'S OWN email only. InkWell never contacts a practitioner.
//  - Usage metadata ONLY. Never entry text, titles, or tag labels.
// ═══════════════════════════════════════════════════════════════════════════
exports.practiceSummary = onRequest({ secrets: [SENDGRID_API_KEY] }, async (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(204).send('');
  const requestId = generateRequestId();
  try {
    const authHeader = req.headers.authorization?.replace('Bearer ', '');
    if (!authHeader) return res.status(401).json({ error: 'Sign in required.' });
    const decoded = await admin.auth().verifyIdToken(authHeader);
    const userId = decoded.uid;
    const email = decoded.email;
    if (!email) return res.status(400).json({ error: 'No email address on your account.' });

    const days = [7, 30, 90].includes(Number(req.body.days)) ? Number(req.body.days) : 30;

    // Light abuse guard: 1 send per minute per user
    const guardRef = admin.firestore().collection('users').doc(userId);
    const g = (await guardRef.get()).data() || {};
    const last = g.lastPracticeSummaryAt?.toMillis ? g.lastPracticeSummaryAt.toMillis() : 0;
    if (Date.now() - last < 60000) {
      return res.status(429).json({ error: 'One summary a minute. Try again shortly.' });
    }

    const cutoff = Date.now() - days * 86400000;
    const snap = await admin.firestore().collection('journalEntries')
      .where('userId', '==', userId).get();

    const dayset = new Set();
    let total = 0, words = 0, voice = 0;
    const feelPairs = []; // self-rated 1-5 before/after practices
    const mix = { gratitude: 0, reframe: 0, sprint: 0, inkblot: 0, journal: 0 };
    const tod = { morning: 0, afternoon: 0, evening: 0 };
    snap.forEach(d => {
      const e = d.data();
      let t = e.createdAt?.toDate?.();
      if (!t && typeof e.createdAt === 'string') t = new Date(e.createdAt);
      if (!t || isNaN(t) || t.getTime() < cutoff) return;
      total++;
      dayset.add(t.toISOString().slice(0, 10));
      words += String(e.text || '').split(/\s+/).filter(Boolean).length;
      if (e.isVoiceEntry) voice++;
      if (e.feelBefore >= 1 && e.feelAfter >= 1) feelPairs.push(e.feelAfter - e.feelBefore);
      const tags = e.tags || [];
      if (e.entryMode === 'gratitude' || tags.includes('gratitude')) mix.gratitude++;
      else if (tags.includes('reframe')) mix.reframe++;
      else if (tags.includes('sprint')) mix.sprint++;
      else if (tags.includes('inkblot')) mix.inkblot++;
      else mix.journal++;
      const h = t.getHours();
      if (h < 12) tod.morning++; else if (h < 17) tod.afternoon++; else tod.evening++;
    });

    // Longest consecutive-day run inside the period
    const sorted = [...dayset].sort();
    let streak = 0, run = 0, prev = null;
    for (const k of sorted) {
      run = (prev && (new Date(k) - new Date(prev) === 86400000)) ? run + 1 : 1;
      if (run > streak) streak = run;
      prev = k;
    }

    const mixLines = Object.entries({
      'Open journaling': mix.journal,
      'Gratitude practices': mix.gratitude,
      'Reframe (perspective) practices': mix.reframe,
      'Writing sprints': mix.sprint,
      'Quick captures (InkBlot)': mix.inkblot
    }).filter(([, v]) => v > 0).map(([k, v]) => `  ${k}: ${v}`).join('\n') || '  (no entries in this period)';
    const todTop = Object.entries(tod).sort((a, b) => b[1] - a[1])[0];

    const body = `INKWELL PRACTICE SUMMARY
Period: last ${days} days
Generated: ${new Date().toISOString().slice(0, 10)}
Account: ${email}

ENGAGEMENT
Entries written: ${total}
Days with at least one entry: ${dayset.size} of ${days}
Longest consecutive-day run: ${streak} day${streak === 1 ? '' : 's'}
Total words written: ${words}${total ? `\nMost common writing time: ${todTop[0]}` : ''}${voice ? `\nVoice entries: ${voice}` : ''}

PRACTICE MIX
${mixLines}${feelPairs.length >= 3 ? `\n\nSELF-RATED SHIFT\nBefore and after practices, on a 1 to 5 heaviness scale the account owner\nrates optionally: average change ${(feelPairs.reduce((a, b) => a + b, 0) / feelPairs.length).toFixed(1)} across ${feelPairs.length} rated practices.\n(A self-rated feel, not a clinical measure.)` : ''}

ABOUT THIS SUMMARY
This summary shows usage patterns only. It contains no journal content,
no titles, and no tags. It was generated and shared by the account owner,
at their own choice. InkWell is a wellness journal, not a medical record
or a clinical tool.

InkWell, inkwelljournal.io`;

    sgMail.setApiKey(SENDGRID_API_KEY.value());
    await sgMail.send({
      to: email, // own email ONLY, never a third party
      from: 'support@inkwelljournal.io',
      subject: `Your InkWell Practice Summary (last ${days} days)`,
      text: body
    });
    await guardRef.set({ lastPracticeSummaryAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    console.log(`[${requestId}] practiceSummary sent to self (${days}d, ${total} entries)`);
    return res.status(200).json({ sent: true });
  } catch (error) {
    console.error(`[${requestId}] practiceSummary failed:`, error.message);
    res.set('Access-Control-Allow-Origin', '*');
    return res.status(500).json({ error: 'Something hiccuped. Please try again.' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
// PLANNER ASSIST (2026-07-02) — Sophy inside the Values-Based Goal Planner.
// One callable, three steps. EXEMPT from the daily Sophy tease-limit (Adam:
// UX beats trivial AI cost) but capped server-side at 15 calls/day/user.
// steps:
//   vivid      — asks 2-3 sensory questions to deepen the day-in-life vision
//   seed       — suggests candidate goals from vision + top values + recent journal
//   wantshould — reflects on the user's pros/cons; flags wants vs shoulds
// ═══════════════════════════════════════════════════════════════════════════
exports.plannerAssist = onRequest({ secrets: [ANTHROPIC_API_KEY, OPENAI_API_KEY] }, async (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(204).send('');
  const requestId = generateRequestId();
  try {
    const authHeader = req.headers.authorization?.replace('Bearer ', '');
    if (!authHeader) return res.status(401).json({ error: 'Sign in required.' });
    const decoded = await admin.auth().verifyIdToken(authHeader);
    const userId = decoded.uid;
    const step = String(req.body.step || '');

    // Server-side cap: 15 planner calls per day per user
    const userRef = admin.firestore().collection('users').doc(userId);
    const u = (await userRef.get()).data() || {};
    const today = new Date().toISOString().slice(0, 10);
    const used = (u.plannerAssistDate === today) ? (u.plannerAssistCount || 0) : 0;
    if (used >= 15) return res.status(429).json({ error: "Sophy has helped a lot with this today. The planner keeps; come back tomorrow." });

    const VOICE = `Write in Sophy's voice: warm, plain, second person. NEVER use em dashes, en dashes, or semicolons (commas and periods only). No clinical terms, no advice-dumping, no preamble, no lists unless asked. If anything in the user's text suggests self-harm or crisis, gently include: call or text 988, or text HOME to 741741, veterans press 1, and say a human matters more than planning right now.`;

    let prompt = null;
    if (step === 'vivid') {
      const vision = String(req.body.vision || '').trim().slice(0, 4000);
      if (vision.length < 20) return res.status(400).json({ error: 'Write a little of your day first, then I can help deepen it.' });
      prompt = `${VOICE}\n\nThe user is writing "a day in my life, 15 years from now." Read their draft, then ask exactly 2 or 3 short sensory questions that would make the scene more vivid and specific. Reference concrete details THEY wrote. Under 80 words total. Output only the questions, each on its own line.\n\nTheir draft:\n${vision}`;
    } else if (step === 'seed') {
      const vision = String(req.body.vision || '').trim().slice(0, 3000);
      const values = (Array.isArray(req.body.topValues) ? req.body.topValues : []).slice(0, 10).map(v => String(v).slice(0, 40)).join(', ');
      // Recent journal themes (titles + first 200 chars, newest 15) — content stays server-side
      const snap = await admin.firestore().collection('journalEntries')
        .where('userId', '==', userId).orderBy('createdAt', 'desc').limit(15).get();
      const themes = [];
      snap.forEach(d => { const e = d.data(); if (e.text) themes.push(String(e.text).slice(0, 200)); });
      prompt = `${VOICE}\n\nThe user is hunting for a goal worth wanting. Suggest exactly 3 candidate goals. Each: one line, specific and startable within 90 days, grounded in what you see below. After each goal add one short "because" clause tying it to their values or journal themes. Under 120 words total. No numbering headers, just three lines starting with a dash.\n\nTheir top values: ${values || '(not provided)'}\nTheir 15-year vision:\n${vision || '(not written yet)'}\n\nRecent journal excerpts:\n${themes.join('\n---\n') || '(no entries yet)'}`;
    } else if (step === 'dedupe') {
      const ideas = (Array.isArray(req.body.ideas) ? req.body.ideas : []).slice(0, 60).map(x => String(x).slice(0, 200));
      if (ideas.length < 3) return res.status(400).json({ error: 'Add some ideas first.' });
      prompt = `You are auditing a goal brainstorm for duplicates. People reword the same idea to hit a count, and the point of this exercise is VOLUME OF DISTINCT THINKING. Group any entries that are the same underlying idea in different words. Be strict about rewordings, lenient about genuinely different angles.\n\nOutput format, exactly:\nLine 1: DISTINCT: <number of truly distinct ideas>\nThen, ONLY if there are duplicate groups, in Sophy's warm plain voice (no em dashes, no semicolons), name each group in one short line like: "X, Y and Z are the same idea wearing different hats." Then one encouraging line pushing them past the obvious into the silly and impossible. Under 90 words after line 1. If everything is distinct, after line 1 write one short congratulation only.\n\nThe list:\n${ideas.map((x, i) => (i + 1) + '. ' + x).join('\n')}`;
    } else if (step === 'wantshould') {
      const notes = String(req.body.notes || '').trim().slice(0, 4000);
      if (notes.length < 20) return res.status(400).json({ error: 'Fill in some pros and cons first, then I can reflect.' });
      prompt = `${VOICE}\n\nThe user compared candidate goals with pros and cons. Read their notes. In under 90 words: reflect back which option sounds like a WANT (their own motivation) and whether any sounds like a SHOULD (outside pressure), and why, using their own words as evidence. End with one question that helps them decide. Do not decide for them.\n\nTheir notes:\n${notes}`;
    } else {
      return res.status(400).json({ error: 'Unknown step.' });
    }

    const data = await callAnthropicWithRetry({
      model: MODELS.FAST,
      role: 'FAST',
      max_tokens: 300,
      messages: [{ role: 'user', content: prompt }]
    }, `plannerAssist.${step}`, requestId);
    let text = data.content[0].text.trim().replace(/\s*[—–]\s*/g, ', ').replace(/;\s*/g, '. ');

    // Deterministic crisis backstop (mirrors askSophy): screen the user's words
    try {
      const crisisPattern = /suicid|kill (myself|me)|end (my|it) (life|all)|don'?t want to (be here|live|exist|wake up)|do not want to (be here|live|exist)|better off without me|no reason to (live|go on)|want (to die|it to end)|wanna die|hurt (myself|me on purpose)|self.?harm|not worth living|take my (own )?life|can'?t go on|ready to give up on (life|everything)/i;
      const userText = String((req.body.vision || '') + ' ' + (req.body.notes || '')).replace(/[\u2018\u2019]/g, "'");
      if (crisisPattern.test(userText) && !/988/.test(text)) {
        console.warn(`[${requestId}] 🚨 CRISIS BACKSTOP FIRED in plannerAssist`);
        text += "\n\nOne more thing, and it matters: if any part of you is thinking about not being here, please reach out to a real person right now. Call or text 988 (Suicide & Crisis Lifeline), or text HOME to 741741 (Crisis Text Line). Veterans can call 988 and press 1. InkWell is a wellness tool, not crisis support. Talking to a human right now matters more than planning.";
      }
    } catch (e) { console.error(`[${requestId}] planner backstop failed (non-blocking):`, e.message); }

    await userRef.set({ plannerAssistDate: today, plannerAssistCount: used + 1 }, { merge: true });
    console.log(`[${requestId}] plannerAssist.${step} ok (${used + 1}/15 today)`);
    return res.status(200).json({ text });
  } catch (error) {
    console.error(`[${requestId}] plannerAssist failed:`, error.message);
    res.set('Access-Control-Allow-Origin', '*');
    return res.status(500).json({ error: 'Something hiccuped. Please try again.' });
  }
});

// Enhanced askSophy with behavioral pattern recognition
exports.askSophy = onRequest({ secrets: [ANTHROPIC_API_KEY, OPENAI_API_KEY] }, async (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(204).send('');

  try {
    const { entry, wishContext, behavioralTrigger } = req.body;
    const requestId = generateRequestId();
    let sophyUserId = null; // for memory retrieval (2026-07-04)
    
    // Get user behavioral data for context
    let behaviorData = null;
    try {
      const authHeader = req.headers.authorization?.replace('Bearer ', '');
      if (authHeader) {
        const decodedToken = await admin.auth().verifyIdToken(authHeader);
        const userId = decodedToken.uid;
        sophyUserId = userId;
        behaviorData = await getUserBehavioralContext(userId);
      }
    } catch (authError) {
      console.log('No auth token provided, using basic Sophy mode');
    }
    
    // Enhanced system prompt with behavioral awareness
    const systemPrompt = `You are Sophy, a supportive journaling assistant informed by Gestalt Therapy, Positive Psychology, Cognitive Behavioral Therapy (CBT), and evidence-based goal achievement research.

${behaviorData ? `
BEHAVIORAL CONTEXT (use thoughtfully, don't reference directly):
- User's WISH engagement pattern: ${behaviorData.engagementLevel}
- Days since last goal check-in: ${behaviorData.daysSinceLastUpdate}
- Completion tendency: ${behaviorData.completionPattern}
- Emotional trend: ${behaviorData.recentEmotionalTrend}
` : ''}

${behavioralTrigger ? `
INTERVENTION CONTEXT: ${behavioralTrigger}
Provide gentle, research-informed guidance without being prescriptive.
` : ''}

COGNITIVE PATTERNS TO GENTLY NOTICE:
When you notice potential cognitive distortions in the user's writing, offer gentle observations and alternative perspectives. Common patterns from CBT include:

- All-or-Nothing Thinking: Seeing things in absolute terms (always/never, perfect/failure). Gently suggest: "I notice some strong words here. What might be true in the middle ground?"

- Overgeneralization: One event becomes a pattern (one setback means everything will fail). Gently suggest: "This feels like a big conclusion from one moment. What other experiences might tell a different story?"

- Mental Filter: Focusing only on negatives while ignoring positives. Gently suggest: "I hear the difficult parts. What else was happening that day that you might be overlooking?"

- Discounting Positives: Dismissing good things as luck or "not counting." Gently suggest: "You mention this positive thing but then dismiss it. What if it actually does count?"

- Jumping to Conclusions: Mind reading (assuming others think negatively) or fortune telling (predicting bad outcomes). Gently suggest: "I notice you're making a prediction about how this will go. What other outcomes are possible?"

- Magnification/Minimization: Blowing negatives out of proportion or shrinking positives. Gently suggest: "This feels very big right now. How might it look a week from now?"

- Emotional Reasoning: "I feel it, so it must be true." Gently suggest: "Your feelings are real and valid. And sometimes feelings can be stronger than the facts. What do you actually know to be true?"

- Should Statements: Harsh rules for yourself or others ("I should," "they must"). Gently suggest: "That 'should' sounds heavy. What would happen if you replaced it with 'I'd like to' or 'it would be nice if'?"

- Labeling: Defining yourself or others by one trait or mistake. Gently suggest: "You're using a big label here. What's a more complete picture of the situation?"

- Personalization: Taking responsibility for things outside your control. Gently suggest: "I hear you taking all the blame. What factors were actually outside your influence?"

IMPORTANT: Be subtle and warm. Don't lecture or list fallacies. Weave one gentle observation naturally into your reflection if you notice a pattern. Use everyday language, not clinical terms. Always validate their feelings first before offering a different perspective.

SILVER LININGS (Use sparingly and only when genuine):
When someone shares something difficult, you might gently offer perspective - but only if it feels authentic to the situation. This isn't about toxic positivity or dismissing pain. It's about noticing what might also be true alongside the hard parts.

Approaches that feel supportive (not patronizing):
- "This sounds really hard. I also notice you [showed resilience/reached out/recognized a pattern/took a step]..."
- "Even in the middle of this, you [did something, learned something, or showed something about yourself]..."
- "What you're going through is real. And I wonder if there's also something here about [growth/clarity/what matters to you]..."
- "It's okay for two things to be true: this is painful, AND [small positive observation]..."

What to AVOID (these feel dismissive):
- "At least..." or "On the bright side..."
- "Everything happens for a reason"
- Rushing to fix or solve
- Minimizing their experience to find a positive
- Forcing optimism where there isn't any

The goal: Help them feel heard AND gently notice their own strength, growth, or clarity when it's genuinely there. Sometimes there's no silver lining, and that's okay too - just being present is enough.

Respond naturally and warmly. Use research-backed insights rather than specific statistics. 
Language should be humble: "Research suggests..." "Many people find..." "This pattern often indicates..." "Sometimes when we're struggling, our mind..."

Respond directly in your own voice - never use stage directions, action descriptions, or phrases like "*responds warmly*" or "*nods empathetically*". Simply speak naturally and warmly.

Keep your reflections brief, focused, and emotionally clear — no more than 2–3 ideas at once. Break thoughts into short, readable paragraphs. Avoid overwhelming the user. If helpful, suggest small, practical actions that build momentum over time.

Begin your response immediately with your reflection - no introductions or narrative text.

IMPORTANT: This is a one-time reflection, not a conversation. Do not include phrases like "Let me know if you'd like to discuss further", "Would you like me to help with...", "Feel free to share more", or any other conversational follow-ups. Just provide your reflection and end naturally.

CONTINUITY (allowed, sparingly): when the entry points at something unresolved or upcoming, you may end with ONE short forward-looking line. Caring anticipation, never an invitation to reply and never an assignment. The feeling of "I am curious where this goes" or "I hope tomorrow is gentler with you". At most half the time, and only when it fits naturally.

CRISIS RESPONSE (this overrides everything above):
If the entry suggests the person may be thinking about suicide, self-harm, or not wanting to be alive - including indirect phrasing like "I don't want to be here anymore", "everyone would be better off without me", "I can't do this anymore", "what's the point" - you MUST:
1. Respond first with warmth and full presence. Take it seriously. Do not analyze, do not reframe, do not offer a journaling insight or a silver lining.
2. ALWAYS include these resources, exactly: call or text 988 (Suicide & Crisis Lifeline), text HOME to 741741 (Crisis Text Line), and for veterans call 988 then press 1.
3. Gently encourage reaching out to a real person right now - a professional, a trusted friend, or one of the lines above.
4. Remind them InkWell is a wellness tool, not crisis support, and that talking to a human right now matters more than journaling.
When in doubt about whether an entry qualifies, include the resources. A wrongly included resource costs nothing. A wrongly omitted one can cost everything.`;

    // ═══ MEMORY RETRIEVAL (2026-07-04) — "the journal that learns you," literally.
    // A cheap FAST pre-call picks up to 3 genuinely-connected past excerpts;
    // PRIME may weave in one or two, always dated. Fail-open: any error here
    // means Sophy simply reflects without memory, never blocks the reflection.
    let memoryBlock = '';
    try {
      if (sophyUserId && String(entry || '').length > 40) {
        const memSnap = await admin.firestore().collection('journalEntries')
          .where('userId', '==', sophyUserId)
          .orderBy('createdAt', 'desc')
          .limit(40).get();
        const past = [];
        memSnap.forEach(d => {
          const e = d.data();
          const t = String(e.text || '').trim();
          if (!t || t === String(entry).trim()) return; // skip empties + the entry being reflected
          let when = e.createdAt?.toDate?.();
          if (!when && typeof e.createdAt === 'string') when = new Date(e.createdAt);
          if (!when || isNaN(when)) return;
          past.push({ when: when.toISOString().slice(0, 10), text: t.slice(0, 220) });
        });
        if (past.length >= 3) {
          const listing = past.map((p, i) => `[${i}] (${p.when}) ${p.text}`).join('\n');
          const pick = await callAnthropicWithRetry({
            model: MODELS.FAST,
            role: 'FAST',
            max_tokens: 30,
            messages: [{ role: 'user', content: `Today's journal entry:\n${String(entry).slice(0, 1500)}\n\nPast excerpts from the same person:\n${listing}\n\nWhich past excerpts genuinely connect to today's entry (same thread, meaningful contrast, or visible progress)? Reply with ONLY the indices comma-separated (max 3), or NONE.` }]
          }, 'askSophy.memoryPick', requestId);
          const raw = pick.content[0].text.trim();
          if (!/NONE/i.test(raw)) {
            const idx = (raw.match(/\d+/g) || []).map(Number).filter(i => i >= 0 && i < past.length).slice(0, 3);
            if (idx.length) {
              memoryBlock = '\n\nMEMORY (excerpts from this person\'s own past entries. Weave in AT MOST one or two, ONLY where genuinely relevant, and always say when it was written in natural language like "a few weeks ago you wrote". If nothing truly connects, use none. Never resurface a past dark moment unless today\'s entry is about that same struggle):\n' +
                idx.map(i => `- (${past[i].when}) "${past[i].text}"`).join('\n');
              console.log(`[${requestId}] memory: ${idx.length} excerpt(s) attached`);
            }
          }
        }
      }
    } catch (memErr) {
      console.warn(`[${requestId}] memory retrieval skipped (non-blocking):`, memErr.message);
    }

    // Call Anthropic with enhanced context — PRIME role: reflection/crisis path,
    // safety-gated (suicide-entry test required before any model change)
    const data = await callAnthropicWithRetry(
      {
        model: MODELS.PRIME,
        role: 'PRIME',
        max_tokens: 400,
        messages: [
          { role: "user", content: `${systemPrompt}${memoryBlock}\n\nUser entry: ${entry}` }
        ]
      },
      "askSophy",
      requestId
    );

    // Track this interaction for learning
    if (behavioralTrigger && behaviorData) {
      try {
        await trackInterventionOutcome(behaviorData.userId, behavioralTrigger, data.content[0].text);
      } catch (trackingError) {
        console.error('Non-critical: Failed to track intervention outcome:', trackingError);
      }
    }

    // Update onboarding progress for Sophy usage (non-blocking)
    if (behaviorData) {
      const onboardingUpdates = {
        hasUsedSophy: true
      };
      
      // Check if this is their first Sophy interaction
      try {
        const userDoc = await admin.firestore().collection("users").doc(behaviorData.userId).get();
        if (userDoc.exists) {
          const userData = userDoc.data();
          const hasUsedSophyBefore = userData.onboardingState?.hasUsedSophy || false;
          
          if (!hasUsedSophyBefore) {
            onboardingUpdates['milestones.firstSophy'] = admin.firestore.FieldValue.serverTimestamp();
            onboardingUpdates.currentMilestone = 'sophy_user';
          }
        }
      } catch (milestoneError) {
        console.warn('Failed to check first Sophy milestone:', milestoneError.message);
      }
      
      await updateOnboardingState(behaviorData.userId, onboardingUpdates);
    }

    // Clean the response to remove any stage directions or narrative text
    let cleanedInsight = data.content[0].text.trim();
    
    // Remove common stage direction patterns
    cleanedInsight = cleanedInsight
      .replace(/^\*[^*]*\*\s*/g, '') // Remove opening stage directions
      .replace(/\*[^*]*\*$/g, '') // Remove ending stage directions
      .replace(/\*[^*]*\*/g, '') // Remove any remaining stage directions
      .replace(/^(Sophy\s+)?(responds?|says?|speaks?|nods?|smiles?|looks?)\s+(warmly|empathetically|thoughtfully|gently|softly)[:\s]*/gi, '')
      .replace(/^Hello,?\s+\w+\.\s*/i, '') // Remove greeting patterns
      .replace(/^\*with\s+warmth\s+and\s+empathy\*\s*/gi, '') // Remove specific empathy stage direction
      .replace(/^\*[^*]*warmth[^*]*\*\s*/gi, '') // Remove warmth-related stage directions
      .replace(/^\*[^*]*empathy[^*]*\*\s*/gi, '') // Remove empathy-related stage directions
      .replace(/^\*[^*]*empathetically[^*]*\*\s*/gi, '') // Remove empathetically stage directions
      .replace(/I\s+sense\s+there\s+is\s+an\s+important\s+wish/gi, '') // Remove specific AI prompt leakage
      .replace(/^Let's\s+take\s+a\s+moment\s+to\s+vividly\s+imagine/gi, '') // Remove prompt instruction leakage
      .trim();

    // ═══ CRISIS BACKSTOP (deterministic, 2026-07-01) ═══
    // The system prompt instructs crisis resources, but the model is not the
    // guarantee — this is. If the ENTRY matches crisis language and the reply
    // doesn't already carry the resources, append them. Screens the user's
    // words, not the model's mood. When in doubt, resources go in.
    try {
      const crisisPattern = /suicid|kill (myself|me)|end (my|it) (life|all)|don'?t want to (be here|live|exist|wake up)|do not want to (be here|live|exist)|better off without me|no reason to (live|go on)|want (to die|it to end)|wanna die|hurt (myself|me on purpose)|self.?harm|not worth living|take my (own )?life|can'?t go on|ready to give up on (life|everything)/i;
      const normalizedEntry = String(entry || '').replace(/[‘’ʼ]/g, "'"); // curly apostrophes → straight (mobile keyboards)
      if (normalizedEntry && crisisPattern.test(normalizedEntry) && !/988/.test(cleanedInsight)) {
        console.warn(`[${requestId}] 🚨 CRISIS BACKSTOP FIRED - entry matched crisis language, model reply lacked resources. Appending.`);
        cleanedInsight += "\n\nOne more thing, and it matters: if any part of you is thinking about not being here, please reach out to a real person right now. Call or text 988 (Suicide & Crisis Lifeline), or text HOME to 741741 (Crisis Text Line). Veterans can call 988 and press 1. InkWell is a wellness tool, not crisis support. Talking to a human right now matters more than journaling.";
      }
    } catch (backstopError) {
      console.error(`[${requestId}] Crisis backstop check failed (non-blocking):`, backstopError.message);
    }

    res.status(200).json({ 
      insight: cleanedInsight,
      behavioralContext: behaviorData?.contextualHint || null
    });

  } catch (error) {
    console.error("Enhanced askSophy error:", error.message);
    const userError = mapErrorToUserMessage(error, 'askSophy');
    res.status(500).json({ 
      error: userError.message,
      code: userError.code,
      retryable: userError.retryable 
    });
  }
});

// Generate Period Insights (7-day or 30-day pattern analysis)
exports.generatePeriodInsights = onRequest({ 
  secrets: [ANTHROPIC_API_KEY, OPENAI_API_KEY],
  invoker: 'public' // Allow unauthenticated HTTP requests (we handle auth via Firebase token)
}, async (req, res) => {
  // Set CORS headers for ALL responses (including errors)
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  
  if (req.method === 'OPTIONS') {
    return res.status(204).send('');
  }

  try {
    const { period } = req.body; // 'weekly' or 'monthly'
    const requestId = generateRequestId();
    
    // Require authentication
    const authHeader = req.headers.authorization?.replace('Bearer ', '');
    if (!authHeader) {
      return res.status(401).json({ error: 'Authentication required' });
    }
    
    const decodedToken = await admin.auth().verifyIdToken(authHeader);
    const userId = decodedToken.uid;
    
    // Check subscription tier (Plus required)
    const userDoc = await admin.firestore().collection('users').doc(userId).get();
    const userData = userDoc.data();
    const tier = userData?.subscriptionTier || 'free';
    
    // For testing: Also check if user has alpha role (which grants Plus features)
    const isAlphaUser = userData?.role === 'alpha' || userData?.roles?.includes('alpha');
    
    if (tier === 'free' && !isAlphaUser) {
      return res.status(200).json({ 
        error: 'Period insights require InkWell Plus',
        upgradeRequired: true 
      });
    }
    
    // Calculate date range
    const now = new Date();
    const daysBack = period === 'monthly' ? 30 : 7;
    const startDate = new Date(now.getTime() - (daysBack * 24 * 60 * 60 * 1000));
    
    console.log(`[${requestId}] Fetching entries for user ${userId}, period: ${period}, since: ${startDate.toISOString()}`);
    
    // Fetch entries for the user, then filter by date in JavaScript
    // (avoids needing a composite index for timestamp field)
    const allEntriesSnap = await admin.firestore()
      .collection('journalEntries')
      .where('userId', '==', userId)
      .orderBy('createdAt', 'desc')
      .limit(100) // Get recent entries to filter
      .get();
    
    console.log(`[${requestId}] Total entries found: ${allEntriesSnap.size}`);
    
    // Filter to entries within the date range
    const filteredDocs = allEntriesSnap.docs.filter(doc => {
      const data = doc.data();
      const entryDate = data.timestamp?.toDate?.() || data.createdAt?.toDate?.() || null;
      return entryDate && entryDate >= startDate;
    });
    
    console.log(`[${requestId}] Entries within ${daysBack} days: ${filteredDocs.length}`);
    
    if (filteredDocs.length < 3) {
      return res.status(200).json({ 
        insight: null,
        insufficientEntries: true,
        entryCount: filteredDocs.length,
        message: `You need at least 3 entries in the last ${daysBack} days for Sophy to find meaningful patterns. Keep journaling — you're building something valuable.`
      });
    }
    
    // Compile entries for analysis
    const entries = filteredDocs.map(doc => {
      const data = doc.data();
      const date = data.timestamp?.toDate?.() || new Date();
      return {
        date: date.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' }),
        content: (data.text || data.content || '').substring(0, 500),
        gratitude: (data.gratitude || '').substring(0, 200),
        wish: (data.wish || '').substring(0, 200),
        mood: data.mood || null,
      };
    });
    
    const entryText = entries.map(e => 
      `[${e.date}]${e.mood ? ` (Mood: ${e.mood})` : ''}\n${e.content}${e.gratitude ? `\nGratitude: ${e.gratitude}` : ''}${e.wish ? `\nWish: ${e.wish}` : ''}`
    ).join('\n\n---\n\n');
    
    const periodLabel = period === 'monthly' ? '30 days' : '7 days';
    
    const systemPrompt = `You are Sophy, an AI journaling companion. You're reviewing ${entries.length} journal entries from the past ${periodLabel} to offer meaningful pattern insights.

You are NOT human and don't pretend to be. You're a thoughtful AI that can notice patterns across entries that might be hard to see day-to-day.

WHAT TO NOTICE:
- Recurring themes or preoccupations that keep emerging
- Unfinished business or tensions that appear across entries
- Shifts in emotional tone over the period
- Character strengths showing up (courage, kindness, curiosity, perseverance, honesty)
- Progress on goals, wishes, or intentions
- Relationship patterns and what matters to this person
- Self-talk patterns (kind vs. critical toward themselves)
- Moments of growth, resilience, or clarity
- Gratitude patterns and what they value

HOW TO REFLECT:
- Speak naturally, like a thoughtful friend sharing observations
- No labels, headers, or modality callouts (don't say "Gestalt says..." or "From a positive psychology perspective...")
- Weave insights together into flowing paragraphs
- Use "I notice..." and "It seems like..." and "Across these entries..." language
- Validate the journey, not just achievements
- Balance noticing challenges with celebrating strengths
- Offer observations as invitations, not conclusions

TONE:
- Warm and curious, not clinical or teacherly
- Honest but kind
- Brief and focused - no overwhelming them with observations
- End with something affirming or a gentle reflection

FORMAT:
- 2-3 flowing paragraphs (no headers, no bullet points, no labeled sections)
- Aim for 200-300 words
- Start by referencing something specific from their entries
- End naturally - no questions, no "let me know if..." - just a closing thought

IMPORTANT: Respond directly - no stage directions, no meta-text, no "Dear..." openings. Just start reflecting.`;

    const data = await callAnthropicWithRetry(
      {
        model: MODELS.FAST,
        role: 'FAST',
        max_tokens: 600,
        messages: [
          { role: "user", content: `${systemPrompt}\n\nJOURNAL ENTRIES FROM THE PAST ${periodLabel.toUpperCase()}:\n\n${entryText}` }
        ]
      },
      "generatePeriodInsights",
      requestId
    );

    // Clean the response
    let cleanedInsight = data.content[0].text.trim()
      .replace(/^\*[^*]*\*\s*/g, '')
      .replace(/\*[^*]*\*$/g, '')
      .replace(/\*[^*]*\*/g, '')
      .trim();

    console.log(`[${requestId}] generatePeriodInsights success: ${period}, ${entries.length} entries`);

    res.status(200).json({ 
      insight: cleanedInsight,
      period: period,
      entryCount: entries.length,
      dateRange: {
        from: startDate.toISOString(),
        to: now.toISOString()
      }
    });

  } catch (error) {
    console.error("generatePeriodInsights error:", error.message);
    const userError = mapErrorToUserMessage(error, 'generatePeriodInsights');
    res.status(500).json({ 
      error: userError.message,
      code: userError.code,
      retryable: userError.retryable 
    });
  }
});

// Get user's behavioral context for Sophy
async function getUserBehavioralContext(userId) {
  try {
    const behaviorRef = admin.firestore()
      .collection('users')
      .doc(userId)
      .collection('behaviorSummary')
      .doc('wishPatterns');
    
    const behaviorSnap = await behaviorRef.get();
    if (!behaviorSnap.exists) return null;
    
    const data = behaviorSnap.data();
    const daysSinceLastUpdate = calculateDaysSince(data.lastUpdateTimestamp);
    
    return {
      userId: userId,
      engagementLevel: categorizeEngagement(data.totalUpdates, daysSinceLastUpdate),
      daysSinceLastUpdate: daysSinceLastUpdate,
      completionPattern: categorizeCompletion(data.completionRate),
      recentEmotionalTrend: analyzeEmotionalTrend(data.emotionalTrends),
      contextualHint: generateContextualHint(daysSinceLastUpdate, data)
    };
  } catch (error) {
    console.error('Error getting behavioral context:', error);
    return null;
  }
}

// Helper functions for behavioral analysis
function categorizeEngagement(totalUpdates, daysSinceLastUpdate) {
  if (daysSinceLastUpdate > 14) return 'low_recent_activity';
  if (totalUpdates > 10) return 'highly_engaged';
  if (totalUpdates > 3) return 'moderately_engaged';
  return 'getting_started';
}

function categorizeCompletion(completionRate) {
  if (completionRate > 0.7) return 'high_completion';
  if (completionRate > 0.3) return 'moderate_completion';
  return 'struggles_with_completion';
}

function analyzeEmotionalTrend(emotionalTrends) {
  if (!emotionalTrends || emotionalTrends.length === 0) return 'insufficient_data';
  
  const recentTrends = emotionalTrends.slice(-3);
  const stressedCount = recentTrends.filter(t => t.tone === 'stressed' || t.tone === 'anxious').length;
  const positiveCount = recentTrends.filter(t => t.tone === 'confident' || t.tone === 'hopeful').length;
  
  if (stressedCount > positiveCount) return 'recently_stressed';
  if (positiveCount > stressedCount) return 'recently_positive';
  return 'emotionally_neutral';
}

function generateContextualHint(daysSinceLastUpdate, behaviorData) {
  if (daysSinceLastUpdate > 10) {
    return "Consider a gentle goal check-in - research shows regular reflection supports progress";
  }
  if (behaviorData.emotionalTrends?.some(t => t.tone === 'stressed')) {
    return "Goal stress is normal - adjusting timelines often helps maintain motivation";
  }
  return null;
}

function calculateDaysSince(timestamp) {
  if (!timestamp) return 0;
  const now = new Date();
  const past = timestamp.toDate ? timestamp.toDate() : new Date(timestamp);
  const diffTime = Math.abs(now - past);
  return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
}

// Track intervention outcomes for learning (non-critical)
async function trackInterventionOutcome(userId, trigger, response) {
  try {
    await admin.firestore().collection('interventionOutcomes').add({
      userId: userId,
      trigger: trigger,
      responseLength: response.length,
      timestamp: admin.firestore.FieldValue.serverTimestamp()
    });
  } catch (error) {
    console.error('Non-critical: Failed to track intervention:', error);
  }
}

// Check for behavioral triggers and suggest interventions
exports.checkUserBehavioralTriggers = onRequest({ secrets: [ANTHROPIC_API_KEY, OPENAI_API_KEY] }, async (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(204).send('');

  try {
    const authHeader = req.headers.authorization?.replace('Bearer ', '');
    if (!authHeader) {
      return res.status(401).json({ error: 'No authorization token' });
    }

    const decodedToken = await admin.auth().verifyIdToken(authHeader);
    const userId = decodedToken.uid;
    
    const behaviorData = await getUserBehavioralContext(userId);
    
    if (!behaviorData) {
      return res.status(200).json({ interventionSuggested: false });
    }
    
    // Check for intervention triggers
    let interventionMessage = null;
    let interventionType = null;
    
    // Long inactivity trigger
    if (behaviorData.daysSinceLastUpdate > 7) {
      interventionMessage = "Research suggests that regular goal check-ins improve progress by 40%. How are you feeling about your WISH journey lately?";
      interventionType = "inactivity_check";
    }
    // Stress pattern trigger
    else if (behaviorData.recentEmotionalTrend === 'recently_stressed') {
      interventionMessage = "It sounds like you've been feeling some stress around your goals. Many people find that breaking big wishes into smaller steps reduces overwhelm.";
      interventionType = "stress_support";
    }
    // Low completion pattern trigger
    else if (behaviorData.completionPattern === 'struggles_with_completion' && behaviorData.engagementLevel === 'moderately_engaged') {
      interventionMessage = "You're staying engaged with your goals, which is wonderful! Research shows that celebrating small wins can boost completion rates.";
      interventionType = "completion_support";
    }
    
    if (interventionMessage) {
      // Track that we showed this intervention
      try {
        await admin.firestore().collection('interventionsShown').add({
          userId: userId,
          type: interventionType,
          message: interventionMessage,
          behavioralContext: behaviorData.engagementLevel,
          timestamp: admin.firestore.FieldValue.serverTimestamp()
        });
      } catch (trackingError) {
        console.error('Non-critical: Failed to track intervention shown:', trackingError);
      }
      
      return res.status(200).json({
        interventionSuggested: true,
        message: interventionMessage,
        type: interventionType
      });
    }
    
    return res.status(200).json({ interventionSuggested: false });
    
  } catch (error) {
    console.error('Error checking behavioral triggers:', error);
    return res.status(500).json({ error: 'Failed to check triggers' });
  }
});

// Save manifest statement for authenticated user
exports.saveManifest = onCall(async (data, context) => {
  const uid = context.auth?.uid;
  if (!uid) {
    throw new HttpsError("unauthenticated", "User must be authenticated.");
  }

  const { statement } = data;
  if (!statement || typeof statement !== "string") {
    throw new HttpsError("invalid-argument", "Manifest statement must be a non-empty string.");
  }

  try {
    await admin.firestore().collection("manifests").doc(uid).set({
      statement,
      timestamp: admin.firestore.FieldValue.serverTimestamp()
    });
    
    // Update onboarding progress for WISH creation (non-blocking)
    const onboardingUpdates = {
      hasCreatedWish: true
    };
    
    // Check if this is their first wish
    try {
      const userDoc = await admin.firestore().collection("users").doc(uid).get();
      if (userDoc.exists) {
        const userData = userDoc.data();
        const hasCreatedWishBefore = userData.onboardingState?.hasCreatedWish || false;
        
        if (!hasCreatedWishBefore) {
          onboardingUpdates['milestones.firstWish'] = admin.firestore.FieldValue.serverTimestamp();
          onboardingUpdates.currentMilestone = 'wish_creator';
        }
      }
    } catch (milestoneError) {
      console.warn('Failed to check first wish milestone:', milestoneError.message);
    }
    
    await updateOnboardingState(uid, onboardingUpdates);
    
    return { success: true };
  } catch (error) {
    console.error("Error saving manifest:", error);
    throw new HttpsError("internal", "Unable to save your manifest statement right now. Please try again.", { 
      code: 'SAVE_ERROR', 
      retryable: true 
    });
  }
});

// Load manifest statement for authenticated user
exports.loadManifest = onCall(async (data, context) => {
  const uid = context.auth?.uid;
  if (!uid) {
    throw new HttpsError("unauthenticated", "User must be authenticated.");
  }

  try {
    const doc = await admin.firestore().collection("manifests").doc(uid).get();
    if (!doc.exists) {
      return { statement: "" };
    }
    return { statement: doc.data().statement || "" };
  } catch (error) {
    console.error("Error loading manifest:", error);
    throw new HttpsError("internal", "Unable to load your manifest statement right now. Please try again.", { 
      code: 'LOAD_ERROR', 
      retryable: true 
    });
  }
});


// Ask Sophy to refine manifest statement
exports.refineManifest = onCall({ secrets: [ANTHROPIC_API_KEY, OPENAI_API_KEY] }, async (data, context) => {
  const uid = context.auth?.uid;
  if (!uid) {
    throw new HttpsError("unauthenticated", "User must be authenticated.");
  }

  const { statement } = data;
  if (!statement || typeof statement !== "string") {
    throw new HttpsError("invalid-argument", "Manifest statement must be a non-empty string.");
  }

  const prompt = `Please help refine this personal manifest statement to make it meaningful, clear, and inspiring:\n"${statement}"`;

  try {
    const requestId = generateRequestId();
    
    // Safe logging - don't log full statement content
    console.log(`[${requestId}] refineManifest - statement length: ${statement?.length || 0} chars`);

    const result = await callAnthropicWithRetry(
      {
        model: MODELS.FAST,
        role: 'FAST',
        max_tokens: 300,
        messages: [
          { role: "user", content: `You are a journaling assistant that helps users articulate their vision and purpose in a supportive, emotionally aware tone.\n\n${prompt}` }
        ]
      },
      "refineManifest",
      requestId
    );

    console.log(`[${requestId}] refineManifest success`);
    
    // Clean the response to remove any stage directions or narrative text
    let cleanedResult = result.content[0].text.trim();
    
    // Remove common stage direction patterns
    cleanedResult = cleanedResult
      .replace(/^\*[^*]*\*\s*/g, '') // Remove opening stage directions
      .replace(/\*[^*]*\*$/g, '') // Remove ending stage directions
      .replace(/\*[^*]*\*/g, '') // Remove any remaining stage directions
      .replace(/^(Sophy\s+)?(responds?|says?|speaks?|nods?|smiles?|looks?)\s+(warmly|empathetically|thoughtfully|gently|softly)[:\s]*/gi, '')
      .replace(/^Hello,?\s+\w+\.\s*/i, '') // Remove greeting patterns
      .replace(/^\*with\s+warmth\s+and\s+empathy\*\s*/gi, '') // Remove specific empathy stage direction
      .replace(/^\*[^*]*warmth[^*]*\*\s*/gi, '') // Remove warmth-related stage directions
      .replace(/^\*[^*]*empathy[^*]*\*\s*/gi, '') // Remove empathy-related stage directions
      .replace(/I\s+sense\s+there\s+is\s+an\s+important\s+wish/gi, '') // Remove specific AI prompt leakage
      .replace(/Let's\s+take\s+a\s+moment\s+to\s+vividly\s+imagine/gi, '') // Remove prompt instruction leakage
      .trim();
    
    return { refined: cleanedResult };
  } catch (error) {
    console.error("Manifest refinement failed:", error.message);
    const userError = mapErrorToUserMessage(error, 'refineManifest');
    throw new HttpsError("internal", userError.message, { 
      code: userError.code, 
      retryable: userError.retryable 
    });
  }
});

// Clean up rough voice transcript into readable text (HTTP endpoint with CORS)
exports.cleanVoiceTranscript = onRequest({ secrets: [ANTHROPIC_API_KEY, OPENAI_API_KEY] }, async (req, res) => {
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.status(204).send("");

  const transcript = req.body.transcript || req.body.rawText;
  console.log("🧾 Received raw transcript:", transcript);
  if (!transcript || typeof transcript !== "string" || transcript.trim().length < 2) {
    console.warn("⚠️ Invalid or too short transcript received.");
    return res.status(400).json({ error: "No cleaned text received." });
  }

  try {
    const requestId = generateRequestId();
    
    // Safe logging - don't log full transcript content
    console.log(`[${requestId}] cleanVoiceTranscript - transcript length: ${transcript?.length || 0} chars`);

    const data = await callAnthropicWithRetry(
      {
        model: MODELS.FAST,
        role: 'FAST',
        max_tokens: 300,
        messages: [
          {
            role: "user",
            content: `Clean this voice transcript by adding proper punctuation, capitalization, and fixing minor grammar errors. Keep the exact same words and natural speech patterns. Return ONLY the cleaned speech with no introductions, explanations, or commentary.

Examples:
Input: "trying ink out loud to see how it works my name is adam and im sitting at my desk"
Output: "Trying Ink Out Loud to see how it works. My name is Adam and I'm sitting at my desk."

Input: "today was really good i went to the store and bought some groceries then came home"
Output: "Today was really good. I went to the store and bought some groceries, then came home."

Transcript to clean:
${transcript}`
          }
        ]
      },
      "cleanVoiceTranscript",
      requestId
    );

    let cleanedText = data?.content?.[0]?.text?.trim();

    if (!cleanedText) {
      throw new Error("No cleaned text returned from AI.");
    }

    // Strip out common AI narrative introductions
    const narrativePrefixes = [
      /^Here is the transcript with punctuation and minor grammar corrections:\s*/i,
      /^Here is the cleaned transcript:\s*/i,
      /^Here's the cleaned version:\s*/i,
      /^The corrected transcript:\s*/i,
      /^Cleaned transcript:\s*/i,
      /^Here is the corrected version:\s*/i
    ];

    for (const pattern of narrativePrefixes) {
      cleanedText = cleanedText.replace(pattern, '');
    }

    // Remove any remaining quotes that might wrap the content
    cleanedText = cleanedText.replace(/^["']|["']$/g, '').trim();

    console.log(`[${requestId}] cleanVoiceTranscript success`);
    res.status(200).json({ cleanedText });
  } catch (error) {
    console.error("cleanVoiceTranscript error:", error.message);
    const userError = mapErrorToUserMessage(error, 'cleanVoiceTranscript');
    res.status(500).json({ 
      error: userError.message,
      code: userError.code,
      retryable: userError.retryable 
    });
  }
});

// Enhanced voice processing with emotional analysis
exports.processVoiceWithEmotion = onRequest({ 
  secrets: [ANTHROPIC_API_KEY, OPENAI_API_KEY],
  memory: "256MiB",
  timeoutSeconds: 60
}, async (req, res) => {
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  
  if (req.method === "OPTIONS") return res.status(204).send("");

  try {
    // Verify authentication
    const authHeader = req.headers.authorization || '';
    const idToken = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';
    
    if (!idToken) {
      return res.status(401).json({ error: 'No authorization token provided' });
    }

    const decodedToken = await admin.auth().verifyIdToken(idToken);
    const userId = decodedToken.uid;

    const transcript = req.body.transcript;
    const hasAudio = req.body.hasAudio || false;
    
    if (!transcript || typeof transcript !== 'string' || transcript.trim().length === 0) {
      console.warn('No valid transcript provided. Body:', JSON.stringify(req.body, null, 2));
      return res.status(400).json({ error: 'No transcript provided' });
    }

    console.log(`🎭 Processing voice with emotion for user ${userId}, transcript length: ${transcript.length}`);

    // Step 1: Clean the transcript (reuse existing logic)
    const cleanedText = await cleanTranscriptWithAI(transcript);
    
    // Step 2: Analyze emotional content from text
    const emotionalInsights = await analyzeTextEmotion(transcript, cleanedText, userId);
    
    // Step 3: Generate Sophy insight based on emotional context
    const sophyInsight = await generateEmotionalInsight(cleanedText, emotionalInsights, userId);
    
    // Update onboarding progress for voice entry completion (non-blocking)
    const onboardingUpdates = {
      hasCompletedVoiceEntry: true
    };
    
    // Check if this is their first voice entry
    try {
      const userDoc = await admin.firestore().collection("users").doc(userId).get();
      if (userDoc.exists) {
        const userData = userDoc.data();
        const hasUsedVoiceBefore = userData.onboardingState?.hasCompletedVoiceEntry || false;
        
        if (!hasUsedVoiceBefore) {
          onboardingUpdates['milestones.firstVoiceEntry'] = admin.firestore.FieldValue.serverTimestamp();
          onboardingUpdates.currentMilestone = 'voice_user';
        }
      }
    } catch (milestoneError) {
      console.warn('Failed to check first voice entry milestone:', milestoneError.message);
    }
    
    await updateOnboardingState(userId, onboardingUpdates);
    
    res.status(200).json({
      cleanedText: cleanedText,
      emotionalInsights: {
        ...emotionalInsights,
        sophyInsight: sophyInsight
      }
    });

  } catch (error) {
    console.error('Error processing voice with emotion:', error);
    res.status(500).json({ 
      error: 'Failed to process voice input',
      fallback: true // Signal frontend to use fallback
    });
  }
});

// Helper function to clean transcript (extracted from existing cleanVoiceTranscript)
async function cleanTranscriptWithAI(transcript) {
  const requestId = generateRequestId();
  
  const data = await callAnthropicWithRetry(
    {
      model: MODELS.FAST,
        role: 'FAST',
      max_tokens: 300,
      messages: [
        {
          role: "user",
          content: `Clean this voice transcript by adding proper punctuation, capitalization, and fixing minor grammar errors. Keep the exact same words and natural speech patterns. Return ONLY the cleaned speech with no introductions, explanations, or commentary.

Examples:
Input: "trying ink out loud to see how it works my name is adam and im sitting at my desk"
Output: "Trying Ink Out Loud to see how it works. My name is Adam and I'm sitting at my desk."

Input: "today was really good i went to the store and bought some groceries then came home"
Output: "Today was really good. I went to the store and bought some groceries, then came home."

Transcript to clean:
${transcript}`
        }
      ]
    },
    "cleanTranscriptWithAI",
    requestId
  );

  let cleanedText = data?.content?.[0]?.text?.trim();

  if (!cleanedText) {
    throw new Error("No cleaned text returned from AI.");
  }

  // Strip out common AI narrative introductions
  const narrativePrefixes = [
    /^Here is the transcript with punctuation and minor grammar corrections:\s*/i,
    /^Here is the cleaned transcript:\s*/i,
    /^Here's the cleaned version:\s*/i,
    /^The corrected transcript:\s*/i,
    /^Cleaned transcript:\s*/i,
    /^Here is the corrected version:\s*/i
  ];

  for (const pattern of narrativePrefixes) {
    cleanedText = cleanedText.replace(pattern, '');
  }

  // Remove any remaining quotes that might wrap the content
  cleanedText = cleanedText.replace(/^["']|["']$/g, '').trim();

  return cleanedText;
}

// Analyze emotional content from voice transcript
async function analyzeTextEmotion(transcript, cleanedText, userId) {
  const requestId = generateRequestId();

  const prompt = `Analyze the emotional content of this voice transcript. Focus on:
1. Primary emotion (joy, sadness, anger, fear, surprise, disgust, neutral)
2. Energy level (high, medium, low) 
3. Stress indicators (calm, mild tension, moderate stress, high stress)
4. Confidence level of analysis (0-100%)

Consider the raw speech patterns and word choices for emotional context.

Raw transcript: "${transcript}"
Cleaned text: "${cleanedText}"

Respond in JSON format only:
{
  "primaryEmotion": "emotion name",
  "confidence": number,
  "energyLevel": "high/medium/low", 
  "stressLevel": "calm/mild/moderate/high",
  "emotionalContext": "brief description"
}`;

  try {
    const data = await callAnthropicWithRetry(
      {
        model: MODELS.FAST,
        role: 'FAST',
        max_tokens: 300,
        messages: [{ role: 'user', content: prompt }]
      },
      "analyzeTextEmotion",
      requestId
    );

    const responseText = data.content[0].text.trim();
    
    // Try to parse JSON, with fallback
    let emotionalData;
    try {
      emotionalData = JSON.parse(responseText);
    } catch (parseError) {
      console.warn("Failed to parse emotional analysis JSON, using fallback");
      emotionalData = {
        primaryEmotion: "neutral",
        confidence: 50,
        energyLevel: "medium",
        stressLevel: "mild",
        emotionalContext: "Unable to analyze emotional content"
      };
    }

    return emotionalData;
  } catch (error) {
    console.error("Error in emotional analysis:", error);
    return {
      primaryEmotion: "neutral",
      confidence: 40,
      energyLevel: "medium", 
      stressLevel: "mild",
      emotionalContext: "Analysis temporarily unavailable"
    };
  }
}

// Generate Sophy insight based on emotional context
async function generateEmotionalInsight(text, emotionalData, userId) {
  const requestId = generateRequestId();

  const prompt = `You are Sophy, a supportive journaling assistant. Based on this voice journal entry and emotional analysis, provide a brief, caring insight (1-2 sentences max, under 100 words).

Journal text: "${text}"
Emotional analysis: Primary emotion is ${emotionalData.primaryEmotion} with ${emotionalData.confidence}% confidence. Energy level: ${emotionalData.energyLevel}. Stress level: ${emotionalData.stressLevel}.

Respond as Sophy would - warm, encouraging, and focused on the person's wellbeing. Be concise and meaningful. Don't repeat the analysis data, just offer gentle perspective or encouragement.`;

  try {
    const data = await callAnthropicWithRetry(
      {
        model: MODELS.FAST,
        role: 'FAST',
        max_tokens: 80,
        messages: [{ role: 'user', content: prompt }]
      },
      "generateEmotionalInsight",
      requestId
    );

    return data.content[0].text.trim();
  } catch (error) {
    console.error("Error generating emotional insight:", error);
    return "I hear you sharing something meaningful. Thank you for trusting me with your thoughts.";
  }
}

exports.embedAndStoreEntry = onRequest({ secrets: [ANTHROPIC_API_KEY, OPENAI_API_KEY] }, (req, res) => {
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    return res.status(204).send("");
  }
  
  corsHandler(req, res, async () => {
    console.log("Received body:", req.body);
    try {
      const authHeader = req.headers.authorization || '';
      const idToken = authHeader.startsWith('Bearer ') ? authHeader.split('Bearer ')[1] : null;

      if (!idToken) {
        return res.status(401).json({ error: 'Unauthorized: Missing token' });
      }

      const decoded = await admin.auth().verifyIdToken(idToken);
      const uid = decoded.uid;

      const { text, entryId } = req.body;
      if (!text || !entryId) {
        return res.status(400).json({ error: 'Missing required text or entryId' });
      }

      const requestId = generateRequestId();
      
      // Save entry without embedding (we'll use Anthropic for semantic search instead)
      console.log(`[${requestId}] embedAndStoreEntry - saving entry without embedding`);
      
      await admin.firestore().collection("journalEntries").doc(entryId).set({
        userId: uid,
        text,
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
        // Add some basic text processing for simple search fallback
        searchableText: text.toLowerCase()
      }, { merge: true });

      // Update onboarding progress (non-blocking)
      const onboardingUpdates = {
        totalEntries: admin.firestore.FieldValue.increment(1)
      };
      
      // Check if this might be their first entry
      try {
        const userDoc = await admin.firestore().collection("users").doc(uid).get();
        if (userDoc.exists) {
          const userData = userDoc.data();
          const currentEntries = userData.onboardingState?.totalEntries || 0;
          
          if (currentEntries === 0) {
            onboardingUpdates['milestones.firstEntry'] = admin.firestore.FieldValue.serverTimestamp();
            onboardingUpdates.currentMilestone = 'first_entry';
          } else if (currentEntries === 9) { // Will be 10 after increment
            onboardingUpdates['milestones.tenEntries'] = admin.firestore.FieldValue.serverTimestamp();
            onboardingUpdates.currentMilestone = 'active_journaler';
          }
        }
      } catch (milestoneError) {
        console.warn('Failed to check milestone status:', milestoneError.message);
      }
      
      await updateOnboardingState(uid, onboardingUpdates);

      console.log(`[${requestId}] embedAndStoreEntry success (text-based storage)`);
      res.status(200).json({ message: "Entry saved successfully" });
    } catch (error) {
      console.error("Entry storage error:", error.message);
      const userError = mapErrorToUserMessage(error, 'embedAndStoreEntry');
      res.status(500).json({ 
        error: userError.message,
        code: userError.code,
        retryable: userError.retryable 
      });
    }
  });
});

// New semantic search function using Anthropic
exports.semanticSearch = onRequest({ secrets: [ANTHROPIC_API_KEY, OPENAI_API_KEY] }, async (req, res) => {
  const requestId = generateRequestId();
  console.log(`[${requestId}] Semantic search request started`);
  
  // Apply hardened CORS
  if (!setupHardenedCORS(req, res)) {
    console.warn(`[${requestId}] Rejected request from unauthorized origin: ${req.headers.origin}`);
    return res.status(403).send('Forbidden');
  }
  
  if (req.method === 'OPTIONS') {
    return res.status(204).send('');
  }

  try {
    const authHeader = req.headers.authorization || '';
    const idToken = authHeader.startsWith('Bearer ') ? authHeader.split('Bearer ')[1] : null;

    if (!idToken) {
      console.warn(`[${requestId}] Missing authorization token`);
      return res.status(401).json({ error: 'Unauthorized: Missing token' });
    }

    const decoded = await admin.auth().verifyIdToken(idToken);
    const uid = decoded.uid;
    console.log(`[${requestId}] User authenticated: ${uid}`);

    const { query } = req.body;
    if (!query) {
      console.error(`[${requestId}] Missing search query`);
      return res.status(400).json({ error: 'Missing search query' });
    }

    console.log(`[${requestId}] semanticSearch - query length: ${query.length} chars`);

    // Fetch user's journal entries - use createdAt instead of timestamp
    const entriesSnapshot = await admin.firestore()
      .collection("journalEntries")
      .where("userId", "==", uid)
      .orderBy("createdAt", "desc")
      .limit(50) // Limit to recent entries for performance
      .get();

    if (entriesSnapshot.empty) {
      console.log(`[${requestId}] No entries found for user`);
      return res.status(200).json({ results: [] });
    }

    const entries = [];
    entriesSnapshot.forEach(doc => {
      const data = doc.data();
      if (data.text && data.text.trim()) {
        entries.push({
          id: doc.id,
          text: data.text,
          createdAt: data.createdAt,
          tags: data.tags || [],
          contextManifest: data.contextManifest,
          reflectionUsed: data.reflectionUsed
        });
      }
    });

    console.log(`[${requestId}] Found ${entries.length} entries to analyze`);

    if (entries.length === 0) {
      return res.status(200).json({ results: [] });
    }

    // Check if ANTHROPIC_API_KEY is available
    const hasApiKey = ANTHROPIC_API_KEY && ANTHROPIC_API_KEY.value();
    
    if (!hasApiKey) {
      console.warn(`[${requestId}] Anthropic API key not available, using fallback text search`);
      
      // Fallback to simple text search
      const queryLower = query.toLowerCase();
      const rankedResults = entries
        .map((entry, index) => ({
          ...entry,
          score: (entry.text.toLowerCase().includes(queryLower) ? 1 : 0) +
                 (entry.tags?.some(tag => tag.toLowerCase().includes(queryLower)) ? 0.5 : 0) +
                 (entry.contextManifest?.toLowerCase().includes(queryLower) ? 0.3 : 0)
        }))
        .filter(entry => entry.score > 0)
        .sort((a, b) => b.score - a.score)
        .slice(0, 10)
        .map(entry => {
          delete entry.score;
          return entry;
        });

      console.log(`[${requestId}] Fallback search returning ${rankedResults.length} results`);
      return res.status(200).json({ results: rankedResults });
    }

    // Use Anthropic to rank entries by semantic relevance
    const entriesText = entries.map((entry, index) => 
      `Entry ${index + 1}: ${entry.text.substring(0, 300)}${entry.text.length > 300 ? '...' : ''}`
    ).join('\n\n');

    const analysisPrompt = `You are helping with journal search. Given the search query and journal entries below, identify which entries are most semantically relevant to the query. Consider themes, emotions, topics, and concepts - not just keyword matches.

Search Query: "${query}"

Journal Entries:
${entriesText}

Please respond with ONLY a JSON array of entry numbers (1-${entries.length}) ranked by relevance, most relevant first. Include only entries that have meaningful relevance to the query. For example: [3, 7, 1, 12]

If no entries are meaningfully relevant, return an empty array: []`;

    const result = await callAnthropicWithRetry(
      {
        model: MODELS.FAST,
        role: 'FAST',
        max_tokens: 500,
        messages: [
          { role: "user", content: analysisPrompt }
        ]
      },
      "semanticSearch",
      requestId
    );

    const responseText = result.content[0].text.trim();
    console.log(`[${requestId}] Anthropic ranking response: ${responseText}`);

    // Parse the ranking response
    let rankedIndices = [];
    try {
      rankedIndices = JSON.parse(responseText);
      if (!Array.isArray(rankedIndices)) {
        throw new Error("Response is not an array");
      }
    } catch (parseError) {
      console.warn(`[${requestId}] Failed to parse ranking, falling back to text search`);
      // Fallback to simple text search
      const queryLower = query.toLowerCase();
      rankedIndices = entries
        .map((entry, index) => ({
          index: index + 1,
          score: (entry.text.toLowerCase().includes(queryLower) ? 1 : 0) +
                 (entry.tags?.some(tag => tag.toLowerCase().includes(queryLower)) ? 0.5 : 0)
        }))
        .filter(item => item.score > 0)
        .sort((a, b) => b.score - a.score)
        .map(item => item.index);
    }

    // Convert indices to actual entries and return results
    const rankedResults = rankedIndices
      .map(index => entries[index - 1])
      .filter(entry => entry) // Remove any invalid indices
      .slice(0, 10); // Return top 10 results

    console.log(`[${requestId}] semanticSearch success - returning ${rankedResults.length} results`);
    res.status(200).json({ results: rankedResults });

  } catch (error) {
    console.error(`[${requestId}] Semantic search error:`, {
      message: error.message,
      stack: error.stack,
      code: error.code
    });
    
    const userError = mapErrorToUserMessage(error, 'semanticSearch');
    res.status(500).json({ 
      error: userError.message,
      code: userError.code,
      retryable: userError.retryable 
    });
  }
});
// Log search query function
exports.logSearchQuery = onRequest(async (req, res) => {
  // CORS: Always set these headers FIRST!
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  // Preflight support
  if (req.method === 'OPTIONS') {
    return res.status(204).send('');
  }

  // Log the entry for debugging
  console.log("logSearchQuery called. Method:", req.method, "Body:", req.body);
  console.log("DEBUG req.headers:", req.headers);

  try {
    // Enforce POST only
    if (req.method !== 'POST') {
      console.warn("Method not allowed:", req.method);
      return res.status(405).json({ error: "Method not allowed" });
    }

    // Extract Bearer token
    const authHeader = req.headers.authorization || '';
    const idToken = authHeader.startsWith('Bearer ') ? authHeader.split('Bearer ')[1] : null;
    if (!idToken) {
      console.warn("Missing token in Authorization header.");
      return res.status(401).json({ error: 'Unauthorized: Missing token' });
    }
    let decoded;
    try {
      decoded = await admin.auth().verifyIdToken(idToken);
    } catch (e) {
      console.error("Failed to verify ID token:", e.message);
      return res.status(401).json({ error: 'Unauthorized: Invalid token' });
    }
    if (!decoded || !decoded.uid) {
      console.warn("Decoded token missing UID");
      return res.status(401).json({ error: 'Unauthorized: Invalid token' });
    }
    const uid = decoded.uid;

    // Validate input
    const { query } = req.body;
    if (!query || typeof query !== 'string') {
      console.warn("Query missing or not a string:", req.body);
      return res.status(400).json({ error: 'Query must be a non-empty string.' });
    }

    // Save to Firestore
    await admin.firestore().collection('searchLogs').add({
      userId: uid,
      query,
      timestamp: admin.firestore.FieldValue.serverTimestamp()
    });
    console.log("Logged search query for user:", uid, "query:", query);
    return res.status(200).json({ success: true });
  } catch (error) {
    console.error("Error logging search query:", error);
    // CORS headers already set above, don't need to repeat
    return res.status(500).json({ error: "Failed to log search query." });
  }
});

// HTTP function with explicit CORS handling for coach replies
exports.saveCoachReplyHTTP = onRequest({
  cors: true,
  secrets: [TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]
}, async (req, res) => {
  try {
    console.log("🔍 saveCoachReplyHTTP called with method:", req.method);
    console.log("🔍 Headers:", req.headers);
    console.log("🔍 Body:", req.body);

    // Handle preflight OPTIONS request
    if (req.method === 'OPTIONS') {
      res.set('Access-Control-Allow-Origin', '*');
      res.set('Access-Control-Allow-Methods', 'POST');
      res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
      res.status(204).send('');
      return;
    }

    // Only allow POST requests
    if (req.method !== 'POST') {
      res.status(405).json({ error: 'Method not allowed' });
      return;
    }

    // Get the ID token from the Authorization header
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      console.error("❌ No authorization header found");
      res.status(401).json({ error: 'Practitioner must be authenticated.' });
      return;
    }

    const idToken = authHeader.split('Bearer ')[1];
    console.log("🔑 ID token received:", idToken?.substring(0, 20) + "...");

    // Verify the ID token
    let decodedToken;
    try {
      decodedToken = await admin.auth().verifyIdToken(idToken);
      console.log("✅ Token verified for user:", decodedToken.uid);
    } catch (tokenError) {
      console.error("❌ Token verification failed:", tokenError);
      res.status(401).json({ error: 'Invalid authentication token.' });
      return;
    }

    const coachUid = decodedToken.uid;

    // Verify the user has coach role
    try {
      const userDoc = await admin.firestore().collection("users").doc(coachUid).get();
      console.log("📋 User document exists:", userDoc.exists);
      if (userDoc.exists) {
        const userData = userDoc.data();
        console.log("👤 User data:", {
          userRole: userData?.userRole,
          email: userData?.email
        });
      }
      
      if (!userDoc.exists || userDoc.data()?.userRole !== "coach") {
        console.error("❌ User does not have coach role");
        res.status(403).json({ error: 'User does not have practitioner permissions.' });
        return;
      }
    } catch (roleError) {
      console.error("❌ Error checking coach role:", roleError);
      res.status(500).json({ error: 'Unable to verify practitioner permissions.' });
      return;
    }

    console.log("✅ Coach role verified");

    const { entryId, replyText } = req.body;
    
    if (!entryId || !replyText || typeof replyText !== "string") {
      console.error("❌ Invalid data:", { entryId: !!entryId, replyText: !!replyText, replyTextType: typeof replyText });
      res.status(400).json({ error: 'Entry ID and reply text are required.' });
      return;
    }

    try {
      const replyRef = admin.firestore()
        .collection("journalEntries")
        .doc(entryId)
        .collection("coachReplies")
        .doc(coachUid);

      await replyRef.set({
        replyText,
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
        coachUid
      });

      await admin.firestore()
        .collection("journalEntries")
        .doc(entryId)
        .update({ newCoachReply: true });

      console.log("✅ Coach reply saved successfully");

      // Send notifications to user (SMS + Push)
      try {
        // Get the journal entry to find the user
        const entryDoc = await admin.firestore().collection("journalEntries").doc(entryId).get();
        if (entryDoc.exists) {
          const entryData = entryDoc.data();
          const userId = entryData.userId;
          
          // Get user's preferences
          const userDoc = await admin.firestore().collection("users").doc(userId).get();
          if (userDoc.exists) {
            const userData = userDoc.data();
            
            // Debug log user notification preferences
            console.log("📱 User notification settings:", {
              userId,
              fcmToken: userData.fcmToken ? "present" : "missing",
              pushEnabled: userData.pushPreferences?.enabled,
              pushCoachReplies: userData.pushPreferences?.coachReplies,
              smsOptIn: userData.smsOptIn,
              phoneNumber: userData.phoneNumber ? "present" : "missing",
              smsCoachReplies: userData.smsPreferences?.coachReplies
            });
            
            // Get coach's name
            const coachDoc = await admin.firestore().collection("users").doc(coachUid).get();
            const coachName = coachDoc.exists ? coachDoc.data().displayName || 'Your coach' : 'Your coach';
            
            // Send FCM Push Notification if user has it enabled
            const shouldSendPush = userData.fcmToken && userData.pushPreferences?.enabled && userData.pushPreferences?.coachReplies !== false;
            console.log("📲 Push notification decision:", { shouldSendPush, hasFcmToken: !!userData.fcmToken, pushEnabled: userData.pushPreferences?.enabled, coachReplies: userData.pushPreferences?.coachReplies });
            
            if (shouldSendPush) {
              try {
                const pushSent = await sendPushNotification(
                  userData.fcmToken,
                  '💬 New Coach Reply',
                  `${coachName} replied to your journal entry!`,
                  {
                    type: 'coach_reply',
                    entryId: entryId,
                    coachName: coachName,
                  },
                  { badge: 1 }  // Red dot for coach replies
                );
                if (pushSent) {
                  console.log(`✅ Push notification sent to user ${userId} for coach reply`);
                }
              } catch (pushError) {
                console.error("❌ Failed to send push notification (non-fatal):", pushError.message);
              }
            } else {
              console.log("⏭️ Skipping push notification - conditions not met");
            }
            
            // Send SMS if user has SMS enabled and wants coach reply notifications
            // Default coachReplies to true if not explicitly set to false
            const shouldSendSms = userData.smsOptIn && userData.phoneNumber && userData.smsPreferences?.coachReplies !== false;
            console.log("📱 SMS notification decision:", { shouldSendSms });
            
            if (shouldSendSms) {
              // Send SMS
              const twilio = require('twilio');
              const client = twilio(
                TWILIO_ACCOUNT_SID.value(),
                TWILIO_AUTH_TOKEN.value()
              );
              
              const messageText = `💬 InkWell: ${coachName} replied to your journal entry! Log in to read their message.\n\nReply STOP to unsubscribe`;
              
              await client.messages.create({
                body: messageText,
                from: TWILIO_PHONE_NUMBER.value(),
                to: userData.phoneNumber
              });
              
              console.log(`✅ Practitioner reply SMS sent to user ${userId}`);
            } else {
              console.log("⏭️ Skipping SMS - conditions not met");
            }
          } else {
            console.log("⚠️ User document not found for notifications:", userId);
          }
        } else {
          console.log("⚠️ Journal entry not found for notifications:", entryId);
        }
      } catch (notifyError) {
        // Don't fail the whole operation if notifications fail
        console.error("❌ Failed to send notifications (non-fatal):", notifyError);
      }

      res.status(200).json({ success: true });
    } catch (error) {
      console.error("❌ Error saving coach reply:", error);
      res.status(500).json({ 
        error: 'Unable to save the coach reply right now. Please try again.',
        code: 'SAVE_ERROR',
        retryable: true 
      });
    }
  } catch (error) {
    console.error("❌ Unexpected error in saveCoachReplyHTTP:", error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Keep the original callable function as backup
exports.saveCoachReply = onCall({
  cors: true
}, async (data, context) => {
  console.log("🔍 saveCoachReply called with:", {
    hasAuth: !!context.auth,
    uid: context.auth?.uid,
    dataKeys: Object.keys(data || {}),
    hasEntryId: !!data?.entryId,
    hasReplyText: !!data?.replyText
  });

  const coachUid = context.auth?.uid;
  if (!coachUid) {
    console.error("❌ No authenticated user found");
    throw new HttpsError("unauthenticated", "Coach must be authenticated.");
  }

  console.log("✅ User authenticated, checking coach role for UID:", coachUid);

  // Verify the user has coach role
  try {
    const userDoc = await admin.firestore().collection("users").doc(coachUid).get();
    console.log("📋 User document exists:", userDoc.exists);
    if (userDoc.exists) {
      const userData = userDoc.data();
      console.log("👤 User data:", {
        userRole: userData?.userRole,
        email: userData?.email
      });
    }
    
    if (!userDoc.exists || userDoc.data()?.userRole !== "coach") {
      console.error("❌ User does not have coach role");
      throw new HttpsError("permission-denied", "User does not have coach permissions.");
    }
  } catch (roleError) {
    console.error("❌ Error checking coach role:", roleError);
    if (roleError.code === "permission-denied") {
      throw roleError;
    }
    throw new HttpsError("internal", "Unable to verify coach permissions.");
  }

  console.log("✅ Coach role verified");

  const { entryId, replyText } = data;
  if (!entryId || !replyText || typeof replyText !== "string") {
    console.error("❌ Invalid data:", { entryId: !!entryId, replyText: !!replyText, replyTextType: typeof replyText });
    throw new HttpsError("invalid-argument", "Entry ID and reply text are required.");
  }

  try {
    const replyRef = admin.firestore()
      .collection("journalEntries")
      .doc(entryId)
      .collection("coachReplies")
      .doc(coachUid);

    await replyRef.set({
      replyText,
      timestamp: admin.firestore.FieldValue.serverTimestamp(),
      coachUid
    });

    await admin.firestore()
      .collection("journalEntries")
      .doc(entryId)
      .update({ newCoachReply: true });

    console.log("✅ Coach reply saved successfully");
    return { success: true };
  } catch (error) {
    console.error("❌ Error saving coach reply:", error);
    throw new HttpsError("internal", "Unable to save the coach reply right now. Please try again.", { 
      code: 'SAVE_ERROR', 
      retryable: true 
    });
  }
});

// Mark coach replies as read
exports.markCoachRepliesAsRead = onCall({
  cors: true
}, async (data, context) => {
  const userId = context.auth?.uid;
  if (!userId) {
    throw new HttpsError("unauthenticated", "User must be authenticated.");
  }

  const { entryId } = data;
  if (!entryId) {
    throw new HttpsError("invalid-argument", "Entry ID is required.");
  }

  try {
    // Verify the entry belongs to the authenticated user
    const entryRef = admin.firestore().collection("journalEntries").doc(entryId);
    const entrySnap = await entryRef.get();
    
    if (!entrySnap.exists) {
      throw new HttpsError("not-found", "Journal entry not found.");
    }
    
    const entryData = entrySnap.data();
    if (entryData.userId !== userId) {
      throw new HttpsError("permission-denied", "You can only mark your own entries as read.");
    }

    // Clear the newCoachReply flag
    await entryRef.update({ newCoachReply: false });
    
    console.log(`✅ Marked coach replies as read for entry ${entryId} by user ${userId}`);
    return { success: true };
  } catch (error) {
    console.error("❌ Error marking coach replies as read:", error);
    throw new HttpsError("internal", "Unable to mark replies as read. Please try again.", { 
      code: 'MARK_READ_ERROR', 
      retryable: true 
    });
  }
});


exports.notifyCoachOfTaggedEntry = onRequest({ secrets: [SENDGRID_API_KEY] }, async (req, res) => {
  const requestId = generateRequestId();
  console.log(`[${requestId}] Coach notification request started`);
  
  // Apply hardened CORS
  if (!setupHardenedCORS(req, res)) {
    console.warn(`[${requestId}] Rejected request from unauthorized origin: ${req.headers.origin}`);
    return res.status(403).send('Forbidden');
  }
  
  if (req.method === 'OPTIONS') {
    return res.status(204).send('');
  }

  if (req.method !== 'POST') {
    return sendSecureErrorResponse(res, 405, 'Method not allowed');
  }

  try {
    // Verify authentication
    const authHeader = req.headers.authorization;
    let authenticatedUserId = null;
    
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      console.warn(`[${requestId}] Missing or invalid authorization header`);
      return sendSecureErrorResponse(res, 401, 'Authentication required');
    }
    
    try {
      const idToken = authHeader.split('Bearer ')[1];
      const decodedToken = await admin.auth().verifyIdToken(idToken);
      authenticatedUserId = decodedToken.uid;
      console.log(`[${requestId}] User authenticated: ${authenticatedUserId}`);
    } catch (authError) {
      console.error(`[${requestId}] Authentication failed:`, authError.message);
      return sendSecureErrorResponse(res, 401, 'Invalid authentication token');
    }

    // Validate SendGrid API key (more lenient for local development)
    const apiKey = SENDGRID_API_KEY.value();
    if (!apiKey) {
      console.error(`[${requestId}] SendGrid API key is missing - this is expected in local development`);
      
      // In local development, simulate success for testing purposes
      if (process.env.NODE_ENV !== 'production' && (req.headers.host?.includes('localhost') || req.headers.host?.includes('127.0.0.1'))) {
        console.log(`[${requestId}] Local development mode - simulating successful email send`);
        
        const { entryId } = req.body || {};
        if (entryId) {
          // Still update the entry to mark as notified
          await admin.firestore().collection("journalEntries").doc(entryId).update({
            coachNotifiedAt: admin.firestore.FieldValue.serverTimestamp()
          });
        }
        
        return res.status(200).json({ message: "Coach notified successfully (simulated in local dev)" });
      }
      
      return sendSecureErrorResponse(res, 500, 'Email service configuration error');
    }
    
    if (!apiKey.startsWith("SG.")) {
      console.error(`[${requestId}] SendGrid API key format is invalid`);
      return sendSecureErrorResponse(res, 500, 'Email service configuration error');
    }

    sgMail.setApiKey(apiKey);
    console.log(`[${requestId}] SendGrid API key configured`);

    const { entryId, userId } = req.body || {};
    console.log(`[${requestId}] Payload received:`, { entryId, userId });

    if (!entryId) {
      console.error(`[${requestId}] Missing entryId for journal entry notification`);
      return sendSecureErrorResponse(res, 400, 'Missing entry ID');
    }

    // Verify user owns the entry or is authorized
    if (userId && userId !== authenticatedUserId) {
      console.warn(`[${requestId}] User ${authenticatedUserId} attempted to notify for entry owned by ${userId}`);
      return sendSecureErrorResponse(res, 403, 'Not authorized to notify for this entry');
    }

    const timestampNote = `<p style="font-size:0.85em; color:#777;">This message was sent at: ${new Date().toLocaleString()}</p>`;

    // Load the journal entry
    const entryDoc = await admin.firestore().collection("journalEntries").doc(entryId).get();
    if (!entryDoc.exists) {
      console.error(`[${requestId}] Entry not found in Firestore for ID: ${entryId}`);
      return sendSecureErrorResponse(res, 404, 'Entry not found');
    }

    const entry = entryDoc.data();
    
    // Verify the entry belongs to the authenticated user
    if (entry.userId !== authenticatedUserId) {
      console.warn(`[${requestId}] Entry ${entryId} belongs to ${entry.userId}, not ${authenticatedUserId}`);
      return sendSecureErrorResponse(res, 403, 'Not authorized to notify for this entry');
    }
    
    // Look up the user's connected practitioner
    const userDoc = await admin.firestore().collection('users').doc(authenticatedUserId).get();
    const userData = userDoc.data();
    
    let coachEmail = null;
    let coachName = 'Coach';
    
    // Check for connectedPractitioner (new format)
    if (userData?.connectedPractitioner?.email) {
      coachEmail = userData.connectedPractitioner.email;
      coachName = userData.connectedPractitioner.name || 'Coach';
      console.log(`[${requestId}] Found connectedPractitioner: ${coachEmail}`);
    } 
    // Fallback: check legacy practitioners array
    else if (userData?.practitioners && userData.practitioners.length > 0) {
      const practitionerId = userData.practitioners[0];
      const practDoc = await admin.firestore().collection('users').doc(practitionerId).get();
      if (practDoc.exists) {
        coachEmail = practDoc.data()?.email;
        coachName = practDoc.data()?.displayName || 'Coach';
        console.log(`[${requestId}] Found practitioner from array: ${coachEmail}`);
      }
    }
    
    if (!coachEmail) {
      console.warn(`[${requestId}] No connected practitioner found for user ${authenticatedUserId}`);
      return sendSecureErrorResponse(res, 400, 'No coach connected. Please connect to a coach in Settings first.');
    }
    
    // Check throttling - don't send duplicate notifications
    const lastNotified = entry?.coachNotifiedAt?.toDate?.();
    if (lastNotified && Date.now() - lastNotified.getTime() < 10 * 60 * 1000) {
      console.warn(`[${requestId}] Email already sent recently. Skipping notification.`);
      return res.status(200).json({ message: "Already notified recently" });
    }

    const dateStr = entry.createdAt?.toDate?.().toLocaleString?.() || "Unknown date";
    const manifest = entry.contextManifest || "";
    const entryText = entry.text?.substring(0, 1000) || "(No content)";

    const msg = {
      to: coachEmail,
      from: "support@inkwelljournal.io",
      subject: "New Journal Entry Tagged for Coach Review",
      text: `Hi,

A new journal entry was tagged for your review on ${dateStr}.

${manifest ? `Manifest: ${manifest}\n\n` : ""}Entry Preview:

${entryText}

Reply: https://inkwelljournal.io/coach.html?entryId=${entryId}

– InkWell by Pegasus Realm`,
      html: `
        <p><strong>Hi,</strong></p>
        <p>A new entry has been tagged for your review on <strong>${dateStr}</strong>.</p>
        ${manifest ? `<p><strong>Manifest:</strong> ${manifest}</p>` : ""}
        <p><strong>Journal Entry Preview:</strong></p>
        <blockquote style="background:#f9f9f9;padding:1em;border-left:4px solid #FFA76D;">
          ${(entryText || "").replace(/\n/g, "<br/>")}
        </blockquote>
        <p><a href="https://inkwelljournal.io/coach.html?entryId=${entryId}">Click here to reply</a></p>
        ${timestampNote}
        <hr/>
        <p style="font-size:0.9em;color:#777;">
          InkWell by Pegasus Realm • <a href="mailto:support@inkwelljournal.io">support@inkwelljournal.io</a>
        </p>
      `,
    };

    try {
      await sgMail.send(msg);
      console.log(`[${requestId}] Email sent successfully to: ${coachEmail}`);
    } catch (sendError) {
      console.error(`[${requestId}] SendGrid email failed:`, {
        message: sendError.message,
        code: sendError.code,
        response: sendError.response?.body
      });
      
      // Check if it's a billing/credits issue
      const errorBody = sendError.response?.body;
      const isCreditsIssue = errorBody && (
        JSON.stringify(errorBody).includes('billing') ||
        JSON.stringify(errorBody).includes('credit') ||
        JSON.stringify(errorBody).includes('quota') ||
        JSON.stringify(errorBody).includes('limit')
      );
      
      if (isCreditsIssue) {
        console.warn(`[${requestId}] SendGrid billing/credits issue - marking entry but not sending email`);
        
        // Still update the entry to prevent repeated attempts
        await admin.firestore().collection("journalEntries").doc(entryId).update({
          coachNotifiedAt: admin.firestore.FieldValue.serverTimestamp(),
          coachNotificationStatus: 'pending_billing_resolution'
        });
        
        // Return success to user but log the issue
        console.log(`[${requestId}] Coach notification marked as pending due to billing issue`);
        return res.status(200).json({ 
          message: "Entry saved successfully. Coach notification will be sent once service is restored.",
          status: "pending"
        });
      }
      
      return sendSecureErrorResponse(res, 502, 'Email service temporarily unavailable', sendError);
    }

    // Update entry with notification timestamp
    await admin.firestore().collection("journalEntries").doc(entryId).update({
      coachNotifiedAt: admin.firestore.FieldValue.serverTimestamp()
    });

    console.log(`[${requestId}] Coach notification completed successfully`);
    return res.status(200).json({ message: "Coach notified successfully" });
    
  } catch (err) {
    console.error(`[${requestId}] Coach notification failed:`, {
      message: err.message,
      stack: err.stack,
      code: err.code
    });
    
    // Return appropriate error based on error type
    if (err.message.includes('auth')) {
      return sendSecureErrorResponse(res, 401, 'Authentication failed', err);
    } else if (err.message.includes('not found')) {
      return sendSecureErrorResponse(res, 404, 'Entry not found', err);
    } else if (err.message.includes('SendGrid') || err.message.includes('email')) {
      return sendSecureErrorResponse(res, 502, 'Email service temporarily unavailable', err);
    } else {
      return sendSecureErrorResponse(res, 500, 'Failed to notify coach', err);
    }
  }
});


// Create user profile if not exists (callable)
exports.createUserProfile = onCall(async (data, context) => {
  const uid = context.auth?.uid;
  if (!uid) {
    throw new HttpsError("unauthenticated", "User must be authenticated.");
  }
  const { email } = data;
  if (!email || typeof email !== "string") {
    throw new HttpsError("invalid-argument", "Email is required.");
  }
  try {
    const result = await createUserProfileIfNotExists(uid, email);
    
    // Check if user had requested account deletion and cancel it
    const userDoc = await admin.firestore().collection('users').doc(uid).get();
    if (userDoc.exists) {
      const userData = userDoc.data();
      if (userData.deletionRequested) {
        console.log(`🔄 Canceling account deletion for user ${uid} - user logged back in`);
        await admin.firestore().collection('users').doc(uid).update({
          deletionRequested: admin.firestore.FieldValue.delete(),
          deletionScheduledFor: admin.firestore.FieldValue.delete(),
          deletionCanceledAt: admin.firestore.FieldValue.serverTimestamp()
        });
        console.log(`✅ Account deletion canceled for user ${uid}`);
      }
    }
    
    return { success: true, created: result.created };
  } catch (error) {
    console.error("Error creating user profile:", error);
    throw new HttpsError("internal", "Unable to create your profile right now. Please try again.", { 
      code: 'PROFILE_ERROR', 
      retryable: true 
    });
  }
});

/**
 * Cancel Account Deletion (callable)
 * Allows users to cancel their deletion request before the 30-day period expires
 */
exports.cancelAccountDeletion = onCall(async (data, context) => {
  const uid = context.auth?.uid;
  if (!uid) {
    throw new HttpsError("unauthenticated", "User must be authenticated.");
  }
  
  try {
    const userRef = admin.firestore().collection('users').doc(uid);
    const userDoc = await userRef.get();
    
    if (!userDoc.exists) {
      throw new HttpsError("not-found", "User profile not found.");
    }
    
    const userData = userDoc.data();
    
    if (!userData.deletionRequested) {
      return { success: true, message: "No deletion request to cancel." };
    }
    
    // Cancel the deletion
    await userRef.update({
      deletionRequested: admin.firestore.FieldValue.delete(),
      deletionScheduledFor: admin.firestore.FieldValue.delete(),
      deletionCanceledAt: admin.firestore.FieldValue.serverTimestamp()
    });
    
    console.log(`✅ Account deletion canceled for user ${uid}`);
    
    return { 
      success: true, 
      message: "Account deletion has been canceled successfully." 
    };
    
  } catch (error) {
    console.error("Error canceling account deletion:", error);
    throw new HttpsError("internal", "Failed to cancel account deletion. Please try again.");
  }
});

// Verify reCAPTCHA token (callable)
exports.verifyRecaptcha = onCall({ secrets: [RECAPTCHA_SECRET_KEY] }, async (request) => {
  console.log("🔐 verifyRecaptcha called with data:", JSON.stringify(request.data));
  const { token } = request.data;
  
  if (!token) {
    console.error("❌ No token provided in request.data:", request.data);
    throw new HttpsError("invalid-argument", "reCAPTCHA token is required.");
  }

  try {
    console.log("🌐 Making request to Google reCAPTCHA API...");
    
    const response = await fetch("https://www.google.com/recaptcha/api/siteverify", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: `secret=${RECAPTCHA_SECRET_KEY.value()}&response=${token}`,
    });

    console.log("📡 Got response from Google, status:", response.status);
    
    if (!response.ok) {
      throw new Error(`Google API returned status ${response.status}`);
    }

    const result = await response.json();
    console.log("🔍 Google reCAPTCHA API response:", result);
    
    if (!result.success) {
      console.warn("❌ reCAPTCHA verification failed:", result["error-codes"]);
      throw new HttpsError("permission-denied", `reCAPTCHA verification failed: ${result["error-codes"]?.join(", ") || "Unknown error"}`);
    }

    console.log("✅ reCAPTCHA verification successful");
    return { success: true };
    
  } catch (error) {
    console.error("❌ reCAPTCHA verification error:", error);
    
    // If it's already an HttpsError, re-throw it
    if (error.code) {
      throw error;
    }
    
    // Otherwise wrap it as an internal error
    throw new HttpsError("internal", `reCAPTCHA verification service error: ${error.message}`);
  }
});

// =============================================================================
// PRACTITIONER INVITATION VALIDATION
// =============================================================================

/**
 * Validate a practitioner invitation token
 * This replaces direct Firestore reads for security - prevents enumeration attacks
 * Returns only the data needed for registration, not sensitive details
 */
exports.validateInvitation = onRequest(async (req, res) => {
  // Set CORS headers
  const origin = req.headers.origin;
  const allowedOrigins = [
    'https://inkwelljournal.io',
    'https://www.inkwelljournal.io',
    'http://localhost:5000',
    'http://127.0.0.1:5000'
  ];
  
  if (allowedOrigins.includes(origin)) {
    res.set('Access-Control-Allow-Origin', origin);
  } else {
    res.set('Access-Control-Allow-Origin', 'https://inkwelljournal.io');
  }
  
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type');
  
  if (req.method === 'OPTIONS') {
    return res.status(204).send('');
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const { token } = req.body;
    
    if (!token || typeof token !== 'string' || token.length < 20) {
      return res.status(400).json({ error: 'Invalid invitation token' });
    }

    // Look up the invitation
    const inviteDoc = await admin.firestore().collection('practitionerInvitations').doc(token).get();
    
    if (!inviteDoc.exists) {
      // Don't reveal whether token exists or is invalid format
      return res.status(404).json({ error: 'Invitation not found or expired' });
    }
    
    const inviteData = inviteDoc.data();
    
    // Check if invitation is still valid
    if (inviteData.status !== 'pending') {
      return res.status(400).json({ error: 'This invitation has already been used or is no longer valid' });
    }
    
    // Check expiration (30 days from creation)
    if (inviteData.expiresAt && inviteData.expiresAt.toDate() < new Date()) {
      return res.status(400).json({ error: 'This invitation has expired. Please request a new one.' });
    }

    // Return only the safe, non-sensitive data needed for registration
    res.json({
      success: true,
      invitation: {
        fromUserName: inviteData.fromUserName || 'InkWell User',
        practitionerName: inviteData.practitionerName || '',
        practitionerEmail: inviteData.practitionerEmail || '',
        status: inviteData.status
      }
    });

  } catch (error) {
    console.error('❌ Error validating invitation:', error);
    res.status(500).json({ error: 'Failed to validate invitation' });
  }
});

// Send practitioner invitation email
exports.sendPractitionerInvitation = onRequest({ secrets: [SENDGRID_API_KEY] }, async (req, res) => {
  // Set CORS headers for both domains
  const origin = req.headers.origin;
  const allowedOrigins = [
    'https://inkwelljournal.io',
    'https://www.inkwelljournal.io',
    'http://localhost:5000',
    'http://127.0.0.1:5000'
  ];
  
  if (allowedOrigins.includes(origin)) {
    res.set('Access-Control-Allow-Origin', origin);
  } else {
    res.set('Access-Control-Allow-Origin', '*');
  }
  
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  
  if (req.method === 'OPTIONS') {
    return res.status(204).send('');
  }

  try {
    // Verify authentication
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'Authentication required' });
    }

    const idToken = authHeader.split('Bearer ')[1];
    const decodedToken = await admin.auth().verifyIdToken(idToken);
    const userId = decodedToken.uid;

    // Get user info
    const userDoc = await admin.firestore().collection("users").doc(userId).get();
    if (!userDoc.exists) {
      return res.status(404).json({ error: 'User not found' });
    }

    const userData = userDoc.data();
    const userName = userData.signupUsername || userData.displayName || userData.email || 'InkWell User';

    const apiKey = SENDGRID_API_KEY.value();
    sgMail.setApiKey(apiKey);

    const { practitionerEmail, practitionerName } = req.body;
    
    console.log('📧 Coach invitation request received:', { practitionerEmail, practitionerName, fromUser: userName });

    // Create unique invitation token
    const invitationToken = Math.random().toString(36).substring(2, 15) + 
                           Math.random().toString(36).substring(2, 15);

    const registrationUrl = `https://inkwelljournal.io/practitioner-register.html?token=${invitationToken}`;

    const emailContent = {
      to: practitionerEmail,
      from: "support@inkwelljournal.io",
      subject: `${userName} has invited you to InkWell - Wellness Journaling Platform`,
      html: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
          <div style="text-align: center; margin-bottom: 30px;">
            <img src="https://inkwelljournal.io/InkWell-Logo.png" alt="InkWell" style="max-width: 200px;">
          </div>
          
          <h2 style="color: #2A6972; text-align: center;">You've Been Invited to InkWell</h2>
          
          <p style="font-size: 16px; line-height: 1.6;">Hello ${practitionerName},</p>
          
          <p style="font-size: 16px; line-height: 1.6;">
            <strong>${userName}</strong> (${userData.email}) has invited you to join InkWell as their coach. 
            InkWell is a wellness journaling platform that connects clients with their coaches.
          </p>
          
          <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #2A6972;">
            <h3 style="margin-top: 0; color: #2A6972;">What is InkWell?</h3>
            <ul style="margin: 10px 0;">
              <li>Evidence-based journaling & manifesting platform for wellness and personal growth</li>
              <li>Secure communication between clients and coaches</li>
              <li>Custom built wellness and growth AI-assisted reflection tools (Sophy) to support clients</li>
              <li>Built by wellness professionals for wellness professionals</li>
            </ul>
          </div>
          
          <p style="font-size: 16px; line-height: 1.6;">
            To get started and connect with ${userName}, please complete your coach registration:
          </p>
          
          <div style="text-align: center; margin: 30px 0;">
            <a href="${registrationUrl}" 
               style="background: #2A6972; color: white; padding: 15px 30px; text-decoration: none; border-radius: 5px; font-weight: bold; display: inline-block;">
              Complete Registration
            </a>
          </div>
          
          <p style="font-size: 14px; color: #666; line-height: 1.5;">
            This invitation will expire in 30 days. If you have any questions about InkWell or need support, 
            please contact us at <a href="mailto:support@inkwelljournal.io">support@inkwelljournal.io</a>.
          </p>
          
          <hr style="margin: 30px 0; border: none; border-top: 1px solid #eee;">
          
          <p style="font-size: 12px; color: #999; text-align: center;">
            InkWell by Pegasus Realm LLC<br>
            Wellness Journaling Platform<br>
            <a href="https://www.inkwelljournal.io">inkwelljournal.io</a>
          </p>
        </div>
      `
    };

    console.log('📤 Sending email via SendGrid to:', practitionerEmail);
    
    try {
      await sgMail.send(emailContent);
      console.log('✅ Coach invitation email sent successfully to:', practitionerEmail);
    } catch (sendError) {
      console.error('❌ SendGrid email failed:', sendError.message);
      if (sendError.response) {
        console.error('❌ SendGrid response body:', sendError.response.body);
      }
      throw sendError; // Re-throw to trigger error response
    }
    
    // Calculate expiration date (30 days from now)
    const expirationDate = new Date();
    expirationDate.setDate(expirationDate.getDate() + 30);
    
    // Only save to Firestore AFTER email succeeds
    await admin.firestore().collection("practitionerInvitations").doc(invitationToken).set({
      fromUserId: userId,
      fromUserName: userName,
      fromUserEmail: userData.email,
      practitionerEmail: practitionerEmail,
      practitionerName: practitionerName,
      status: 'pending',
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      expiresAt: expirationDate
    });
    console.log('💾 Invitation saved to Firestore with token:', invitationToken, 'expires:', expirationDate);

    res.json({ success: true, message: 'Invitation sent successfully' });

  } catch (error) {
    console.error('❌ Error sending practitioner invitation:', error);
    res.status(500).json({ error: 'Failed to send invitation: ' + error.message });
  }
});

// Send notification email when user expresses practitioner interest during signup
exports.sendPractitionerInquiryNotification = onRequest(
  { 
    cors: true,
    secrets: [SENDGRID_API_KEY] 
  }, 
  async (req, res) => {
    try {
      console.log('📧 Practitioner inquiry notification triggered');
      
      // Handle CORS preflight
      if (req.method === 'OPTIONS') {
        res.set('Access-Control-Allow-Origin', '*');
        res.set('Access-Control-Allow-Methods', 'POST');
        res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
        res.status(204).send('');
        return;
      }

      const { userName, userEmail, userId } = req.body;

      if (!userName || !userEmail || !userId) {
        return res.status(400).json({ error: 'Missing required fields' });
      }

      const sgMail = require('@sendgrid/mail');
      sgMail.setApiKey(SENDGRID_API_KEY.value());

      const adminDashboardUrl = 'https://inkwelljournal.io/admin.html';

      const emailContent = {
        to: 'support@inkwelljournal.io',
        from: 'noreply@inkwelljournal.io',
        subject: `🆕 New Practitioner Inquiry: ${userName}`,
        html: `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; background: #f8f9fa; border-radius: 8px;">
            <div style="background: linear-gradient(135deg, #2A6972 0%, #1e5055 100%); padding: 20px; border-radius: 8px 8px 0 0; text-align: center;">
              <h1 style="color: white; margin: 0; font-size: 24px;">🆕 New Practitioner Inquiry</h1>
            </div>
            
            <div style="background: white; padding: 30px; border-radius: 0 0 8px 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
              <p style="font-size: 16px; color: #333; margin-bottom: 20px;">
                A new user has expressed interest in becoming an InkWell Practitioner during signup:
              </p>
              
              <div style="background: #f0f8ff; border-left: 4px solid #2A6972; padding: 15px; margin: 20px 0; border-radius: 4px;">
                <p style="margin: 8px 0; color: #0D3F45;"><strong>Name:</strong> ${userName}</p>
                <p style="margin: 8px 0; color: #0D3F45;"><strong>Email:</strong> ${userEmail}</p>
                <p style="margin: 8px 0; color: #0D3F45;"><strong>User ID:</strong> ${userId}</p>
                <p style="margin: 8px 0; color: #0D3F45;"><strong>Inquiry Date:</strong> ${new Date().toLocaleString('en-US', { timeZone: 'America/New_York' })} EST</p>
              </div>
              
              <div style="background: #fff8e1; border-left: 4px solid #ffc107; padding: 15px; margin: 20px 0; border-radius: 4px;">
                <p style="margin: 0; color: #856404; font-size: 14px;">
                  <strong>📋 Status:</strong> This inquiry has been saved to the <code style="background: #ffe082; padding: 2px 6px; border-radius: 3px;">practitionerRequests</code> collection with status: <strong>inquiry</strong>
                </p>
              </div>
              
              <h3 style="color: #2A6972; margin-top: 30px;">Next Steps:</h3>
              <ol style="color: #333; line-height: 1.8;">
                <li>Review the inquiry in your admin dashboard</li>
                <li>Assess whether the applicant is a good fit for InkWell</li>
                <li>Send a practitioner invitation link if approved</li>
              </ol>
              
              <div style="text-align: center; margin: 30px 0;">
                <a href="${adminDashboardUrl}" 
                   style="display: inline-block; background: #2A6972; color: white; padding: 14px 28px; text-decoration: none; border-radius: 8px; font-weight: 600; font-size: 16px; box-shadow: 0 2px 8px rgba(42,105,114,0.3);">
                  🔗 Open Admin Dashboard
                </a>
              </div>
              
              <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 30px 0;">
              
              <p style="font-size: 13px; color: #666; text-align: center; margin: 0;">
                This is an automated notification from InkWell<br>
                <a href="https://inkwelljournal.io" style="color: #2A6972;">inkwelljournal.io</a>
              </p>
            </div>
          </div>
        `
      };

      await sgMail.send(emailContent);
      console.log('✅ Practitioner inquiry notification sent to support@inkwelljournal.io');

      res.json({ success: true, message: 'Notification sent successfully' });

    } catch (error) {
      console.error('❌ Error sending practitioner inquiry notification:', error);
      res.status(500).json({ error: 'Failed to send notification', details: error.message });
    }
  }
);

// File upload function to handle attachments
exports.uploadFile = onRequest(async (req, res) => {
  corsHandler(req, res, async () => {
    try {
      // Check authentication
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Unauthorized: Missing or invalid token' });
      }

      const idToken = authHeader.split('Bearer ')[1];
      const decodedToken = await admin.auth().verifyIdToken(idToken);
      const userId = decodedToken.uid;

      if (req.method !== 'POST') {
        return res.status(405).json({ error: 'Method not allowed' });
      }

      // Handle multipart form data for file upload
      const busboy = require('busboy');
      const bb = busboy({ headers: req.headers });
      
      let fileData = null;
      let fileName = null;
      let fileType = null;

      bb.on('file', (name, file, info) => {
        fileName = info.filename;
        fileType = info.mimeType;
        const chunks = [];
        
        file.on('data', (data) => {
          chunks.push(data);
        });
        
        file.on('end', () => {
          fileData = Buffer.concat(chunks);
        });
      });

      bb.on('finish', async () => {
        try {
          if (!fileData || !fileName) {
            return res.status(400).json({ error: 'No file uploaded' });
          }

          // Upload to Firebase Storage
          const bucket = admin.storage().bucket();
          const uniqueFileName = `attachments/${Date.now()}_${userId}_${fileName}`;
          const file = bucket.file(uniqueFileName);
          
          await file.save(fileData, {
            metadata: {
              contentType: fileType,
              metadata: {
                uploadedBy: userId,
                originalName: fileName
              }
            }
          });

          // Make file publicly readable (adjust based on your security needs)
          await file.makePublic();
          
          const publicUrl = `https://storage.googleapis.com/${bucket.name}/${uniqueFileName}`;
          
          res.status(200).json({
            success: true,
            url: publicUrl,
            name: fileName
          });
          
        } catch (uploadError) {
          console.error('File upload error:', uploadError);
          res.status(500).json({ error: 'File upload failed' });
        }
      });

      bb.end(req.body);
      
    } catch (error) {
      console.error('Upload function error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }

    // NEW: Notify admin when practitioner registers
exports.notifyAdminOfPractitionerRegistration = onDocumentCreated(
  "practitionerRequests/{requestId}",
  async (event) => {
    try {
      const requestData = event.data.data();
      const requestId = event.params.requestId;
      
      console.log('🔔 New practitioner registration:', requestId);
      
      // Get SendGrid API key
      const apiKey = SENDGRID_API_KEY.value();
      sgMail.setApiKey(apiKey);
      
      // Format the registration date
      const registeredDate = requestData.requestedAt?.toDate?.() 
        ? requestData.requestedAt.toDate().toLocaleString()
        : 'Unknown date';
      
      // Create the admin notification email
      const adminEmail = {
        to: "support@inkwelljournal.io",
        from: "support@inkwelljournal.io",
        subject: `🔔 New Practitioner Registration: ${requestData.fullName || 'Unknown'}`,
        html: `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="text-align: center; margin-bottom: 30px;">
              <img src="https://inkwelljournal.io/InkWell-Logo.png" alt="InkWell" style="max-width: 150px;">
            </div>
            
            <div style="background: #fff3cd; border: 1px solid #ffeaa7; color: #856404; padding: 1.5em; border-radius: 8px; margin-bottom: 2em;">
              <h2 style="margin-top: 0; color: #856404;">🔔 New Practitioner Registration</h2>
              <p style="margin: 0;"><strong>Action Required:</strong> A new practitioner has registered and requires approval.</p>
            </div>
            
            <div style="background: #f8f9fa; padding: 1.5em; border-radius: 8px; margin-bottom: 1.5em;">
              <h3 style="color: #2A6972; margin-top: 0;">Practitioner Details</h3>
              <div style="display: grid; grid-template-columns: 1fr 2fr; gap: 0.5em; align-items: start;">
                <strong>Name:</strong><span>${requestData.fullName || 'Not provided'}</span>
                <strong>Email:</strong><span>${requestData.email || 'Not provided'}</span>
                <strong>Credentials:</strong><span>${requestData.credentials || 'Not provided'}</span>
                <strong>Practice Type:</strong><span>${requestData.practiceType || 'Not provided'}</span>
                <strong>License #:</strong><span>${requestData.licenseNumber || 'Not provided'}</span>
                <strong>Location:</strong><span>${requestData.practiceLocation || 'Not provided'}</span>
                <strong>Registered:</strong><span>${registeredDate}</span>
              </div>
            </div>
            
            ${requestData.practiceDescription ? `
            <div style="background: #e8f4f8; padding: 1.5em; border-radius: 8px; border-left: 4px solid #2A6972; margin-bottom: 1.5em;">
              <h4 style="color: #2A6972; margin-top: 0;">Practice Description</h4>
              <p style="margin: 0; font-style: italic;">"${requestData.practiceDescription}"</p>
            </div>
            ` : ''}
            
            <div style="text-align: center; margin: 30px 0;">
              <a href="https://inkwelljournal.io/admin.html" 
                 style="background: #2A6972; color: white; padding: 15px 30px; text-decoration: none; border-radius: 5px; font-weight: bold; display: inline-block;">
                🔍 Review & Approve Registration
              </a>
            </div>
            
            <div style="background: #f1f3f4; padding: 1em; border-radius: 4px; margin-top: 2em;">
              <p style="margin: 0; font-size: 0.9em; color: #666;">
                <strong>Next Steps:</strong><br>
                1. Click the button above to access the admin dashboard<br>
                2. Review the practitioner's credentials and information<br>
                3. Approve or deny the registration request<br>
                4. The practitioner will be notified via email of your decision
              </p>
            </div>
            
            <hr style="margin: 30px 0; border: none; border-top: 1px solid #eee;">
            
            <p style="font-size: 12px; color: #999; text-align: center;">
              InkWell Admin Notification System<br>
              <a href="https://www.inkwelljournal.io">inkwelljournal.io</a>
            </p>
          </div>
        `
      };
      
      // Send the notification email
      await sgMail.send(adminEmail);
      console.log('✅ Admin notification sent for practitioner registration:', requestId);
      
    } catch (error) {
      console.error('❌ Error sending admin notification:', error);
      // Don't throw - we don't want the registration to fail if email fails
    }
  }
);

// Add this function at the end of your index.js file
exports.notifyAdminOfPractitionerRegistration = onDocumentCreated({
  secrets: [SENDGRID_API_KEY]
}, "practitionerRequests/{requestId}", async (event) => {
  try {
    const requestData = event.data.data();
    const requestId = event.params.requestId;
    
    console.log('🔔 New practitioner registration:', requestId);
    
    // Get SendGrid API key
    const apiKey = SENDGRID_API_KEY.value();
    sgMail.setApiKey(apiKey);
    
    // Format the registration date
    const registeredDate = requestData.requestedAt?.toDate?.() 
      ? requestData.requestedAt.toDate().toLocaleString()
      : 'Unknown date';
    
    // Create the admin notification email
    const adminEmail = {
      to: "support@inkwelljournal.io",
      from: "support@inkwelljournal.io",
      subject: `🔔 New Practitioner Registration: ${requestData.fullName || 'Unknown'}`,
      html: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
          <div style="text-align: center; margin-bottom: 30px;">
            <img src="https://inkwelljournal.io/InkWell-Logo.png" alt="InkWell" style="max-width: 150px;">
          </div>
          
          <div style="background: #fff3cd; border: 1px solid #ffeaa7; color: #856404; padding: 1.5em; border-radius: 8px; margin-bottom: 2em;">
            <h2 style="margin-top: 0; color: #856404;">🔔 New Practitioner Registration</h2>
            <p style="margin: 0;"><strong>Action Required:</strong> A new practitioner has registered and requires approval.</p>
          </div>
          
          <div style="background: #f8f9fa; padding: 1.5em; border-radius: 8px; margin-bottom: 1.5em;">
            <h3 style="color: #2A6972; margin-top: 0;">Practitioner Details</h3>
            <div style="display: grid; grid-template-columns: 1fr 2fr; gap: 0.5em; align-items: start;">
              <strong>Name:</strong><span>${requestData.fullName || 'Not provided'}</span>
              <strong>Email:</strong><span>${requestData.email || 'Not provided'}</span>
              <strong>Credentials:</strong><span>${requestData.credentials || 'Not provided'}</span>
              <strong>Practice Type:</strong><span>${requestData.practiceType || 'Not provided'}</span>
              <strong>License #:</strong><span>${requestData.licenseNumber || 'Not provided'}</span>
              <strong>Location:</strong><span>${requestData.practiceLocation || 'Not provided'}</span>
              <strong>Registered:</strong><span>${registeredDate}</span>
            </div>
          </div>
          
          ${requestData.practiceDescription ? `
          <div style="background: #e8f4f8; padding: 1.5em; border-radius: 8px; border-left: 4px solid #2A6972; margin-bottom: 1.5em;">
            <h4 style="color: #2A6972; margin-top: 0;">Practice Description</h4>
            <p style="margin: 0; font-style: italic;">"${requestData.practiceDescription}"</p>
          </div>
          ` : ''}
          
          <div style="text-align: center; margin: 30px 0;">
            <a href="https://inkwelljournal.io/admin.html" 
               style="background: #2A6972; color: white; padding: 15px 30px; text-decoration: none; border-radius: 5px; font-weight: bold; display: inline-block;">
              🔍 Review & Approve Registration
            </a>
          </div>
          
          <hr style="margin: 30px 0; border: none; border-top: 1px solid #eee;">
          
          <p style="font-size: 12px; color: #999; text-align: center;">
            InkWell Admin Notification System<br>
            <a href="https://www.inkwelljournal.io">inkwelljournal.io</a>
          </p>
        </div>
      `
    };
     
    // Send the notification email
    await sgMail.send(adminEmail);
    console.log('✅ Admin notification sent for practitioner registration:', requestId);
    
  } catch (error) {
    console.error('❌ Error sending admin notification:', error);
    // Don't throw - we don't want the registration to fail if email fails
  }
});
  });
});

// Delete file from Firebase Storage
exports.deleteFile = onRequest(async (req, res) => {
  const requestId = generateRequestId();
  console.log(`[${requestId}] Delete file request started`);
  
  // Apply hardened CORS
  if (!setupHardenedCORS(req, res)) {
    console.warn(`[${requestId}] Rejected request from unauthorized origin: ${req.headers.origin}`);
    return res.status(403).send('Forbidden');
  }
  
  if (req.method === 'OPTIONS') {
    return res.status(204).send('');
  }

  if (req.method !== 'POST') {
    return sendSecureErrorResponse(res, 405, 'Method not allowed');
  }

  try {
    // Verify authentication
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      console.warn(`[${requestId}] Missing or invalid authorization header`);
      return sendSecureErrorResponse(res, 401, 'Authentication required');
    }
    
    const idToken = authHeader.split('Bearer ')[1];
    const decodedToken = await admin.auth().verifyIdToken(idToken);
    console.log(`[${requestId}] User authenticated: ${decodedToken.uid}`);

    const { filePath } = req.body;
    if (!filePath) {
      console.error(`[${requestId}] Missing filePath in request body`);
      return sendSecureErrorResponse(res, 400, 'File path is required');
    }

    // Validate that the file path belongs to the authenticated user
    // File paths should include the user ID for security
    if (!filePath.includes(decodedToken.uid)) {
      console.warn(`[${requestId}] User ${decodedToken.uid} attempted to delete file not owned by them: ${filePath}`);
      return sendSecureErrorResponse(res, 403, 'Not authorized to delete this file');
    }

    // Delete from Firebase Storage
    const bucket = admin.storage().bucket();
    const file = bucket.file(filePath);
    
    try {
      await file.delete();
      console.log(`[${requestId}] File deleted successfully: ${filePath}`);
      return res.status(200).json({ message: 'File deleted successfully' });
    } catch (deleteError) {
      // Check if file doesn't exist (not an error for our purposes)
      if (deleteError.code === 404) {
        console.log(`[${requestId}] File not found (already deleted): ${filePath}`);
        return res.status(200).json({ message: 'File already deleted' });
      }
      
      console.error(`[${requestId}] Storage deletion failed:`, deleteError);
      return sendSecureErrorResponse(res, 500, 'Failed to delete file from storage', deleteError);
    }
    
  } catch (error) {
    console.error(`[${requestId}] Delete file operation failed:`, error);
    
    if (error.message.includes('auth')) {
      return sendSecureErrorResponse(res, 401, 'Authentication failed', error);
    } else {
      return sendSecureErrorResponse(res, 500, 'Failed to delete file', error);
    }
  }
});

// ===== SOPHY'S INSIGHTS SYSTEM =====

// Test function for single user insights (for troubleshooting)
exports.testUserInsights = onCall({
  secrets: [OPENAI_API_KEY, SENDGRID_API_KEY]
}, async (request) => {
  const requestId = generateRequestId();
  console.log(`[${requestId}] Test insights for user: ${request.auth?.uid}`);
  
  // Verify user is authenticated
  if (!request.auth) {
    throw new HttpsError('unauthenticated', 'Must be authenticated to test insights');
  }
  
  const userId = request.auth.uid;
  
  try {
    // Get user data
    const userDoc = await admin.firestore().collection('users').doc(userId).get();
    if (!userDoc.exists) {
      throw new HttpsError('not-found', 'User profile not found');
    }
    
    const userData = userDoc.data();
    if (!userData.email) {
      throw new HttpsError('failed-precondition', 'User email not found');
    }

    console.log(`[${requestId}] Generating test weekly insights`);
    const weeklyData = await collectWeeklyUserData(userId, requestId);
    
    if (weeklyData.stats.totalEntries === 0) {
      return {
        status: 'skipped', 
        message: 'No journal or manifest entries found for the past 7 days'
      };
    }
    
    const { journalEntries, manifestEntries, stats } = weeklyData;
    
    const insights = await generateInsightsWithOpenAI(
      journalEntries, 
      manifestEntries, 
      stats, 
      'weekly', 
      userData.signupUsername || userData.displayName || 'Friend',
      requestId
    );
    
    await sendInsightsEmail(userData.email, insights, 'weekly', userData.signupUsername || userData.displayName);
    
    return {
      status: 'success',
      message: `Weekly insights sent to ${userData.email}`,
      stats: {
        journalEntries: journalEntries.length,
        manifestEntries: manifestEntries.length,
        totalWords: stats.totalWords,
        daysActive: stats.daysActive
      }
    };
    
  } catch (error) {
    console.error(`[${requestId}] Test insights failed:`, error);
    throw new HttpsError('internal', `Test insights failed: ${error.message}`);
  }
});

// Scheduled function for weekly insights (runs every Monday at 9 AM UTC)
exports.sendWeeklyInsights = onRequest({
  secrets: [OPENAI_API_KEY, SENDGRID_API_KEY],
  cors: ALLOWED_ORIGINS
}, async (req, res) => {
  const requestId = generateRequestId();
  console.log(`[${requestId}] Weekly insights generation started`);
  
  try {
    setupHardenedCORS(req, res);
    
    if (req.method !== 'POST') {
      return sendSecureErrorResponse(res, 405, 'Method not allowed', null);
    }
    
    await generateAndSendInsights('weekly', requestId);
    res.status(200).json({ success: true, message: 'Weekly insights sent successfully' });
    
  } catch (error) {
    console.error(`[${requestId}] Weekly insights failed:`, error);
    return sendSecureErrorResponse(res, 500, 'Failed to generate weekly insights', error);
  }
});

// Main insights generation function
async function generateAndSendInsights(period, requestId) {
  console.log(`[${requestId}] Starting ${period} insights generation`);
  
  // Get all users with insights enabled
  const usersSnapshot = await admin.firestore().collection('users').get();
  const processedUsers = [];
  
  for (const userDoc of usersSnapshot.docs) {
    const userData = userDoc.data();
    const userId = userDoc.id;
    
    // Check if user has opted in for this period
    const insightsEnabled = period === 'weekly' 
      ? userData.insightsPreferences?.weeklyEnabled 
      : userData.insightsPreferences?.monthlyEnabled;
      
    if (!insightsEnabled || !userData.email) {
      continue;
    }
    
    try {
      console.log(`[${requestId}] Processing ${period} insights for user: ${userId}`);
      
      // Get user's journal entries and manifest entries for the period
      const { journalEntries, manifestEntries, stats } = await getUserDataForPeriod(userId, period);
      
      if (journalEntries.length === 0 && manifestEntries.length === 0) {
        console.log(`[${requestId}] No entries found for user ${userId}, skipping`);
        continue;
      }
      
      // Generate insights using OpenAI
      const insights = await generateInsightsWithOpenAI(
        journalEntries, 
        manifestEntries, 
        stats, 
        period, 
        userData.signupUsername || userData.displayName || 'Friend',
        requestId
      );
      
      // Send email with insights
      await sendInsightsEmail(userData.email, insights, period, userData.signupUsername || userData.displayName);
      
      processedUsers.push(userId);
      console.log(`[${requestId}] Successfully sent ${period} insights to user: ${userId}`);
      
      // Small delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 1000));
      
    } catch (error) {
      console.error(`[${requestId}] Failed to process ${period} insights for user ${userId}:`, error);
      // Continue with other users even if one fails
    }
  }
  
  console.log(`[${requestId}] ${period} insights completed. Processed ${processedUsers.length} users.`);
}

// Single user insights generation (for testing)
async function testGenerateSingleUserInsights(userId, period, requestId) {
  console.log(`[${requestId}] Testing ${period} insights for user: ${userId}`);
  
  try {
    // Get user data
    const userDoc = await admin.firestore().collection('users').doc(userId).get();
    if (!userDoc.exists) {
      throw new Error('User not found');
    }
    
    const userData = userDoc.data();
    if (!userData.email) {
      throw new Error('User email not found');
    }
    
    // Get user's journal entries and manifest entries for the period
    const { journalEntries, manifestEntries, stats } = await getUserDataForPeriod(userId, period);
    
    if (journalEntries.length === 0 && manifestEntries.length === 0) {
      return {
        status: 'skipped',
        message: `No ${period === 'weekly' ? '7-day' : '30-day'} entries found`,
        stats: stats
      };
    }
    
    // Generate insights using OpenAI
    const insights = await generateInsightsWithOpenAI(
      journalEntries, 
      manifestEntries, 
      stats, 
      period, 
      userData.signupUsername || userData.displayName || 'Friend',
      requestId
    );
    
    // Send email with insights
    await sendInsightsEmail(userData.email, insights, period, userData.signupUsername || userData.displayName);
    
    console.log(`[${requestId}] Successfully sent test ${period} insights to user: ${userId}`);
    
    return {
      status: 'success',
      message: 'Email sent successfully',
      stats: stats
    };
    
  } catch (error) {
    console.error(`[${requestId}] Failed to generate test ${period} insights for user ${userId}:`, error);
    return {
      status: 'failed',
      error: error.message,
      stats: { journalEntries: 0, manifestEntries: 0, totalWords: 0, daysActive: 0 }
    };
  }
}

// Get user's data for the specified period
async function getUserDataForPeriod(userId, period) {
  console.log(`Getting data for user ${userId}, period: ${period}`);
  
  // FOR TESTING: Let's check both possible collection names and inspect the data structure
  
  try {
    // Check journals collection
    const journalSnapshot = await admin.firestore()
      .collection('journals')
      .where('userId', '==', userId)
      .limit(50) 
      .get();
      
    console.log(`Found ${journalSnapshot.docs.length} entries in 'journals' collection`);
    
    // Also check journalEntries collection in case that's where the data is
    const journalEntriesSnapshot = await admin.firestore()
      .collection('journalEntries')
      .where('userId', '==', userId)
      .limit(50)
      .get();
      
    console.log(`Found ${journalEntriesSnapshot.docs.length} entries in 'journalEntries' collection`);
    
    // Check manifest entries
    const manifestSnapshot = await admin.firestore()
      .collection('manifests')
      .where('userId', '==', userId)
      .limit(50)
      .get();
      
    console.log(`Found ${manifestSnapshot.docs.length} manifest entries`);
    
    // Debug: Show the actual data structure of found entries
    if (journalSnapshot.docs.length > 0) {
      const firstJournal = journalSnapshot.docs[0].data();
      console.log(`Sample journal data:`, JSON.stringify(firstJournal, null, 2));
    }
    
    if (journalEntriesSnapshot.docs.length > 0) {
      const firstJournalEntry = journalEntriesSnapshot.docs[0].data();
      console.log(`Sample journalEntry data:`, JSON.stringify(firstJournalEntry, null, 2));
    }
    
    if (manifestSnapshot.docs.length > 0) {
      const firstManifest = manifestSnapshot.docs[0].data();
      console.log(`Sample manifest data:`, JSON.stringify(firstManifest, null, 2));
    }
    
    // Use whichever collection has the data
    const actualJournalSnapshot = journalEntriesSnapshot.docs.length > 0 ? journalEntriesSnapshot : journalSnapshot;
    console.log(`Using ${journalEntriesSnapshot.docs.length > 0 ? 'journalEntries' : 'journals'} collection for journal data`);
  
    // FOR TESTING: Use much more lenient date filtering - last 30 days for both weekly and monthly
    const now = new Date();
    const startDate = new Date(now.getTime() - (30 * 24 * 60 * 60 * 1000)); // Last 30 days
    
    console.log(`Date filtering: Looking for entries after ${startDate.toISOString()}`);
    
    const journalEntries = actualJournalSnapshot.docs
      .map(doc => {
        const data = doc.data();
        const createdAt = data.createdAt?.toDate();
        console.log(`Journal entry ${doc.id}: createdAt = ${createdAt?.toISOString()}, content preview = ${data.content?.substring(0, 50)}...`);
        return {
          id: doc.id,
          ...data,
          createdAt
        };
      })
      .filter(entry => {
        const include = entry.createdAt && entry.createdAt >= startDate;
        console.log(`Journal ${entry.id}: ${include ? 'INCLUDED' : 'EXCLUDED'} (${entry.createdAt?.toISOString()})`);
        return include;
      })
      .sort((a, b) => a.createdAt - b.createdAt);
      
    const manifestEntries = manifestSnapshot.docs
      .map(doc => {
        const data = doc.data();
        const createdAt = data.createdAt?.toDate();
        console.log(`Manifest entry ${doc.id}: createdAt = ${createdAt?.toISOString()}, data keys = ${Object.keys(data).join(', ')}`);
        return {
          id: doc.id,
          ...data,
          createdAt
        };
      })
      .filter(entry => {
        const include = entry.createdAt && entry.createdAt >= startDate;
        console.log(`Manifest ${entry.id}: ${include ? 'INCLUDED' : 'EXCLUDED'} (${entry.createdAt?.toISOString()})`);
        return include;
      })
      .sort((a, b) => a.createdAt - b.createdAt);
  
    // Calculate basic stats
    const stats = {
      totalJournalEntries: journalEntries.length,
      totalManifestEntries: manifestEntries.length,
      totalWords: journalEntries.reduce((sum, entry) => 
        sum + (entry.content?.split(/\s+/).length || 0), 0),
      daysActive: new Set([
        ...journalEntries.map(e => e.createdAt?.toDate?.()?.toDateString?.() || e.createdAt?.toDateString?.()),
        ...manifestEntries.map(e => e.createdAt?.toDate?.()?.toDateString?.() || e.createdAt?.toDateString?.())
      ].filter(Boolean)).size,
      periodDays: period === 'weekly' ? 7 : 30
    };
    
    console.log(`Final filtered data - Journals: ${journalEntries.length}, Manifests: ${manifestEntries.length}`);
    return { journalEntries, manifestEntries, stats };
    
  } catch (error) {
    console.error('Error in getUserDataForPeriod:', error);
    throw error;
  }
}

// Generate insights using Anthropic Claude
async function generateInsightsWithOpenAI(journalEntries, manifestEntries, stats, period, userName, requestId) {
  console.log(`[${requestId}] Generating ${period} insights for ${userName} with ${stats.totalJournalEntries} journal entries and ${stats.totalManifestEntries} manifest entries`);
  
  // Create content summary (heavily limited to prevent token overflow)
  // Helper to safely get date string from Firestore Timestamp or Date
  const getDateString = (timestamp) => {
    if (!timestamp) return 'Unknown date';
    if (timestamp.toDate) return timestamp.toDate().toDateString(); // Firestore Timestamp
    if (timestamp.toDateString) return timestamp.toDateString(); // JavaScript Date
    return 'Unknown date';
  };
  
  const journalContent = journalEntries.slice(0, 5) // Limit to 5 most recent entries
    .map(entry => `Date: ${getDateString(entry.createdAt)}\nEntry: ${entry.content?.substring(0, 200) || ''}`) // Reduced from 600 to 200 chars
    .join('\n\n---\n\n');
    
  const manifestContent = manifestEntries.slice(0, 3) // Limit to 3 most recent manifests  
    .map(entry => `Date: ${getDateString(entry.createdAt)}\nWish: ${entry.wish?.substring(0, 150) || ''}\nGratitude: ${entry.gratitude?.substring(0, 150) || ''}`) // Reduced from 300 to 150 chars
    .join('\n\n---\n\n');
  
  // Different prompts for weekly vs monthly to ensure different content
  const periodSpecific = period === 'weekly' 
    ? {
        timeframe: 'this past week',
        focus: 'recent patterns and immediate insights from your week',
        approach: 'a quick check-in on your weekly practice'
      }
    : {
        timeframe: 'this past month', 
        focus: 'deeper trends, evolution over time, and comprehensive growth patterns across the month',
        approach: 'a comprehensive reflection on your monthly journey with deeper psychological insights'
      };
  
  // Create a comprehensive prompt that analyzes both journal and manifest data separately
  const hasJournals = journalEntries.length > 0;
  const hasManifests = manifestEntries.length > 0;
  
  let prompt = `You are Sophy, a compassionate AI wellness companion. You're creating a ${period} reflection for ${userName} about ${periodSpecific.timeframe}.

## YOUR ANALYSIS FRAMEWORK:

`;

  // Then-vs-now (v2 Phase 4, 2026-07-02): make the journal's compounding visible
  if (period === 'monthly') {
    prompt += `### THEN-VS-NOW (include ONLY if the entries genuinely support it):
In one or two sentences, draw one explicit comparison between this month and an earlier point in their writing ("Earlier you framed X as...; this month it reads more like..."). Ground it in their actual words. If the history is too thin to support a real comparison, skip this entirely rather than forcing one.

`;
  }

  // Add journal analysis section if they have journal entries
  if (hasJournals) {
    prompt += `### JOURNAL REFLECTION ANALYSIS:
**Drawing from Gestalt Therapy, Positive Psychology, and Atomic Habits:**

JOURNAL ENTRIES (${stats.totalJournalEntries} entries):
${journalContent}

**Look for:**
- **GESTALT PERSPECTIVE:** Themes of awareness, present-moment experiences, emotional processing patterns, what emerges as figure vs. background
- **POSITIVE PSYCHOLOGY:** Evidence of PERMA (Positive emotions, Engagement, Relationships, Meaning, Achievement), character strengths, resilience, flourishing moments
- **ATOMIC HABITS:** Identity shifts, 1% improvements, habit patterns, system improvements, process vs. outcome focus

`;
  }

  // Add manifest analysis section if they have manifest entries  
  if (hasManifests) {
    prompt += `### MANIFEST REFLECTION ANALYSIS:
**Using the WISH Framework (Want → Imagine → Snags → How):**

MANIFEST ENTRIES (${stats.totalManifestEntries} entries):
${manifestContent}

**Look for:**
- **WANT:** Clarity and realistic goal-setting patterns
- **IMAGINE:** How they visualize success and emotional outcomes  
- **SNAGS:** Their awareness of obstacles and challenges
- **HOW:** Their problem-solving and backup planning abilities
- **Progress:** Celebration of small steps, effort over results

`;
  }

  prompt += `## YOUR RESPONSE STRUCTURE:

**WARM GREETING** 
Acknowledge their ${periodSpecific.timeframe} commitment (${stats.daysActive} active days)

**WEEKLY SNAPSHOT** (Brief data summary in supportive tone)
- ${stats.totalJournalEntries} journal ${stats.totalJournalEntries === 1 ? 'entry' : 'entries'}${hasManifests ? `\n- ${stats.totalManifestEntries} WISH manifestations explored` : ''}
- Present this as celebration of their consistency and engagement

`;

  if (hasJournals) {
    prompt += `**JOURNAL INSIGHTS** (2-3 key observations)
- What themes, emotions, or awareness patterns emerge?
- Which growth moments or strengths do you notice?
- What habit or identity shifts are taking root?

`;
  }

  if (hasManifests) {
    prompt += `**MANIFESTING PATTERNS** (Focus on their WISH process)
- How clear and realistic are their wants?
- What does their visualization reveal about their values?
- How well do they anticipate and plan for obstacles?
- What progress or effort deserves celebration?

`;
  }

  prompt += `**GENTLE ENCOURAGEMENT**
Connect insights to their journey ahead, focusing on building on existing strengths

## YOUR VOICE:
- Speak directly and warmly - be genuinely personal
- Reference specific content from their entries
- Celebrate progress while acknowledging struggles with compassion
- Use their own language and themes when possible
- ${period === 'monthly' ? '200-300 words total - deeper reflection with more comprehensive insights' : '150-200 words total'}
- Focus on what you actually observe, not generic advice
${period === 'monthly' ? '- For monthly: Look for longer-term patterns, evolution over time, and deeper psychological insights' : ''}
- DO NOT include any signature or sign-off - the email template handles that

Make them feel truly seen and understood based on their actual content.`;
  try {
    // Enhanced logging for debugging
    console.log(`[${requestId}] Starting AI generation for ${period} insights...`);
    console.log(`[${requestId}] Data summary: ${journalEntries.length} journals, ${manifestEntries.length} manifests`);
    
    const apiKey = OPENAI_API_KEY.value();
    if (!apiKey) {
      console.error(`[${requestId}] ❌ OpenAI API key not found in secrets`);
      throw new Error('OpenAI API key not found');
    }
    
    console.log(`[${requestId}] ✅ OpenAI API key found (${apiKey.length} chars)`);
    
    // Log a sample of the content we're sending
    if (journalContent) {
      console.log(`[${requestId}] Sample journal content: ${journalContent.substring(0, 200)}...`);
    }
    
    console.log(`[${requestId}] Making OpenAI API request...`);
    console.log(`[${requestId}] Prompt length: ${prompt.length} characters`);
    console.log(`[${requestId}] Prompt preview: ${prompt.substring(0, 300)}...`);
    
    const requestBody = {
      model: 'gpt-4o-mini', // Using GPT-4o mini for cost efficiency
      max_tokens: 2000, // Increased to allow for comprehensive Gestalt/WISH responses
      messages: [{
        role: 'user',
        content: prompt
      }],
      temperature: 0.7
    };
    
    console.log(`[${requestId}] Request body prepared, making fetch call...`);
    
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`
      },
      body: JSON.stringify(requestBody)
    });

    console.log(`[${requestId}] OpenAI API response received with status: ${response.status}`);
    console.log(`[${requestId}] Response headers:`, Object.fromEntries(response.headers.entries()));
    
    if (!response.ok) {
      const errorText = await response.text();
      console.error(`[${requestId}] ❌ OpenAI API error ${response.status}: ${errorText}`);
      throw new Error(`OpenAI API error: ${response.status} - ${errorText}`);
    }

    console.log(`[${requestId}] Reading response body...`);
    const result = await response.json();
    console.log(`[${requestId}] ✅ Response parsed successfully`);
    
    if (!result.choices || !result.choices[0] || !result.choices[0].message) {
      console.error(`[${requestId}] ❌ Unexpected OpenAI response structure:`, JSON.stringify(result, null, 2));
      throw new Error('Unexpected OpenAI response format');
    }
    
    const rawInsights = result.choices[0].message.content;
    console.log(`[${requestId}] ✅ Generated ${period} insights successfully (${rawInsights.length} characters)`);
    console.log(`[${requestId}] Insights preview: ${rawInsights.substring(0, 150)}...`);
    
    // Clean up markdown formatting and add signature
    const cleanedInsights = rawInsights
      .replace(/\*\*(.*?)\*\*/g, '$1') // Remove **bold** formatting
      .replace(/\*(.*?)\*/g, '$1')     // Remove *italic* formatting 
      .replace(/#{1,6}\s+/g, '')       // Remove markdown headers
      .trim();
    
    // Add proper signature from Sophy and the Inkwell team
    const signedInsights = `${cleanedInsights}

Keep up the great work—every small step counts.
Sophy & The Inkwell Team ✨`;
    
    return signedInsights;
    
  } catch (error) {
    console.error(`[${requestId}] ❌ Error generating ${period} insights:`, error.message);
    console.error(`[${requestId}] Full error details:`, error);
    
    // Enhanced period-specific fallback with clear indication
    const periodText = period === 'weekly' ? 'week' : 'month';
    const timeframe = period === 'weekly' ? 'this week' : 'this month';
    const encouragement = period === 'weekly' 
      ? `This week's practice shows your dedication to consistent self-care. Each entry is a gift to your future self!` 
      : `This month's journey demonstrates your ongoing commitment to personal growth. These regular practices are building something beautiful.`;
    
    return `[FALLBACK MODE - OpenAI API failed: ${error.message}] 

Hi ${userName}! 

I wanted to reach out with your ${periodText}ly reflection. I can see you've been showing up for yourself with ${stats.totalJournalEntries + stats.totalManifestEntries} entries across ${stats.daysActive} days ${timeframe}.

That commitment to self-reflection is truly meaningful. Each time you write, you're creating space for growth and understanding.

${encouragement}

Keep nurturing this beautiful practice - your future self will thank you for these moments of mindfulness and intention.

With warmth,
Sophy ✨`;
  }
}

// Send insights email via SendGrid
async function sendInsightsEmail(userEmail, insights, period, userName) {
  console.log(`📧 Attempting to send ${period} insights email to ${userEmail}`);
  
  // Check if SendGrid API key is available
  const apiKey = SENDGRID_API_KEY.value();
  if (!apiKey) {
    console.error('❌ SendGrid API key is missing');
    throw new Error('SendGrid API key not configured');
  }
  
  if (!apiKey.startsWith("SG.")) {
    console.error('❌ SendGrid API key format is invalid');
    throw new Error('SendGrid API key format invalid');
  }
  
  console.log('✅ SendGrid API key is properly configured');
  sgMail.setApiKey(apiKey);
  
  // Different visual themes for weekly vs monthly
  const theme = period === 'weekly' 
    ? {
        headerColor: '#2A6972',
        gradientStart: '#f0f8ff',
        gradientEnd: '#f8ffff',
        borderColor: '#2A6972',
        icon: '✨',
        subtitle: 'Your weekly reflection from Sophy'
      }
    : {
        headerColor: '#D49489',
        gradientStart: '#fff8f5',
        gradientEnd: '#fef6f4',
        borderColor: '#D49489',
        icon: '�',
        subtitle: 'Your monthly journey insights from Sophy'
      };
  
  const subject = period === 'weekly' 
    ? `Your Weekly Reflection from Sophy ${theme.icon}` 
    : `Your Monthly Journey Insights from Sophy ${theme.icon}`;
  
  const htmlContent = `
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>InkWell Insights</title>
    </head>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px; background-color: #fafafa;">
      <div style="text-align: center; margin-bottom: 30px;">
        <h1 style="color: ${theme.headerColor}; font-size: 1.8em; margin-bottom: 10px; font-weight: 600;">InkWell Insights</h1>
        <p style="color: #666; font-size: 0.9em; margin: 0;">${theme.subtitle}</p>
      </div>
      
      <div style="background: linear-gradient(135deg, ${theme.gradientStart} 0%, ${theme.gradientEnd} 100%); padding: 30px; border-radius: 12px; border-left: 4px solid ${theme.borderColor}; margin-bottom: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
        <div style="white-space: pre-line; font-size: 1em; line-height: 1.7; color: #2d2d2d;">${insights}</div>
      </div>
      
      <div style="text-align: center; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.85em;">
        <p style="margin-bottom: 10px;">This email was sent because you opted in to receive ${period} insights in your InkWell settings.</p>
        <p style="margin: 0;"><a href="https://inkwelljournal.io" style="color: ${theme.headerColor}; text-decoration: none; font-weight: 500;">Visit InkWell</a> • <a href="#" style="color: #666; text-decoration: none;">Manage Preferences</a></p>
      </div>
    </body>
    </html>
  `;
  
  const msg = {
    to: userEmail,
    from: {
      email: 'sophy@inkwelljournal.io',
      name: 'Sophy from InkWell'
    },
    subject: subject,
    html: htmlContent,
    text: insights // Plain text fallback
  };
  
  console.log('📨 Sending email with config:', {
    to: userEmail,
    from: 'sophy@inkwelljournal.io',
    subject: subject
  });
  
  try {
    const result = await sgMail.send(msg);
    console.log('✅ SendGrid email sent successfully:', result[0].statusCode);
    return result;
  } catch (error) {
    console.error('❌ SendGrid send failed:', error);
    console.error('❌ SendGrid error details:', error.response?.body || error.message);
    throw error;
  }
}

// Monthly-specific email function with coral theme
async function sendMonthlyInsightsEmail(userEmail, insights, period, userName) {
  console.log(`📧 Attempting to send ${period} insights email to ${userEmail}`);
  
  // Check if SendGrid API key is available
  const apiKey = SENDGRID_API_KEY.value();
  if (!apiKey) {
    console.error('❌ SendGrid API key is missing');
    throw new Error('SendGrid API key not configured');
  }
  
  if (!apiKey.startsWith("SG.")) {
    console.error('❌ SendGrid API key format is invalid');
    throw new Error('SendGrid API key format invalid');
  }
  
  console.log('✅ SendGrid API key is properly configured');
  sgMail.setApiKey(apiKey);
  
  // Coral theme for monthly insights
  const theme = {
    headerColor: '#D49489',
    gradientStart: '#fff8f5',
    gradientEnd: '#fef6f4',
    borderColor: '#D49489',
    accentColor: '#E6A497',
    icon: '🌺',
    subtitle: 'Your monthly journey insights from Sophy'
  };
  
  const subject = `Your Monthly Journey Insights from Sophy ${theme.icon}`;
  
  const htmlContent = `
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>InkWell Monthly Insights</title>
    </head>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px; background-color: #fafafa;">
      <div style="text-align: center; margin-bottom: 30px;">
        <h1 style="color: ${theme.headerColor}; font-size: 1.8em; margin-bottom: 10px; font-weight: 600;">InkWell Monthly Insights</h1>
        <p style="color: #666; font-size: 0.9em; margin: 0;">${theme.subtitle}</p>
      </div>
      
      <div style="background: linear-gradient(135deg, ${theme.gradientStart} 0%, ${theme.gradientEnd} 100%); padding: 30px; border-radius: 12px; border-left: 4px solid ${theme.borderColor}; margin-bottom: 30px; box-shadow: 0 2px 8px rgba(212, 148, 137, 0.15);">
        <div style="white-space: pre-line; font-size: 1em; line-height: 1.7; color: #2d2d2d;">${insights}</div>
      </div>
      
      <div style="text-align: center; padding-top: 20px; border-top: 1px solid ${theme.accentColor}; color: #666; font-size: 0.85em;">
        <p style="margin-bottom: 10px;">This email was sent because you opted in to receive monthly insights in your InkWell settings.</p>
        <p style="margin: 0;"><a href="https://inkwelljournal.io" style="color: ${theme.headerColor}; text-decoration: none; font-weight: 500;">Visit InkWell</a> • <a href="#" style="color: #666; text-decoration: none;">Manage Preferences</a></p>
      </div>
    </body>
    </html>
  `;
  
  const msg = {
    to: userEmail,
    from: {
      email: 'sophy@inkwelljournal.io',
      name: 'Sophy from InkWell'
    },
    subject: subject,
    html: htmlContent,
    text: insights // Plain text fallback
  };
  
  console.log('📨 Sending monthly email with config:', {
    to: userEmail,
    from: 'sophy@inkwelljournal.io',
    subject: subject
  });
  
  try {
    const result = await sgMail.send(msg);
    console.log('✅ SendGrid monthly email sent successfully:', result[0].statusCode);
    return result;
  } catch (error) {
    console.error('❌ SendGrid monthly send failed:', error);
    console.error('❌ SendGrid monthly error details:', error.response?.body || error.message);
    throw error;
  }
}

// Collect user's weekly data from Firestore
async function collectWeeklyUserData(userId, requestId) {
  console.log(`[${requestId}] 📊 Collecting weekly data for user: ${userId}`);
  
  try {
    // Calculate date range for the past week (Monday to Sunday)
    const now = new Date();
    const oneWeekAgo = new Date(now.getTime() - (7 * 24 * 60 * 60 * 1000));
    
    console.log(`[${requestId}] Date range: ${oneWeekAgo.toISOString()} to ${now.toISOString()}`);
    
    // Get user's journal entries from the past week - simplified query first
    console.log(`[${requestId}] 📊 Querying journal entries for ${userId} since ${oneWeekAgo.toISOString()}`);
    const journalEntriesRef = admin.firestore().collection('journalEntries');
    
    // Try simple query first to test connectivity
    let journalSnapshot;
    try {
      journalSnapshot = await journalEntriesRef
        .where('userId', '==', userId)
        .orderBy('createdAt', 'desc')
        .limit(100)  // Get recent entries and filter by date after
        .get();
      
      console.log(`[${requestId}] 📝 Found ${journalSnapshot.size} total journal entries for user`);
    } catch (queryError) {
      console.error(`[${requestId}] ❌ Journal query failed:`, queryError);
      throw new Error(`Journal query failed: ${queryError.message}`);
    }
    
    
    // Get user's WISH/manifest entries from the past week - direct document access
    console.log(`[${requestId}] 📊 Accessing manifest document for ${userId} since ${oneWeekAgo.toISOString()}`);
    
    let manifestSnapshot;
    try {
      // Manifests are stored as a single document per user, not a collection
      const manifestDocRef = admin.firestore().collection('manifests').doc(userId);
      const manifestDoc = await manifestDocRef.get();
      
      if (manifestDoc.exists) {
        // Create a mock snapshot structure for consistency
        manifestSnapshot = {
          size: 1,
          docs: [manifestDoc]
        };
        console.log(`[${requestId}] 🎯 Found manifest document for user`);
      } else {
        manifestSnapshot = { size: 0, docs: [] };
        console.log(`[${requestId}] 📝 No manifest document found for user`);
      }
    } catch (queryError) {
      console.error(`[${requestId}] ❌ Manifest query failed:`, queryError);
      throw new Error(`Manifest query failed: ${queryError.message}`);
    }
    
    // Process journal entries and filter by date range
    const journalEntries = [];
    const journalDates = new Set();
    
    journalSnapshot.forEach(doc => {
      try {
        const entry = doc.data();
        console.log(`[${requestId}] 📝 Processing journal entry ${doc.id}, createdAt type:`, typeof entry.createdAt, entry.createdAt);
        
        // Safely handle createdAt date conversion
        if (entry.createdAt && typeof entry.createdAt.toDate === 'function') {
          const entryDate = entry.createdAt.toDate();
          console.log(`[${requestId}] ✅ Converted date:`, entryDate);
          
          // Filter by date range in JavaScript
          if (entryDate && entryDate >= oneWeekAgo && entryDate <= now) {
            journalEntries.push({
              id: doc.id,
              text: entry.text || '',
              createdAt: entry.createdAt,
              contextManifest: entry.contextManifest || ''
            });
            
            // Track unique days for statistics
            journalDates.add(entryDate.toDateString());
            console.log(`[${requestId}] ✅ Added journal entry from ${entryDate.toDateString()}`);
          } else {
            console.log(`[${requestId}] ⏭️ Journal entry outside date range: ${entryDate}`);
          }
        } else {
          console.log(`[${requestId}] ⚠️ Journal entry has invalid createdAt:`, entry.createdAt);
        }
      } catch (error) {
        console.error(`[${requestId}] ❌ Error processing journal entry ${doc.id}:`, error);
        // Continue processing other entries
      }
    });
    
    console.log(`[${requestId}] ✅ Filtered to ${journalEntries.length} journal entries from past week`);
    
    // Process WISH/manifest document (single doc per user)
    const manifestEntries = [];
    const manifestDates = new Set();
    
    manifestSnapshot.docs.forEach(doc => {
      const manifestData = doc.data();
      
      // Check if there's a recent update to the manifest
      if (manifestData.createdAt || manifestData.updatedAt) {
        const relevantDate = manifestData.updatedAt || manifestData.createdAt;
        if (relevantDate && typeof relevantDate.toDate === 'function') {
          const entryDate = relevantDate.toDate();
          
          // Filter by date range in JavaScript
          if (entryDate && entryDate >= oneWeekAgo && entryDate <= now) {
            manifestEntries.push({
              id: doc.id,
              text: manifestData.text || manifestData.content || '',
              createdAt: relevantDate,
              type: 'manifest'
            });
            
            // Track unique days for statistics
            manifestDates.add(entryDate.toDateString());
          }
        }
      }
    });
    
    console.log(`[${requestId}] ✅ Filtered to ${manifestEntries.length} manifest entries from past week`);
    
    // Calculate statistics
    const allDates = new Set([...journalDates, ...manifestDates]);
    const stats = {
      totalJournalEntries: journalEntries.length,
      totalManifestEntries: manifestEntries.length,
      totalEntries: journalEntries.length + manifestEntries.length,
      daysActive: allDates.size,
      journalDaysActive: journalDates.size,
      manifestDaysActive: manifestDates.size
    };
    
    console.log(`[${requestId}] ✅ Weekly data collected:`, stats);
    
    return {
      journalEntries,
      manifestEntries,
      stats,
      dateRange: {
        start: oneWeekAgo,
        end: now
      }
    };
    
  } catch (error) {
    console.error(`[${requestId}] ❌ Error collecting weekly data for user ${userId}:`, error);
    throw error;
  }
}

// Main function to process weekly insights for all eligible users
async function processWeeklyInsights(requestId) {
  console.log(`[${requestId}] 🚀 Starting weekly insights processing`);
  
  try {
    // Get ALL users then filter in code (more reliable than nested field queries)
    const usersRef = admin.firestore().collection('users');
    const usersSnapshot = await usersRef.get();
    
    // Filter to users with weekly insights enabled
    const eligibleUsers = usersSnapshot.docs.filter(doc => {
      const data = doc.data();
      return data.insightsPreferences?.weeklyEnabled === true;
    });
    
    console.log(`[${requestId}] Found ${eligibleUsers.length} users with weekly insights enabled (out of ${usersSnapshot.size} total)`);
    
    const processedUsers = [];
    const errors = [];
    
    // Process each eligible user sequentially to avoid overwhelming APIs
    for (const userDoc of eligibleUsers) {
      const userId = userDoc.id;
      const userData = userDoc.data();
      const userEmail = userData.email;
      const userName = userData.displayName || userData.signupUsername || 'Friend';
      
      try {
        console.log(`[${requestId}] 📝 Processing weekly insights for ${userId} (${userEmail})`);
        console.log(`[${requestId}] 👤 User role: ${userData.userRole}`);
        console.log(`[${requestId}] ⚙️ Weekly enabled: ${userData.insightsPreferences?.weeklyEnabled}`);
        
        // Collect user's weekly data
        console.log(`[${requestId}] 📊 Collecting weekly data for ${userId}...`);
        const weeklyData = await collectWeeklyUserData(userId, requestId);
        console.log(`[${requestId}] ✅ Data collection completed for ${userId}:`, weeklyData.stats);
        
        // Skip users with no activity this week
        if (weeklyData.stats.totalEntries === 0) {
          console.log(`[${requestId}] ⏭️ Skipping ${userId} - no activity this week`);
          continue;
        }
        
        // Generate insights using OpenAI
        const insights = await generateInsightsWithOpenAI(
          weeklyData.journalEntries,
          weeklyData.manifestEntries,
          weeklyData.stats,
          'weekly',
          userName,
          requestId
        );
        
        // Send email with insights
        await sendInsightsEmail(userEmail, insights, 'weekly', userName);
        
        // Send push notification for weekly insights (if user has FCM token)
        if (userData.fcmToken) {
          await sendPushNotification(
            userData.fcmToken,
            '📊 Your Weekly Insights Are Ready!',
            `Hi ${userName}, your personalized weekly reflection from Sophy is waiting for you.`,
            { type: 'weekly_insights' }
          );
          console.log(`[${requestId}] 📱 Push notification sent for weekly insights`);
        }
        
        processedUsers.push({
          userId,
          email: userEmail,
          stats: weeklyData.stats
        });
        
        console.log(`[${requestId}] ✅ Weekly insights sent successfully to ${userEmail}`);
        
        // Add small delay between users to be respectful to APIs
        await new Promise(resolve => setTimeout(resolve, 1000));
        
      } catch (userError) {
        console.error(`[${requestId}] ❌ Error processing user ${userId}:`, userError);
        errors.push({
          userId,
          email: userEmail,
          error: userError.message
        });
      }
    }
    
    console.log(`[${requestId}] 🎉 Weekly insights processing completed`);
    console.log(`[${requestId}] Successfully processed: ${processedUsers.length} users`);
    console.log(`[${requestId}] Errors: ${errors.length} users`);
    
    return {
      success: true,
      processedUsers,
      errors,
      totalEligible: usersSnapshot.size
    };
    
  } catch (error) {
    console.error(`[${requestId}] ❌ Fatal error in weekly insights processing:`, error);
    throw error;
  }
}

// Scheduled function to send weekly insights every Monday at 9 AM Hawaii time (UTC-10)
exports.weeklyInsightsScheduler = onSchedule({
  schedule: "0 19 * * 1", // Every Monday at 19:00 UTC (9:00 AM Hawaii time UTC-10)
  timeZone: "Pacific/Honolulu", // Hawaii timezone
  secrets: [OPENAI_API_KEY, SENDGRID_API_KEY]
}, async (event) => {
  const requestId = generateRequestId();
  console.log(`[${requestId}] 📅 Weekly insights scheduled function triggered`);
  
  try {
    const result = await processWeeklyInsights(requestId);
    console.log(`[${requestId}] ✅ Scheduled weekly insights completed:`, result);
    return result;
  } catch (error) {
    console.error(`[${requestId}] ❌ Scheduled weekly insights failed:`, error);
    throw error;
  }
});

// GHOST-FREE WEEKLY INSIGHTS - Completely new function to avoid all legacy code
async function ghostFreeWeeklyInsights(requestId) {
  console.log(`[${requestId}] 👻 Starting GHOST-FREE weekly insights`);
  
  try {
    // Get users with weekly insights enabled - simple query
    const usersRef = admin.firestore().collection('users');
    const usersSnapshot = await usersRef.get();
    
    console.log(`[${requestId}] Found ${usersSnapshot.size} total users`);
    
    const processedUsers = [];
    const errors = [];
    
    for (const userDoc of usersSnapshot.docs) {
      const userData = userDoc.data();
      const userId = userDoc.id;
      
      // Check if weekly insights enabled
      if (!userData.insightsPreferences?.weeklyEnabled) {
        console.log(`[${requestId}] Skipping ${userId} - weekly insights not enabled`);
        continue;
      }
      
      try {
        console.log(`[${requestId}] Processing user ${userId} (${userData.email})`);
        
        // Get recent journal entries - ULTRA SIMPLE approach (no orderBy to avoid index issues)
        const journalRef = admin.firestore().collection('journalEntries');
        const journalDocs = await journalRef
          .where('userId', '==', userId)
          .limit(50) // Get more entries since we can't order, then sort manually
          .get();
        
        console.log(`[${requestId}] Found ${journalDocs.size} journal entries for ${userId}`);
        
        // Skip if no activity
        if (journalDocs.size === 0) {
          console.log(`[${requestId}] Skipping ${userId} - no journal entries`);
          continue;
        }
        
        // Get manifest entries too for complete insights - ULTRA SIMPLE approach
        const manifestRef = admin.firestore().collection('manifests');
        const manifestDocs = await manifestRef
          .where('userId', '==', userId)
          .limit(20) // Get more then sort manually  
          .get();
          
        console.log(`[${requestId}] Found ${manifestDocs.size} manifest entries for ${userId}`);
        
        // Prepare data for OpenAI analysis
        const entryCount = journalDocs.size;
        const userName = userData.displayName || userData.signupUsername || 'Friend';
        
        // Convert Firestore documents to arrays and sort manually by date (most recent first)
        const journalEntries = journalDocs.docs
          .map(doc => ({
            content: doc.data().content,
            createdAt: doc.data().createdAt?.toDate()
          }))
          .filter(entry => entry.createdAt) // Filter out entries without dates
          .sort((a, b) => b.createdAt - a.createdAt) // Sort by date desc
          .slice(0, 20); // Take most recent 20
        
        const manifestEntries = manifestDocs.docs
          .map(doc => ({
            wish: doc.data().wish,
            gratitude: doc.data().gratitude, 
            createdAt: doc.data().createdAt?.toDate()
          }))
          .filter(entry => entry.createdAt) // Filter out entries without dates
          .sort((a, b) => b.createdAt - a.createdAt) // Sort by date desc  
          .slice(0, 10); // Take most recent 10
        
        // Calculate stats
        const stats = {
          totalJournalEntries: journalEntries.length,
          totalManifestEntries: manifestEntries.length,
          totalWords: journalEntries.reduce((sum, entry) => sum + (entry.content?.split(' ').length || 0), 0),
          daysActive: Math.min(7, journalEntries.length) // Simple approximation for weekly
        };
        
        console.log(`[${requestId}] Generating AI insights for ${userName}...`);
        
        // Generate AI insights using actual content analysis
        const insights = await generateInsightsWithOpenAI(
          journalEntries, 
          manifestEntries, 
          stats, 
          'weekly', 
          userName, 
          requestId
        );

        // Send email using existing function
        await sendInsightsEmail(userData.email, insights, 'weekly', userName);
        
        // Send push notification for weekly insights (if user has FCM token)
        if (userData.fcmToken) {
          await sendPushNotification(
            userData.fcmToken,
            '📊 Your Weekly Insights Are Ready!',
            `Hi ${userName}, your personalized weekly reflection from Sophy is waiting for you.`,
            { type: 'weekly_insights' }
          );
          console.log(`[${requestId}] 📱 Push notification sent for weekly insights`);
        }
        
        processedUsers.push({
          userId,
          email: userData.email,
          stats: {
            totalJournalEntries: stats.totalJournalEntries,
            totalManifestEntries: stats.totalManifestEntries,
            totalWords: stats.totalWords,
            daysActive: stats.daysActive
          }
        });
        
        console.log(`[${requestId}] ✅ SUCCESS for ${userId}`);
        
      } catch (userError) {
        console.error(`[${requestId}] ❌ Error for user ${userId}:`, userError);
        errors.push({
          userId,
          email: userData.email,
          error: userError.message
        });
      }
    }
    
    console.log(`[${requestId}] 👻 Ghost-free processing complete`);
    return {
      success: true,
      processedUsers,
      errors,
      totalEligible: usersSnapshot.size
    };
    
  } catch (error) {
    console.error(`[${requestId}] ❌ Ghost-free function failed:`, error);
    throw error;
  }
}

// Admin callable function to test insights for a specific user
exports.testInsightsForUser = onCall({
  secrets: [OPENAI_API_KEY, SENDGRID_API_KEY]
}, async (request) => {
  const requestId = generateRequestId();
  
  // Verify admin
  if (!request.auth) {
    throw new HttpsError('unauthenticated', 'Must be authenticated');
  }
  
  const adminDoc = await admin.firestore().collection('users').doc(request.auth.uid).get();
  if (!adminDoc.exists || adminDoc.data().userRole !== 'admin') {
    throw new HttpsError('permission-denied', 'Must be admin');
  }
  
  const targetEmail = request.data.email;
  const period = request.data.period || 'weekly'; // 'weekly' or 'monthly'
  
  if (!targetEmail) {
    throw new HttpsError('invalid-argument', 'Email is required');
  }
  
  console.log(`[${requestId}] 🧪 Admin testing ${period} insights for ${targetEmail}`);
  
  try {
    // Find user by email
    const usersSnap = await admin.firestore().collection('users')
      .where('email', '==', targetEmail)
      .get();
    
    if (usersSnap.empty) {
      return { success: false, error: `User not found: ${targetEmail}` };
    }
    
    const userDoc = usersSnap.docs[0];
    const userData = userDoc.data();
    const userId = userDoc.id;
    const userName = userData.displayName || userData.signupUsername || 'Friend';
    
    console.log(`[${requestId}] Found user ${userId}`);
    console.log(`[${requestId}] insightsPreferences:`, JSON.stringify(userData.insightsPreferences));
    
    // Check preferences
    const prefKey = period === 'weekly' ? 'weeklyEnabled' : 'monthlyEnabled';
    const isEnabled = userData.insightsPreferences?.[prefKey];
    console.log(`[${requestId}] ${prefKey}: ${isEnabled}`);
    
    // Collect weekly/monthly data (use weekly collector, period affects email template)
    console.log(`[${requestId}] Collecting user data...`);
    const data = await collectWeeklyUserData(userId, requestId);
    
    console.log(`[${requestId}] Data collected:`, data.stats);
    
    if (data.stats.totalEntries === 0) {
      return { 
        success: false, 
        error: 'No journal entries found for this period',
        insightsPreferences: userData.insightsPreferences,
        stats: data.stats
      };
    }
    
    // Generate insights
    console.log(`[${requestId}] Generating insights...`);
    const insights = await generateInsightsWithOpenAI(
      data.journalEntries,
      data.manifestEntries,
      data.stats,
      period,
      userName,
      requestId
    );
    
    // Send email
    console.log(`[${requestId}] Sending email to ${targetEmail}...`);
    await sendInsightsEmail(targetEmail, insights, period, userName);
    
    return {
      success: true,
      message: `${period} insights sent to ${targetEmail}`,
      insightsPreferences: userData.insightsPreferences,
      stats: data.stats
    };
    
  } catch (error) {
    console.error(`[${requestId}] Test failed:`, error);
    return { success: false, error: error.message };
  }
});

// Manual trigger for testing weekly insights (hidden button in production)
// COMMENTED OUT TO SAVE CPU QUOTA
/* exports.triggerWeeklyInsightsTest = onRequest({ 
  secrets: [OPENAI_API_KEY, SENDGRID_API_KEY] 
}, async (req, res) => {
  const requestId = generateRequestId();
  console.log(`[${requestId}] 🧪 Manual weekly insights test triggered`);
  
  // Apply CORS
  if (!setupHardenedCORS(req, res)) {
    console.warn(`[${requestId}] Rejected request from unauthorized origin: ${req.headers.origin}`);
    return res.status(403).send('Forbidden');
  }
  
  if (req.method === 'OPTIONS') {
    return res.status(204).send('');
  }

  if (req.method !== 'POST') {
    return sendSecureErrorResponse(res, 405, 'Method not allowed');
  }

  try {
    // Verify authentication for test function
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      console.warn(`[${requestId}] Missing authorization for test trigger`);
      return sendSecureErrorResponse(res, 401, 'Authentication required');
    }
    
    try {
      const idToken = authHeader.split('Bearer ')[1];
      const decodedToken = await admin.auth().verifyIdToken(idToken);
      console.log(`[${requestId}] Test triggered by authenticated user: ${decodedToken.uid}`);
    } catch (authError) {
      console.error(`[${requestId}] Authentication failed for test:`, authError.message);
      return sendSecureErrorResponse(res, 401, 'Invalid authentication token');
    }

    // GHOST-FREE: Run completely new weekly insights logic
    const result = await ghostFreeWeeklyInsights(requestId);
    
    console.log(`[${requestId}] ✅ Manual weekly insights test completed`);
    res.status(200).json({
      success: true,
      message: 'Weekly insights test completed successfully',
      result
    });
    
  } catch (error) {
    console.error(`[${requestId}] ❌ Weekly insights test failed:`, error);
    res.status(500).json({
      success: false,
      message: 'Weekly insights test failed',
      error: error.message
    });
  }
});
*/

// ===== MONTHLY INSIGHTS FUNCTIONS =====

// Monthly Insights Scheduler - First day of every month at 9AM Hawaii time
exports.monthlyInsightsScheduler = onSchedule({
  schedule: "0 19 1 * *", // First day of every month at 19:00 UTC (9:00 AM Hawaii time UTC-10)
  timeZone: "Pacific/Honolulu", // Hawaii timezone
  secrets: [OPENAI_API_KEY, SENDGRID_API_KEY]
}, async (event) => {
  const requestId = generateRequestId();
  console.log(`[${requestId}] 📅 Monthly insights scheduled function triggered`);
  
  try {
    const result = await ghostFreeMonthlyInsights(requestId);
    console.log(`[${requestId}] ✅ Scheduled monthly insights completed:`, result);
    return result;
  } catch (error) {
    console.error(`[${requestId}] ❌ Scheduled monthly insights failed:`, error);
    throw error;
  }
});

// GHOST-FREE MONTHLY INSIGHTS - Based on weekly version but for monthly timeframe
async function ghostFreeMonthlyInsights(requestId) {
  console.log(`[${requestId}] 👻 Starting GHOST-FREE monthly insights`);
  
  try {
    // Get users with monthly insights enabled - simple query
    const usersRef = admin.firestore().collection('users');
    const usersSnapshot = await usersRef.get();
    
    console.log(`[${requestId}] Found ${usersSnapshot.size} total users`);
    
    const processedUsers = [];
    const errors = [];
    
    for (const userDoc of usersSnapshot.docs) {
      const userData = userDoc.data();
      const userId = userDoc.id;
      
      // Check if monthly insights enabled
      if (!userData.insightsPreferences?.monthlyEnabled) {
        console.log(`[${requestId}] Skipping ${userId} - monthly insights not enabled`);
        continue;
      }
      
      try {
        console.log(`[${requestId}] Processing user ${userId} (${userData.email})`);
        
        // Get recent journal entries - ULTRA SIMPLE approach (no orderBy to avoid index issues)
        const journalRef = admin.firestore().collection('journalEntries');
        const journalDocs = await journalRef
          .where('userId', '==', userId)
          .limit(100) // Get more entries for monthly analysis, then sort manually
          .get();
        
        console.log(`[${requestId}] Found ${journalDocs.size} journal entries for ${userId}`);
        
        // Skip if no activity
        if (journalDocs.size === 0) {
          console.log(`[${requestId}] Skipping ${userId} - no journal entries`);
          continue;
        }
        
        // Get manifest entries too for complete insights - ULTRA SIMPLE approach
        const manifestRef = admin.firestore().collection('manifests');
        const manifestDocs = await manifestRef
          .where('userId', '==', userId)
          .limit(50) // Get more then sort manually  
          .get();
          
        console.log(`[${requestId}] Found ${manifestDocs.size} manifest entries for ${userId}`);
        
        // Prepare data for OpenAI analysis
        const entryCount = journalDocs.size;
        const userName = userData.displayName || userData.signupUsername || 'Friend';
        
        // Convert Firestore documents to arrays and sort manually by date (most recent first)
        // For monthly, we want last 30 days of data
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
        
        const journalEntries = journalDocs.docs
          .map(doc => ({
            content: doc.data().text, // Journal content is stored in 'text' field
            createdAt: doc.data().createdAt?.toDate()
          }))
          .filter(entry => {
            // Filter for last 30 days AND non-empty content  
            const hasDate = entry.createdAt && entry.createdAt >= thirtyDaysAgo;
            const hasContent = entry.content && entry.content.trim().length >= 3; // At least 3 characters
            
            return hasDate && hasContent;
          })
          .sort((a, b) => b.createdAt - a.createdAt) // Sort by date desc
          .slice(0, 30); // Take most recent 30
        
        // Skip if no meaningful content in the last 30 days
        console.log(`[${requestId}] After filtering: ${journalEntries.length} entries with content for ${userId}`);
        if (journalEntries.length === 0) {
          console.log(`[${requestId}] Skipping ${userId} - no journal entries with content in last 30 days`);
          continue;
        }
        
        const manifestEntries = manifestDocs.docs
          .map(doc => ({
            wish: doc.data().wish,
            gratitude: doc.data().gratitude, 
            createdAt: doc.data().createdAt?.toDate()
          }))
          .filter(entry => entry.createdAt && entry.createdAt >= thirtyDaysAgo) // Filter last 30 days
          .sort((a, b) => b.createdAt - a.createdAt) // Sort by date desc  
          .slice(0, 15); // Take most recent 15
        
        // Calculate stats for monthly - properly count unique active days
        const uniqueDays = new Set();
        journalEntries.forEach(entry => {
          if (entry.createdAt) {
            const dateString = entry.createdAt.toDateString();
            uniqueDays.add(dateString);
          }
        });
        
        const stats = {
          totalJournalEntries: journalEntries.length,
          totalManifestEntries: manifestEntries.length,
          totalWords: journalEntries.reduce((sum, entry) => sum + (entry.content?.split(' ').length || 0), 0),
          daysActive: uniqueDays.size // Actual count of unique days with entries
        };
        
        console.log(`[${requestId}] Generating AI insights for ${userName} (MONTHLY)...`);
        
        // Generate AI insights using actual content analysis - MONTHLY period
        const insights = await generateInsightsWithOpenAI(
          journalEntries, 
          manifestEntries, 
          stats, 
          'monthly', // MONTHLY period instead of weekly
          userName, 
          requestId
        );

        // Send email using monthly email function
        await sendMonthlyInsightsEmail(userData.email, insights, 'monthly', userName);
        
        processedUsers.push({
          userId,
          email: userData.email,
          stats: {
            totalJournalEntries: stats.totalJournalEntries,
            totalManifestEntries: stats.totalManifestEntries,
            totalWords: stats.totalWords,
            daysActive: stats.daysActive
          }
        });
        
        console.log(`[${requestId}] ✅ SUCCESS for ${userId} (MONTHLY)`);
        
      } catch (userError) {
        console.error(`[${requestId}] ❌ Error for user ${userId}:`, userError);
        errors.push({
          userId,
          email: userData.email,
          error: userError.message
        });
      }
    }
    
    console.log(`[${requestId}] 👻 Ghost-free monthly processing complete`);
    return {
      success: true,
      processedUsers,
      errors,
      totalEligible: usersSnapshot.size
    };
    
  } catch (error) {
    console.error(`[${requestId}] ❌ Ghost-free monthly function failed:`, error);
    throw error;
  }
}

// Monthly Insights Test Function
// COMMENTED OUT TO SAVE CPU QUOTA
/* exports.triggerMonthlyInsightsTest = onRequest({ 
  secrets: [OPENAI_API_KEY, SENDGRID_API_KEY] 
}, async (req, res) => {
  const requestId = generateRequestId();
  console.log(`[${requestId}] 🧪 Manual monthly insights test triggered`);
  
  // Apply CORS
  if (!setupHardenedCORS(req, res)) {
    return;
  }
  
  // Add test logic here if needed
  res.json({ success: true, message: 'Test function executed' });
});
*/

// === MailChimp Integration ===
// ═══════════════════════════════════════════════════════════════════════════
// SIGNUP EMAIL SYNC → ActiveCampaign (2026-07-02).
// Callable name kept as addToMailchimp for client back-compat (web auth.js
// calls it fire-and-forget at signup) — Mailchimp subscription is DEAD and
// this had been silently failing. Now: contact/sync → Master Contact List →
// tags [platform tag + Audience], resolved by name, created if missing.
// Mobile parity: call with { email, platform: 'mobile' }.
// ═══════════════════════════════════════════════════════════════════════════
exports.addToMailchimp = onCall({
  secrets: [AC_API_KEY]
}, async (request) => {
  const requestId = generateRequestId();
  console.log(`[${requestId}] 📧 Syncing signup email to ActiveCampaign`);

  const { email, platform } = request.data;
  if (!email) {
    console.error(`[${requestId}] ❌ Email is required`);
    throw new HttpsError('invalid-argument', 'Email is required');
  }

  const AC_URL = 'https://pegasusrealm.api-us1.com';
  const LIST_NAME = 'Master Contact List';
  const headers = {
    'Api-Token': AC_API_KEY.value(),
    'Content-Type': 'application/json'
  };

  try {
    // 1. Create-or-update the contact (idempotent)
    const syncRes = await fetch(`${AC_URL}/api/3/contact/sync`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ contact: { email } })
    });
    if (!syncRes.ok) {
      const errText = await syncRes.text();
      console.error(`[${requestId}] ❌ AC contact/sync failed ${syncRes.status}: ${errText.slice(0, 300)}`);
      throw new HttpsError('internal', 'ActiveCampaign contact sync failed');
    }
    const contactId = (await syncRes.json()).contact?.id;
    if (!contactId) throw new HttpsError('internal', 'ActiveCampaign returned no contact id');
    console.log(`[${requestId}] ✅ AC contact synced (id ${contactId})`);

    // 2. Subscribe to Master Contact List (resolved by name).
    // Brackets MUST be percent-encoded — literal [ ] gets an empty response
    // from AC's edge, which killed this step silently (found live 2026-07-02).
    const listRes = await fetch(`${AC_URL}/api/3/lists?filters%5Bname%5D=${encodeURIComponent(LIST_NAME)}`, { headers });
    const listText = await listRes.text();
    if (!listRes.ok || !listText) {
      console.error(`[${requestId}] ⚠️ AC lists lookup failed ${listRes.status}: ${(listText || '(empty body)').slice(0, 200)}`);
    }
    const listId = listText ? (JSON.parse(listText).lists?.[0]?.id) : null;
    if (listId) {
      await fetch(`${AC_URL}/api/3/contactLists`, {
        method: 'POST',
        headers,
        body: JSON.stringify({ contactList: { list: listId, contact: contactId, status: 1 } })
      });
      console.log(`[${requestId}] ✅ Subscribed to "${LIST_NAME}" (list ${listId})`);
    } else {
      console.error(`[${requestId}] ⚠️ List "${LIST_NAME}" not found in AC — contact created but not subscribed to a list`);
    }

    // 3. Tags: platform tag + Audience (resolve by exact name, create if missing)
    const tagNames = [platform === 'mobile' ? 'InkWell Mobile' : 'InkWell Web', 'Audience'];
    for (const name of tagNames) {
      let tagId = null;
      const q = await fetch(`${AC_URL}/api/3/tags?search=${encodeURIComponent(name)}`, { headers });
      const found = ((await q.json()).tags || []).find(t => t.tag === name);
      if (found) {
        tagId = found.id;
      } else {
        const created = await fetch(`${AC_URL}/api/3/tags`, {
          method: 'POST',
          headers,
          body: JSON.stringify({ tag: { tag: name, tagType: 'contact', description: 'Auto-created by InkWell signup sync' } })
        });
        tagId = (await created.json()).tag?.id;
        console.log(`[${requestId}] ➕ Created missing AC tag "${name}" (id ${tagId})`);
      }
      if (tagId) {
        await fetch(`${AC_URL}/api/3/contactTags`, {
          method: 'POST',
          headers,
          body: JSON.stringify({ contactTag: { contact: contactId, tag: tagId } })
        });
      }
    }
    console.log(`[${requestId}] ✅ Tagged: ${tagNames.join(', ')}`);

    return {
      success: true,
      message: 'Email synced to ActiveCampaign',
      email: email,
      tags: tagNames
    };

  } catch (error) {
    if (error instanceof HttpsError) throw error;
    console.error(`[${requestId}] ❌ ActiveCampaign sync failed:`, error);
    throw new HttpsError('internal', `Failed to sync email to ActiveCampaign: ${error.message}`);
  }
});

// User Data Migration Function - Migrate existing users to new format
// COMMENTED OUT TO SAVE CPU QUOTA  
/* exports.migrateUserData = onCall(async (data, context) => {
  // Only allow admin users to run this function
  if (!context.auth) {
    throw new HttpsError("unauthenticated", "User must be authenticated.");
  }
  
  // Check if user is an admin
  const uid = context.auth.uid;
  try {
    const userDoc = await admin.firestore().collection("users").doc(uid).get();
    if (!userDoc.exists || userDoc.data().userRole !== "admin") {
      throw new HttpsError("permission-denied", "Only admin users can run migration.");
    }
  } catch (error) {
    console.error("Error checking admin status:", error);
    throw new HttpsError("permission-denied", "Unable to verify admin status.");
  }
  
  try {
    console.log("🔄 Starting user data migration...");
    
    const usersRef = admin.firestore().collection("users");
    const snapshot = await usersRef.get();
    
    let migrated = 0;
    let skipped = 0;
    let errors = 0;
    
    const batch = admin.firestore().batch();
    
    for (const doc of snapshot.docs) {
      try {
        const userData = doc.data();
        const userId = doc.id;
        
        // Check if user needs migration (missing new fields)
        const needsMigration = 
          !userData.userId || 
          !userData.createdAt || 
          userData.special_code === undefined ||
          !userData.insightsPreferences ||
          !userData.onboardingState;
        
        if (!needsMigration) {
          skipped++;
          continue;
        }
        
        console.log(`📝 Migrating user: ${userId} (${userData.email || 'no email'})`);
        
        // Prepare updated data while preserving existing values
        const updatedData = {
          // Preserve all existing data
          ...userData,
          
          // Add missing standard fields (only if not already present)
          userId: userData.userId || userId,
          email: userData.email || "", // Preserve existing email or set empty if missing
          displayName: userData.displayName || userData.signupUsername || (userData.email ? userData.email.split('@')[0] : ""),
          signupUsername: userData.signupUsername || userData.displayName || (userData.email ? userData.email.split('@')[0] : ""),
          userRole: userData.userRole || "journaler", // Preserve existing role
          avatar: userData.avatar || "",
          
          // Set special_code to beta for users who don't have it, preserve existing values
          special_code: userData.special_code !== undefined ? userData.special_code : "beta",
          
          // Add missing timestamps
          createdAt: userData.createdAt || admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          
          // Add insights preferences if missing
          insightsPreferences: userData.insightsPreferences || {
            weeklyEnabled: true,
            monthlyEnabled: true,
            createdAt: admin.firestore.FieldValue.serverTimestamp()
          },
          
          // Add progressive onboarding state if missing
          onboardingState: userData.onboardingState || {
            hasCompletedVoiceEntry: false,
            hasSeenWishTab: false,
            hasCreatedWish: false,
            hasUsedSophy: false,
            totalEntries: 0,
            currentMilestone: "existing_user", // Mark as existing vs new_user
            milestones: {
              firstEntry: null,
              firstVoiceEntry: null,
              firstWish: null,
              firstSophy: null,
              tenEntries: null,
              monthlyUser: null
            },
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
            lastMilestoneAt: admin.firestore.FieldValue.serverTimestamp()
          }
        };
        
        batch.update(doc.ref, updatedData);
        migrated++;
        
        // Commit batch every 400 operations (Firestore limit is 500)
        if (migrated % 400 === 0) {
          await batch.commit();
          console.log(`💾 Committed batch of ${migrated} migrations`);
        }
        
      } catch (error) {
        console.error(`❌ Error migrating user ${doc.id}:`, error);
        errors++;
      }
    }
    
    // Commit any remaining operations
    if (migrated % 400 !== 0) {
      await batch.commit();
    }
    
    const result = {
      totalUsers: snapshot.size,
      migrated: migrated,
      skipped: skipped,
      errors: errors,
      success: true
    };
    
    console.log("✅ Migration completed:", result);
    return result;
    
  } catch (error) {
    console.error("❌ Migration failed:", error);
    throw new HttpsError("internal", "Migration failed: " + error.message);
  }
});
*/

// Helper function to calculate days since last update
async function calculateDaysSinceLastUpdate(userId, wishId) {
  try {
    const recentBehavior = await admin.firestore()
      .collection('wishBehavior')
      .where('userId', '==', userId)
      .where('wishId', '==', wishId)
      .where('action', 'in', ['updated', 'created'])
      .orderBy('timestamp', 'desc')
      .limit(1)
      .get();
    
    if (recentBehavior.empty) return 0;
    
    const lastUpdate = recentBehavior.docs[0].data().timestamp.toDate();
    const now = new Date();
    const diffTime = Math.abs(now - lastUpdate);
    return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
  } catch (error) {
    console.error('Error calculating days since last update:', error);
    return 0;
  }
}

// Enhanced WISH lifecycle tracking
exports.trackWishBehavior = onCall({ secrets: [ANTHROPIC_API_KEY, OPENAI_API_KEY] }, async (request) => {
  const { auth } = request;
  const { wishId, action, sectionType, emotionalTone, complexity } = request.data;
  
  if (!auth) throw new HttpsError('unauthenticated', 'Must be logged in');
  
  try {
    const behaviorData = {
      userId: auth.uid,
      wishId: wishId,
      action: action, // 'created', 'updated', 'viewed', 'completed', 'abandoned'
      sectionType: sectionType, // 'want', 'imagine', 'snags', 'how'
      timestamp: admin.firestore.FieldValue.serverTimestamp(),
      emotionalTone: emotionalTone, // from voice analysis if available
      complexity: complexity, // word count, complexity score
      daysSinceLastUpdate: await calculateDaysSinceLastUpdate(auth.uid, wishId)
    };
    
    // Store individual behavior event
    await admin.firestore()
      .collection('wishBehavior')
      .add(behaviorData);
    
    // Update user's behavioral summary
    await updateUserBehavioralSummary(auth.uid, behaviorData);
    
    return { success: true };
  } catch (error) {
    console.error('Error tracking WISH behavior:', error);
    throw new HttpsError('internal', 'Failed to track behavior');
  }
});

// Calculate user behavioral patterns
async function updateUserBehavioralSummary(userId, newBehavior) {
  const userRef = admin.firestore().collection('users').doc(userId);
  const behaviorRef = userRef.collection('behaviorSummary').doc('wishPatterns');
  
  const currentSummary = await behaviorRef.get();
  const summary = currentSummary.exists ? currentSummary.data() : {
    totalWishCreated: 0,
    totalUpdates: 0,
    averageUpdateFrequency: 0,
    completionRate: 0,
    abandonmentRate: 0,
    lastUpdateTimestamp: null,
    longestInactivityPeriod: 0,
    preferredUpdateSections: {},
    emotionalTrends: []
  };
  
  // Update patterns based on new behavior
  if (newBehavior.action === 'created') summary.totalWishCreated++;
  if (newBehavior.action === 'updated') summary.totalUpdates++;
  
  // Calculate inactivity patterns
  if (newBehavior.daysSinceLastUpdate > summary.longestInactivityPeriod) {
    summary.longestInactivityPeriod = newBehavior.daysSinceLastUpdate;
  }
  
  // Track emotional trends
  if (newBehavior.emotionalTone) {
    summary.emotionalTrends.push({
      tone: newBehavior.emotionalTone,
      timestamp: newBehavior.timestamp
    });
    // Keep only last 10 emotional data points
    if (summary.emotionalTrends.length > 10) {
      summary.emotionalTrends = summary.emotionalTrends.slice(-10);
    }
  }
  
  summary.lastUpdateTimestamp = newBehavior.timestamp;
  
  await behaviorRef.set(summary, { merge: true });
}

// Simple admin migration endpoint - bypasses client auth issues
// Enabled for pre-TestFlight data standardization
exports.runAdminMigration = onRequest({
  cors: true
}, async (req, res) => {
  // Simple secret key check instead of Firebase auth
  const adminKey = req.body.adminKey || req.query.adminKey;
  if (adminKey !== "migrate-users-2024-beta") {
    res.status(403).json({ error: "Invalid admin key" });
    return;
  }

  try {
    console.log("🔄 Starting admin user migration...");
    
    const usersRef = admin.firestore().collection("users");
    const snapshot = await usersRef.get();
    
    let migrated = 0;
    let skipped = 0;
    let errors = 0;
    const results = [];
    
    for (const doc of snapshot.docs) {
      try {
        const userData = doc.data();
        const userId = doc.id;
        
        // Check if user needs migration (missing new fields OR has photoURL that needs conversion)
        const needsMigration = 
          !userData.userId || 
          !userData.createdAt || 
          userData.special_code === undefined ||
          !userData.insightsPreferences ||
          !userData.onboardingState ||
          (userData.photoURL !== undefined && userData.avatar === undefined); // photoURL -> avatar conversion
        
        if (!needsMigration) {
          skipped++;
          results.push({ userId, status: "skipped", reason: "Already migrated" });
          continue;
        }
        
        console.log(`Migrating user: ${userId}`);
        
        // Prepare migration data
        const migrationData = {};
        
        if (!userData.userId) {
          migrationData.userId = userId;
        }
        
        if (!userData.createdAt) {
          migrationData.createdAt = admin.firestore.FieldValue.serverTimestamp();
        }
        
        if (userData.special_code === undefined) {
          migrationData.special_code = "beta";
        }
        
        // Convert photoURL to avatar if needed
        if (userData.photoURL !== undefined && userData.avatar === undefined) {
          migrationData.avatar = userData.photoURL || "";
          // Note: We'll keep photoURL for now but avatar is the canonical field
        }
        
        // Ensure avatar field exists
        if (userData.avatar === undefined && userData.photoURL === undefined) {
          migrationData.avatar = "";
        }
        
        if (!userData.insightsPreferences) {
          migrationData.insightsPreferences = {
            weeklyEnabled: true,
            monthlyEnabled: true,
            createdAt: admin.firestore.FieldValue.serverTimestamp()
          };
        }
        
        if (!userData.onboardingState) {
          migrationData.onboardingState = {
            hasCompletedVoiceEntry: false,
            hasSeenWishTab: false,
            hasCreatedWish: false,
            hasUsedSophy: false,
            totalEntries: 0,
            currentMilestone: "existing_user",
            milestones: {
              firstEntry: null,
              firstVoiceEntry: null,
              firstWish: null,
              firstSophyChat: null,
              migratedAt: admin.firestore.FieldValue.serverTimestamp()
            },
            createdAt: admin.firestore.FieldValue.serverTimestamp()
          };
        }
        
        // Apply migration
        await doc.ref.update(migrationData);
        
        migrated++;
        results.push({ 
          userId, 
          status: "migrated", 
          fields: Object.keys(migrationData) 
        });
        
      } catch (userError) {
        console.error(`Error migrating user ${doc.id}:`, userError);
        errors++;
        results.push({ 
          userId: doc.id, 
          status: "error", 
          error: userError.message 
        });
      }
    }
    
    const summary = {
      success: true,
      totalUsers: snapshot.size,
      migrated,
      skipped,
      errors,
      results: results.slice(0, 20) // Limit results to prevent large responses
    };
    
    console.log("✅ Migration completed:", summary);
    res.json(summary);
    
  } catch (error) {
    console.error("❌ Migration failed:", error);
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stack
    });
  }
});

// Delete User Data - Comprehensive account deletion function
exports.deleteUserData = onRequest({ secrets: [SENDGRID_API_KEY] }, async (req, res) => {
  try {
    console.log("🗑️ Starting user data deletion process");
    
    // Verify authentication
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ 
        success: false, 
        error: 'Authentication required' 
      });
    }

    const idToken = authHeader.split('Bearer ')[1];
    let decodedToken;
    
    try {
      decodedToken = await admin.auth().verifyIdToken(idToken);
    } catch (error) {
      console.error("❌ Token verification failed:", error);
      return res.status(401).json({ 
        success: false, 
        error: 'Invalid authentication token' 
      });
    }

    const userId = decodedToken.uid;
    const userEmail = decodedToken.email;
    
    console.log(`🔍 Processing deletion for user: ${userId} (${userEmail})`);
    
    const db = admin.firestore();
    const deletionReport = {
      userId,
      userEmail,
      timestamp: new Date().toISOString(),
      deletedCollections: [],
      totalDocuments: 0,
      errors: []
    };

    // Define all collections to clean up
    const collectionsToDelete = [
      { name: 'users', field: 'userId' },
      { name: 'journalEntries', field: 'userId' },
      { name: 'wishBehavior', field: 'userId' },
      { name: 'interventionOutcomes', field: 'userId' },
      { name: 'coachReplies', field: 'userId' },
      { name: 'searchQueries', field: 'userId' },
      { name: 'behavioralTriggers', field: 'userId' },
      { name: 'practitionerRegistrations', field: 'email' } // Use email for practitioner data
    ];

    // Delete data from each collection
    for (const collection of collectionsToDelete) {
      try {
        console.log(`🧹 Cleaning ${collection.name} collection...`);
        
        const fieldValue = collection.field === 'email' ? userEmail : userId;
        const query = db.collection(collection.name).where(collection.field, '==', fieldValue);
        const snapshot = await query.get();
        
        if (!snapshot.empty) {
          const batch = db.batch();
          let batchCount = 0;
          let totalInCollection = 0;
          
          for (const doc of snapshot.docs) {
            // Check for and delete subcollections
            const subcollections = await doc.ref.listCollections();
            for (const subcollection of subcollections) {
              console.log(`🗂️ Deleting subcollection: ${subcollection.id}`);
              const subDocs = await subcollection.get();
              for (const subDoc of subDocs.docs) {
                batch.delete(subDoc.ref);
                batchCount++;
                totalInCollection++;
                
                // Commit batch if it gets too large
                if (batchCount >= 400) {
                  await batch.commit();
                  batchCount = 0;
                }
              }
            }
            
            // Delete main document
            batch.delete(doc.ref);
            batchCount++;
            totalInCollection++;
            
            // Commit batch if it gets too large
            if (batchCount >= 400) {
              await batch.commit();
              batchCount = 0;
            }
          }
          
          // Commit remaining operations
          if (batchCount > 0) {
            await batch.commit();
          }
          
          deletionReport.deletedCollections.push({
            collection: collection.name,
            documentsDeleted: totalInCollection
          });
          deletionReport.totalDocuments += totalInCollection;
          
          console.log(`✅ Deleted ${totalInCollection} documents from ${collection.name}`);
        } else {
          console.log(`ℹ️ No documents found in ${collection.name}`);
          deletionReport.deletedCollections.push({
            collection: collection.name,
            documentsDeleted: 0
          });
        }
      } catch (collectionError) {
        console.error(`❌ Error deleting from ${collection.name}:`, collectionError);
        deletionReport.errors.push({
          collection: collection.name,
          error: collectionError.message
        });
      }
    }

    // Send confirmation email if SendGrid is available
    try {
      if (SENDGRID_API_KEY.value()) {
        sgMail.setApiKey(SENDGRID_API_KEY.value());
        
        const confirmationEmail = {
          to: userEmail,
          from: 'hello@pegasusrealm.com',
          subject: '✅ InkWell Account Deletion Confirmed',
          html: `
            <!DOCTYPE html>
            <html>
            <head>
              <meta charset="utf-8">
              <meta name="viewport" content="width=device-width, initial-scale=1.0">
              <title>Account Deletion Confirmed - InkWell</title>
            </head>
            <body style="font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; margin: 0; padding: 20px; background-color: #f8f9fa;">
              <div style="max-width: 600px; margin: 0 auto; background: white; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); overflow: hidden;">
                
                <!-- Header -->
                <div style="background: linear-gradient(135deg, #2A6972 0%, #4A9BA8 100%); padding: 30px; text-align: center;">
                  <h1 style="color: white; margin: 0; font-size: 24px; font-weight: 600;">Account Deletion Confirmed</h1>
                  <p style="color: rgba(255,255,255,0.9); margin: 10px 0 0 0; font-size: 16px;">InkWell Digital Sanctuary</p>
                </div>
                
                <!-- Content -->
                <div style="padding: 30px;">
                  <div style="text-align: center; margin-bottom: 30px;">
                    <div style="font-size: 48px; margin-bottom: 15px;">✅</div>
                    <h2 style="color: #2A6972; margin: 0; font-size: 20px; font-weight: 600;">Your Account Has Been Successfully Deleted</h2>
                  </div>
                  
                  <p style="color: #4A5568; line-height: 1.6; margin-bottom: 20px;">
                    This email confirms that your InkWell account and all associated data have been permanently removed from our systems on <strong>${new Date().toLocaleDateString()}</strong>.
                  </p>
                  
                  <div style="background: #f0f8ff; border-radius: 8px; padding: 20px; margin: 25px 0;">
                    <h3 style="color: #2A6972; margin: 0 0 15px 0; font-size: 16px;">What Was Deleted:</h3>
                    <ul style="color: #4A5568; margin: 0; padding-left: 20px; line-height: 1.8;">
                      <li>All journal entries and reflections</li>
                      <li>All WISH manifests and progress data</li>
                      <li>Personal profile and settings</li>
                      <li>Behavioral analytics and insights</li>
                      <li>Coach connections and shared data</li>
                      <li>Account access and authentication</li>
                    </ul>
                  </div>
                  
                  <p style="color: #4A5568; line-height: 1.6; margin-bottom: 25px;">
                    <strong>Total items removed:</strong> ${deletionReport.totalDocuments} documents across ${deletionReport.deletedCollections.length} data categories.
                  </p>
                  
                  <p style="color: #4A5568; line-height: 1.6; margin-bottom: 25px;">
                    If this deletion was made in error or if you have any questions, please contact our support team within the next 7 days. After that time, this deletion cannot be reversed.
                  </p>
                  
                  <div style="background: #fff9f0; border-radius: 8px; padding: 20px; margin: 25px 0; border-left: 4px solid #D69E2E;">
                    <p style="color: #744210; margin: 0; font-size: 14px; line-height: 1.5;">
                      <strong>Thank you</strong> for being part of the InkWell community. We wish you well on your continued journey of reflection and growth.
                    </p>
                  </div>
                </div>
                
                <!-- Footer -->
                <div style="background: #f8f9fa; padding: 20px; text-align: center; border-top: 1px solid #e2e8f0;">
                  <p style="color: #718096; margin: 0; font-size: 14px;">
                    InkWell - Your Digital Sanctuary for Reflection<br>
                    <a href="mailto:hello@pegasusrealm.com" style="color: #2A6972; text-decoration: none;">hello@pegasusrealm.com</a>
                  </p>
                </div>
              </div>
            </body>
            </html>
          `
        };
        
        await sgMail.send(confirmationEmail);
        console.log("✅ Deletion confirmation email sent");
        deletionReport.emailSent = true;
      }
    } catch (emailError) {
      console.error("⚠️ Failed to send confirmation email:", emailError);
      deletionReport.emailSent = false;
      deletionReport.emailError = emailError.message;
    }

    // Log the deletion for admin tracking
    try {
      await db.collection('accountDeletions').add({
        ...deletionReport,
        completedAt: admin.firestore.FieldValue.serverTimestamp()
      });
      console.log("📋 Deletion logged for admin tracking");
    } catch (logError) {
      console.error("⚠️ Failed to log deletion:", logError);
    }

    console.log("✅ User data deletion completed successfully");
    console.log("📊 Deletion Summary:", {
      userId,
      totalDocuments: deletionReport.totalDocuments,
      collections: deletionReport.deletedCollections.length,
      errors: deletionReport.errors.length
    });

    res.json({
      success: true,
      message: 'All user data has been successfully deleted',
      deletionReport: {
        userId,
        totalDocuments: deletionReport.totalDocuments,
        collectionsProcessed: deletionReport.deletedCollections.length,
        timestamp: deletionReport.timestamp,
        emailSent: deletionReport.emailSent || false
      }
    });

  } catch (error) {
    console.error("❌ User data deletion failed:", error);
    res.status(500).json({
      success: false,
      error: 'Failed to delete user data',
      details: error.message
    });
  }
});

// =============================================================================
// TWILIO SMS FUNCTIONS
// =============================================================================

/**
 * Send test SMS to verify phone number
 */
exports.sendTestSMS = onCall(
  { secrets: [TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER] },
  async (request) => {
    // Verify user is authenticated
    if (!request.auth) {
      throw new HttpsError('unauthenticated', 'User must be logged in to send SMS');
    }

    const { phoneNumber } = request.data;

    if (!phoneNumber) {
      throw new HttpsError('invalid-argument', 'Phone number is required');
    }

    try {
      // Initialize Twilio client
      const twilio = require('twilio');
      const client = twilio(
        TWILIO_ACCOUNT_SID.value(),
        TWILIO_AUTH_TOKEN.value()
      );

      // Send test message
      const message = await client.messages.create({
        body: '🌱 Hello from InkWell! This is a test message to confirm your phone number is working. Reply STOP to unsubscribe.',
        from: TWILIO_PHONE_NUMBER.value(),
        to: phoneNumber
      });

      console.log('✅ Test SMS sent:', message.sid);

      return {
        success: true,
        messageSid: message.sid,
        status: message.status
      };
    } catch (error) {
      console.error('❌ Failed to send test SMS:', error);
      throw new HttpsError('internal', `Failed to send SMS: ${error.message}`);
    }
  }
);

/**
 * Send WISH milestone reminder SMS
 */
exports.sendWishMilestone = onCall(
  { secrets: [TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER] },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError('unauthenticated', 'User must be logged in');
    }

    const { phoneNumber, milestone, daysElapsed, totalDays } = request.data;

    if (!phoneNumber || !milestone) {
      throw new HttpsError('invalid-argument', 'Phone number and milestone are required');
    }

    try {
      // Server-side deduplication: Check if we've sent this milestone to this phone number this week
      const now = new Date();
      const weekStart = new Date(now);
      weekStart.setDate(now.getDate() - now.getDay()); // Start of week (Sunday)
      const weekKey = weekStart.toISOString().split('T')[0]; // YYYY-MM-DD of week start
      const dedupeKey = `sms_${phoneNumber.replace(/\D/g, '')}_${milestone}_week_${weekKey}`;
      
      const dedupeRef = admin.firestore().collection('smsDeduplication').doc(dedupeKey);
      const dedupeSnap = await dedupeRef.get();
      
      if (dedupeSnap.exists) {
        console.log(`⚠️ Duplicate SMS blocked: ${dedupeKey} already sent this week`);
        return {
          success: true,
          deduplicated: true,
          message: 'SMS already sent for this milestone this week'
        };
      }
      
      // Mark as sent BEFORE sending to prevent race conditions
      await dedupeRef.set({
        phoneNumber: phoneNumber,
        milestone: milestone,
        sentAt: admin.firestore.FieldValue.serverTimestamp(),
        userId: request.auth.uid
      });
      
      const twilio = require('twilio');
      const client = twilio(
        TWILIO_ACCOUNT_SID.value(),
        TWILIO_AUTH_TOKEN.value()
      );

      let messageText = '';
      const appLink = '\n\nOpen InkWell: https://inkwelljournal.io/app.html';
      
      if (milestone === 'quarter') {
        messageText = `🌱 InkWell: You're 25% through your WISH journey! (${daysElapsed}/${totalDays} days). Keep growing!${appLink}`;
      } else if (milestone === 'half') {
        messageText = `🍀 InkWell: Halfway there! You've completed ${daysElapsed} of ${totalDays} days. Your WISH is blooming!${appLink}`;
      } else if (milestone === 'three-quarters') {
        messageText = `🌿 InkWell: 75% complete! Only ${totalDays - daysElapsed} days left on your WISH journey. You're amazing!${appLink}`;
      } else if (milestone === 'complete') {
        messageText = `🌳 InkWell: Congratulations! You've completed your ${totalDays}-day WISH journey! Time to reflect and set a new WISH.${appLink}`;
      } else {
        messageText = `🌱 InkWell: WISH milestone reached! Keep up the great work on your journey.${appLink}`;
      }

      const message = await client.messages.create({
        body: messageText,
        from: TWILIO_PHONE_NUMBER.value(),
        to: phoneNumber
      });

      console.log('✅ WISH milestone SMS sent:', message.sid);

      return {
        success: true,
        messageSid: message.sid
      };
    } catch (error) {
      console.error('❌ Failed to send WISH milestone SMS:', error);
      throw new HttpsError('internal', `Failed to send SMS: ${error.message}`);
    }
  }
);

/**
 * Send daily journal prompt SMS
 */
exports.sendDailyPrompt = onCall(
  { secrets: [TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER] },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError('unauthenticated', 'User must be logged in');
    }

    const { phoneNumber, prompt } = request.data;

    if (!phoneNumber) {
      throw new HttpsError('invalid-argument', 'Phone number is required');
    }

    try {
      const twilio = require('twilio');
      const client = twilio(
        TWILIO_ACCOUNT_SID.value(),
        TWILIO_AUTH_TOKEN.value()
      );

      const defaultPrompt = '✍️ InkWell: Time to reflect. What went well today? What are you grateful for?';
      const appLink = '\n\nTap to journal: https://inkwelljournal.io/app.html\n\nReply STOP to unsubscribe';
      const messageText = `✍️ InkWell Daily Prompt:\n\n${prompt || defaultPrompt}${appLink}`;

      const message = await client.messages.create({
        body: messageText,
        from: TWILIO_PHONE_NUMBER.value(),
        to: phoneNumber
      });

      console.log('✅ Daily prompt SMS sent:', message.sid);

      return {
        success: true,
        messageSid: message.sid
      };
    } catch (error) {
      console.error('❌ Failed to send daily prompt SMS:', error);
      throw new HttpsError('internal', `Failed to send SMS: ${error.message}`);
    }
  }
);

/**
 * Send daily gratitude prompt SMS - simple, clean, no links
 */
exports.sendGratitudePrompt = onCall(
  { secrets: [TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER] },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError('unauthenticated', 'User must be logged in');
    }

    const { phoneNumber, prompt } = request.data;

    if (!phoneNumber) {
      throw new HttpsError('invalid-argument', 'Phone number is required');
    }

    try {
      const twilio = require('twilio');
      const client = twilio(
        TWILIO_ACCOUNT_SID.value(),
        TWILIO_AUTH_TOKEN.value()
      );

      const defaultPrompt = '🙏 What small thing made you smile today?';
      const messageText = `${prompt || defaultPrompt}\n\nReply STOP to unsubscribe`;

      const message = await client.messages.create({
        body: messageText,
        from: TWILIO_PHONE_NUMBER.value(),
        to: phoneNumber
      });

      console.log('✅ Gratitude prompt SMS sent:', message.sid);

      return {
        success: true,
        messageSid: message.sid
      };
    } catch (error) {
      console.error('❌ Failed to send gratitude prompt SMS:', error);
      throw new HttpsError('internal', `Failed to send SMS: ${error.message}`);
    }
  }
);

/**
 * Send coach reply notification SMS
 */
exports.sendCoachReplyNotification = onCall(
  { secrets: [TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER] },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError('unauthenticated', 'User must be logged in');
    }

    const { phoneNumber, coachName } = request.data;

    if (!phoneNumber) {
      throw new HttpsError('invalid-argument', 'Phone number is required');
    }

    try {
      const twilio = require('twilio');
      const client = twilio(
        TWILIO_ACCOUNT_SID.value(),
        TWILIO_AUTH_TOKEN.value()
      );

      const messageText = `💬 InkWell: ${coachName || 'Your coach'} replied to your journal entry! Log in to read their message.`;

      const message = await client.messages.create({
        body: messageText,
        from: TWILIO_PHONE_NUMBER.value(),
        to: phoneNumber
      });

      console.log('✅ Coach reply notification SMS sent:', message.sid);

      return {
        success: true,
        messageSid: message.sid
      };
    } catch (error) {
      console.error('❌ Failed to send coach reply notification SMS:', error);
      throw new HttpsError('internal', `Failed to send SMS: ${error.message}`);
    }
  }
);

/**
 * Send generic SMS notification
 */
exports.sendSMS = onCall(
  { secrets: [TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER] },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError('unauthenticated', 'User must be logged in');
    }

    const { phoneNumber, message } = request.data;

    if (!phoneNumber || !message) {
      throw new HttpsError('invalid-argument', 'Phone number and message are required');
    }

    try {
      const twilio = require('twilio');
      const client = twilio(
        TWILIO_ACCOUNT_SID.value(),
        TWILIO_AUTH_TOKEN.value()
      );

      const smsMessage = await client.messages.create({
        body: message,
        from: TWILIO_PHONE_NUMBER.value(),
        to: phoneNumber
      });

      console.log('✅ SMS sent:', smsMessage.sid);

      return {
        success: true,
        messageSid: smsMessage.sid,
        status: smsMessage.status
      };
    } catch (error) {
      console.error('❌ Failed to send SMS:', error);
      throw new HttpsError('internal', `Failed to send SMS: ${error.message}`);
    }
  }
);

// =============================================================================
// SCHEDULED DAILY PROMPTS SYSTEM
// =============================================================================

// Generic prompt library (80% of prompts come from here)
const GENERIC_PROMPTS = [
  "What made you smile today?",
  "What's one thing you're grateful for right now?",
  "What would make today feel complete?",
  "What's weighing on your mind?",
  "What gave you energy today?",
  "What do you need to let go of?",
  "What's one small win from today?",
  "How are you really feeling?",
  "What brought you peace today?",
  "What's one thing you learned recently?",
  "What would your future self thank you for doing today?",
  "What's something kind you did or received today?",
  "What challenge helped you grow?",
  "What are you looking forward to?",
  "What does self-care look like for you today?",
  "What boundaries do you need to set?",
  "What relationship brought you joy today?",
  "What's something you accomplished that you're proud of?",
  "What fear are you ready to face?",
  "What pattern have you noticed about yourself lately?",
  "What does success mean to you today?",
  "What would you tell a friend going through your situation?",
  "What's one thing you did just for yourself today?",
  "What made you feel most alive recently?",
  "What's one thing you want to remember about today?",
  "What surprised you today?",
  "What's a belief you're ready to challenge?",
  "What gives your life meaning?",
  "What would it look like to be gentle with yourself?",
  "What progress have you made, even if it's small?",
  "What emotion showed up most today?",
  "What do you need more of in your life?",
  "What do you need less of?",
  "What's something you've been avoiding thinking about?",
  "What would change if you trusted yourself more?",
  "What makes you feel grounded?",
  "What conversation do you need to have?",
  "What part of your day felt most authentic to you?",
  "What's one thing you want to create?",
  "What healing happened today, even in small ways?"
];

// =============================================================================
// NEURO-TRAINING GRATITUDE + MANIFESTING PROTOCOL
// Based on research showing gratitude increases dopamine/serotonin, while
// mental contrasting + implementation intentions create strong cue-behavior links
// =============================================================================

// THREE GOOD THINGS - Morning gratitude (3 concrete items + why they happened)
const GRATITUDE_THREE_GOOD_THINGS = [
  "🌅 Name three good things from yesterday. For each, ask: why did this happen?",
  "🌅 What are 3 specific moments you're grateful for? What made each one possible?",
  "🌅 List 3 wins from yesterday, no matter how small. What role did you play in each?",
  "🌅 What 3 things went well recently? How did your choices contribute to each?",
  "🌅 Name 3 people who showed up for you lately. What made those connections happen?",
  "🌅 What 3 challenges turned into opportunities? What allowed that shift?",
  "🌅 Recall 3 moments of calm or peace. What conditions made them possible?",
  "🌅 What 3 resources helped you this week? How did they come into your life?"
];

// TEN-FINGER GRATITUDE - Micro-wins (sleep, pets, safe home, body, etc.)
const GRATITUDE_MICRO_WINS = [
  "🙏 Right now: notice your breath, your heartbeat, your body working. What function are you grateful for?",
  "🙏 Look around your space. What object makes your daily life easier that you rarely acknowledge?",
  "🙏 Think about last night's sleep. What about your rest deserves appreciation?",
  "🙏 Consider your home. What about it provides safety or comfort you might overlook?",
  "🙏 What simple technology (hot water, electricity, phone) made today possible?",
  "🙏 What part of your body worked well today that you normally take for granted?",
  "🙏 What meal or drink nourished you recently? Savor that memory for 10 seconds.",
  "🙏 What access do you have today that you once wished for? (mobility, connection, stability)",
  "🙏 Consider your pet, a plant, or something living near you. What joy does it bring?",
  "🙏 What clean resource (water, air, clothes) supported you today?",
  "🙏 Name one sensory pleasure available right now: a texture, scent, sound, or sight.",
  "🙏 What small luxury do you have that would have amazed your younger self?",
  "🙏 What did your hands do for you today?",
  "🙏 What about your morning routine worked smoothly?",
  "🙏 What everyday convenience made life easier without you noticing?"
];

// GOAL PRIMING + MENTAL CONTRASTING - Visualize success, identify obstacles
const GRATITUDE_GOAL_PRIMING = [
  "✨ Picture one key goal accomplished. See it vividly. Now name the biggest obstacle standing in your way.",
  "✨ Imagine your ideal outcome for this week. Feel the success. What habit or emotion might derail you?",
  "✨ Visualize a project fully complete. Now identify what internal resistance might slow you down.",
  "✨ See yourself achieving today's priority. Hold that image. What distraction is most likely to appear?",
  "✨ Picture a relationship thriving. Now ask: what old pattern might I need to release?",
  "✨ Imagine feeling energized and focused. What circumstance typically drains that energy?",
  "✨ Envision a health goal met. Now identify the temptation that challenges you most.",
  "✨ See your day ending with satisfaction. What early warning sign tells you you're veering off track?",
  "✨ Picture yourself calm under pressure. What trigger usually disrupts that state?",
  "✨ Visualize completing something you've been avoiding. What feeling comes up when you think about starting?"
];

// IMPLEMENTATION INTENTIONS - If-then plans linking obstacles to micro-behaviors
const GRATITUDE_IF_THEN = [
  "🎯 Complete this: \"If [obstacle appears], then I will [one small action].\"",
  "🎯 What's your biggest barrier today? Create an if-then rule: \"If I notice [X], I will [Y].\"",
  "🎯 Name one temptation. Now decide: \"If tempted, I will instead ___.\"",
  "🎯 When do you typically lose focus? Write: \"If I drift, I will [specific micro-action].\"",
  "🎯 What emotion derails you? Plan: \"If I feel [emotion], my next move is ___.\"",
  "🎯 What's your escape behavior? Decide: \"If I reach for [escape], I will first ___.\"",
  "🎯 When does procrastination hit? Create: \"If I want to delay, I'll commit to just 2 minutes of ___.\"",
  "🎯 What situation triggers stress? Plan: \"If stress rises, I will immediately ___.\"",
  "🎯 What excuse do you often use? Counter with: \"If I hear that excuse, I will ___.\"",
  "🎯 What environment cue pulls you off course? Decide: \"When I see [cue], I will instead ___.\""
];

// EVENING SAVORING / CONSOLIDATION - Review wins, let good feelings land in body
const GRATITUDE_EVENING_SAVORING = [
  "🌙 Name 3 things that went well today. For each, take 20 seconds to let the feeling settle in your body.",
  "🌙 What choice did you make today that you're proud of? Breathe into that feeling for 30 seconds.",
  "🌙 Recall a small win. Notice where warmth or relaxation shows up in your body as you remember it.",
  "🌙 What moment brought you peace today? Close your eyes and relive it for 20 seconds.",
  "🌙 How did you show up well for yourself or others? Let your nervous system register that goodness.",
  "🌙 What challenge did you meet today? Take a breath and appreciate your resilience.",
  "🌙 What connection felt meaningful today? Hold that memory and notice any relaxation in your body.",
  "🌙 Where did you make progress today? Savor it—let the reward pathway encode this win.",
  "🌙 What went better than expected? Take 30 seconds to fully absorb that positive surprise.",
  "🌙 What did you learn about yourself today? Let that insight settle as you breathe slowly."
];

// BRAIN-BASED EDUCATION - Explaining the neuroscience in accessible terms
const GRATITUDE_BRAIN_BASED = [
  "🧠 Gratitude reps increase dopamine and serotonin. Each time you notice good, you're training your brain to see more good.",
  "🧠 Mental contrasting + if-then plans create automatic behavior links. You're programming your brain like GPS—when X, do Y.",
  "🧠 Visualization and savoring use neuroplasticity. You're strengthening the same pathways needed for real-world success.",
  "🧠 When you pause to savor, you activate the parasympathetic nervous system—your brain's rest-and-repair mode.",
  "🧠 Each gratitude practice literally rewires your brain to notice resources, safety, and support instead of threat.",
  "🧠 If-then plans recruit the fronto-striatal circuits for self-regulation. You're not relying on willpower—you're automating good decisions.",
  "🧠 Savoring for 20+ seconds moves experiences from short-term to long-term memory. You're encoding success.",
  "🧠 Gratitude turns what we have into enough. Each practice shifts your brain from scarcity mode to abundance mode."
];

// Combined prompts array - weighted for variety
const GRATITUDE_PROMPTS = [
  // Three Good Things (8 prompts)
  ...GRATITUDE_THREE_GOOD_THINGS,
  // Micro-wins - more frequent (15 prompts)
  ...GRATITUDE_MICRO_WINS,
  // Goal Priming (10 prompts)
  ...GRATITUDE_GOAL_PRIMING,
  // If-Then Planning (10 prompts)
  ...GRATITUDE_IF_THEN,
  // Evening Savoring (10 prompts)
  ...GRATITUDE_EVENING_SAVORING,
  // Brain-based education (8 prompts, less frequent)
  ...GRATITUDE_BRAIN_BASED
];

/**
 * Scheduled function to send daily prompts (runs every 3 hours)
 */
exports.scheduledDailyPrompts = onSchedule({
  schedule: 'every 3 hours',
  timeZone: 'UTC', // Run in UTC so we check all timezones
  secrets: [TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER, ANTHROPIC_API_KEY, OPENAI_API_KEY]
}, async (event) => {
  console.log('🕐 Running scheduled daily prompts check (all timezones)...');
  
  try {
    const now = new Date();
    console.log(`🌍 UTC time: ${now.toISOString()}`);
    
    // Get all users who need prompts
    const usersSnapshot = await admin.firestore().collection('users').get();
    let journalSmsSentCount = 0;
    let journalPushSentCount = 0;
    let gratitudeSmsSentCount = 0;
    let gratitudePushSentCount = 0;
    let skippedCount = 0;
    
    for (const userDoc of usersSnapshot.docs) {
      const userData = userDoc.data();
      const userId = userDoc.id;
      
      // Determine what channels this user can receive notifications on
      const hasSmsSetup = userData.smsOptIn && userData.phoneNumber;
      const hasPushSetup = userData.fcmToken && userData.pushPreferences?.enabled;
      
      // Skip if user has neither channel set up
      if (!hasSmsSetup && !hasPushSetup) continue;
      
      // SUBSCRIPTION CHECK: SMS is a Plus/Connect feature (web only)
      // Push notifications are free for all users
      // During beta testing, also allow 'beta' and 'alpha' special_code users for SMS
      const tier = userData.subscriptionTier || 'free';
      const specialCode = userData.special_code || '';
      const isBetaTester = ['alpha', 'beta'].includes(specialCode);
      const hasSmsAccess = hasSmsSetup && (['plus', 'connect'].includes(tier) || isBetaTester);
      // Push is free - just need token and enabled
      const hasPushAccess = hasPushSetup;
      
      // Skip if user has no access to any channel
      if (!hasSmsAccess && !hasPushAccess) {
        continue;
      }
      
      // Get user's timezone (default to America/New_York for US users)
      const userTimezone = userData.timezone || 'America/New_York';
      
      // Get current hour in USER'S timezone
      const userHour = parseInt(now.toLocaleString('en-US', { 
        timeZone: userTimezone,
        hour: 'numeric',
        hour12: false 
      }));
      
      // FIXED TIME WINDOWS to prevent overlapping:
      // - GRATITUDE: Lunchtime window (11 AM - 3 PM local time) - widened to ensure scheduler catches all timezones
      // - JOURNAL: Evening window (6-10 PM local time) - widened for same reason
      // Note: Scheduler runs every 3 hours, so windows need to be >= 3 hours to guarantee a hit
      const isGratitudeWindow = userHour >= 11 && userHour < 15;  // 11 AM - 3 PM local
      const isJournalWindow = userHour >= 18 && userHour < 22;    // 6 PM - 10 PM local
      
      // Skip if user is outside both windows in their timezone
      if (!isGratitudeWindow && !isJournalWindow) {
        continue;
      }
      
      console.log(`📍 User ${userId} in ${userTimezone}: hour ${userHour}, gratitudeWindow=${isGratitudeWindow}, journalWindow=${isJournalWindow}, sms=${hasSmsAccess}, push=${hasPushAccess}`);
      
      // =======================================================================
      // JOURNAL PROMPTS - EVENING ONLY (6-10 PM local time)
      // =======================================================================
      // Check if user wants journal prompts via SMS or Push
      const wantsSmsJournal = hasSmsAccess && userData.smsPreferences?.dailyPrompts;
      const wantsPushJournal = hasPushAccess && userData.pushPreferences?.dailyPrompts;
      
      if ((wantsSmsJournal || wantsPushJournal) && isJournalWindow) {
        // Check if already sent today
        const lastSent = userData.lastPromptSent?.toDate?.();
        let shouldSendJournal = true;
        
        if (lastSent) {
          const hoursSinceLastPrompt = (now - lastSent) / (1000 * 60 * 60);
          if (hoursSinceLastPrompt < 20) { // At least 20 hours between prompts
            shouldSendJournal = false;
          }
        }
        
        if (shouldSendJournal) {
          // Check if user already journaled today (skip logic)
          const todayStart = new Date();
          todayStart.setHours(0, 0, 0, 0);
          
          const entriesSnapshot = await admin.firestore()
            .collection('journalEntries')
            .where('userId', '==', userId)
            .where('createdAt', '>=', todayStart)
            .limit(1)
            .get();
          
          if (!entriesSnapshot.empty) {
            console.log(`✅ User ${userId} already journaled today, skipping journal prompt`);
            skippedCount++;
          } else {
            // Generate prompt (80% generic, 20% personalized)
            let promptText = '';
            const usePersonalized = Math.random() < 0.2; // 20% chance
            
            if (usePersonalized && ANTHROPIC_API_KEY && ANTHROPIC_API_KEY.value()) {
              try {
                // Get recent entries for context
                const recentEntries = await admin.firestore()
                  .collection('journalEntries')
                  .where('userId', '==', userId)
                  .orderBy('createdAt', 'desc')
                  .limit(3)
                  .get();
                
                if (!recentEntries.empty) {
                  const recentText = recentEntries.docs
                    .map(doc => doc.data().text?.substring(0, 200))
                    .filter(text => text && text.trim())
                    .join(' ');
                  
                  // Only generate personalized prompt if we have actual text
                  if (recentText.trim()) {
                    // Generate personalized prompt
                    const aiResponse = await callAnthropicWithRetry({
                      model: MODELS.FAST,
        role: 'FAST',
                      max_tokens: 150,
                      messages: [{
                        role: "user",
                        content: `You are Sophy, a warm and encouraging journaling companion. Based on these recent journal entries: "${recentText}", create a thoughtful follow-up journaling prompt (max 100 characters). Be curious and supportive. Respond with ONLY the prompt question, no explanations or apologies.`
                      }]
                    }, "dailyPromptPersonalized", generateRequestId());
                    
                    promptText = aiResponse.content[0].text.trim().substring(0, 120);
                  }
                }
              } catch (error) {
                console.error('Failed to generate personalized prompt, using generic:', error);
                promptText = ''; // Will fallback to generic
              }
            }
            
            // Use generic if personalized failed or wasn't selected
            if (!promptText) {
              promptText = GENERIC_PROMPTS[Math.floor(Math.random() * GENERIC_PROMPTS.length)];
            }
            
            // Send journal prompt via SMS (if enabled)
            if (wantsSmsJournal) {
              try {
                const twilio = require('twilio');
                const client = twilio(
                  TWILIO_ACCOUNT_SID.value(),
                  TWILIO_AUTH_TOKEN.value()
                );
                
                const appLink = '\n\nTap to journal: https://inkwelljournal.io/app.html\n\nReply STOP to unsubscribe';
                const messageText = `✍️ InkWell Daily Prompt:\n\n${promptText}${appLink}`;
                
                await client.messages.create({
                  body: messageText,
                  from: TWILIO_PHONE_NUMBER.value(),
                  to: userData.phoneNumber
                });
                
                journalSmsSentCount++;
                console.log(`✅ Sent journal SMS to user ${userId}`);
                
              } catch (smsError) {
                console.error(`❌ Failed to send journal SMS to user ${userId}:`, smsError);
              }
            }
            
            // Send journal prompt via Push Notification (if enabled)
            if (wantsPushJournal) {
              try {
                const pushSent = await sendPushNotification(
                  userData.fcmToken,
                  '✍️ Time to Journal',
                  promptText,
                  { type: 'journal_prompt', screen: 'Journal' }
                );
                
                if (pushSent) {
                  journalPushSentCount++;
                  console.log(`✅ Sent journal push to user ${userId}`);
                }
              } catch (pushError) {
                console.error(`❌ Failed to send journal push to user ${userId}:`, pushError);
              }
            }
            
            // Update user document with last sent time
            await admin.firestore().collection('users').doc(userId).update({
              lastPromptSent: admin.firestore.FieldValue.serverTimestamp()
            });
            
            // Rate limiting: small delay between sends
            await new Promise(resolve => setTimeout(resolve, 100));
          }
        } else {
          skippedCount++;
        }
      }
      
      // =======================================================================
      // GRATITUDE PROMPTS - LUNCHTIME ONLY (11 AM - 3 PM local time)
      // =======================================================================
      // Check if user wants gratitude prompts via SMS or Push
      const wantsSmsGratitude = hasSmsAccess && userData.smsPreferences?.dailyGratitude;
      const wantsPushGratitude = hasPushAccess && userData.pushPreferences?.gratitudePrompts;
      
      if ((wantsSmsGratitude || wantsPushGratitude) && isGratitudeWindow) {
        // Check if already sent gratitude today
        const lastGratitudeSent = userData.lastGratitudeSent?.toDate?.();
        let shouldSendGratitude = true;
        
        if (lastGratitudeSent) {
          const hoursSinceLastGratitude = (now - lastGratitudeSent) / (1000 * 60 * 60);
          if (hoursSinceLastGratitude < 20) { // At least 20 hours between gratitude prompts
            shouldSendGratitude = false;
          }
        }
        
        // No need for stagger check - fixed windows ensure 5+ hours separation
        // Gratitude: 11 AM - 3 PM, Journal: 6-10 PM (minimum 3 hours apart)
        
        if (shouldSendGratitude) {
          // Select random gratitude prompt
          const gratitudeText = GRATITUDE_PROMPTS[Math.floor(Math.random() * GRATITUDE_PROMPTS.length)];
          
          // Send gratitude SMS (if enabled)
          if (wantsSmsGratitude) {
            try {
              const twilio = require('twilio');
              const client = twilio(
                TWILIO_ACCOUNT_SID.value(),
                TWILIO_AUTH_TOKEN.value()
              );
              
              const messageText = `${gratitudeText}\n\nReply STOP to unsubscribe`;
              
              await client.messages.create({
                body: messageText,
                from: TWILIO_PHONE_NUMBER.value(),
                to: userData.phoneNumber
              });
              
              gratitudeSmsSentCount++;
              console.log(`✅ Sent gratitude SMS to user ${userId}`);
              
            } catch (smsError) {
              console.error(`❌ Failed to send gratitude SMS to user ${userId}:`, smsError);
            }
          }
          
          // Send gratitude Push (if enabled)
          if (wantsPushGratitude) {
            try {
              const pushSent = await sendPushNotification(
                userData.fcmToken,
                '🙏 Gratitude Moment',
                gratitudeText,
                { type: 'gratitude_prompt', screen: 'Journal' }
              );
              
              if (pushSent) {
                gratitudePushSentCount++;
                console.log(`✅ Sent gratitude push to user ${userId}`);
              }
            } catch (pushError) {
              console.error(`❌ Failed to send gratitude push to user ${userId}:`, pushError);
            }
          }
          
          // Update user document with last gratitude sent time
          await admin.firestore().collection('users').doc(userId).update({
            lastGratitudeSent: admin.firestore.FieldValue.serverTimestamp()
          });
          
          // Rate limiting: small delay between sends
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      }
    }
    
    console.log(`📊 Daily prompts complete: SMS(journal=${journalSmsSentCount}, gratitude=${gratitudeSmsSentCount}), Push(journal=${journalPushSentCount}, gratitude=${gratitudePushSentCount}), skipped=${skippedCount}`);
    return { 
      success: true, 
      sms: { journal: journalSmsSentCount, gratitude: gratitudeSmsSentCount },
      push: { journal: journalPushSentCount, gratitude: gratitudePushSentCount },
      skipped: skippedCount 
    };
    
  } catch (error) {
    console.error('❌ Scheduled daily prompts failed:', error);
    throw error;
  }
});

// =============================================================================
// SCHEDULED WEEKLY INSIGHTS SMS
// =============================================================================

/**
 * Send weekly insights via SMS - runs every Sunday at 9 PM ET
 * Provides a brief summary of the user's week with key stats
 */
exports.scheduledWeeklyInsightsSMS = onSchedule({
  schedule: '0 21 * * 0', // Every Sunday at 9 PM
  timeZone: 'America/New_York',
  secrets: [TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]
}, async (event) => {
  console.log('📅 Running scheduled weekly insights SMS check...');
  
  try {
    const now = new Date();
    
    // Calculate date range for the past week (Monday to Sunday)
    const weekEnd = new Date(now);
    weekEnd.setHours(23, 59, 59, 999);
    
    const weekStart = new Date(now);
    weekStart.setDate(weekStart.getDate() - 6); // Go back 6 days for full week
    weekStart.setHours(0, 0, 0, 0);
    
    console.log(`📊 Analyzing week: ${weekStart.toLocaleDateString()} - ${weekEnd.toLocaleDateString()}`);
    
    // Get all users who have SMS weekly insights enabled
    const usersSnapshot = await admin.firestore().collection('users').get();
    let sentCount = 0;
    let skippedCount = 0;
    
    for (const userDoc of usersSnapshot.docs) {
      const userData = userDoc.data();
      const userId = userDoc.id;
      
      // Check eligibility
      if (!userData.smsOptIn || !userData.phoneNumber) {
        continue;
      }
      
      if (!userData.smsPreferences?.weeklyInsights) {
        continue;
      }
      
      try {
        // Get user's journal entries for the week
        const entriesSnapshot = await admin.firestore()
          .collection('journalEntries')
          .where('userId', '==', userId)
          .where('createdAt', '>=', weekStart)
          .where('createdAt', '<=', weekEnd)
          .get();
        
        const entryCount = entriesSnapshot.size;
        
        // Skip if no activity this week
        if (entryCount === 0) {
          console.log(`📭 User ${userId} had no entries this week, skipping`);
          skippedCount++;
          continue;
        }
        
        // Calculate basic stats
        let totalWords = 0;
        let voiceEntries = 0;
        const emotions = {};
        
        entriesSnapshot.docs.forEach(doc => {
          const entry = doc.data();
          
          // Count words
          if (entry.entry) {
            totalWords += entry.entry.split(/\s+/).length;
          }
          
          // Count voice entries
          if (entry.isVoice) {
            voiceEntries++;
          }
          
          // Track emotions
          if (entry.primaryEmotion) {
            emotions[entry.primaryEmotion] = (emotions[entry.primaryEmotion] || 0) + 1;
          }
        });
        
        // Find most common emotion
        let topEmotion = null;
        let topEmotionCount = 0;
        for (const [emotion, count] of Object.entries(emotions)) {
          if (count > topEmotionCount) {
            topEmotion = emotion;
            topEmotionCount = count;
          }
        }
        
        // Get last week's entry count for comparison
        const lastWeekEnd = new Date(weekStart);
        lastWeekEnd.setDate(lastWeekEnd.getDate() - 1);
        lastWeekEnd.setHours(23, 59, 59, 999);
        const lastWeekStart = new Date(lastWeekEnd);
        lastWeekStart.setDate(lastWeekStart.getDate() - 6);
        lastWeekStart.setHours(0, 0, 0, 0);
        
        const lastWeekSnapshot = await admin.firestore()
          .collection('journalEntries')
          .where('userId', '==', userId)
          .where('createdAt', '>=', lastWeekStart)
          .where('createdAt', '<=', lastWeekEnd)
          .get();
        const lastWeekCount = lastWeekSnapshot.size;
        
        // Calculate current streak (consecutive days with entries)
        let streak = 0;
        const today = new Date();
        for (let i = 0; i < 30; i++) {
          const checkDate = new Date(today);
          checkDate.setDate(checkDate.getDate() - i);
          checkDate.setHours(0, 0, 0, 0);
          const nextDay = new Date(checkDate);
          nextDay.setDate(nextDay.getDate() + 1);
          
          const dayEntry = await admin.firestore()
            .collection('journalEntries')
            .where('userId', '==', userId)
            .where('createdAt', '>=', checkDate)
            .where('createdAt', '<', nextDay)
            .limit(1)
            .get();
          
          if (dayEntry.size > 0) {
            streak++;
          } else if (i > 0) {
            break; // Streak broken
          }
        }
        
        // Build SMS message
        let messageText = `📊 InkWell Weekly Summary\n\n`;
        messageText += `This week you journaled ${entryCount} ${entryCount === 1 ? 'time' : 'times'}`;
        
        if (voiceEntries > 0) {
          messageText += `, with ${voiceEntries} voice ${voiceEntries === 1 ? 'entry' : 'entries'}`;
        }
        
        messageText += `.\n\n`;
        
        // Current streak
        if (streak > 0) {
          if (streak >= 7) {
            messageText += `🔥 ${streak}-day streak! Amazing consistency!\n`;
          } else if (streak >= 3) {
            messageText += `🔥 ${streak}-day streak! Keep it going!\n`;
          } else {
            messageText += `✨ Current streak: ${streak} ${streak === 1 ? 'day' : 'days'}\n`;
          }
        }
        
        // Week-over-week comparison
        if (lastWeekCount > 0) {
          if (entryCount > lastWeekCount) {
            messageText += `📈 Up from ${lastWeekCount} entries last week!\n`;
          } else if (entryCount === lastWeekCount) {
            messageText += `📊 Consistent with last week - nice rhythm!\n`;
          }
        } else if (entryCount >= 3) {
          messageText += `🌱 Great start to your journaling habit!\n`;
        }
        
        // Most common emotion
        if (topEmotion) {
          const emotionEmojis = {
            'happy': '😊',
            'joy': '😊',
            'grateful': '🙏',
            'calm': '😌',
            'peaceful': '😌',
            'excited': '🎉',
            'sad': '😢',
            'anxious': '😰',
            'worried': '😟',
            'angry': '😠',
            'frustrated': '😤',
            'stressed': '😓',
            'tired': '😴',
            'confused': '😕',
            'hopeful': '🌟',
            'proud': '💪',
            'loved': '❤️',
            'content': '😊'
          };
          
          const emoji = emotionEmojis[topEmotion.toLowerCase()] || '💭';
          messageText += `${emoji} Most common feeling: ${topEmotion}\n`;
        }
        
        messageText += `\nKeep up the great work! 🌱\n\nReply STOP to unsubscribe`;
        
        // Send SMS
        const twilio = require('twilio');
        const client = twilio(
          TWILIO_ACCOUNT_SID.value(),
          TWILIO_AUTH_TOKEN.value()
        );
        
        await client.messages.create({
          body: messageText,
          from: TWILIO_PHONE_NUMBER.value(),
          to: userData.phoneNumber
        });
        
        sentCount++;
        console.log(`✅ Sent weekly insights SMS to user ${userId}`);
        
        // Rate limiting
        await new Promise(resolve => setTimeout(resolve, 100));
        
      } catch (userError) {
        console.error(`❌ Failed to send weekly insights SMS to user ${userId}:`, userError);
        skippedCount++;
      }
    }
    
    console.log(`📊 Weekly insights SMS complete: ${sentCount} sent, ${skippedCount} skipped`);
    return { success: true, sent: sentCount, skipped: skippedCount };
    
  } catch (error) {
    console.error('❌ Scheduled weekly insights SMS failed:', error);
    throw error;
  }
});

// ============================================================================
// SUBSCRIPTION & PAYMENT FUNCTIONS (Stripe Integration)
// ============================================================================

/**
 * Create a Stripe Checkout session for subscription or one-time purchase
 * Supports: Plus subscription, Connect subscription, extra interactions
 */
exports.createCheckoutSession = onCall({
  secrets: [STRIPE_SECRET_KEY],
  cors: true,
}, async (request) => {
  try {
    console.log('🔷 Creating checkout session, user:', request.auth?.uid);
    console.log('🔷 Request data:', JSON.stringify(request.data));
    
    const stripe = require('stripe')(STRIPE_SECRET_KEY.value());
    const { priceId, mode, metadata, successUrl, cancelUrl, giftCode } = request.data;
    const userId = request.auth?.uid;
    
    if (!userId) {
      console.error('❌ No user ID in request');
      throw new HttpsError('unauthenticated', 'User must be authenticated');
    }

    // Get or create Stripe customer
    const userDoc = await admin.firestore().collection('users').doc(userId).get();
    const userData = userDoc.data();
    
    console.log('🔷 User data exists:', !!userData);
    console.log('🔷 User email:', userData?.email);
    
    let customerId = userData?.stripeCustomerId;
    
    // Verify existing customer still exists in Stripe, or create new one
    if (customerId) {
      try {
        await stripe.customers.retrieve(customerId);
        console.log('🔷 Verified existing Stripe customer:', customerId);
      } catch (err) {
        console.warn('⚠️ Stored customer ID invalid, creating new customer:', err.message);
        customerId = null; // Will trigger creation below
      }
    }
    
    if (!customerId) {
      const customer = await stripe.customers.create({
        email: userData.email,
        metadata: {
          firebaseUID: userId,
        },
      });
      customerId = customer.id;
      console.log('✅ Created new Stripe customer:', customerId);
      
      // Save customer ID to Firestore
      await admin.firestore().collection('users').doc(userId).update({
        stripeCustomerId: customerId,
      });
    }

    // Determine discount based on special_code and practitioner selection
    let stripeCouponId = null;
    let discountReason = '';
    let isHollisVerdant = false;
    
    // Hollis Verdant coach UID - founder's pen name for beta/alpha testers
    const HOLLIS_VERDANT_UID = 'ZiNM7YK1jnRgIkAKiCaO1lC6DGx2';
    
    // Pre-created Stripe coupon IDs (created in Stripe Dashboard)
    // NOTE: These are the actual Stripe coupon IDs, not the display names
    const STRIPE_COUPONS = {
      ALPHA_PLUS_80: 'Aa1ztUkB',                      // 80% off Plus Monthly forever
      BETA_PLUS_50: 'oMb5nIdt',                       // 50% off Plus Monthly forever
      ALPHA_PLUS_ANNUAL: 'F6I9Uatr',                  // 80% off Plus Annual forever
      BETA_PLUS_ANNUAL: 'Z3Cr0JR0',                   // 50% off Plus Annual forever
      ALPHA_CONNECT_20: 'VYvieLpA',                   // 20% off Connect forever
      BETA_CONNECT_10: '0vGwlHUs',                    // 10% off Connect forever
    };
    
    // Check for special_code (alpha, beta) - this is the primary discount trigger
    const specialCode = userData?.special_code || null;
    const connectedCoach = userData?.connectedCoach || null;
    const selectedPractitioner = metadata?.practitionerId || connectedCoach;
    
    // Check if user has Hollis Verdant as their coach (free Connect = Plus price only)
    isHollisVerdant = selectedPractitioner === HOLLIS_VERDANT_UID || 
                      connectedCoach === HOLLIS_VERDANT_UID;
    
    // Determine tier from priceId (LIVE MODE)
    let tierName = 'unknown';
    if (priceId === 'price_1SeQaJIu1E0bDEgZq6V8lATE') tierName = 'plus';
    if (priceId === 'price_1SyMozIu1E0bDEgZNZ8zoJt2') tierName = 'plus_annual'; // legacy $69.99/yr
    if (priceId === 'price_1ToXwuIu1E0bDEgZRe1elpOv') tierName = 'plus_annual'; // $49.99/yr (2026-07-01)
    if (priceId === 'price_1SeQcGIu1E0bDEgZQWWqkrjK') tierName = 'connect';
    
    console.log('🔷 Special code:', specialCode, 'Tier:', tierName, 'Hollis:', isHollisVerdant);
    
    // ═══════════════════════════════════════════════════════════════════════════
    // ALPHA/BETA TESTER DISCOUNT POLICY (February 2026)
    // ═══════════════════════════════════════════════════════════════════════════
    // Alpha testers: 80% off Plus forever, 20% off Connect (non-Hollis), Hollis = Plus price
    // Beta testers: 50% off Plus forever, 10% off Connect (non-Hollis), Hollis = Plus price
    // Hollis Verdant (founder coach): No coach fee, users pay Plus price for Connect features
    // ═══════════════════════════════════════════════════════════════════════════
    
    if (specialCode === 'alpha') {
      if (tierName === 'plus') {
        stripeCouponId = STRIPE_COUPONS.ALPHA_PLUS_80;
        discountReason = 'Alpha Tester - 80% Off Plus Monthly Forever';
      } else if (tierName === 'plus_annual') {
        stripeCouponId = STRIPE_COUPONS.ALPHA_PLUS_ANNUAL;
        discountReason = 'Alpha Tester - 80% Off Plus Annual Forever';
      } else if (tierName === 'connect') {
        if (isHollisVerdant) {
          // Hollis Verdant = Plus price (no coach fee) - redirect to Plus checkout instead
          stripeCouponId = STRIPE_COUPONS.ALPHA_PLUS_80;
          discountReason = 'Alpha Tester - Hollis Verdant (Plus Price)';
        } else {
          stripeCouponId = STRIPE_COUPONS.ALPHA_CONNECT_20;
          discountReason = 'Alpha Tester - 20% Off Connect Forever';
        }
      }
    } else if (specialCode === 'beta') {
      if (tierName === 'plus') {
        stripeCouponId = STRIPE_COUPONS.BETA_PLUS_50;
        discountReason = 'Beta Tester - 50% Off Plus Monthly Forever';
      } else if (tierName === 'plus_annual') {
        stripeCouponId = STRIPE_COUPONS.BETA_PLUS_ANNUAL;
        discountReason = 'Beta Tester - 50% Off Plus Annual Forever';
      } else if (tierName === 'connect') {
        if (isHollisVerdant) {
          // Hollis Verdant = Plus price (no coach fee) - redirect to Plus checkout instead
          stripeCouponId = STRIPE_COUPONS.BETA_PLUS_50;
          discountReason = 'Beta Tester - Hollis Verdant (Plus Price)';
        } else {
          stripeCouponId = STRIPE_COUPONS.BETA_CONNECT_10;
          discountReason = 'Beta Tester - 10% Off Connect Forever';
        }
      }
    } else if (userData?.accountType === 'coach' || userData?.isPractitioner) {
      // Mental health professionals get 25% off (created dynamically since no pre-made coupon)
      discountReason = 'Mental Health Professional Discount';
    }
    
    // Check for gift code (can override if higher discount)
    let giftData = null;
    let giftDiscountWasDowngraded = false;
    let effectiveGiftDurationMonths = null;
    let giftDiscountPercent = 0;
    
    if (giftCode) {
      const giftDoc = await admin.firestore()
        .collection('giftMemberships')
        .doc(giftCode)
        .get();
      
      if (giftDoc.exists) {
        giftData = giftDoc.data();
        
        // Validate gift code
        const now = admin.firestore.Timestamp.now();
        const isExpired = giftData.expiresAt && giftData.expiresAt < now;
        const isUsed = giftData.redeemedBy && giftData.redeemedBy.length >= (giftData.maxUses || 1);
        
        if (isExpired) {
          throw new HttpsError('failed-precondition', 'Gift code has expired');
        }
        if (isUsed) {
          throw new HttpsError('failed-precondition', 'Gift code has already been used');
        }
        
        let giftDiscount = giftData.discountPercent * 100; // Convert to percent
        
        // Check if this is a 100% code and user has already used one
        if (giftDiscount === 100 && userData?.usedFreeMonthCode === true) {
          console.log(`⚠️ User ${userId} already used a free month code, downgrading 100% to 50%`);
          giftDiscount = 50;
          giftDiscountWasDowngraded = true;
          effectiveGiftDurationMonths = 1; // Downgraded gets 1 month at 50%
        } else if (giftDiscount === 100) {
          // 100% codes = 1 month only
          effectiveGiftDurationMonths = 1;
        } else {
          // 50% codes = 3 months
          effectiveGiftDurationMonths = 3;
        }
        
        // Gift codes can override alpha/beta discounts if higher value
        // Store gift discount info for later comparison
        giftDiscountPercent = giftDiscount;
      }
    }
    
    console.log('🔷 Coupon:', stripeCouponId || 'none', 'Reason:', discountReason, 'Gift:', giftDiscountPercent || 0);

    // Build session config
    const sessionConfig = {
      customer: customerId,
      mode: mode || 'subscription',
      line_items: [{
        price: priceId,
        quantity: 1,
      }],
      success_url: successUrl || `${process.env.APP_URL}/app.html?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: cancelUrl || `${process.env.APP_URL}/app.html`,
      metadata: {
        firebaseUID: userId,
        specialCode: specialCode || 'none',
        discountReason: discountReason || 'none',
        isHollisVerdant: isHollisVerdant,
        ...(selectedPractitioner ? { practitionerId: selectedPractitioner } : {}),
        ...(metadata || {}),
        ...(giftCode ? { giftCode } : {}),
      },
    };
    
    // Add subscription_data only for subscription mode
    if (mode === 'subscription' || !mode) {
      sessionConfig.subscription_data = {
        metadata: {
          firebaseUID: userId,
          specialCode: specialCode || 'none',
          discountReason: discountReason || 'none',
          isHollisVerdant: isHollisVerdant,
          ...(selectedPractitioner ? { practitionerId: selectedPractitioner } : {}),
        },
      };
      
      // ═══════════════════════════════════════════════════════════════════════════
      // FREE TRIAL PERIODS (February 2026)
      // ═══════════════════════════════════════════════════════════════════════════
      // Alpha testers: 180 days FREE Connect, then lifetime discount applies
      // Beta testers: 120 days FREE Connect, then lifetime discount applies  
      // Regular users: 7-day trial on Plus only (not Connect)
      // ═══════════════════════════════════════════════════════════════════════════
      
      if (specialCode === 'alpha') {
        sessionConfig.subscription_data.trial_period_days = 180;
        console.log('🎁 Alpha tester: 180 days FREE trial (6 months)');
      } else if (specialCode === 'beta') {
        sessionConfig.subscription_data.trial_period_days = 120;
        console.log('🎁 Beta tester: 120 days FREE trial (4 months)');
      } else if (tierName === 'plus' || tierName === 'plus_annual') {
        // Regular users get 7-day trial on Plus only
        sessionConfig.subscription_data.trial_period_days = 7;
        console.log('🎁 Added 7-day free trial for Plus subscription');
      }
    }
    
    console.log('🔷 Session config:', JSON.stringify(sessionConfig, null, 2));

    // Apply discount: prefer pre-created coupons for alpha/beta, create dynamic for gifts
    if (stripeCouponId) {
      // Use pre-created Stripe coupon for alpha/beta testers
      sessionConfig.discounts = [{
        coupon: stripeCouponId,
      }];
      console.log('✅ Applied pre-created coupon:', stripeCouponId);
    } else if (giftData && giftDiscountPercent > 0) {
      // Create dynamic coupon for gift codes (limited duration)
      const couponConfig = {
        percent_off: giftDiscountPercent,
        name: giftDiscountWasDowngraded 
          ? `Gift from ${giftData.practitionerName || 'practitioner'} (adjusted)`
          : `Gift from ${giftData.practitionerName || 'practitioner'}`,
        duration: 'repeating',
        duration_in_months: effectiveGiftDurationMonths || 3,
      };
      
      const coupon = await stripe.coupons.create(couponConfig);
      sessionConfig.discounts = [{ coupon: coupon.id }];
      
      sessionConfig.metadata.giftDiscountWasDowngraded = giftDiscountWasDowngraded ? 'true' : 'false';
      sessionConfig.metadata.giftDurationMonths = effectiveGiftDurationMonths || '';
      
      console.log('✅ Applied gift coupon:', coupon.id, giftDiscountPercent + '%', 'for', effectiveGiftDurationMonths, 'months');
    } else if (discountReason === 'Mental Health Professional Discount') {
      // Create dynamic 25% coupon for coaches
      const coupon = await stripe.coupons.create({
        percent_off: 25,
        name: 'Mental Health Professional Discount',
        duration: 'forever',
      });
      sessionConfig.discounts = [{ coupon: coupon.id }];
      console.log('✅ Applied coach discount coupon:', coupon.id);
    }

    const session = await stripe.checkout.sessions.create(sessionConfig);
    
    console.log('✅ Checkout session created:', session.id);

    return {
      sessionId: session.id,
      url: session.url,
    };
    
  } catch (error) {
    console.error('❌ Error creating checkout session:', error);
    console.error('❌ Error stack:', error.stack);
    throw new HttpsError('internal', `Failed to create checkout: ${error.message}`);
  }
});

/**
 * Create a Stripe Billing Portal session for subscription management
 * Allows users to cancel, update payment method, view invoices
 */
exports.createBillingPortalSession = onCall({
  secrets: [STRIPE_SECRET_KEY],
  cors: true,
}, async (request) => {
  try {
    if (!request.auth) {
      throw new HttpsError('unauthenticated', 'Must be logged in');
    }
    
    const userId = request.auth.uid;
    console.log('🔷 Creating billing portal session for user:', userId);
    
    // Get user's Stripe customer ID
    const userDoc = await admin.firestore().collection('users').doc(userId).get();
    if (!userDoc.exists) {
      throw new HttpsError('not-found', 'User not found');
    }
    
    const userData = userDoc.data();
    const customerId = userData.stripeCustomerId;
    
    if (!customerId) {
      throw new HttpsError('failed-precondition', 'No active subscription found. You may have signed up via Apple Pay on mobile.');
    }
    
    const stripe = require('stripe')(STRIPE_SECRET_KEY.value());
    
    // Create billing portal session
    const session = await stripe.billingPortal.sessions.create({
      customer: customerId,
      return_url: `${request.data.returnUrl || 'https://inkwelljournal.io/app'}`,
    });
    
    console.log('✅ Billing portal session created for user:', userId);
    
    return {
      url: session.url,
    };
    
  } catch (error) {
    console.error('❌ Error creating billing portal session:', error);
    throw new HttpsError('internal', error.message || 'Failed to open subscription management');
  }
});

/**
 * Handle Stripe webhook events
 * Processes: subscription creation, updates, cancellations, payment success/failure
 */
exports.handleStripeWebhook = onRequest({
  secrets: [STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET],
  cors: true,
}, async (req, res) => {
  const stripe = require('stripe')(STRIPE_SECRET_KEY.value());
  const sig = req.headers['stripe-signature'];
  
  let event;
  
  try {
    event = stripe.webhooks.constructEvent(
      req.rawBody,
      sig,
      STRIPE_WEBHOOK_SECRET.value()
    );
  } catch (err) {
    console.error('⚠️ Webhook signature verification failed:', err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  console.log(`📥 Stripe webhook received: ${event.type}`);

  try {
    switch (event.type) {
      case 'checkout.session.completed': {
        const session = event.data.object;
        const userId = session.metadata.firebaseUID;
        const giftCode = session.metadata.giftCode;
        
        if (session.mode === 'subscription') {
          const subscription = await stripe.subscriptions.retrieve(session.subscription);
          const priceId = subscription.items.data[0].price.id;
          
          // Determine tier based on price ID (LIVE MODE price IDs)
          let tier = 'free';
          if (priceId === 'price_1SeQaJIu1E0bDEgZq6V8lATE' || priceId === 'price_1SyMozIu1E0bDEgZNZ8zoJt2' || priceId === 'price_1ToXwuIu1E0bDEgZRe1elpOv') {
            tier = 'plus'; // Plus Monthly or Plus Annual (incl. $49.99/yr, 2026-07-01)
          }
          if (priceId === 'price_1SeQcGIu1E0bDEgZQWWqkrjK') {
            tier = 'connect'; // Connect tier
          }
          
          console.log(`🎯 Webhook: priceId=${priceId}, determined tier=${tier}`);
          
          // Update user document
          await admin.firestore().collection('users').doc(userId).update({
            subscriptionTier: tier,
            subscriptionStatus: 'active',
            stripeSubscriptionId: subscription.id,
            stripeCustomerId: session.customer,
            subscriptionStartedAt: admin.firestore.FieldValue.serverTimestamp(),
            giftedBy: giftCode ? (await admin.firestore().collection('giftMemberships').doc(giftCode).get()).data()?.createdBy : null,
          });
          
          // If Connect tier, initialize interaction tracking
          if (tier === 'connect') {
            await admin.firestore().collection('users').doc(userId).update({
              interactionsThisMonth: 0,
              interactionsLimit: 4,
              extraInteractionsPurchased: 0,
            });
          }
          
          // Mark gift code as redeemed and track subscription
          if (giftCode) {
            const giftRef = admin.firestore().collection('giftMemberships').doc(giftCode);
            const giftDoc = await giftRef.get();
            
            if (giftDoc.exists) {
              await giftRef.update({
                redeemedBy: admin.firestore.FieldValue.arrayUnion(userId),
                activeSubscriptions: admin.firestore.FieldValue.arrayUnion({
                  userId: userId,
                  subscriptionId: subscription.id,
                  redeemedAt: admin.firestore.FieldValue.serverTimestamp(),
                  expiresAt: new Date(Date.now() + (90 * 24 * 60 * 60 * 1000)), // 3 months from now
                }),
              });
              
              // Store gift code info on user for tracking
              await admin.firestore().collection('users').doc(userId).update({
                giftCodeUsed: giftCode,
                giftCodeExpiresAt: new Date(Date.now() + (90 * 24 * 60 * 60 * 1000)),
                giftCodeIssuedBy: giftDoc.data().createdBy,
              });
            }
          }
          
          console.log(`✅ Subscription created for user ${userId}: ${tier}`);
        } else if (session.mode === 'payment') {
          // One-time purchase (extra interactions)
          const quantity = session.metadata.extraInteractions || 1;
          
          await admin.firestore().collection('users').doc(userId).update({
            extraInteractionsPurchased: admin.firestore.FieldValue.increment(parseInt(quantity)),
            interactionsLimit: admin.firestore.FieldValue.increment(parseInt(quantity)),
          });
          
          console.log(`✅ Extra interactions purchased for user ${userId}: ${quantity}`);
        }
        break;
      }
      
      case 'customer.subscription.updated': {
        const subscription = event.data.object;
        const userId = subscription.metadata.firebaseUID;
        
        // Check if subscription is set to cancel at period end but still active
        const periodEnd = new Date(subscription.current_period_end * 1000);
        const isCanceledButStillActive = subscription.cancel_at_period_end === true && subscription.status === 'active';
        
        const updateData = {
          subscriptionStatus: isCanceledButStillActive ? 'active_until_period_end' : subscription.status,
          subscriptionPeriodEnd: periodEnd,
        };
        
        // If canceled at period end, store when access will end (but DON'T downgrade tier yet)
        if (isCanceledButStillActive) {
          updateData.subscriptionCancelAtPeriodEnd = true;
          updateData.subscriptionAccessEndsAt = periodEnd;
          console.log(`⚠️ Subscription set to cancel at period end for user ${userId}. Access until: ${periodEnd.toISOString()}`);
        } else {
          updateData.subscriptionCancelAtPeriodEnd = false;
        }
        
        await admin.firestore().collection('users').doc(userId).update(updateData);
        
        console.log(`✅ Subscription updated for user ${userId}: ${subscription.status}, cancel_at_period_end: ${subscription.cancel_at_period_end}`);
        break;
      }
      
      case 'customer.subscription.deleted': {
        // This fires when the subscription ACTUALLY ends (after period end)
        const subscription = event.data.object;
        const userId = subscription.metadata.firebaseUID;
        
        await admin.firestore().collection('users').doc(userId).update({
          subscriptionTier: 'free',
          subscriptionStatus: 'canceled',
          subscriptionCanceledAt: admin.firestore.FieldValue.serverTimestamp(),
          subscriptionCancelAtPeriodEnd: false,
        });
        
        console.log(`✅ Subscription ended for user ${userId} - downgraded to free`);
        break;
      }
      
      case 'invoice.payment_failed': {
        const invoice = event.data.object;
        const subscription = await stripe.subscriptions.retrieve(invoice.subscription);
        const userId = subscription.metadata.firebaseUID;
        
        await admin.firestore().collection('users').doc(userId).update({
          subscriptionStatus: 'past_due',
          lastPaymentFailed: admin.firestore.FieldValue.serverTimestamp(),
        });
        
        // TODO: Send email notification to user
        console.log(`⚠️ Payment failed for user ${userId}`);
        break;
      }
      
      case 'invoice.payment_succeeded': {
        const invoice = event.data.object;
        
        // Only process subscription invoices (not one-time payments)
        if (!invoice.subscription) break;
        
        const subscription = await stripe.subscriptions.retrieve(invoice.subscription);
        const userId = subscription.metadata.firebaseUID;
        const accountType = subscription.metadata.accountType || 'standard';
        const practitionerId = subscription.metadata.practitionerId;
        const isSpecialPractitioner = subscription.metadata.isSpecialPractitioner === 'true';
        const discountReason = subscription.metadata.discountReason || 'none';
        
        // Determine if this is Connect tier (has practitioner revenue share)
        const priceId = subscription.items.data[0].price.id;
        const isConnectTier = priceId === 'price_1SeQcGIu1E0bDEgZQWWqkrjK';
        
        const amountPaid = invoice.amount_paid / 100; // Convert cents to dollars
        const stripeFee = (invoice.amount_paid * 0.029 + 30) / 100; // 2.9% + $0.30
        
        console.log(`💰 Payment succeeded: $${amountPaid}, Stripe fee: $${stripeFee.toFixed(2)}`);
        
        // Calculate revenue split for Connect tier
        if (isConnectTier && practitionerId && !isSpecialPractitioner) {
          // Standard practitioner - always gets $30
          const practitionerShare = 30.00;
          const platformShare = amountPaid - practitionerShare - stripeFee;
          
          console.log(`📊 Revenue split: Practitioner=$${practitionerShare}, Platform=$${platformShare.toFixed(2)}, Stripe=$${stripeFee.toFixed(2)}`);
          
          // Record revenue transaction
          await admin.firestore().collection('revenueTransactions').add({
            userId: userId,
            practitionerId: practitionerId,
            accountType: accountType,
            discountReason: discountReason,
            subscriptionId: subscription.id,
            invoiceId: invoice.id,
            amountPaid: amountPaid,
            practitionerShare: practitionerShare,
            platformShare: platformShare,
            stripeFee: stripeFee,
            transactionDate: admin.firestore.FieldValue.serverTimestamp(),
            periodStart: new Date(subscription.current_period_start * 1000),
            periodEnd: new Date(subscription.current_period_end * 1000),
          });
          
          // Update practitioner's monthly revenue
          const monthKey = new Date().toISOString().slice(0, 7); // YYYY-MM
          const practitionerRef = admin.firestore()
            .collection('practitioners')
            .doc(practitionerId);
          
          await practitionerRef.set({
            monthlyRevenue: {
              [monthKey]: admin.firestore.FieldValue.increment(practitionerShare),
            },
            totalRevenue: admin.firestore.FieldValue.increment(practitionerShare),
            activeClients: admin.firestore.FieldValue.arrayUnion(userId),
            lastPaymentReceived: admin.firestore.FieldValue.serverTimestamp(),
          }, { merge: true });
          
          console.log(`✅ Practitioner ${practitionerId} credited $${practitionerShare}`);
          
        } else if (isConnectTier && isSpecialPractitioner) {
          // Hollis Verdant - waives practitioner fee
          const practitionerShare = 0.00;
          const platformShare = amountPaid - stripeFee;
          
          console.log(`📊 Special practitioner (Hollis): Platform=$${platformShare.toFixed(2)}, Stripe=$${stripeFee.toFixed(2)}`);
          
          // Still record the transaction
          await admin.firestore().collection('revenueTransactions').add({
            userId: userId,
            practitionerId: practitionerId,
            accountType: accountType,
            discountReason: discountReason,
            isSpecialPractitioner: true,
            subscriptionId: subscription.id,
            invoiceId: invoice.id,
            amountPaid: amountPaid,
            practitionerShare: practitionerShare,
            platformShare: platformShare,
            stripeFee: stripeFee,
            transactionDate: admin.firestore.FieldValue.serverTimestamp(),
            periodStart: new Date(subscription.current_period_start * 1000),
            periodEnd: new Date(subscription.current_period_end * 1000),
          });
        } else {
          // Plus tier or no practitioner - all to platform
          const platformShare = amountPaid - stripeFee;
          
          console.log(`📊 Plus tier: Platform=$${platformShare.toFixed(2)}, Stripe=$${stripeFee.toFixed(2)}`);
          
          await admin.firestore().collection('revenueTransactions').add({
            userId: userId,
            accountType: accountType,
            discountReason: discountReason,
            tier: 'plus',
            subscriptionId: subscription.id,
            invoiceId: invoice.id,
            amountPaid: amountPaid,
            practitionerShare: 0,
            platformShare: platformShare,
            stripeFee: stripeFee,
            transactionDate: admin.firestore.FieldValue.serverTimestamp(),
            periodStart: new Date(subscription.current_period_start * 1000),
            periodEnd: new Date(subscription.current_period_end * 1000),
          });
        }
        
        console.log(`✅ Revenue split recorded for user ${userId}`);
        break;
      }
    }
    
    res.json({ received: true });
  } catch (error) {
    console.error('❌ Error processing webhook:', error);
    res.status(500).send('Webhook processing failed');
  }
});

/**
 * Get subscription status and details for current user
 */
exports.getSubscriptionStatus = onCall({
  cors: true,
}, async (request) => {
  try {
    const userId = request.auth?.uid;
    
    if (!userId) {
      throw new HttpsError('unauthenticated', 'User must be authenticated');
    }

    const userDoc = await admin.firestore().collection('users').doc(userId).get();
    const userData = userDoc.data();
    
    return {
      tier: userData?.subscriptionTier || 'free',
      status: userData?.subscriptionStatus || 'active',
      interactionsThisMonth: userData?.interactionsThisMonth || 0,
      interactionsLimit: userData?.interactionsLimit || 0,
      extraInteractionsPurchased: userData?.extraInteractionsPurchased || 0,
      giftedBy: userData?.giftedBy || null,
      canUpgrade: (userData?.subscriptionTier || 'free') !== 'connect',
    };
    
  } catch (error) {
    console.error('❌ Error getting subscription status:', error);
    throw new HttpsError('internal', error.message);
  }
});

/**
 * Purchase extra practitioner interactions (Connect tier only)
 */
exports.purchaseExtraInteraction = onCall({
  secrets: [STRIPE_SECRET_KEY],
  cors: true,
}, async (request) => {
  try {
    const userId = request.auth?.uid;
    const { quantity } = request.data;
    
    if (!userId) {
      throw new HttpsError('unauthenticated', 'User must be authenticated');
    }

    // Verify user is on Connect tier
    const userDoc = await admin.firestore().collection('users').doc(userId).get();
    const userData = userDoc.data();
    
    if (userData?.subscriptionTier !== 'connect') {
      throw new HttpsError('failed-precondition', 'Must be on Connect tier to purchase extra interactions');
    }
    
    // Check if already at maximum (7 total = 4 included + 3 extra)
    const currentExtra = userData.extraInteractionsPurchased || 0;
    if (currentExtra >= 3) {
      throw new HttpsError('failed-precondition', 'Maximum extra interactions already purchased (3 per month)');
    }
    
    const allowedQuantity = Math.min(quantity || 1, 3 - currentExtra);
    
    // Create checkout session for one-time payment
    const stripe = require('stripe')(STRIPE_SECRET_KEY.value());
    
    const session = await stripe.checkout.sessions.create({
      customer: userData.stripeCustomerId,
      mode: 'payment',
      line_items: [{
        price_data: {
          currency: 'usd',
          product_data: {
            name: 'Extra Practitioner Interaction',
            description: 'Additional monthly interaction with your connected practitioner',
          },
          unit_amount: 999, // $9.99
        },
        quantity: allowedQuantity,
      }],
      success_url: `${process.env.APP_URL}/app.html?extra_purchased=true`,
      cancel_url: `${process.env.APP_URL}/app.html`,
      metadata: {
        firebaseUID: userId,
        extraInteractions: allowedQuantity,
      },
    });

    return {
      sessionId: session.id,
      url: session.url,
      quantity: allowedQuantity,
    };
    
  } catch (error) {
    console.error('❌ Error purchasing extra interaction:', error);
    throw new HttpsError('internal', error.message);
  }
});

/**
 * Create a gift membership code (practitioner only)
 * Allows practitioners to offer discounted Connect memberships to clients
 * Gift codes valid for 3 months and tied to practitioner connection
 */
exports.createGiftMembership = onCall({
  cors: true,
}, async (request) => {
  try {
    const userId = request.auth?.uid;
    const { discountPercent, maxUses, recipientEmail } = request.data;
    
    if (!userId) {
      throw new HttpsError('unauthenticated', 'User must be authenticated');
    }

    // Verify user is an approved practitioner
    const practitionerDoc = await admin.firestore()
      .collection('approvedPractitioners')
      .doc(userId)
      .get();
    
    if (!practitionerDoc.exists) {
      throw new HttpsError('permission-denied', 'Only approved practitioners can create gift memberships');
    }

    const practitionerData = practitionerDoc.data();

    // Validate discount (50-100%)
    const discount = Math.min(Math.max(discountPercent || 0.50, 0.50), 1.0);
    
    // Generate unique gift code
    const giftCode = generateGiftCode();
    
    // Calculate expiration - FIXED at 3 months (90 days)
    const expiresAt = new Date();
    expiresAt.setDate(expiresAt.getDate() + 90);
    
    // Create gift membership document
    await admin.firestore().collection('giftMemberships').doc(giftCode).set({
      code: giftCode,
      createdBy: userId,
      createdByEmail: practitionerData.email,
      practitionerName: practitionerData.name || practitionerData.email,
      discountPercent: discount,
      maxUses: maxUses || 1,
      recipientEmail: recipientEmail || null,
      redeemedBy: [],
      activeSubscriptions: [], // Track active subscriptions using this code
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      expiresAt: admin.firestore.Timestamp.fromDate(expiresAt),
      durationMonths: 3, // 3-month validity from redemption
      status: 'active',
      requiresPractitionerConnection: true, // Expires if practitioner disconnects
    });
    
    console.log(`✅ Gift membership created by practitioner ${userId}: ${giftCode} (${discount * 100}% off, 3-month duration)`);
    
    return {
      giftCode,
      discountPercent: discount,
      expiresAt: expiresAt.toISOString(),
      durationMonths: 3,
      redeemUrl: `${process.env.APP_URL}/redeem?code=${giftCode}`,
      note: 'Code valid for 3 months from redemption. Expires if practitioner connection is removed.',
    };
    
  } catch (error) {
    console.error('❌ Error creating gift membership:', error);
    throw new HttpsError('internal', error.message);
  }
});

/**
 * Validate and get details of a gift code
 */
exports.validateGiftCode = onCall({
  cors: true,
}, async (request) => {
  try {
    const { giftCode } = request.data;
    
    if (!giftCode) {
      throw new HttpsError('invalid-argument', 'Gift code is required');
    }

    const giftDoc = await admin.firestore()
      .collection('giftMemberships')
      .doc(giftCode.toUpperCase())
      .get();
    
    if (!giftDoc.exists) {
      return { valid: false, reason: 'Gift code not found' };
    }
    
    const giftData = giftDoc.data();
    const now = admin.firestore.Timestamp.now();
    
    // Check expiration
    if (giftData.expiresAt && giftData.expiresAt < now) {
      return { valid: false, reason: 'Gift code has expired' };
    }
    
    // Check usage limit
    if (giftData.redeemedBy.length >= giftData.maxUses) {
      return { valid: false, reason: 'Gift code has been fully redeemed' };
    }
    
    return {
      valid: true,
      discountPercent: giftData.discountPercent,
      createdByEmail: giftData.createdByEmail,
      expiresAt: giftData.expiresAt.toDate().toISOString(),
    };
    
  } catch (error) {
    console.error('❌ Error validating gift code:', error);
    throw new HttpsError('internal', error.message);
  }
});

/**
 * Track practitioner interaction (increments monthly counter)
 */
exports.trackPractitionerInteraction = onCall({
  cors: true,
}, async (request) => {
  try {
    const userId = request.auth?.uid;
    
    if (!userId) {
      throw new HttpsError('unauthenticated', 'User must be authenticated');
    }

    const userDoc = await admin.firestore().collection('users').doc(userId).get();
    const userData = userDoc.data();
    
    // Verify Connect tier
    if (userData?.subscriptionTier !== 'connect') {
      throw new HttpsError('permission-denied', 'Must be on Connect tier');
    }
    
    const current = userData.interactionsThisMonth || 0;
    const limit = userData.interactionsLimit || 4;
    
    if (current >= limit) {
      return {
        success: false,
        reason: 'interaction_limit_reached',
        current,
        limit,
      };
    }
    
    // Increment counter
    await admin.firestore().collection('users').doc(userId).update({
      interactionsThisMonth: admin.firestore.FieldValue.increment(1),
    });
    
    return {
      success: true,
      current: current + 1,
      limit,
      remaining: limit - current - 1,
    };
    
  } catch (error) {
    console.error('❌ Error tracking interaction:', error);
    throw new HttpsError('internal', error.message);
  }
});

/**
 * Reset monthly interaction counters (scheduled to run on 1st of each month)
 */
exports.resetMonthlyInteractions = onSchedule({
  schedule: '0 0 1 * *', // Midnight on 1st of every month
  timeZone: 'America/New_York',
}, async (event) => {
  try {
    console.log('🔄 Resetting monthly interaction counters...');
    
    const usersSnapshot = await admin.firestore()
      .collection('users')
      .where('subscriptionTier', '==', 'connect')
      .get();
    
    const batch = admin.firestore().batch();
    let resetCount = 0;
    
    for (const doc of usersSnapshot.docs) {
      batch.update(doc.ref, {
        interactionsThisMonth: 0,
        extraInteractionsPurchased: 0,
        interactionsLimit: 4, // Reset to base 4 interactions
      });
      resetCount++;
    }
    
    await batch.commit();
    
    console.log(`✅ Reset interaction counters for ${resetCount} Connect users`);
    return { success: true, resetCount };
    
  } catch (error) {
    console.error('❌ Error resetting monthly interactions:', error);
    throw error;
  }
});

/**
 * Scheduled Account Deletion - GDPR/CCPA Compliance
 * Runs daily at 2 AM UTC to permanently delete accounts that requested deletion 30+ days ago
 */
exports.scheduledAccountDeletion = onSchedule({
  schedule: 'every day 02:00',
  timeZone: 'UTC',
  secrets: []
}, async (event) => {
  console.log('🗑️ Running scheduled account deletion check...');
  
  try {
    const now = admin.firestore.Timestamp.now();
    
    // Find all users scheduled for deletion where the date has passed
    const usersToDelete = await admin.firestore()
      .collection('users')
      .where('deletionScheduledFor', '<=', now)
      .where('deletionRequested', '!=', null)
      .get();
    
    if (usersToDelete.empty) {
      console.log('✅ No accounts scheduled for deletion');
      return { deletedCount: 0 };
    }
    
    console.log(`📋 Found ${usersToDelete.size} accounts to delete`);
    let deletedCount = 0;
    let errors = [];
    
    for (const userDoc of usersToDelete.docs) {
      const userId = userDoc.id;
      const userData = userDoc.data();
      
      try {
        console.log(`🗑️ Deleting account: ${userId} (${userData.email})`);
        
        // Delete all user's journal entries
        const entriesSnapshot = await admin.firestore()
          .collection('journalEntries')
          .where('userId', '==', userId)
          .get();
        
        const entryBatch = admin.firestore().batch();
        entriesSnapshot.docs.forEach(doc => {
          entryBatch.delete(doc.ref);
        });
        await entryBatch.commit();
        console.log(`  ✓ Deleted ${entriesSnapshot.size} journal entries`);
        
        // Delete user's manifest data
        const manifestRef = admin.firestore().collection('manifests').doc(userId);
        const manifestDoc = await manifestRef.get();
        if (manifestDoc.exists) {
          await manifestRef.delete();
          console.log(`  ✓ Deleted manifest data`);
        }
        
        // Delete any practitioner connections
        const practitionersSnapshot = await admin.firestore()
          .collection('practitioners')
          .where('connectedUsers', 'array-contains', userId)
          .get();
        
        const practitionerBatch = admin.firestore().batch();
        practitionersSnapshot.docs.forEach(doc => {
          practitionerBatch.update(doc.ref, {
            connectedUsers: admin.firestore.FieldValue.arrayRemove(userId)
          });
        });
        await practitionerBatch.commit();
        console.log(`  ✓ Removed from ${practitionersSnapshot.size} practitioner connections`);
        
        // Delete user's storage files (if any)
        try {
          const bucket = admin.storage().bucket();
          const [files] = await bucket.getFiles({ prefix: `users/${userId}/` });
          
          if (files.length > 0) {
            await Promise.all(files.map(file => file.delete()));
            console.log(`  ✓ Deleted ${files.length} storage files`);
          }
        } catch (storageError) {
          console.warn(`  ⚠️ Storage deletion error (non-critical): ${storageError.message}`);
        }
        
        // Delete Firebase Auth account
        try {
          await admin.auth().deleteUser(userId);
          console.log(`  ✓ Deleted Firebase Auth account`);
        } catch (authError) {
          console.warn(`  ⚠️ Auth deletion error: ${authError.message}`);
        }
        
        // Finally, delete the user document
        await admin.firestore().collection('users').doc(userId).delete();
        console.log(`  ✓ Deleted user document`);
        
        deletedCount++;
        console.log(`✅ Successfully deleted account: ${userId}`);
        
      } catch (error) {
        console.error(`❌ Error deleting account ${userId}:`, error);
        errors.push({ userId, error: error.message });
      }
    }
    
    console.log(`\n📊 Account Deletion Summary:`);
    console.log(`   Total scheduled: ${usersToDelete.size}`);
    console.log(`   Successfully deleted: ${deletedCount}`);
    console.log(`   Errors: ${errors.length}`);
    
    if (errors.length > 0) {
      console.error('❌ Deletion errors:', JSON.stringify(errors, null, 2));
    }
    
    return { 
      success: true, 
      deletedCount,
      errorCount: errors.length,
      errors 
    };
    
  } catch (error) {
    console.error('❌ Fatal error in scheduled account deletion:', error);
    throw error;
  }
});

/**
 * Expire Gift Code Discounts - Revenue Protection
 * Runs daily at 3 AM UTC to remove expired gift code coupons from Stripe subscriptions
 * Gift codes expire after 3 months OR when user disconnects from practitioner
 */
exports.expireGiftCodeDiscounts = onSchedule({
  schedule: 'every day 03:00',
  timeZone: 'UTC',
  secrets: [STRIPE_SECRET_KEY]
}, async (event) => {
  console.log('⏰ Running gift code expiration check...');
  
  try {
    const stripe = require('stripe')(STRIPE_SECRET_KEY.value());
    const now = new Date();
    
    // Find users with gift codes that have expired
    const usersWithExpiredGifts = await admin.firestore()
      .collection('users')
      .where('giftCodeExpiresAt', '<=', now)
      .where('giftCodeUsed', '!=', null)
      .get();
    
    if (usersWithExpiredGifts.empty) {
      console.log('✅ No expired gift codes to process');
      return { expiredCount: 0 };
    }
    
    console.log(`📋 Found ${usersWithExpiredGifts.size} expired gift codes`);
    let processedCount = 0;
    let errors = [];
    
    for (const userDoc of usersWithExpiredGifts.docs) {
      const userId = userDoc.id;
      const userData = userDoc.data();
      
      try {
        console.log(`⏰ Expiring gift code for user ${userId}`);
        
        // Get the subscription
        const subscriptionId = userData.stripeSubscriptionId;
        if (!subscriptionId) {
          console.log(`⚠️ No subscription found for user ${userId}`);
          continue;
        }
        
        const subscription = await stripe.subscriptions.retrieve(subscriptionId);
        
        // Check if subscription has discount
        if (!subscription.discount) {
          console.log(`ℹ️ User ${userId} has no active discount`);
        } else {
          // Remove the discount
          await stripe.subscriptions.update(subscriptionId, {
            discount: null, // Remove discount
          });
          console.log(`✅ Removed discount from subscription ${subscriptionId}`);
        }
        
        // Check if user has role-based discount to apply instead
        const accountType = userData.accountType || 'standard';
        let newDiscount = null;
        
        if (accountType === 'alpha' && userData.subscriptionTier === 'connect') {
          newDiscount = 25; // 25% for alpha Connect
        } else if (accountType === 'beta' && userData.subscriptionTier === 'connect') {
          newDiscount = 25; // 25% for beta Connect
        } else if (accountType === 'coach') {
          newDiscount = 25; // 25% for coaches
        }
        
        // Apply role-based discount if exists
        if (newDiscount) {
          const coupon = await stripe.coupons.create({
            percent_off: newDiscount,
            duration: 'forever',
            name: `${accountType.charAt(0).toUpperCase() + accountType.slice(1)} Role Discount`,
          });
          
          await stripe.subscriptions.update(subscriptionId, {
            discount: { coupon: coupon.id },
          });
          
          console.log(`✅ Applied ${newDiscount}% role discount for ${accountType} user ${userId}`);
        }
        
        // Update user document
        await admin.firestore().collection('users').doc(userId).update({
          giftCodeUsed: admin.firestore.FieldValue.delete(),
          giftCodeExpiresAt: admin.firestore.FieldValue.delete(),
          giftCodeIssuedBy: admin.firestore.FieldValue.delete(),
          lastDiscountUpdate: admin.firestore.FieldValue.serverTimestamp(),
        });
        
        // TODO: Send email notification about discount expiration
        
        processedCount++;
        
      } catch (error) {
        console.error(`❌ Error expiring gift code for ${userId}:`, error);
        errors.push({ userId, error: error.message });
      }
    }
    
    console.log(`\n📊 Gift Code Expiration Summary:`);
    console.log(`   Total expired: ${usersWithExpiredGifts.size}`);
    console.log(`   Successfully processed: ${processedCount}`);
    console.log(`   Errors: ${errors.length}`);
    
    return { 
      success: true, 
      expiredCount: processedCount,
      errorCount: errors.length,
      errors 
    };
    
  } catch (error) {
    console.error('❌ Fatal error in gift code expiration:', error);
    throw error;
  }
});

/**
 * Check Grace Periods and Auto-Downgrade - Connect Tier Management
 * Runs daily at 4 AM UTC to check users who disconnected from practitioners
 * Sends reminder emails on days 7 and 13, auto-downgrades on day 14
 */
exports.checkGracePeriods = onSchedule({
  schedule: 'every day 04:00',
  timeZone: 'UTC',
  secrets: [STRIPE_SECRET_KEY]
}, async (event) => {
  console.log('⏰ Running grace period check...');
  
  try {
    const stripe = require('stripe')(STRIPE_SECRET_KEY.value());
    const now = new Date();
    
    // Find users with expired grace periods (no practitioner connection)
    const usersInGracePeriod = await admin.firestore()
      .collection('users')
      .where('gracePeriodEndsAt', '!=', null)
      .where('subscriptionTier', '==', 'connect')
      .get();
    
    if (usersInGracePeriod.empty) {
      console.log('✅ No users in grace period');
      return { 
        downgradedCount: 0, 
        remindersSent: 0,
        finalWarningsSent: 0 
      };
    }
    
    console.log(`📋 Found ${usersInGracePeriod.size} users in grace period`);
    
    let downgradedCount = 0;
    let remindersSent = 0;
    let finalWarningsSent = 0;
    let errors = [];
    
    for (const userDoc of usersInGracePeriod.docs) {
      const userId = userDoc.id;
      const userData = userDoc.data();
      const gracePeriodEnds = userData.gracePeriodEndsAt.toDate();
      const daysRemaining = Math.ceil((gracePeriodEnds - now) / (1000 * 60 * 60 * 24));
      
      try {
        // Check if user reconnected to a practitioner
        if (userData.connectedPractitioner) {
          console.log(`✅ User ${userId} reconnected - clearing grace period`);
          await admin.firestore().collection('users').doc(userId).update({
            gracePeriodEndsAt: admin.firestore.FieldValue.delete(),
            practitionerDisconnectedAt: admin.firestore.FieldValue.delete(),
          });
          continue;
        }
        
        // EXPIRED - Auto-downgrade to Plus
        if (daysRemaining <= 0) {
          console.log(`⏬ Auto-downgrading user ${userId} (grace period expired)`);
          
          const subscriptionId = userData.stripeSubscriptionId;
          if (!subscriptionId) {
            console.log(`⚠️ No subscription found for user ${userId}`);
            continue;
          }
          
          // Get current subscription
          const subscription = await stripe.subscriptions.retrieve(subscriptionId);
          
          // Update to Plus tier price
          const plusPriceId = 'price_XXXXXXXX'; // Plus $6.99/mo - UPDATE WITH NEW STRIPE PRICE ID
          
          await stripe.subscriptions.update(subscriptionId, {
            items: [{
              id: subscription.items.data[0].id,
              price: plusPriceId,
            }],
            proration_behavior: 'always_invoice', // Prorate the change
            metadata: {
              ...subscription.metadata,
              downgradedFrom: 'connect',
              downgradedAt: new Date().toISOString(),
              downgradeReason: 'grace_period_expired',
            },
          });
          
          console.log(`✅ Stripe subscription downgraded to Plus`);
          
          // Update Firestore
          await admin.firestore().collection('users').doc(userId).update({
            subscriptionTier: 'plus',
            gracePeriodEndsAt: admin.firestore.FieldValue.delete(),
            practitionerDisconnectedAt: admin.firestore.FieldValue.delete(),
            downgradedAt: admin.firestore.FieldValue.serverTimestamp(),
            downgradedFrom: 'connect',
          });
          
          // Send downgrade confirmation email
          await sendGracePeriodEmail(userId, userData.email, 'downgraded', gracePeriodEnds);
          
          downgradedCount++;
          
        // Day 13 - Final warning (1 day left)
        } else if (daysRemaining === 1 && !userData.finalWarningEmailSent) {
          console.log(`⚠️ Sending final warning to user ${userId} (1 day left)`);
          
          // Send final warning email
          await sendGracePeriodEmail(userId, userData.email, 'final', gracePeriodEnds);
          
          await admin.firestore().collection('users').doc(userId).update({
            finalWarningEmailSent: true,
          });
          
          finalWarningsSent++;
          
        // Day 7 - Mid-grace reminder (7 days left)
        } else if (daysRemaining === 7 && !userData.midGraceReminderSent) {
          console.log(`📬 Sending mid-grace reminder to user ${userId} (7 days left)`);
          
          // Send reminder email
          await sendGracePeriodEmail(userId, userData.email, 'reminder', gracePeriodEnds);
          
          await admin.firestore().collection('users').doc(userId).update({
            midGraceReminderSent: true,
          });
          
          remindersSent++;
        }
        
      } catch (error) {
        console.error(`❌ Error processing grace period for ${userId}:`, error);
        errors.push({ userId, error: error.message });
      }
    }
    
    console.log(`\n📊 Grace Period Summary:`);
    console.log(`   Users checked: ${usersInGracePeriod.size}`);
    console.log(`   Auto-downgrades: ${downgradedCount}`);
    console.log(`   Final warnings sent: ${finalWarningsSent}`);
    console.log(`   Mid-grace reminders: ${remindersSent}`);
    console.log(`   Errors: ${errors.length}`);
    
    return { 
      success: true, 
      downgradedCount,
      finalWarningsSent,
      remindersSent,
      errorCount: errors.length,
      errors 
    };
    
  } catch (error) {
    console.error('❌ Fatal error in grace period check:', error);
    throw error;
  }
});

// Helper function to generate unique gift codes
function generateGiftCode(length = 8) {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // Exclude ambiguous chars
  let code = '';
  for (let i = 0; i < length; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return code;
}
/**
 * Disconnect from Practitioner
 * Handles practitioner disconnection and manages gift code expiration
 * User has 14-day grace period to reconnect before auto-downgrade to Plus
 */
exports.disconnectPractitioner = onCall({
  cors: true,
  secrets: [STRIPE_SECRET_KEY],
}, async (request) => {
  try {
    const userId = request.auth?.uid;
    const { practitionerId } = request.data;
    
    if (!userId) {
      throw new HttpsError('unauthenticated', 'User must be authenticated');
    }
    
    const userDoc = await admin.firestore().collection('users').doc(userId).get();
    const userData = userDoc.data();
    
    console.log(`🔗 User ${userId} disconnecting from practitioner ${practitionerId}`);
    
    // Remove practitioner connection
    await admin.firestore().collection('users').doc(userId).update({
      connectedPractitioner: admin.firestore.FieldValue.delete(),
      practitionerDisconnectedAt: admin.firestore.FieldValue.serverTimestamp(),
      gracePeriodEndsAt: new Date(Date.now() + (14 * 24 * 60 * 60 * 1000)), // 14 days from now
    });
    
    // If user has gift code from this practitioner, expire it immediately
    if (userData.giftCodeIssuedBy === practitionerId) {
      const stripe = require('stripe')(STRIPE_SECRET_KEY.value());
      const subscriptionId = userData.stripeSubscriptionId;
      
      if (subscriptionId) {
        // Remove gift code discount
        const subscription = await stripe.subscriptions.retrieve(subscriptionId);
        
        if (subscription.discount) {
          await stripe.subscriptions.update(subscriptionId, {
            discount: null,
          });
          console.log(`✅ Removed gift code discount from subscription ${subscriptionId}`);
        }
        
        // Apply role-based discount if user qualifies
        const accountType = userData.accountType || 'standard';
        let roleDiscount = null;
        
        if (accountType === 'alpha' && userData.subscriptionTier === 'connect') {
          roleDiscount = 25;
        } else if (accountType === 'beta' && userData.subscriptionTier === 'connect') {
          roleDiscount = 25;
        } else if (accountType === 'coach') {
          roleDiscount = 25;
        }
        
        if (roleDiscount) {
          const coupon = await stripe.coupons.create({
            percent_off: roleDiscount,
            duration: 'forever',
            name: `${accountType.charAt(0).toUpperCase() + accountType.slice(1)} Discount`,
          });
          
          await stripe.subscriptions.update(subscriptionId, {
            discount: { coupon: coupon.id },
          });
          console.log(`✅ Applied ${roleDiscount}% role discount`);
        }
      }
      
      // Clear gift code tracking
      await admin.firestore().collection('users').doc(userId).update({
        giftCodeUsed: admin.firestore.FieldValue.delete(),
        giftCodeExpiresAt: admin.firestore.FieldValue.delete(),
        giftCodeIssuedBy: admin.firestore.FieldValue.delete(),
      });
    }
    
    // Send initial grace period email (Day 0)
    const gracePeriodEndDate = new Date(Date.now() + (14 * 24 * 60 * 60 * 1000));
    await sendGracePeriodEmail(userId, userData.email, 'initial', gracePeriodEndDate);
    
    console.log(`✅ Practitioner disconnected. User has 14-day grace period.`);
    
    return {
      success: true,
      message: 'Practitioner disconnected. You have 14 days to connect with a new practitioner or your account will be downgraded to Plus tier.',
      gracePeriodEndsAt: gracePeriodEndDate.toISOString(),
      canReconnect: true,
    };
    
  } catch (error) {
    console.error('❌ Error disconnecting practitioner:', error);
    throw new HttpsError('internal', error.message);
  }
});
/**
 * Helper: Send Grace Period Email Notifications
 * Sends emails at different stages of the grace period using SendGrid
 */
async function sendGracePeriodEmail(userId, email, type, gracePeriodEnds) {
  try {
    const apiKey = SENDGRID_API_KEY.value();
    if (!apiKey) {
      console.warn('⚠️ SendGrid API key not configured - skipping email');
      return { sent: false, reason: 'no_api_key' };
    }
    
    sgMail.setApiKey(apiKey);
    
    const daysRemaining = Math.ceil((gracePeriodEnds - new Date()) / (1000 * 60 * 60 * 24));
    const appUrl = process.env.APP_URL || 'https://inkwelljournal.io';
    
    let subject, html;
    
    switch (type) {
      case 'initial':
        subject = '🔔 InkWell: Practitioner Disconnected - 14 Day Grace Period';
        html = `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #2A6972;">Your Practitioner Has Been Disconnected</h2>
            <p>Your practitioner connection has been removed from your InkWell Connect subscription.</p>
            
            <div style="background: #FFF9E6; padding: 15px; border-left: 4px solid #FFC107; margin: 20px 0;">
              <strong>⏰ You have 14 days</strong> to reconnect with a practitioner before your Connect subscription is automatically downgraded to Plus tier.
            </div>
            
            <h3>What Happens Next?</h3>
            <ul>
              <li><strong>Connect with a new practitioner</strong> within 14 days to keep your Connect tier</li>
              <li><strong>Manually downgrade</strong> to Plus tier anytime if you prefer</li>
              <li><strong>Auto-downgrade</strong> on ${gracePeriodEnds.toLocaleDateString()} if no action is taken</li>
            </ul>
            
            <div style="margin: 30px 0;">
              <a href="${appUrl}/app.html?action=find_practitioner" style="background: #2A6972; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">Find a New Practitioner</a>
            </div>
            
            <p style="color: #666; font-size: 14px;">Questions? Contact us at <a href="mailto:support@inkwelljournal.io">support@inkwelljournal.io</a></p>
          </div>
        `;
        break;
        
      case 'reminder':
        subject = '⏰ InkWell: 7 Days Left to Reconnect with a Practitioner';
        html = `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #2A6972;">Reminder: 7 Days Remaining</h2>
            <p>You have <strong>${daysRemaining} days left</strong> to connect with a practitioner before your Connect tier is downgraded to Plus.</p>
            
            <div style="background: #FFF9E6; padding: 15px; border-left: 4px solid #FFC107; margin: 20px 0;">
              <strong>Grace period ends:</strong> ${gracePeriodEnds.toLocaleDateString()}
            </div>
            
            <div style="margin: 30px 0;">
              <a href="${appUrl}/app.html?action=find_practitioner" style="background: #2A6972; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">Find a Practitioner Now</a>
            </div>
          </div>
        `;
        break;
        
      case 'final':
        subject = '⚠️ InkWell: FINAL NOTICE - 1 Day to Reconnect';
        html = `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #D32F2F;">⚠️ Final Notice: 1 Day Remaining</h2>
            <p>Your Connect tier will be <strong>downgraded to Plus tomorrow</strong> unless you connect with a practitioner.</p>
            
            <div style="background: #FFEBEE; padding: 15px; border-left: 4px solid #D32F2F; margin: 20px 0;">
              <strong>Last chance!</strong> Downgrade happens on ${gracePeriodEnds.toLocaleDateString()}
            </div>
            
            <div style="margin: 30px 0;">
              <a href="${appUrl}/app.html?action=find_practitioner" style="background: #D32F2F; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">Connect Now</a>
            </div>
          </div>
        `;
        break;
        
      case 'downgraded':
        subject = '✅ InkWell: Your Subscription Has Been Updated';
        html = `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #2A6972;">Your Subscription Has Been Downgraded to Plus</h2>
            <p>Since you didn't reconnect with a practitioner within 14 days, your subscription has been automatically downgraded from Connect to Plus tier.</p>
            
            <h3>You Still Have:</h3>
            <ul style="color: #2A6972;">
              <li>✅ Unlimited AI-powered journaling</li>
              <li>✅ SMS notifications</li>
              <li>✅ All InkWell core features</li>
            </ul>
            
            <p><strong>Want to upgrade back to Connect?</strong><br>You can reconnect with a practitioner anytime to restore your Connect tier benefits.</p>
            
            <div style="margin: 30px 0;">
              <a href="${appUrl}/app.html?action=find_practitioner" style="background: #2A6972; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">Upgrade to Connect</a>
            </div>
          </div>
        `;
        break;
    }
    
    const msg = {
      to: email,
      from: {
        email: 'noreply@inkwelljournal.io',
        name: 'InkWell Journal'
      },
      subject,
      html,
    };
    
    await sgMail.send(msg);
    console.log(`✅ Grace period email sent: ${type} to ${email}`);
    
    return { sent: true, type, email };
    
  } catch (error) {
    console.error('❌ Error sending grace period email:', error);
    return { sent: false, error: error.message, type };
  }
}

/**
 * ============================================================================
 * PRACTITIONER VERIFICATION & APPROVAL SYSTEM
 * ============================================================================
 */

/**
 * Approve a practitioner application
 * Sets accountType to 'coach' (for 25% discount), creates practitioner revenue tracking
 */
exports.approvePractitioner = onCall({ secrets: [SENDGRID_API_KEY] }, async (request) => {
  try {
    const { practitionerId } = request.data;
    const callerId = request.auth?.uid;
    
    if (!callerId) {
      throw new Error('Not authenticated');
    }
    
    // Check if caller is admin via userRole in users collection
    const callerDoc = await admin.firestore().collection('users').doc(callerId).get();
    if (!callerDoc.exists || callerDoc.data().userRole !== 'admin') {
      throw new Error('Unauthorized - admin access required');
    }
    
    if (!practitionerId) {
      throw new Error('practitionerId required');
    }
    
    // Get practitioner application
    const applicationRef = admin.firestore().collection('practitionerApplications').doc(practitionerId);
    const applicationDoc = await applicationRef.get();
    
    if (!applicationDoc.exists) {
      throw new Error('Practitioner application not found');
    }
    
    const applicationData = applicationDoc.data();
    
    // Update user account type to 'coach' for discount eligibility
    await admin.firestore().collection('users').doc(practitionerId).update({
      userRole: 'coach',
      accountType: 'coach',
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });
    
    // Create approved practitioner record
    await admin.firestore().collection('approvedPractitioners').doc(practitionerId).set({
      userId: practitionerId,
      status: 'approved',
      name: applicationData.name || '',
      email: applicationData.email || '',
      credentials: applicationData.credentials || '',
      specialty: applicationData.specialty || '',
      approvedAt: admin.firestore.FieldValue.serverTimestamp(),
      approvedBy: callerId,
      monthlyRevenue: 0,
      totalRevenue: 0,
      activeClients: 0,
    });
    
    // Update application status
    await applicationRef.update({
      status: 'approved',
      approvedAt: admin.firestore.FieldValue.serverTimestamp(),
      approvedBy: callerId,
    });
    
    // Create practitioner revenue tracking document
    await admin.firestore().collection('practitioners').doc(practitionerId).set({
      userId: practitionerId,
      name: applicationData.name || '',
      email: applicationData.email || '',
      monthlyRevenue: 0,
      totalRevenue: 0,
      activeClients: 0,
      joinedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });
    
    // Send approval email
    try {
      const apiKey = SENDGRID_API_KEY.value();
      if (apiKey) {
        sgMail.setApiKey(apiKey);
        
        const appUrl = process.env.APP_URL || 'https://inkwelljournal.io';
        
        const msg = {
          to: applicationData.email,
          from: {
            email: 'noreply@inkwelljournal.io',
            name: 'InkWell Journal'
          },
          subject: '🎉 Welcome to InkWell Practitioner Network!',
          html: `
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
              <h2 style="color: #2A6972;">Congratulations, ${applicationData.name}!</h2>
              <p>Your practitioner application has been <strong>approved</strong>. Welcome to the InkWell Practitioner Network!</p>
              
              <div style="background: #E8F5E9; padding: 15px; border-left: 4px solid #4CAF50; margin: 20px 0;">
                <strong>✅ You're all set!</strong> Your practitioner portal is now active.
              </div>
              
              <h3>What's Next?</h3>
              <ul>
                <li><strong>Access your portal:</strong> Log in to view your dashboard, revenue, and client connections</li>
                <li><strong>Create gift codes:</strong> Generate 50-100% discount codes for your clients</li>
                <li><strong>Track earnings:</strong> Monitor your monthly revenue and download 1099s</li>
                <li><strong>Enjoy 25% off:</strong> Your practitioner discount applies to both Plus and Connect tiers</li>
              </ul>
              
              <div style="margin: 30px 0;">
                <a href="${appUrl}/practitioner-portal.html" style="background: #2A6972; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">Access Your Portal</a>
              </div>
              
              <p style="color: #666; font-size: 14px;">Questions? Contact us at <a href="mailto:support@inkwelljournal.io">support@inkwelljournal.io</a></p>
            </div>
          `,
        };
        
        await sgMail.send(msg);
        console.log(`✅ Approval email sent to ${applicationData.email}`);
      }
    } catch (emailError) {
      console.error('⚠️ Error sending approval email:', emailError);
      // Don't fail the approval if email fails
    }
    
    return {
      success: true,
      practitionerId,
      accountType: 'coach',
      message: 'Practitioner approved successfully',
    };
    
  } catch (error) {
    console.error('❌ Error approving practitioner:', error);
    throw new Error(`Failed to approve practitioner: ${error.message}`);
  }
});

/**
 * Reject a practitioner application
 */
exports.rejectPractitioner = onCall({ secrets: [SENDGRID_API_KEY] }, async (request) => {
  try {
    const { practitionerId, reason } = request.data;
    const callerId = request.auth?.uid;
    
    if (!callerId) {
      throw new Error('Not authenticated');
    }
    
    // Check if caller is admin via userRole in users collection
    const callerDoc = await admin.firestore().collection('users').doc(callerId).get();
    if (!callerDoc.exists || callerDoc.data().userRole !== 'admin') {
      throw new Error('Unauthorized - admin access required');
    }
    
    if (!practitionerId) {
      throw new Error('practitionerId required');
    }
    
    // Get practitioner application
    const applicationRef = admin.firestore().collection('practitionerApplications').doc(practitionerId);
    const applicationDoc = await applicationRef.get();
    
    if (!applicationDoc.exists) {
      throw new Error('Practitioner application not found');
    }
    
    const applicationData = applicationDoc.data();
    
    // Update application status
    await applicationRef.update({
      status: 'rejected',
      rejectedAt: admin.firestore.FieldValue.serverTimestamp(),
      rejectedBy: callerId,
      rejectionReason: reason || 'Application did not meet requirements',
    });
    
    // Send rejection email
    try {
      const apiKey = SENDGRID_API_KEY.value();
      if (apiKey) {
        sgMail.setApiKey(apiKey);
        
        const msg = {
          to: applicationData.email,
          from: {
            email: 'noreply@inkwelljournal.io',
            name: 'InkWell Journal'
          },
          subject: 'InkWell Practitioner Application Update',
          html: `
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
              <h2 style="color: #2A6972;">Thank You for Your Interest</h2>
              <p>Thank you for applying to join the InkWell Practitioner Network. After careful review, we are unable to approve your application at this time.</p>
              
              ${reason ? `<div style="background: #FFF9E6; padding: 15px; border-left: 4px solid #FFC107; margin: 20px 0;">
                <strong>Reason:</strong> ${reason}
              </div>` : ''}
              
              <p>You are welcome to reapply in the future if your qualifications or circumstances change.</p>
              
              <p style="color: #666; font-size: 14px;">Questions? Contact us at <a href="mailto:support@inkwelljournal.io">support@inkwelljournal.io</a></p>
            </div>
          `,
        };
        
        await sgMail.send(msg);
        console.log(`✅ Rejection email sent to ${applicationData.email}`);
      }
    } catch (emailError) {
      console.error('⚠️ Error sending rejection email:', emailError);
    }
    
    return {
      success: true,
      practitionerId,
      message: 'Practitioner rejected',
    };
    
  } catch (error) {
    console.error('❌ Error rejecting practitioner:', error);
    throw new Error(`Failed to reject practitioner: ${error.message}`);
  }
});

/**
 * Get pending practitioner applications (admin only)
 */
exports.getPendingPractitioners = onCall({}, async (request) => {
  try {
    const callerId = request.auth?.uid;
    
    if (!callerId) {
      throw new Error('Not authenticated');
    }
    
    // Check if caller is admin
    const adminDoc = await admin.firestore().collection('admins').doc(callerId).get();
    if (!adminDoc.exists) {
      throw new Error('Unauthorized - admin access required');
    }
    
    // Get pending applications
    const pendingSnapshot = await admin.firestore()
      .collection('practitionerApplications')
      .where('status', '==', 'pending')
      .orderBy('createdAt', 'desc')
      .get();
    
    const applications = [];
    pendingSnapshot.forEach(doc => {
      applications.push({
        id: doc.id,
        ...doc.data(),
      });
    });
    
    return { success: true, applications };
    
  } catch (error) {
    console.error('❌ Error getting pending practitioners:', error);
    throw new Error(`Failed to get pending practitioners: ${error.message}`);
  }
});

// ===================================================================
// STRIPE CONNECT - Practitioner Payment Integration
// ===================================================================

/**
 * Create Stripe Connect Express account for practitioner
 * This allows practitioners to receive their $30/month revenue share
 */
exports.createStripeConnectAccount = onCall(
  {
    secrets: [STRIPE_SECRET_KEY],
    region: "us-central1",
  },
  async (request) => {
    const userId = request.auth?.uid;
    
    if (!userId) {
      throw new HttpsError('unauthenticated', 'User must be authenticated');
    }

    try {
      console.log('🏦 Creating Stripe Connect account for user:', userId);
      
      // Verify user is an approved practitioner
      const userDoc = await admin.firestore().collection('users').doc(userId).get();
      const userData = userDoc.data();
      
      if (!userData) {
        throw new HttpsError('not-found', 'User profile not found');
      }
      
      const isPractitioner = userData.accountType === 'coach' || userData.userRole === 'coach';
      if (!isPractitioner) {
        throw new HttpsError('permission-denied', 'Only approved practitioners can create Connect accounts');
      }
      
      // Check if Connect account already exists
      const practitionerDoc = await admin.firestore()
        .collection('practitioners')
        .doc(userId)
        .get();
      
      if (practitionerDoc.exists && practitionerDoc.data().stripeConnectAccountId) {
        console.log('✅ Connect account already exists:', practitionerDoc.data().stripeConnectAccountId);
        return {
          success: true,
          accountId: practitionerDoc.data().stripeConnectAccountId,
          alreadyExists: true
        };
      }
      
      // Initialize Stripe
      const stripe = require('stripe')(STRIPE_SECRET_KEY.value());
      
      // Create Stripe Connect Express account
      const account = await stripe.accounts.create({
        type: 'express',
        country: 'US',
        email: userData.email,
        capabilities: {
          card_payments: { requested: true },
          transfers: { requested: true },
        },
        business_type: 'individual',
        metadata: {
          practitionerId: userId,
          practitionerEmail: userData.email,
          practitionerName: userData.displayName || userData.email,
        }
      });
      
      console.log('✅ Created Stripe Connect account:', account.id);
      
      // Store Connect account ID in practitioners collection
      await admin.firestore().collection('practitioners').doc(userId).set({
        stripeConnectAccountId: account.id,
        stripeConnectStatus: 'pending',
        stripeConnectCreatedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      
      return {
        success: true,
        accountId: account.id,
        alreadyExists: false
      };
      
    } catch (error) {
      console.error('❌ Error creating Stripe Connect account:', error);
      throw new HttpsError('internal', `Failed to create Connect account: ${error.message}`);
    }
  }
);

/**
 * Generate Stripe Connect onboarding link for practitioner
 * Returns URL to complete Express account setup
 */
exports.createStripeConnectOnboardingLink = onCall(
  {
    secrets: [STRIPE_SECRET_KEY],
    region: "us-central1",
  },
  async (request) => {
    const userId = request.auth?.uid;
    
    if (!userId) {
      throw new HttpsError('unauthenticated', 'User must be authenticated');
    }

    try {
      console.log('🔗 Creating onboarding link for user:', userId);
      
      // Get practitioner's Connect account ID
      const practitionerDoc = await admin.firestore()
        .collection('practitioners')
        .doc(userId)
        .get();
      
      if (!practitionerDoc.exists || !practitionerDoc.data().stripeConnectAccountId) {
        throw new HttpsError('failed-precondition', 'No Connect account found. Create account first.');
      }
      
      const accountId = practitionerDoc.data().stripeConnectAccountId;
      
      // Initialize Stripe
      const stripe = require('stripe')(STRIPE_SECRET_KEY.value());
      
      // Create account link for onboarding
      const accountLink = await stripe.accountLinks.create({
        account: accountId,
        refresh_url: 'https://inkwelljournal.io/practitioner-portal.html?refresh=true',
        return_url: 'https://inkwelljournal.io/practitioner-portal.html?setup=complete',
        type: 'account_onboarding',
      });
      
      console.log('✅ Created onboarding link:', accountLink.url);
      
      return {
        success: true,
        url: accountLink.url
      };
      
    } catch (error) {
      console.error('❌ Error creating onboarding link:', error);
      throw new HttpsError('internal', `Failed to create onboarding link: ${error.message}`);
    }
  }
);

/**
 * Check Stripe Connect account status
 * Returns whether account is fully onboarded and can receive payouts
 */
exports.checkStripeConnectStatus = onCall(
  {
    secrets: [STRIPE_SECRET_KEY],
    region: "us-central1",
  },
  async (request) => {
    const userId = request.auth?.uid;
    
    if (!userId) {
      throw new HttpsError('unauthenticated', 'User must be authenticated');
    }

    try {
      console.log('🔍 Checking Connect status for user:', userId);
      
      // Get practitioner's Connect account ID
      const practitionerDoc = await admin.firestore()
        .collection('practitioners')
        .doc(userId)
        .get();
      
      if (!practitionerDoc.exists || !practitionerDoc.data().stripeConnectAccountId) {
        return {
          hasAccount: false,
          isComplete: false
        };
      }
      
      const accountId = practitionerDoc.data().stripeConnectAccountId;
      
      // Initialize Stripe
      const stripe = require('stripe')(STRIPE_SECRET_KEY.value());
      
      // Retrieve account details
      const account = await stripe.accounts.retrieve(accountId);
      
      // Check if charges and payouts are enabled
      const isComplete = account.charges_enabled && account.payouts_enabled;
      
      console.log(`✅ Connect status - Charges: ${account.charges_enabled}, Payouts: ${account.payouts_enabled}`);
      
      // Update Firestore with current status
      await admin.firestore().collection('practitioners').doc(userId).update({
        stripeConnectStatus: isComplete ? 'active' : 'pending',
        stripeConnectChargesEnabled: account.charges_enabled,
        stripeConnectPayoutsEnabled: account.payouts_enabled,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      
      return {
        hasAccount: true,
        isComplete: isComplete,
        chargesEnabled: account.charges_enabled,
        payoutsEnabled: account.payouts_enabled,
        detailsSubmitted: account.details_submitted,
      };
      
    } catch (error) {
      console.error('❌ Error checking Connect status:', error);
      throw new HttpsError('internal', `Failed to check Connect status: ${error.message}`);
    }
  }
);

// =============================================================================
// ADMIN: UPGRADE ALL USERS TO PLUS (BETA TESTING)
// =============================================================================

/**
 * HTTP endpoint to upgrade all users to Plus tier
 * This is a one-time migration function for beta testing
 * 
 * Usage: curl -X POST https://us-central1-inkwell-alpha.cloudfunctions.net/upgradeAllUsersToPlus?key=ADMIN_SECRET
 */
exports.upgradeAllUsersToPlus = onRequest({
  cors: true,
  timeoutSeconds: 540, // 9 minutes for large user bases
}, async (req, res) => {
  // Simple security check - require a secret key
  const providedKey = req.query.key || req.body?.key;
  const expectedKey = 'inkwell-beta-upgrade-2026'; // Simple key for this one-time operation
  
  if (providedKey !== expectedKey) {
    res.status(403).json({ error: 'Invalid key' });
    return;
  }
  
  console.log('🚀 Starting bulk upgrade to Plus tier...');
  
  try {
    const usersSnapshot = await admin.firestore().collection('users').get();
    
    if (usersSnapshot.empty) {
      res.json({ success: true, message: 'No users found', count: 0 });
      return;
    }
    
    console.log(`Found ${usersSnapshot.size} users to upgrade`);
    
    let successCount = 0;
    let errorCount = 0;
    const errors = [];
    
    // Process in batches of 500 (Firestore batch limit)
    const batchSize = 500;
    let batch = admin.firestore().batch();
    let batchCount = 0;
    
    for (const userDoc of usersSnapshot.docs) {
      const userRef = admin.firestore().collection('users').doc(userDoc.id);
      
      batch.update(userRef, {
        subscriptionTier: 'plus',
        subscriptionStatus: 'active',
        'betaProgress.tierOverride': {
          tier: 'plus',
          setAt: admin.firestore.FieldValue.serverTimestamp(),
          setBy: 'admin-migration-api'
        }
      });
      
      batchCount++;
      
      // Commit batch when it reaches the limit
      if (batchCount >= batchSize) {
        try {
          await batch.commit();
          successCount += batchCount;
          console.log(`✅ Committed batch of ${batchCount} users (total: ${successCount})`);
        } catch (batchError) {
          errorCount += batchCount;
          errors.push(`Batch failed: ${batchError.message}`);
          console.error(`❌ Batch failed:`, batchError);
        }
        
        // Reset batch
        batch = admin.firestore().batch();
        batchCount = 0;
      }
    }
    
    // Commit remaining users
    if (batchCount > 0) {
      try {
        await batch.commit();
        successCount += batchCount;
        console.log(`✅ Committed final batch of ${batchCount} users`);
      } catch (batchError) {
        errorCount += batchCount;
        errors.push(`Final batch failed: ${batchError.message}`);
        console.error(`❌ Final batch failed:`, batchError);
      }
    }
    
    const result = {
      success: true,
      message: 'Bulk upgrade complete',
      totalUsers: usersSnapshot.size,
      successCount,
      errorCount,
      errors: errors.length > 0 ? errors : undefined
    };
    
    console.log('✅ Migration complete:', result);
    res.json(result);
    
  } catch (error) {
    console.error('❌ Migration failed:', error);
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// =============================================================================
// ADMIN: UPGRADE ALL USERS TO CONNECT (FINAL BETA PHASE)
// =============================================================================

/**
 * HTTP endpoint to upgrade all users to Connect tier
 * This is for the final beta testing phase - unlocks coach access
 * 
 * Usage: curl -X POST https://us-central1-inkwell-alpha.cloudfunctions.net/upgradeAllUsersToConnect?key=ADMIN_SECRET
 */
exports.upgradeAllUsersToConnect = onRequest({
  cors: true,
  timeoutSeconds: 540, // 9 minutes for large user bases
}, async (req, res) => {
  // Simple security check - require a secret key
  const providedKey = req.query.key || req.body?.key;
  const expectedKey = 'inkwell-beta-connect-2026'; // Key for Connect upgrade
  
  if (providedKey !== expectedKey) {
    res.status(403).json({ error: 'Invalid key' });
    return;
  }
  
  console.log('🚀 Starting bulk upgrade to Connect tier (final beta phase)...');
  
  try {
    const usersSnapshot = await admin.firestore().collection('users').get();
    
    if (usersSnapshot.empty) {
      res.json({ success: true, message: 'No users found', count: 0 });
      return;
    }
    
    console.log(`Found ${usersSnapshot.size} users to upgrade to Connect`);
    
    let successCount = 0;
    let errorCount = 0;
    const errors = [];
    
    // Process in batches of 500 (Firestore batch limit)
    const batchSize = 500;
    let batch = admin.firestore().batch();
    let batchCount = 0;
    
    for (const userDoc of usersSnapshot.docs) {
      const userRef = admin.firestore().collection('users').doc(userDoc.id);
      
      batch.update(userRef, {
        subscriptionTier: 'connect',
        subscriptionStatus: 'active',
        'betaProgress.tierOverride': {
          tier: 'connect',
          setAt: admin.firestore.FieldValue.serverTimestamp(),
          setBy: 'admin-connect-migration-api'
        }
      });
      
      batchCount++;
      
      // Commit batch when it reaches the limit
      if (batchCount >= batchSize) {
        try {
          await batch.commit();
          successCount += batchCount;
          console.log(`✅ Committed batch of ${batchCount} users to Connect (total: ${successCount})`);
        } catch (batchError) {
          errorCount += batchCount;
          errors.push(`Batch failed: ${batchError.message}`);
          console.error(`❌ Batch failed:`, batchError);
        }
        
        // Reset batch
        batch = admin.firestore().batch();
        batchCount = 0;
      }
    }
    
    // Commit remaining users
    if (batchCount > 0) {
      try {
        await batch.commit();
        successCount += batchCount;
        console.log(`✅ Committed final batch of ${batchCount} users to Connect`);
      } catch (batchError) {
        errorCount += batchCount;
        errors.push(`Final batch failed: ${batchError.message}`);
        console.error(`❌ Final batch failed:`, batchError);
      }
    }
    
    const result = {
      success: true,
      message: 'Bulk upgrade to Connect complete - coach access unlocked!',
      totalUsers: usersSnapshot.size,
      successCount,
      errorCount,
      errors: errors.length > 0 ? errors : undefined
    };
    
    console.log('✅ Connect migration complete:', result);
    res.json(result);
    
  } catch (error) {
    console.error('❌ Connect migration failed:', error);
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// =============================================================================
// ADMIN: ASSIGN ALL USERS TO COACH HOLLIS VERDANT (BETA TESTING)
// =============================================================================

/**
 * HTTP endpoint to assign all users to coach Hollis Verdant
 * This is for beta testing - gives everyone access to coach features
 * 
 * Usage: curl -X POST "https://us-central1-inkwell-alpha.cloudfunctions.net/assignAllUsersToHollis?key=ADMIN_SECRET"
 */
exports.assignAllUsersToHollis = onRequest({
  cors: true,
  timeoutSeconds: 540, // 9 minutes for large user bases
}, async (req, res) => {
  // Simple security check - require a secret key
  const providedKey = req.query.key || req.body?.key;
  const expectedKey = 'inkwell-beta-hollis-2026'; // Key for Hollis assignment
  
  if (providedKey !== expectedKey) {
    res.status(403).json({ error: 'Invalid key' });
    return;
  }
  
  console.log('🎯 Starting bulk assignment to coach Hollis Verdant...');
  
  try {
    const usersSnapshot = await admin.firestore().collection('users').get();
    
    if (usersSnapshot.empty) {
      res.json({ success: true, message: 'No users found', count: 0 });
      return;
    }
    
    console.log(`Found ${usersSnapshot.size} users to assign to Hollis Verdant`);
    
    let successCount = 0;
    let errorCount = 0;
    let alreadyConnectedCount = 0;
    const errors = [];
    
    // Process in batches of 500 (Firestore batch limit)
    const batchSize = 500;
    let batch = admin.firestore().batch();
    let batchCount = 0;
    
    for (const userDoc of usersSnapshot.docs) {
      const userData = userDoc.data();
      
      // Skip if already connected to a practitioner (optional - remove if you want to overwrite)
      // if (userData.connectedPractitioner) {
      //   alreadyConnectedCount++;
      //   continue;
      // }
      
      const userRef = admin.firestore().collection('users').doc(userDoc.id);
      
      batch.update(userRef, {
        connectedPractitioner: {
          email: 'coach@inkwelljournal.io',
          name: 'Hollis Verdant',
          practitionerId: 'ZiNM7YK1jnRgIkAKiCaO1lC6DGx2',
          connectedAt: admin.firestore.FieldValue.serverTimestamp(),
          connectionType: 'beta_assignment'
        },
        practitioners: admin.firestore.FieldValue.arrayUnion('ZiNM7YK1jnRgIkAKiCaO1lC6DGx2')
      });
      
      batchCount++;
      
      // Commit batch when it reaches the limit
      if (batchCount >= batchSize) {
        try {
          await batch.commit();
          successCount += batchCount;
          console.log(`✅ Committed batch of ${batchCount} users to Hollis (total: ${successCount})`);
        } catch (batchError) {
          errorCount += batchCount;
          errors.push(`Batch failed: ${batchError.message}`);
          console.error(`❌ Batch failed:`, batchError);
        }
        
        // Reset batch
        batch = admin.firestore().batch();
        batchCount = 0;
      }
    }
    
    // Commit remaining users
    if (batchCount > 0) {
      try {
        await batch.commit();
        successCount += batchCount;
        console.log(`✅ Committed final batch of ${batchCount} users to Hollis`);
      } catch (batchError) {
        errorCount += batchCount;
        errors.push(`Final batch failed: ${batchError.message}`);
        console.error(`❌ Final batch failed:`, batchError);
      }
    }
    
    const result = {
      success: true,
      message: 'All users assigned to coach Hollis Verdant!',
      totalUsers: usersSnapshot.size,
      successCount,
      errorCount,
      alreadyConnectedCount,
      errors: errors.length > 0 ? errors : undefined
    };
    
    console.log('✅ Hollis assignment complete:', result);
    res.json(result);
    
  } catch (error) {
    console.error('❌ Hollis assignment failed:', error);
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

/**
 * Monthly Insurance Expiration Check
 * Runs on the 1st of every month at 9 AM ET
 * - Finds coaches with insurance expiring within 60 days
 * - Sends warning emails to coaches
 * - Updates admin portal with expiring insurance list
 */
exports.checkExpiringInsurance = onSchedule({
  schedule: '0 9 1 * *', // 9 AM on 1st of every month
  timeZone: 'America/New_York',
  secrets: [SENDGRID_API_KEY]
}, async (event) => {
  console.log('🔍 Running monthly insurance expiration check...');
  
  try {
    sgMail.setApiKey(SENDGRID_API_KEY.value());
    
    const now = new Date();
    const sixtyDaysFromNow = new Date(now.getTime() + (60 * 24 * 60 * 60 * 1000));
    
    // Find all approved coaches
    const coachesSnapshot = await admin.firestore()
      .collection('users')
      .where('userRole', '==', 'practitioner')
      .get();
    
    console.log(`📋 Found ${coachesSnapshot.size} coaches to check`);
    
    const expiringCoaches = [];
    const warningsSent = [];
    const errors = [];
    
    for (const coachDoc of coachesSnapshot.docs) {
      const coach = coachDoc.data();
      
      // Get their latest insurance record
      const insuranceSnapshot = await admin.firestore()
        .collection('coachInsuranceRecords')
        .where('coachId', '==', coachDoc.id)
        .where('status', '==', 'current')
        .orderBy('uploadedAt', 'desc')
        .limit(1)
        .get();
      
      if (insuranceSnapshot.empty) {
        console.log(`⚠️ No insurance record found for coach ${coach.email}`);
        continue;
      }
      
      const insuranceData = insuranceSnapshot.docs[0].data();
      const expirationDate = insuranceData.expirationDate ? new Date(insuranceData.expirationDate) : null;
      
      if (!expirationDate) {
        console.log(`⚠️ No expiration date for coach ${coach.email}`);
        continue;
      }
      
      // Check if expiring within 60 days
      if (expirationDate <= sixtyDaysFromNow) {
        const daysUntilExpiration = Math.ceil((expirationDate - now) / (24 * 60 * 60 * 1000));
        
        expiringCoaches.push({
          coachId: coachDoc.id,
          email: coach.email,
          name: coach.displayName || coach.email,
          expirationDate: expirationDate.toISOString(),
          daysUntilExpiration
        });
        
        // Send warning email if not already sent this month
        const lastWarningKey = `insuranceWarning_${now.getFullYear()}_${now.getMonth()}`;
        if (!coach[lastWarningKey]) {
          try {
            const msg = {
              to: coach.email,
              from: {
                email: 'support@inkwelljournal.io',
                name: 'InkWell Support'
              },
              subject: `Action Required: Your Insurance Expires in ${daysUntilExpiration} Days`,
              html: `
                <div style="font-family: 'Georgia', serif; max-width: 600px; margin: 0 auto; padding: 20px;">
                  <div style="background: linear-gradient(135deg, #2A6972 0%, #1e4d54 100%); padding: 30px; border-radius: 12px 12px 0 0; text-align: center;">
                    <h1 style="color: white; margin: 0; font-size: 28px;">Insurance Update Required</h1>
                  </div>
                  <div style="background: #f9f6f1; padding: 30px; border-radius: 0 0 12px 12px;">
                    <p style="color: #2d3748; font-size: 16px; line-height: 1.6;">
                      Hi ${coach.displayName || 'Coach'},
                    </p>
                    <p style="color: #2d3748; font-size: 16px; line-height: 1.6;">
                      Our records show your coaching insurance will expire on <strong>${expirationDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })}</strong> 
                      (${daysUntilExpiration} days from now).
                    </p>
                    <p style="color: #2d3748; font-size: 16px; line-height: 1.6;">
                      To maintain your active coach status and continue receiving client connections, please update your insurance documentation before it expires.
                    </p>
                    <div style="text-align: center; margin: 30px 0;">
                      <a href="https://inkwelljournal.io/coach.html" 
                         style="background: #2A6972; color: white; padding: 14px 28px; text-decoration: none; border-radius: 8px; font-weight: bold;">
                        Update Insurance Now
                      </a>
                    </div>
                    <p style="color: #718096; font-size: 14px; margin-top: 20px;">
                      If you have questions, reply to this email and we'll help you through the process.
                    </p>
                    <p style="color: #2d3748; font-size: 16px; margin-top: 20px;">
                      Best,<br>
                      <strong>The InkWell Team</strong>
                    </p>
                  </div>
                </div>
              `
            };
            
            await sgMail.send(msg);
            warningsSent.push(coach.email);
            
            // Mark that we sent warning this month
            await coachDoc.ref.update({
              [lastWarningKey]: admin.firestore.FieldValue.serverTimestamp()
            });
            
            console.log(`📧 Sent expiration warning to ${coach.email}`);
          } catch (emailError) {
            console.error(`❌ Failed to send warning to ${coach.email}:`, emailError);
            errors.push({ email: coach.email, error: emailError.message });
          }
        }
      }
    }
    
    // Store expiring coaches list for admin portal
    if (expiringCoaches.length > 0) {
      await admin.firestore().collection('adminReports').doc('expiringInsurance').set({
        coaches: expiringCoaches,
        lastChecked: admin.firestore.FieldValue.serverTimestamp(),
        totalExpiring: expiringCoaches.length
      });
    }
    
    const result = {
      success: true,
      totalCoachesChecked: coachesSnapshot.size,
      expiringCount: expiringCoaches.length,
      warningsSent: warningsSent.length,
      errors: errors.length > 0 ? errors : undefined
    };
    
    console.log('✅ Insurance check complete:', result);
    return result;
    
  } catch (error) {
    console.error('❌ Error checking insurance expirations:', error);
    throw error;
  }
});

/**
 * Manual trigger for insurance expiration check (for admin testing)
 */
exports.triggerInsuranceCheck = onCall({
  secrets: [SENDGRID_API_KEY]
}, async (request) => {
  // Verify admin
  if (!request.auth) {
    throw new HttpsError('unauthenticated', 'Must be authenticated');
  }
  
  const userDoc = await admin.firestore().collection('users').doc(request.auth.uid).get();
  if (!userDoc.exists || userDoc.data().userRole !== 'admin') {
    throw new HttpsError('permission-denied', 'Must be admin');
  }
  
  console.log('🔧 Admin triggered manual insurance check');
  
  // Run the same logic as scheduled function
  sgMail.setApiKey(SENDGRID_API_KEY.value());
  
  const now = new Date();
  const sixtyDaysFromNow = new Date(now.getTime() + (60 * 24 * 60 * 60 * 1000));
  
  const coachesSnapshot = await admin.firestore()
    .collection('users')
    .where('userRole', '==', 'practitioner')
    .get();
  
  const expiringCoaches = [];
  
  for (const coachDoc of coachesSnapshot.docs) {
    const coach = coachDoc.data();
    
    const insuranceSnapshot = await admin.firestore()
      .collection('coachInsuranceRecords')
      .where('coachId', '==', coachDoc.id)
      .where('status', '==', 'current')
      .orderBy('uploadedAt', 'desc')
      .limit(1)
      .get();
    
    if (!insuranceSnapshot.empty) {
      const insuranceData = insuranceSnapshot.docs[0].data();
      const expirationDate = insuranceData.expirationDate ? new Date(insuranceData.expirationDate) : null;
      
      if (expirationDate && expirationDate <= sixtyDaysFromNow) {
        const daysUntilExpiration = Math.ceil((expirationDate - now) / (24 * 60 * 60 * 1000));
        
        expiringCoaches.push({
          coachId: coachDoc.id,
          email: coach.email,
          name: coach.displayName || coach.email,
          expirationDate: expirationDate.toISOString(),
          daysUntilExpiration
        });
      }
    }
  }
  
  // Update admin report
  await admin.firestore().collection('adminReports').doc('expiringInsurance').set({
    coaches: expiringCoaches,
    lastChecked: admin.firestore.FieldValue.serverTimestamp(),
    totalExpiring: expiringCoaches.length,
    triggeredBy: request.auth.uid
  });
  
  return {
    success: true,
    expiringCoaches,
    totalChecked: coachesSnapshot.size
  };
});

// ========================================
// SMS DEDUPLICATION CLEANUP
// ========================================

/**
 * Monthly SMS Deduplication Cleanup
 * Runs on the 1st of every month at 3 AM ET
 * - Deletes smsDeduplication records older than 30 days
 * - Keeps the collection from growing indefinitely
 */
exports.cleanupSmsDeduplication = onSchedule({
  schedule: '0 3 1 * *', // 3 AM on 1st of every month
  timeZone: 'America/New_York'
}, async (event) => {
  console.log('🧹 Running monthly SMS deduplication cleanup...');
  
  try {
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    
    // Query for old records
    const oldRecordsSnapshot = await admin.firestore()
      .collection('smsDeduplication')
      .where('sentAt', '<', thirtyDaysAgo)
      .get();
    
    if (oldRecordsSnapshot.empty) {
      console.log('✅ No old deduplication records to clean up');
      return { deleted: 0 };
    }
    
    console.log(`🗑️ Found ${oldRecordsSnapshot.size} records older than 30 days`);
    
    // Delete in batches of 500 (Firestore limit)
    const batchSize = 500;
    let deleted = 0;
    let batch = admin.firestore().batch();
    let batchCount = 0;
    
    for (const doc of oldRecordsSnapshot.docs) {
      batch.delete(doc.ref);
      batchCount++;
      deleted++;
      
      if (batchCount >= batchSize) {
        await batch.commit();
        console.log(`🗑️ Deleted batch of ${batchCount} records`);
        batch = admin.firestore().batch();
        batchCount = 0;
      }
    }
    
    // Commit remaining
    if (batchCount > 0) {
      await batch.commit();
      console.log(`🗑️ Deleted final batch of ${batchCount} records`);
    }
    
    console.log(`✅ SMS deduplication cleanup complete. Deleted ${deleted} old records.`);
    
    // Log cleanup in admin reports
    await admin.firestore().collection('adminReports').doc('smsDeduplicationCleanup').set({
      lastRun: admin.firestore.FieldValue.serverTimestamp(),
      recordsDeleted: deleted,
      cutoffDate: thirtyDaysAgo.toISOString()
    });
    
    return { deleted };
    
  } catch (error) {
    console.error('❌ SMS deduplication cleanup error:', error);
    throw error;
  }
});

/**
 * Admin-callable function to manually trigger SMS deduplication cleanup
 */
exports.triggerSmsDeduplicationCleanup = onCall({
  secrets: []
}, async (request) => {
  // Verify admin
  if (!request.auth) {
    throw new HttpsError('unauthenticated', 'Must be logged in');
  }
  
  const userDoc = await admin.firestore().collection('users').doc(request.auth.uid).get();
  const userData = userDoc.data();
  
  if (!userData || userData.userRole !== 'admin') {
    throw new HttpsError('permission-denied', 'Must be admin');
  }
  
  console.log('🔧 Admin triggered manual SMS deduplication cleanup');
  
  const daysToKeep = request.data?.daysToKeep || 30;
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);
  
  // Get all records (for count) then filter old ones
  const allRecordsSnapshot = await admin.firestore()
    .collection('smsDeduplication')
    .get();
  
  const oldRecordsSnapshot = await admin.firestore()
    .collection('smsDeduplication')
    .where('sentAt', '<', cutoffDate)
    .get();
  
  if (oldRecordsSnapshot.empty) {
    return {
      success: true,
      deleted: 0,
      totalRecords: allRecordsSnapshot.size,
      message: `No records older than ${daysToKeep} days found`
    };
  }
  
  // Delete in batches
  const batchSize = 500;
  let deleted = 0;
  let batch = admin.firestore().batch();
  let batchCount = 0;
  
  for (const doc of oldRecordsSnapshot.docs) {
    batch.delete(doc.ref);
    batchCount++;
    deleted++;
    
    if (batchCount >= batchSize) {
      await batch.commit();
      batch = admin.firestore().batch();
      batchCount = 0;
    }
  }
  
  if (batchCount > 0) {
    await batch.commit();
  }
  
  // Log cleanup
  await admin.firestore().collection('adminReports').doc('smsDeduplicationCleanup').set({
    lastRun: admin.firestore.FieldValue.serverTimestamp(),
    recordsDeleted: deleted,
    cutoffDate: cutoffDate.toISOString(),
    triggeredBy: request.auth.uid
  });
  
  return {
    success: true,
    deleted,
    totalRecordsBefore: allRecordsSnapshot.size,
    totalRecordsAfter: allRecordsSnapshot.size - deleted,
    cutoffDate: cutoffDate.toISOString()
  };
});
// =============================================================================
// TEST PUSH NOTIFICATION - For debugging push setup
// =============================================================================

/**
 * Send a test push notification to verify FCM is working
 * Call via: firebase functions:call testPushNotification --data '{"userId":"YOUR_USER_ID"}'
 * Or from admin console
 */
exports.testPushNotification = onCall(async (request) => {
  const { userId } = request.data;
  
  if (!userId) {
    throw new HttpsError('invalid-argument', 'userId is required');
  }
  
  // Get user document
  const userDoc = await admin.firestore().collection('users').doc(userId).get();
  
  if (!userDoc.exists) {
    return { success: false, error: 'User not found' };
  }
  
  const userData = userDoc.data();
  
  console.log(`🔍 User ${userId} data:`, {
    fcmToken: userData.fcmToken ? `${userData.fcmToken.substring(0, 20)}...` : 'NOT SET',
    platform: userData.platform,
    lastTokenUpdate: userData.lastTokenUpdate,
    pushPreferences: userData.pushPreferences
  });
  
  if (!userData.fcmToken) {
    return { 
      success: false, 
      error: 'No FCM token found for this user',
      debug: {
        hasPushPreferences: !!userData.pushPreferences,
        pushEnabled: userData.pushPreferences?.enabled
      }
    };
  }
  
  // Try to send test notification
  try {
    const result = await sendPushNotification(
      userData.fcmToken,
      '🔔 Test Notification',
      'If you see this, push notifications are working!',
      { type: 'test', timestamp: Date.now().toString() }
    );
    
    return { 
      success: result, 
      message: result ? 'Push notification sent successfully!' : 'Failed to send push notification',
      fcmTokenPrefix: userData.fcmToken.substring(0, 30) + '...'
    };
  } catch (error) {
    return { 
      success: false, 
      error: error.message,
      code: error.code
    };
  }
});

// =============================================================================
// WISH MILESTONE NOTIFICATIONS - Scheduled daily check
// =============================================================================
/**
 * Scheduled function to check WISH progress and send milestone notifications
 * Runs once daily at 10 AM UTC, checks all users' WISH progress
 * Sends SMS and/or push at 25%, 50%, 75%, 100% completion
 * Gentle reminders - not daily nags
 */
exports.scheduledWishMilestones = onSchedule({
  schedule: 'every day 10:00',
  timeZone: 'UTC',
  secrets: [TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]
}, async (event) => {
  console.log('🎯 Running scheduled WISH milestone check...');
  
  const now = new Date();
  const milestones = [
    { percent: 25, key: 'quarter', emoji: '🌱' },
    { percent: 50, key: 'half', emoji: '🍀' },
    { percent: 75, key: 'three-quarters', emoji: '🌿' },
    { percent: 100, key: 'complete', emoji: '🌳' }
  ];
  
  // Get all users who might have active WISHes
  const usersSnapshot = await admin.firestore().collection('users').get();
  
  let smsSentCount = 0;
  let pushSentCount = 0;
  let skippedCount = 0;
  let noWishCount = 0;
  
  for (const userDoc of usersSnapshot.docs) {
    const userData = userDoc.data();
    const userId = userDoc.id;
    
    try {
      // Check notification preferences
      const hasSmsSetup = userData.smsOptIn && userData.phoneNumber;
      const hasPushSetup = userData.fcmToken && userData.pushPreferences?.enabled;
      
      // Check SMS access (Plus/Connect tier or beta tester)
      const tier = userData.subscriptionTier || 'free';
      const specialCode = userData.special_code || '';
      const isBetaTester = ['alpha', 'beta'].includes(specialCode);
      const hasSmsAccess = hasSmsSetup && (['plus', 'connect'].includes(tier) || isBetaTester);
      
      // Check if user wants WISH milestone notifications
      const wantsSms = hasSmsAccess && userData.smsPreferences?.wishMilestones !== false;
      const wantsPush = hasPushSetup && userData.pushPreferences?.wishMilestones !== false;
      
      if (!wantsSms && !wantsPush) {
        skippedCount++;
        continue;
      }
      
      // Get user's manifest
      const manifestDoc = await admin.firestore().collection('manifests').doc(userId).get();
      
      if (!manifestDoc.exists) {
        noWishCount++;
        continue;
      }
      
      const manifest = manifestDoc.data();
      
      // Check if WISH is active (has start date and timeline)
      if (!manifest.startDate || !manifest.timelineDays) {
        noWishCount++;
        continue;
      }
      
      // Calculate progress
      const startDate = new Date(manifest.startDate);
      const daysElapsed = Math.floor((now.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24));
      const totalDays = manifest.timelineDays;
      const progressPercent = Math.min((daysElapsed / totalDays) * 100, 100);
      
      // Skip if WISH is in the past (over 100% + buffer) - old completed WISH
      if (daysElapsed > totalDays + 7) {
        continue;
      }
      
      // Get milestones already sent for this manifest
      const milestonesSent = manifest.milestonesSent || [];
      
      // Find the current milestone that should be sent
      let milestoneToSend = null;
      for (const milestone of milestones) {
        if (progressPercent >= milestone.percent && !milestonesSent.includes(milestone.key)) {
          milestoneToSend = milestone;
          break; // Only send ONE milestone per day max
        }
      }
      
      if (!milestoneToSend) {
        continue; // No new milestone to send
      }
      
      console.log(`📊 User ${userId}: ${progressPercent.toFixed(1)}% (${daysElapsed}/${totalDays} days), sending ${milestoneToSend.key} milestone`);
      
      // Craft the message
      let messageTitle = '';
      let messageBody = '';
      
      if (milestoneToSend.key === 'quarter') {
        messageTitle = '🌱 25% Through Your WISH!';
        messageBody = `You're 25% through your WISH journey! (${daysElapsed}/${totalDays} days). Keep growing!`;
      } else if (milestoneToSend.key === 'half') {
        messageTitle = '🍀 Halfway There!';
        messageBody = `Amazing! You've completed ${daysElapsed} of ${totalDays} days. Your WISH is blooming!`;
      } else if (milestoneToSend.key === 'three-quarters') {
        messageTitle = '🌿 75% Complete!';
        messageBody = `Only ${totalDays - daysElapsed} days left on your WISH journey. You're doing amazing!`;
      } else if (milestoneToSend.key === 'complete') {
        messageTitle = '🌳 WISH Journey Complete!';
        messageBody = `Congratulations! You've completed your ${totalDays}-day WISH journey! Time to reflect and set a new WISH.`;
      }
      
      // Send Push Notification
      if (wantsPush) {
        try {
          const pushResult = await sendPushNotification(
            userData.fcmToken,
            messageTitle,
            messageBody,
            { type: 'wish_milestone', milestone: milestoneToSend.key }
          );
          if (pushResult) {
            pushSentCount++;
            console.log(`✅ Push sent to ${userId} for ${milestoneToSend.key}`);
          }
        } catch (pushError) {
          console.error(`❌ Push failed for ${userId}:`, pushError.message);
        }
      }
      
      // Send SMS
      if (wantsSms) {
        try {
          const twilio = require('twilio');
          const client = twilio(
            TWILIO_ACCOUNT_SID.value(),
            TWILIO_AUTH_TOKEN.value()
          );
          
          const appLink = '\n\nOpen InkWell: https://inkwelljournal.io/app.html';
          const smsText = `${milestoneToSend.emoji} InkWell: ${messageBody}${appLink}`;
          
          await client.messages.create({
            body: smsText,
            from: TWILIO_PHONE_NUMBER.value(),
            to: userData.phoneNumber
          });
          
          smsSentCount++;
          console.log(`✅ SMS sent to ${userId} for ${milestoneToSend.key}`);
        } catch (smsError) {
          console.error(`❌ SMS failed for ${userId}:`, smsError.message);
        }
      }
      
      // Mark milestone as sent in manifest document
      await admin.firestore().collection('manifests').doc(userId).update({
        milestonesSent: admin.firestore.FieldValue.arrayUnion(milestoneToSend.key),
        lastMilestoneSentAt: admin.firestore.FieldValue.serverTimestamp()
      });
      
    } catch (userError) {
      console.error(`❌ Error processing user ${userId}:`, userError.message);
    }
  }
  
  console.log(`🎯 WISH Milestones complete: ${pushSentCount} push, ${smsSentCount} SMS sent, ${skippedCount} skipped (no prefs), ${noWishCount} no active WISH`);
});

/**
 * Set Hollis Verdant as a coach
 * One-time utility to ensure Hollis has userRole: 'coach'
 */
exports.setHollisAsCoach = onRequest({
  region: 'us-central1',
  cors: true,
}, async (req, res) => {
  try {
    const { secretKey } = req.body?.data || req.body || {};
    
    if (secretKey !== 'inkwell-beta-hollis-2026') {
      return res.status(403).json({ success: false, error: 'Invalid secret key' });
    }
    
    const hollisUid = 'ZiNM7YK1jnRgIkAKiCaO1lC6DGx2';
    
    // Get current Hollis data
    const hollisDoc = await admin.firestore().collection('users').doc(hollisUid).get();
    
    if (!hollisDoc.exists) {
      return res.status(404).json({ success: false, error: 'Hollis user not found' });
    }
    
    const currentData = hollisDoc.data();
    
    // Update to set coach role AND freeAgentOptIn
    await admin.firestore().collection('users').doc(hollisUid).update({
      userRole: 'coach',
      isPractitioner: true,
      practitionerVerified: true,
      accountType: 'coach',
      freeAgentOptIn: true,
      acceptingClients: 'yes'
    });
    
    res.json({
      success: true,
      message: 'Hollis Verdant is now set as a public coach',
      previousData: {
        email: currentData.email,
        displayName: currentData.displayName,
        userRole: currentData.userRole,
        isPractitioner: currentData.isPractitioner,
        practitionerVerified: currentData.practitionerVerified,
        freeAgentOptIn: currentData.freeAgentOptIn
      }
    });
    
  } catch (error) {
    console.error('❌ Error setting Hollis as coach:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * Set any user as a public coach by email
 * Utility function to set freeAgentOptIn for any coach
 */
exports.setCoachPublic = onRequest({
  region: 'us-central1',
  cors: true,
}, async (req, res) => {
  try {
    const { secretKey, email } = req.body?.data || req.body || {};
    
    if (secretKey !== 'inkwell-beta-admin-2026') {
      return res.status(403).json({ success: false, error: 'Invalid secret key' });
    }
    
    if (!email) {
      return res.status(400).json({ success: false, error: 'Email required' });
    }
    
    // Find user by email
    const usersRef = admin.firestore().collection('users');
    const snapshot = await usersRef.where('email', '==', email).get();
    
    if (snapshot.empty) {
      return res.status(404).json({ success: false, error: 'User not found with that email' });
    }
    
    const userDoc = snapshot.docs[0];
    const currentData = userDoc.data();
    
    // Update to set coach role AND freeAgentOptIn
    await userDoc.ref.update({
      userRole: 'coach',
      isPractitioner: true,
      practitionerVerified: true,
      freeAgentOptIn: true,
      acceptingClients: 'yes'
    });
    
    res.json({
      success: true,
      message: `${currentData.displayName || email} is now a public coach`,
      uid: userDoc.id,
      previousData: {
        email: currentData.email,
        displayName: currentData.displayName,
        userRole: currentData.userRole,
        freeAgentOptIn: currentData.freeAgentOptIn
      }
    });
    
  } catch (error) {
    console.error('❌ Error setting coach public:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * Fix admin account and get coach profiles
 * One-time utility to restore tfershi@pm.me to admin and capture coach data
 */
exports.fixAdminGetCoaches = onRequest({
  region: 'us-central1',
  cors: true,
}, async (req, res) => {
  try {
    const { secretKey, action } = req.body?.data || req.body || {};
    
    if (secretKey !== 'inkwell-beta-admin-2026') {
      return res.status(403).json({ success: false, error: 'Invalid secret key' });
    }
    
    const db = admin.firestore();
    const result = {};
    
    // Get coach profiles
    const coach1Doc = await db.collection('users').doc('ZiNM7YK1jnRgIkAKiCaO1lC6DGx2').get();
    const coach2Doc = await db.collection('users').doc('14QhSBZSxyOmk0bdWvuCNPQnRgZ2').get();
    
    result.coaches = {
      hollisVerdant: coach1Doc.exists ? coach1Doc.data() : null,
      adamGrimm: coach2Doc.exists ? coach2Doc.data() : null
    };
    
    // Fix admin account if action is 'fix'
    if (action === 'fix') {
      await db.collection('users').doc('4FeEdZPE5AOM7jQpii3y4LYnC3I2').update({
        userRole: 'admin',
        freeAgentOptIn: admin.firestore.FieldValue.delete(),
        isPractitioner: admin.firestore.FieldValue.delete(),
        practitionerVerified: admin.firestore.FieldValue.delete()
      });
      result.adminFixed = true;
      result.message = 'Admin account (tfershi@pm.me) restored to userRole=admin, coach fields removed';
    } else {
      result.adminFixed = false;
      result.message = 'Use action: "fix" to restore admin account';
    }
    
    res.json({ success: true, ...result });
    
  } catch (error) {
    console.error('❌ Error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * Migrate user documents - clean up deprecated/duplicate fields
 * Preserves coach-specific fields for userRole === 'coach'
 */
exports.migrateUserFields = onRequest({
  region: 'us-central1',
  cors: true,
  timeoutSeconds: 540, // 9 minutes for large user base
}, async (req, res) => {
  try {
    const { secretKey, dryRun = true, batchSize = 50 } = req.body?.data || req.body || {};
    
    if (secretKey !== 'inkwell-beta-admin-2026') {
      return res.status(403).json({ success: false, error: 'Invalid secret key' });
    }
    
    const db = admin.firestore();
    const usersRef = db.collection('users');
    const snapshot = await usersRef.get();
    
    // Fields to delete from ALL users (duplicates/deprecated)
    const fieldsToDeleteAll = [
      'signupUsername',           // Duplicate of displayName
      'photoURL',                 // Legacy OAuth; use avatar
      'lastLoginAt',              // Not actively used
      'lastTokenUpdate',          // Not actively used
      'accountType',              // Use userRole
      'practitioners',            // Legacy array; use connectedPractitioner
      'connectedCoach',           // Legacy alias
    ];
    
    // Coach-specific fields to delete ONLY from non-coaches
    const coachOnlyFields = [
      'isPractitioner',
      'practitionerVerified',
      'freeAgentOptIn',
      'bio',
      'credentials',
      'practiceLocation',
      'specialties',
      'acceptingClients',
      'practitionerBio',
      'practitionerCredentials',
      'practitionerLocation',
      'practitionerSpecialties',
    ];
    
    const results = {
      totalUsers: snapshot.size,
      processed: 0,
      updated: 0,
      skipped: 0,
      errors: [],
      dryRun: dryRun,
      fieldsRemoved: {}
    };
    
    const batch = db.batch();
    let batchCount = 0;
    
    for (const doc of snapshot.docs) {
      const data = doc.data();
      const isCoach = data.userRole === 'coach';
      const isAdmin = data.userRole === 'admin';
      const updates = {};
      const deletions = [];
      
      // Always delete deprecated fields
      for (const field of fieldsToDeleteAll) {
        if (data[field] !== undefined) {
          updates[field] = admin.firestore.FieldValue.delete();
          deletions.push(field);
        }
      }
      
      // Delete nested duplicates in smsPreferences
      if (data.smsPreferences) {
        if (data.smsPreferences.phoneNumber !== undefined) {
          updates['smsPreferences.phoneNumber'] = admin.firestore.FieldValue.delete();
          deletions.push('smsPreferences.phoneNumber');
        }
        if (data.smsPreferences.timezone !== undefined) {
          updates['smsPreferences.timezone'] = admin.firestore.FieldValue.delete();
          deletions.push('smsPreferences.timezone');
        }
        if (data.smsPreferences.gratitudePrompts !== undefined) {
          updates['smsPreferences.gratitudePrompts'] = admin.firestore.FieldValue.delete();
          deletions.push('smsPreferences.gratitudePrompts');
        }
      }
      
      // Delete betaProgress (deprecated)
      if (data.betaProgress !== undefined) {
        updates['betaProgress'] = admin.firestore.FieldValue.delete();
        deletions.push('betaProgress');
      }
      
      // Delete coach-only fields from non-coaches (but not admins who might test)
      if (!isCoach && !isAdmin) {
        for (const field of coachOnlyFields) {
          if (data[field] !== undefined) {
            updates[field] = admin.firestore.FieldValue.delete();
            deletions.push(field);
          }
        }
      }
      
      // If there are updates to make
      if (Object.keys(updates).length > 0) {
        if (!dryRun) {
          batch.update(doc.ref, updates);
          batchCount++;
          
          // Commit in batches to avoid memory issues
          if (batchCount >= batchSize) {
            await batch.commit();
            batchCount = 0;
          }
        }
        
        results.updated++;
        deletions.forEach(field => {
          results.fieldsRemoved[field] = (results.fieldsRemoved[field] || 0) + 1;
        });
      } else {
        results.skipped++;
      }
      
      results.processed++;
    }
    
    // Commit any remaining updates
    if (!dryRun && batchCount > 0) {
      await batch.commit();
    }
    
    results.message = dryRun 
      ? `DRY RUN: Would update ${results.updated} users, skip ${results.skipped} users`
      : `COMPLETED: Updated ${results.updated} users, skipped ${results.skipped} users`;
    
    res.json({ success: true, ...results });
    
  } catch (error) {
    console.error('❌ Migration error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});