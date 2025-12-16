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
const MAILCHIMP_API_KEY = defineSecret("MAILCHIMP_API_KEY");
const MAILCHIMP_LIST_ID = defineSecret("MAILCHIMP_LIST_ID");
const TWILIO_ACCOUNT_SID = defineSecret("TWILIO_ACCOUNT_SID");
const TWILIO_AUTH_TOKEN = defineSecret("TWILIO_AUTH_TOKEN");
const TWILIO_PHONE_NUMBER = defineSecret("TWILIO_PHONE_NUMBER");
const STRIPE_SECRET_KEY = defineSecret("STRIPE_SECRET_KEY");
const STRIPE_WEBHOOK_SECRET = defineSecret("STRIPE_WEBHOOK_SECRET");
const { onDocumentCreated } = require("firebase-functions/v2/firestore");
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
      special_code: "beta", // Tag all new users with beta
      agreementAccepted: false,
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
  'https://inkwell-alpha.web.app',
  'https://inkwell-alpha.firebaseapp.com',
  'https://inkwelljournal.io',      // Production domain
  'https://www.inkwelljournal.io'   // Production domain with www
];

function setupHardenedCORS(req, res) {
  const origin = req.headers.origin;
  
  // Always set Vary: Origin for proper caching behavior
  res.set('Vary', 'Origin');
  
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
async function callAnthropicWithRetry(options, functionName, requestId) {
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
      throw new Error(userError.message);
      
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
        throw new Error(userError.message);
      }
      
      // Network or other errors
      console.error(`[${requestId}] Anthropic network error on attempt ${attempt}:`, error.message);
      if (attempt < maxRetries) {
        const delay = Math.pow(2, attempt - 1) * 1000;
        await sleep(delay);
        continue;
      }
      
      const userError = mapErrorToUserMessage(error, functionName);
      throw new Error(userError.message);
    }
  }
}

exports.generatePrompt = onRequest({ secrets: [ANTHROPIC_API_KEY] }, async (req, res) => {
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
        model: "claude-3-haiku-20240307",
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

// Enhanced askSophy with behavioral pattern recognition
exports.askSophy = onRequest({ secrets: [ANTHROPIC_API_KEY] }, async (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(204).send('');

  try {
    const { entry, wishContext, behavioralTrigger } = req.body;
    const requestId = generateRequestId();
    
    // Get user behavioral data for context
    let behaviorData = null;
    try {
      const authHeader = req.headers.authorization?.replace('Bearer ', '');
      if (authHeader) {
        const decodedToken = await admin.auth().verifyIdToken(authHeader);
        const userId = decodedToken.uid;
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

Respond naturally and warmly. Use research-backed insights rather than specific statistics. 
Language should be humble: "Research suggests..." "Many people find..." "This pattern often indicates..." "Sometimes when we're struggling, our mind..."

Respond directly in your own voice - never use stage directions, action descriptions, or phrases like "*responds warmly*" or "*nods empathetically*". Simply speak naturally and warmly.

Keep your reflections brief, focused, and emotionally clear — no more than 2–3 ideas at once. Break thoughts into short, readable paragraphs. Avoid overwhelming the user. If helpful, suggest small, practical actions that build momentum over time.

Begin your response immediately with your reflection - no introductions or narrative text.

IMPORTANT: This is a one-time reflection, not a conversation. Do not include phrases like "Let me know if you'd like to discuss further", "Would you like me to help with...", "Feel free to share more", or any other conversational follow-ups. Just provide your reflection and end naturally.`;

    // Call Anthropic with enhanced context
    const data = await callAnthropicWithRetry(
      {
        model: "claude-3-haiku-20240307",
        max_tokens: 400,
        messages: [
          { role: "user", content: `${systemPrompt}\n\nUser entry: ${entry}` }
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
exports.checkUserBehavioralTriggers = onRequest({ secrets: [ANTHROPIC_API_KEY] }, async (req, res) => {
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
exports.refineManifest = onCall({ secrets: [ANTHROPIC_API_KEY] }, async (data, context) => {
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
        model: "claude-3-haiku-20240307",
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
exports.cleanVoiceTranscript = onRequest({ secrets: [ANTHROPIC_API_KEY] }, async (req, res) => {
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
        model: "claude-3-haiku-20240307",
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
  secrets: [ANTHROPIC_API_KEY],
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
      model: "claude-3-haiku-20240307",
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
        model: 'claude-3-haiku-20240307',
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
        model: 'claude-3-haiku-20240307',
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

exports.embedAndStoreEntry = onRequest({ secrets: [ANTHROPIC_API_KEY] }, (req, res) => {
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
exports.semanticSearch = onRequest({ secrets: [ANTHROPIC_API_KEY] }, async (req, res) => {
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
        model: "claude-3-haiku-20240307",
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
  cors: true
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

      // Send SMS notification to user if they have it enabled
      try {
        // Get the journal entry to find the user
        const entryDoc = await admin.firestore().collection("journalEntries").doc(entryId).get();
        if (entryDoc.exists) {
          const entryData = entryDoc.data();
          const userId = entryData.userId;
          
          // Get user's SMS preferences
          const userDoc = await admin.firestore().collection("users").doc(userId).get();
          if (userDoc.exists) {
            const userData = userDoc.data();
            
            // Check if user has SMS enabled and wants practitioner reply notifications
            if (userData.smsOptIn && userData.phoneNumber && userData.smsPreferences?.coachReplies) {
              // Get coach's name
              const coachDoc = await admin.firestore().collection("users").doc(coachUid).get();
              const coachName = coachDoc.exists ? coachDoc.data().displayName || 'Your practitioner' : 'Your practitioner';
              
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
            }
          }
        }
      } catch (smsError) {
        // Don't fail the whole operation if SMS fails
        console.error("❌ Failed to send practitioner reply SMS (non-fatal):", smsError);
      }

      res.status(200).json({ success: true });
    } catch (error) {
      console.error("❌ Error saving coach reply:", error);
      res.status(500).json({ 
        error: 'Unable to save the practitioner reply right now. Please try again.',
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

    const coachEmail = "coach@inkwelljournal.io";
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
      subject: "New Journal Entry Tagged for Practitioner Review",
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
    return { success: true, created: result.created };
  } catch (error) {
    console.error("Error creating user profile:", error);
    throw new HttpsError("internal", "Unable to create your profile right now. Please try again.", { 
      code: 'PROFILE_ERROR', 
      retryable: true 
    });
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

// Send practitioner invitation email
exports.sendPractitionerInvitation = onRequest({ secrets: [SENDGRID_API_KEY] }, async (req, res) => {
  // Set CORS headers for both domains
  const origin = req.headers.origin;
  const allowedOrigins = [
    'https://inkwell-alpha.web.app',
    'https://inkwelljournal.io',
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

    // Create unique invitation token
    const invitationToken = Math.random().toString(36).substring(2, 15) + 
                           Math.random().toString(36).substring(2, 15);

    // Save invitation to Firestore
    await admin.firestore().collection("practitionerInvitations").doc(invitationToken).set({
      fromUserId: userId,
      fromUserName: userName,
      fromUserEmail: userData.email,
      practitionerEmail: practitionerEmail,
      practitionerName: practitionerName,
      status: 'pending',
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      expiresAt: admin.firestore.FieldValue.serverTimestamp() // Add 30 days in production
    });

    const registrationUrl = `https://inkwelljournal.io/practitioner-register.html?token=${invitationToken}`;

    const emailContent = {
      to: practitionerEmail,
      from: "support@inkwelljournal.io",
      subject: `${userName} has invited you to InkWell - Professional Mental Health Journaling Platform`,
      html: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
          <div style="text-align: center; margin-bottom: 30px;">
            <img src="https://inkwelljournal.io/InkWell-Logo.png" alt="InkWell" style="max-width: 200px;">
          </div>
          
          <h2 style="color: #2A6972; text-align: center;">You've Been Invited to InkWell</h2>
          
          <p style="font-size: 16px; line-height: 1.6;">Hello ${practitionerName},</p>
          
          <p style="font-size: 16px; line-height: 1.6;">
            <strong>${userName}</strong> (${userData.email}) has invited you to join InkWell as their practitioner. 
            InkWell is a professional mental health journaling platform that connects clients with their practitioners.
          </p>
          
          <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #2A6972;">
            <h3 style="margin-top: 0; color: #2A6972;">What is InkWell?</h3>
            <ul style="margin: 10px 0;">
              <li>Evidence-based journaling & manifesting platform for mental health and wellness</li>
              <li>Secure communication between clients and practitioners</li>
              <li>Custom built wellness and growth AI-assisted reflection tools (Sophy) to support clients</li>
              <li>Built by mental health professionals for mental health professionals</li>
            </ul>
          </div>
          
          <p style="font-size: 16px; line-height: 1.6;">
            To get started and connect with ${userName}, please complete your practitioner registration:
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
            Professional Mental Health Journaling Platform<br>
            <a href="https://www.inkwelljournal.io">inkwelljournal.io</a>
          </p>
        </div>
      `
    };

    await sgMail.send(emailContent);
    console.log('✅ Practitioner invitation sent to:', practitionerEmail);

    res.json({ success: true, message: 'Invitation sent successfully' });

  } catch (error) {
    console.error('❌ Error sending practitioner invitation:', error);
    res.status(500).json({ error: 'Failed to send invitation' });
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
        ...journalEntries.map(e => e.createdAt?.toDateString()),
        ...manifestEntries.map(e => e.createdAt?.toDateString())
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
  const journalContent = journalEntries.slice(0, 5) // Limit to 5 most recent entries
    .map(entry => `Date: ${entry.createdAt?.toDateString()}\nEntry: ${entry.content?.substring(0, 200) || ''}`) // Reduced from 600 to 200 chars
    .join('\n\n---\n\n');
    
  const manifestContent = manifestEntries.slice(0, 3) // Limit to 3 most recent manifests  
    .map(entry => `Date: ${entry.createdAt?.toDateString()}\nWish: ${entry.wish?.substring(0, 150) || ''}\nGratitude: ${entry.gratitude?.substring(0, 150) || ''}`) // Reduced from 300 to 150 chars
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

  // Add journal analysis section if they have journal entries
  if (hasJournals) {
    prompt += `### JOURNAL REFLECTION ANALYSIS:
**Drawing from Gestalt Therapy, Positive Psychology, and Atomic Habits:**

JOURNAL ENTRIES (${stats.totalJournalEntries} entries, ${stats.totalWords} words):
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
- ${stats.totalJournalEntries} journal entries with ${stats.totalWords} words written${hasManifests ? `\n- ${stats.totalManifestEntries} WISH manifestations explored` : ''}
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
    // Get all users who have weekly insights enabled
    const usersRef = admin.firestore().collection('users');
    const usersSnapshot = await usersRef
      .where('insightsPreferences.weeklyEnabled', '==', true)
      .get();
    
    console.log(`[${requestId}] Found ${usersSnapshot.size} users with weekly insights enabled`);
    
    const processedUsers = [];
    const errors = [];
    
    // Process each user sequentially to avoid overwhelming APIs
    for (const userDoc of usersSnapshot.docs) {
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
exports.addToMailchimp = onCall({ 
  secrets: [MAILCHIMP_API_KEY, MAILCHIMP_LIST_ID] 
}, async (request) => {
  const requestId = generateRequestId();
  console.log(`[${requestId}] 📧 Adding email to MailChimp`);
  
  const { email } = request.data;
  if (!email) {
    console.error(`[${requestId}] ❌ Email is required`);
    throw new HttpsError('invalid-argument', 'Email is required');
  }

  try {
    const apiKey = MAILCHIMP_API_KEY.value();
    const listId = MAILCHIMP_LIST_ID.value();
    const serverPrefix = apiKey.split('-')[1];
    
    const url = `https://${serverPrefix}.api.mailchimp.com/3.0/lists/${listId}/members`;
    const body = {
      email_address: email,
      status: 'subscribed',
      tags: ['InkWell Web']
    };

    console.log(`[${requestId}] 🔄 Calling MailChimp API for ${email}`);
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `apikey ${apiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      const error = await response.json();
      console.error(`[${requestId}] ❌ MailChimp API error:`, error);
      throw new HttpsError('internal', error.detail || 'MailChimp error');
    }

    const result = await response.json();
    console.log(`[${requestId}] ✅ Successfully added ${email} to MailChimp with tag 'InkWell Web'`);
    
    return { 
      success: true,
      message: 'Email added to MailChimp successfully',
      email: email,
      tags: ['InkWell Web']
    };
    
  } catch (error) {
    console.error(`[${requestId}] ❌ MailChimp integration failed:`, error);
    throw new HttpsError('internal', `Failed to add email to MailChimp: ${error.message}`);
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
exports.trackWishBehavior = onCall({ secrets: [ANTHROPIC_API_KEY] }, async (request) => {
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
// COMMENTED OUT TO SAVE CPU QUOTA
/* exports.runAdminMigration = onRequest({
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
        
        // Check if user needs migration (missing new fields)
        const needsMigration = 
          !userData.userId || 
          !userData.createdAt || 
          userData.special_code === undefined ||
          !userData.insightsPreferences ||
          !userData.onboardingState;
        
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
*/

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

// Gratitude prompts library - simple, clean, focused on appreciation
const GRATITUDE_PROMPTS = [
  "🙏 What small thing made you smile today?",
  "🙏 Name one person you're thankful for right now.",
  "🙏 What's something you're taking for granted that deserves appreciation?",
  "🙏 What comfort do you have today that you're grateful for?",
  "🙏 What made today a little bit easier?",
  "🙏 Who showed you kindness recently?",
  "🙏 What's one thing your body did for you today?",
  "🙏 What brought you a moment of peace?",
  "🙏 What's working well in your life right now?",
  "🙏 What challenge taught you something valuable?",
  "🙏 What meal or flavor brought you joy today?",
  "🙏 What sound or song lifted your spirits?",
  "🙏 What lesson are you grateful to have learned?",
  "🙏 What place makes you feel safe?",
  "🙏 What small pleasure did you enjoy today?",
  "🙏 Wear gratitude like a cloak and it will feed every corner of your life. - Rumi",
  "🙏 Gratitude turns what we have into enough. - Aesop",
  "🙏 The way to develop the habit of savoring is to pause. - Brené Brown",
  "🙏 Acknowledging the good that you already have is the foundation for all abundance. - Eckhart Tolle",
  "🙏 This is it. This is your life. It's right in front of you. - Mary Oliver",
  "🙏 Let gratitude be the pillow upon which you kneel. - Marcus Aurelius",
  "🙏 When you arise in the morning, think of what a precious privilege it is to be alive. - Marcus Aurelius",
  "🙏 What simple gift did today offer you?",
  "🙏 What made you feel connected to others?",
  "🙏 What ability do you have that makes life easier?",
  "🙏 What beauty did you notice today?",
  "🙏 What made you feel cared for?",
  "🙏 What brought unexpected joy?",
  "🙏 What are you glad didn't happen today?",
  "🙏 What strength did you discover in yourself?",
  "🙏 Trade your expectation for appreciation and the world changes instantly. - Tony Robbins",
  "🙏 Gratitude is not only the greatest of virtues, but the parent of all others. - Cicero",
  "🙏 What moment today deserves to be remembered?",
  "🙏 What problem got solved today?",
  "🙏 What made you laugh or feel lighter?",
  "🙏 Who or what helped you today?",
  "🙏 What's one thing you learned that you're thankful for?",
  "🙏 What part of your routine brings you comfort?",
  "🙏 What do you appreciate about where you are right now?",
  "🙏 The root of joy is gratefulness. - David Steindl-Rast",
  "🙏 What memory makes you smile when you think of it?",
  "🙏 What opportunity came your way recently?",
  "🙏 What simple pleasure are you looking forward to?",
  "🙏 What choice are you glad you made?",
  "🙏 What natural wonder brought you peace today?",
  "🙏 What conversation brightened your day?",
  "🙏 What resource do you have access to that helps you?",
  "🙏 What quality in yourself are you grateful for?",
  "🙏 When we focus on our gratitude, the tide of disappointment goes out. - Kristin Armstrong",
  "🙏 What act of self-care did you manage today?",
  "🙏 What progress, however small, did you make?",
  "🙏 What relationship enriches your life?",
  "🙏 What made you feel proud of yourself?",
  "🙏 What gives your life meaning?",
  "🙏 Enjoy the little things, for one day you may look back and realize they were the big things. - Robert Brault",
  "🙏 What made today better than you expected?",
  "🙏 What simple joy can you appreciate right now?",
  "🙏 What's one thing you have that you once wished for?",
  "🙏 Sometimes the smallest things take up the most room in your heart. - Winnie the Pooh"
];

/**
 * Scheduled function to send daily prompts (runs every 3 hours)
 */
exports.scheduledDailyPrompts = onSchedule({
  schedule: 'every 3 hours',
  timeZone: 'America/New_York', // Adjust to your primary timezone
  secrets: [TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER, ANTHROPIC_API_KEY]
}, async (event) => {
  console.log('🕐 Running scheduled daily prompts check...');
  
  try {
    const now = new Date();
    const currentHour = now.getHours();
    
    // Determine which time window we're in
    let timeWindow = '';
    if (currentHour >= 8 && currentHour < 10) timeWindow = 'morning';
    else if (currentHour >= 12 && currentHour < 14) timeWindow = 'midday';
    else if (currentHour >= 15 && currentHour < 17) timeWindow = 'afternoon';
    else if (currentHour >= 19 && currentHour < 21) timeWindow = 'evening';
    else {
      console.log('⏰ Outside prompt windows, skipping');
      return null;
    }
    
    console.log(`📅 Current time window: ${timeWindow}`);
    
    // Get all users who need prompts
    const usersSnapshot = await admin.firestore().collection('users').get();
    let journalSentCount = 0;
    let gratitudeSentCount = 0;
    let skippedCount = 0;
    
    for (const userDoc of usersSnapshot.docs) {
      const userData = userDoc.data();
      const userId = userDoc.id;
      
      // Check base eligibility
      if (!userData.smsOptIn || !userData.phoneNumber) continue;
      
      // Check if user's time slot matches current window
      const userTimeSlot = userData.promptTimeSlot || 'morning';
      if (userTimeSlot !== timeWindow) {
        continue;
      }
      
      // =======================================================================
      // JOURNAL PROMPTS
      // =======================================================================
      if (userData.smsPreferences?.dailyPrompts) {
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
                    .map(doc => doc.data().entry?.substring(0, 200))
                    .join(' ');
                  
                  // Generate personalized prompt
                  const aiResponse = await callAnthropicWithRetry({
                    model: "claude-3-haiku-20240307",
                    max_tokens: 150,
                    messages: [{
                      role: "user",
                      content: `You are Sophy, a supportive journaling assistant. Based on these recent journal excerpts: "${recentText}", generate a single thoughtful journaling prompt (max 100 characters). Respond with ONLY the prompt text, no quotes or prefixes.`
                    }]
                  }, "dailyPromptPersonalized", generateRequestId());
                  
                  promptText = aiResponse.content[0].text.trim().substring(0, 120);
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
            
            // Send journal prompt SMS
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
              
              // Update user document with last sent time and rotate time slot
              const nextSlots = { morning: 'midday', midday: 'afternoon', afternoon: 'evening', evening: 'morning' };
              await admin.firestore().collection('users').doc(userId).update({
                lastPromptSent: admin.firestore.FieldValue.serverTimestamp(),
                promptTimeSlot: nextSlots[userTimeSlot] || 'morning'
              });
              
              journalSentCount++;
              console.log(`✅ Sent journal prompt to user ${userId}`);
              
              // Rate limiting: small delay between sends
              await new Promise(resolve => setTimeout(resolve, 100));
              
            } catch (smsError) {
              console.error(`❌ Failed to send journal SMS to user ${userId}:`, smsError);
            }
          }
        } else {
          skippedCount++;
        }
      }
      
      // =======================================================================
      // GRATITUDE PROMPTS - Separate from journal prompts
      // =======================================================================
      if (userData.smsPreferences?.dailyGratitude) {
        // Check if already sent gratitude today
        const lastGratitudeSent = userData.lastGratitudeSent?.toDate?.();
        let shouldSendGratitude = true;
        
        if (lastGratitudeSent) {
          const hoursSinceLastGratitude = (now - lastGratitudeSent) / (1000 * 60 * 60);
          if (hoursSinceLastGratitude < 20) { // At least 20 hours between gratitude prompts
            shouldSendGratitude = false;
          }
        }
        
        // Ensure we don't send both journal and gratitude on same day
        const lastJournalSent = userData.lastPromptSent?.toDate?.();
        if (lastJournalSent) {
          const todayStart = new Date();
          todayStart.setHours(0, 0, 0, 0);
          if (lastJournalSent >= todayStart) {
            shouldSendGratitude = false; // Already sent journal today, skip gratitude
          }
        }
        
        if (shouldSendGratitude) {
          // Select random gratitude prompt
          const gratitudeText = GRATITUDE_PROMPTS[Math.floor(Math.random() * GRATITUDE_PROMPTS.length)];
          
          // Send gratitude SMS - simple, no links
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
            
            // Update user document with last gratitude sent time
            await admin.firestore().collection('users').doc(userId).update({
              lastGratitudeSent: admin.firestore.FieldValue.serverTimestamp()
            });
            
            gratitudeSentCount++;
            console.log(`✅ Sent gratitude prompt to user ${userId}`);
            
            // Rate limiting: small delay between sends
            await new Promise(resolve => setTimeout(resolve, 100));
            
          } catch (smsError) {
            console.error(`❌ Failed to send gratitude SMS to user ${userId}:`, smsError);
          }
        }
      }
    }
    
    console.log(`📊 Daily prompts complete: ${journalSentCount} journal, ${gratitudeSentCount} gratitude, ${skippedCount} skipped`);
    return { success: true, journalSent: journalSentCount, gratitudeSent: gratitudeSentCount, skipped: skippedCount };
    
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
        
        // Build SMS message
        const avgWords = Math.round(totalWords / entryCount);
        let messageText = `📊 InkWell Weekly Summary\n\n`;
        messageText += `This week you journaled ${entryCount} ${entryCount === 1 ? 'time' : 'times'}`;
        
        if (voiceEntries > 0) {
          messageText += `, with ${voiceEntries} voice ${voiceEntries === 1 ? 'entry' : 'entries'}`;
        }
        
        messageText += `.\n\n`;
        messageText += `📝 Average: ${avgWords} words per entry\n`;
        
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
    
    if (!customerId) {
      const customer = await stripe.customers.create({
        email: userData.email,
        metadata: {
          firebaseUID: userId,
        },
      });
      customerId = customer.id;
      
      // Save customer ID to Firestore
      await admin.firestore().collection('users').doc(userId).update({
        stripeCustomerId: customerId,
      });
    }

    // Check if gift code provided and valid
    let discountAmount = 0;
    let giftData = null;
    
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
        
        discountAmount = giftData.discountPercent; // 0.50 to 1.0
      }
    }

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
        ...(metadata || {}),
        ...(giftCode ? { giftCode } : {}),
      },
    };
    
    // Add subscription_data only for subscription mode
    if (mode === 'subscription' || !mode) {
      sessionConfig.subscription_data = {
        metadata: {
          firebaseUID: userId,
        },
      };
    }
    
    console.log('🔷 Session config:', JSON.stringify(sessionConfig, null, 2));

    // Apply discount if gift code valid
    if (discountAmount > 0) {
      const coupon = await stripe.coupons.create({
        percent_off: discountAmount * 100,
        duration: 'forever', // Discount applies for life of subscription
        name: `Gift from practitioner (${discountAmount * 100}% off)`,
      });
      
      sessionConfig.discounts = [{
        coupon: coupon.id,
      }];
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
          
          // Determine tier based on price ID
          let tier = 'free';
          if (priceId.includes('plus')) tier = 'plus';
          if (priceId.includes('connect')) tier = 'connect';
          
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
          
          // Mark gift code as redeemed
          if (giftCode) {
            const giftRef = admin.firestore().collection('giftMemberships').doc(giftCode);
            await giftRef.update({
              redeemedBy: admin.firestore.FieldValue.arrayUnion(userId),
              redeemedAt: admin.firestore.FieldValue.serverTimestamp(),
            });
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
        
        await admin.firestore().collection('users').doc(userId).update({
          subscriptionStatus: subscription.status,
          subscriptionPeriodEnd: new Date(subscription.current_period_end * 1000),
        });
        
        console.log(`✅ Subscription updated for user ${userId}: ${subscription.status}`);
        break;
      }
      
      case 'customer.subscription.deleted': {
        const subscription = event.data.object;
        const userId = subscription.metadata.firebaseUID;
        
        await admin.firestore().collection('users').doc(userId).update({
          subscriptionTier: 'free',
          subscriptionStatus: 'canceled',
          subscriptionCanceledAt: admin.firestore.FieldValue.serverTimestamp(),
        });
        
        console.log(`✅ Subscription canceled for user ${userId}`);
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
 */
exports.createGiftMembership = onCall({
  cors: true,
}, async (request) => {
  try {
    const userId = request.auth?.uid;
    const { discountPercent, maxUses, expirationDays, recipientEmail } = request.data;
    
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

    // Validate discount (50-100%)
    const discount = Math.min(Math.max(discountPercent || 0.50, 0.50), 1.0);
    
    // Generate unique gift code
    const giftCode = generateGiftCode();
    
    // Calculate expiration
    const expiresAt = new Date();
    expiresAt.setDate(expiresAt.getDate() + (expirationDays || 90));
    
    // Create gift membership document
    await admin.firestore().collection('giftMemberships').doc(giftCode).set({
      code: giftCode,
      createdBy: userId,
      createdByEmail: practitionerDoc.data().email,
      discountPercent: discount,
      maxUses: maxUses || 1,
      recipientEmail: recipientEmail || null,
      redeemedBy: [],
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      expiresAt: admin.firestore.Timestamp.fromDate(expiresAt),
      status: 'active',
    });
    
    console.log(`✅ Gift membership created by practitioner ${userId}: ${giftCode} (${discount * 100}% off)`);
    
    return {
      giftCode,
      discountPercent: discount,
      expiresAt: expiresAt.toISOString(),
      redeemUrl: `${process.env.APP_URL}/redeem?code=${giftCode}`,
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

// Helper function to generate unique gift codes
function generateGiftCode(length = 8) {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // Exclude ambiguous chars
  let code = '';
  for (let i = 0; i < length; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return code;
}
