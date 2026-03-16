import twilio from 'twilio';

const TWILIO_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const TWILIO_FROM = process.env.TWILIO_PHONE_NUMBER;

let client = null;
if (TWILIO_SID && TWILIO_TOKEN) {
  client = twilio(TWILIO_SID, TWILIO_TOKEN);
}

/**
 * Send alert SMS via Twilio.
 * Returns { success, to, error? } so the caller can report errors to the frontend.
 */
export async function sendAlertSMS(to, alert) {
  const body = `[ELORA ${alert.severity.toUpperCase()}] ${alert.type.replace(/_/g, ' ')}: ${alert.message}`;

  if (!client) {
    const msg = 'Twilio not configured — TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN are required';
    console.warn(`[SMS] ${msg}`);
    return { success: false, to, error: msg };
  }

  if (!TWILIO_FROM) {
    const msg = 'TWILIO_PHONE_NUMBER not set in environment variables';
    console.warn(`[SMS] ${msg}`);
    return { success: false, to, error: msg };
  }

  // Basic phone number format validation
  const cleaned = to.replace(/[\s\-()]/g, '');
  if (!/^\+\d{7,15}$/.test(cleaned)) {
    const msg = `Invalid phone number format: "${to}". Must be in E.164 format (e.g. +61412345678)`;
    console.warn(`[SMS] ${msg}`);
    return { success: false, to, error: msg };
  }

  try {
    const message = await client.messages.create({
      body,
      from: TWILIO_FROM,
      to: cleaned,
    });

    console.log(`[SMS] Alert sent to ${to} (SID: ${message.sid})`);
    return { success: true, to, sid: message.sid };
  } catch (err) {
    // Extract meaningful Twilio error details
    let errorMessage = err.message || 'Unknown Twilio error';

    // Twilio-specific error codes
    if (err.code) {
      const twilioErrors = {
        21211: `Invalid phone number: "${to}". The number is not a valid phone number.`,
        21214: `Phone number "${to}" is not a valid, SMS-capable number.`,
        21608: `The Twilio phone number ${TWILIO_FROM} is not enabled for SMS or not owned by your account.`,
        21610: `Cannot send SMS to "${to}" — the recipient has opted out / unsubscribed.`,
        21612: `The "from" number ${TWILIO_FROM} is not a valid phone number or is not configured for SMS.`,
        21617: `The "from" number ${TWILIO_FROM} is not a mobile number and cannot send SMS.`,
        21408: `Account does not have permission to reach "${to}". Check your Twilio geographic permissions.`,
        21219: `Phone number "${to}" is not verified. In trial mode, you can only send to verified numbers.`,
        20003: 'Twilio authentication failed — check TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN.',
        20404: 'Twilio resource not found — check your account configuration.',
      };
      errorMessage = twilioErrors[err.code] || `Twilio error ${err.code}: ${err.message}`;
    }

    console.error(`[SMS] Failed to send to ${to}: ${errorMessage}`);
    return { success: false, to, error: errorMessage, code: err.code };
  }
}
