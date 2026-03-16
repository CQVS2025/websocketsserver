/**
 * Mailgun email integration for Elora Alerts.
 * Uses the same Mailgun pattern as the main project's Supabase Edge Functions.
 */

const MAILGUN_API_KEY = process.env.MAILGUN_API_KEY;
const MAILGUN_DOMAIN = process.env.MAILGUN_DOMAIN;
const MAILGUN_BASE_URL = (process.env.MAILGUN_BASE_URL || 'https://api.mailgun.net').replace(/\/$/, '');

const ELORA_LOGO_URL = 'https://yyqspdpk0yebvddv.public.blob.vercel-storage.com/233633501.png';
const PRIMARY_COLOR = '#003DA5';
const SECONDARY_COLOR = '#00A3E0';
const COMPANY_NAME = 'ELORA Solutions';

const SEVERITY_COLORS = {
  critical: '#dc2626',
  warning: '#f59e0b',
  info: '#3b82f6',
  resolved: '#22c55e',
  upcoming: '#8b5cf6',
};

/**
 * Send email via Mailgun HTTP API (same pattern as Edge Functions).
 */
async function sendViaMailgun(to, subject, html) {
  if (!MAILGUN_API_KEY || !MAILGUN_DOMAIN) {
    console.log(`[Email] Mailgun not configured. Would send to ${to}: ${subject}`);
    return;
  }

  const url = `${MAILGUN_BASE_URL}/v3/${MAILGUN_DOMAIN}/messages`;
  const from = `${COMPANY_NAME} <postmaster@${MAILGUN_DOMAIN}>`;

  const formData = new URLSearchParams();
  formData.append('from', from);
  formData.append('to', to);
  formData.append('subject', subject);
  formData.append('html', html);

  const auth = Buffer.from(`api:${MAILGUN_API_KEY}`).toString('base64');

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${auth}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: formData.toString(),
  });

  const text = await response.text();
  if (!response.ok) {
    let errorMsg = text;
    try {
      const json = JSON.parse(text);
      errorMsg = json.message || json.error || text;
    } catch { /* use raw text */ }
    throw new Error(`Mailgun error (${response.status}): ${errorMsg}`);
  }

  console.log(`[Email] Alert sent to ${to}`);
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

export async function sendAlertEmail(to, alert) {
  const color = SEVERITY_COLORS[alert.severity] || '#3b82f6';
  const severityLabel = (alert.severity || 'info').toUpperCase();
  const typeLabel = alert.type.replace(/_/g, ' ');

  const html = `
<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1.0"/><title>Alert Notification</title></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <div style="max-width:600px;margin:40px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 6px rgba(0,61,165,0.08);">
    <div style="background:linear-gradient(135deg, ${PRIMARY_COLOR} 0%, ${SECONDARY_COLOR} 100%);padding:20px 24px;">
      <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
        <tr>
          <td style="vertical-align:middle;">
            <img src="${ELORA_LOGO_URL}" alt="ELORA" style="height:32px;width:auto;object-fit:contain;filter:brightness(0) invert(1);"/>
          </td>
          <td style="vertical-align:middle;text-align:right;">
            <span style="color:rgba(255,255,255,0.85);font-size:12px;font-weight:600;">Alert Notification</span>
          </td>
        </tr>
      </table>
      <div style="height:3px;background:linear-gradient(90deg,rgba(0,221,57,0.6),#7cc43e);margin-top:12px;"></div>
    </div>
    <div style="padding:0;">
      <div style="background:${color};color:white;padding:16px 24px;">
        <h2 style="margin:0;font-size:18px;">${escapeHtml(typeLabel)}</h2>
        <span style="font-size:12px;opacity:0.9;text-transform:uppercase;">${severityLabel}</span>
      </div>
      <div style="padding:24px;border-bottom:1px solid #e5e7eb;">
        ${alert.entity_name ? `<p style="margin:0 0 8px;font-weight:600;color:#111827;">${escapeHtml(alert.entity_name)}</p>` : ''}
        <p style="margin:0 0 16px;color:#374151;">${escapeHtml(alert.message)}</p>
        <p style="margin:0;font-size:12px;color:#9ca3af;">
          ${new Date(alert.created_at).toLocaleString('en-AU', { timeZone: 'Australia/Sydney' })}
        </p>
      </div>
    </div>
    <div style="background:#f8fafc;padding:20px;text-align:center;border-top:2px solid #e2e8f0;">
      <p style="color:#475569;font-size:12px;margin:0;display:inline-flex;align-items:center;gap:8px;">
        <img src="${ELORA_LOGO_URL}" alt="" style="height:20px;width:auto;object-fit:contain;"/>
        <span>Powered by ELORA &middot; &copy; ${new Date().getFullYear()}</span>
      </p>
    </div>
  </div>
</body>
</html>
  `.trim();

  const subject = `[${severityLabel}] ${typeLabel}`;
  await sendViaMailgun(to, subject, html);
}

export async function sendWeeklyDigestEmail(to, reports) {
  if (!MAILGUN_API_KEY || !MAILGUN_DOMAIN) {
    console.log(`[Email] Mailgun not configured. Would send weekly digest to ${to}: ${reports.length} reports`);
    return;
  }

  const reportRows = reports.map(r => `
    <tr style="background:#fff;">
      <td style="padding:10px 12px;font-size:13px;color:#0f172a;border-bottom:1px solid #e5e7eb;">${escapeHtml(r.report_name || 'Report')}</td>
      <td style="padding:10px 12px;font-size:13px;color:#374151;border-bottom:1px solid #e5e7eb;">${escapeHtml(r.company_name || '-')}</td>
      <td style="padding:10px 12px;font-size:13px;color:#374151;border-bottom:1px solid #e5e7eb;">${escapeHtml(r.due_date || '-')}</td>
    </tr>
  `).join('');

  const html = `
<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1.0"/><title>Weekly Report Digest</title></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <div style="max-width:680px;margin:40px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 6px rgba(0,61,165,0.08);">
    <div style="background:linear-gradient(135deg, ${PRIMARY_COLOR} 0%, ${SECONDARY_COLOR} 100%);padding:24px 20px;">
      <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
        <tr>
          <td style="vertical-align:middle;">
            <img src="${ELORA_LOGO_URL}" alt="ELORA" style="height:32px;width:auto;object-fit:contain;filter:brightness(0) invert(1);"/>
          </td>
          <td style="vertical-align:middle;text-align:center;">
            <h1 style="color:rgba(255,255,255,0.98);margin:0;font-size:22px;font-weight:700;">Weekly Report Digest</h1>
            <p style="color:rgba(255,255,255,0.75);margin:4px 0 0;font-size:13px;">You have ${reports.length} reports scheduled this week</p>
          </td>
          <td style="vertical-align:middle;text-align:right;">
            <span style="color:rgba(255,255,255,0.85);font-size:11px;font-weight:600;">Powered by ELORA</span>
          </td>
        </tr>
      </table>
      <div style="height:3px;background:linear-gradient(90deg,rgba(0,221,57,0.6),#7cc43e);margin-top:12px;"></div>
    </div>
    <div style="padding:30px;">
      <p style="color:#0f172a;font-size:16px;line-height:1.6;margin:0 0 24px;">Hi there, here are your upcoming reports this week:</p>
      <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;">
        <thead>
          <tr style="background:#f8fafc;">
            <th style="padding:10px 12px;text-align:left;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600;">Report</th>
            <th style="padding:10px 12px;text-align:left;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600;">Company</th>
            <th style="padding:10px 12px;text-align:left;font-size:11px;color:#64748b;text-transform:uppercase;font-weight:600;">Due Date</th>
          </tr>
        </thead>
        <tbody>${reportRows}</tbody>
      </table>
    </div>
    <div style="background:#f8fafc;padding:24px 20px;text-align:center;border-top:2px solid #e2e8f0;">
      <p style="color:#475569;font-size:13px;margin:0;">${COMPANY_NAME} Compliance Portal</p>
      <p style="color:#64748b;font-size:12px;margin:8px 0 0;display:inline-flex;align-items:center;gap:8px;">
        <img src="${ELORA_LOGO_URL}" alt="" style="height:20px;width:auto;object-fit:contain;"/>
        <span>Powered by ELORA &middot; &copy; ${new Date().getFullYear()}</span>
      </p>
    </div>
  </div>
</body>
</html>
  `.trim();

  const subject = `Weekly Report Digest — ${reports.length} reports due this week`;
  await sendViaMailgun(to, subject, html);
}
