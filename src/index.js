import 'dotenv/config';
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import { createClient } from '@supabase/supabase-js';
import { sendAlertEmail } from './email.js';
import { sendAlertSMS } from './sms.js';

const app = express();
const httpServer = createServer(app);

const CORS_ORIGIN = process.env.CORS_ORIGIN || 'http://localhost:5173';

const io = new Server(httpServer, {
  cors: {
    origin: CORS_ORIGIN.split(',').map(s => s.trim()),
    methods: ['GET', 'POST'],
  },
});

app.use(cors({ origin: CORS_ORIGIN.split(',').map(s => s.trim()) }));
app.use(express.json());

// Supabase client (service role for server-side operations)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ── Health check ──────────────────────────────────────────────
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', connections: io.engine.clientsCount });
});

// ── Emit alert endpoint (called by Edge Functions or cron) ───
app.post('/emit-alert', async (req, res) => {
  try {
    const { type, category, severity, entity_id, entity_name, message } = req.body;

    if (!type || !message) {
      return res.status(400).json({ error: 'type and message are required' });
    }

    // Look up alert configuration
    const { data: config } = await supabase
      .from('alert_configurations')
      .select('*')
      .eq('alert_type', type)
      .single();

    if (!config || !config.enabled) {
      return res.json({ skipped: true, reason: 'Alert type disabled or not found' });
    }

    const deliveryChannels = [];
    if (config.portal_enabled) deliveryChannels.push('portal');
    if (config.email_enabled) deliveryChannels.push('email');
    if (config.sms_enabled) deliveryChannels.push('sms');

    // Insert alert record
    const { data: alert, error: insertError } = await supabase
      .from('alerts')
      .insert({
        type,
        category: category || config.category,
        severity: severity || 'info',
        entity_id: entity_id || null,
        entity_name: entity_name || null,
        message,
        status: severity === 'resolved' ? 'resolved' : 'active',
        delivery_channels: deliveryChannels,
      })
      .select()
      .single();

    if (insertError) {
      console.error('Insert alert error:', insertError);
      return res.status(500).json({ error: 'Failed to insert alert' });
    }

    // Broadcast to all connected clients
    io.emit('alert_created', alert);

    // Collect notification results to return to frontend
    const notificationResults = { email: [], sms: [] };

    // Send email/SMS notifications
    if (config.email_enabled || config.sms_enabled) {
      const { data: settings } = await supabase
        .from('alert_delivery_settings')
        .select('*')
        .limit(1)
        .single();

      if (settings) {
        // Email notifications
        if (config.email_enabled && settings.email) {
          const emails = settings.email.split(',').map(e => e.trim()).filter(Boolean);
          for (const email of emails) {
            try {
              await sendAlertEmail(email, alert);
              notificationResults.email.push({ success: true, to: email });
            } catch (err) {
              const errorMsg = err.message || 'Failed to send email';
              console.error('Email send error:', errorMsg);
              notificationResults.email.push({ success: false, to: email, error: errorMsg });
            }
          }
        }

        // SMS notifications
        if (config.sms_enabled && settings.sms_number) {
          // Check quiet hours
          const now = new Date();
          const currentTime = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
          let inQuietHours = false;

          if (settings.quiet_hours_start && settings.quiet_hours_end) {
            const start = settings.quiet_hours_start;
            const end = settings.quiet_hours_end;
            if (start > end) {
              inQuietHours = currentTime >= start || currentTime <= end;
            } else {
              inQuietHours = currentTime >= start && currentTime <= end;
            }
          }

          if (inQuietHours) {
            notificationResults.sms.push({
              success: false,
              error: `SMS suppressed — quiet hours active (${settings.quiet_hours_start} to ${settings.quiet_hours_end})`,
            });
          } else {
            const numbers = settings.sms_number.split(',').map(n => n.trim()).filter(Boolean);
            for (const number of numbers) {
              const result = await sendAlertSMS(number, alert);
              notificationResults.sms.push(result);
            }
          }
        }
      } else {
        if (config.email_enabled) {
          notificationResults.email.push({ success: false, error: 'No delivery settings configured — set email in Delivery Settings' });
        }
        if (config.sms_enabled) {
          notificationResults.sms.push({ success: false, error: 'No delivery settings configured — set SMS number in Delivery Settings' });
        }
      }
    }

    // Build response with notification status
    const hasErrors = [
      ...notificationResults.email,
      ...notificationResults.sms,
    ].some(r => !r.success);

    res.json({
      success: true,
      alert,
      notifications: notificationResults,
      hasNotificationErrors: hasErrors,
    });
  } catch (err) {
    console.error('Emit alert error:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// ── Socket.IO connection handling ────────────────────────────
io.on('connection', (socket) => {
  console.log(`Client connected: ${socket.id}`);

  socket.on('disconnect', () => {
    console.log(`Client disconnected: ${socket.id}`);
  });
});

// ── Start server ─────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
httpServer.listen(PORT, () => {
  console.log(`Elora Alerts Server running on port ${PORT}`);
});
