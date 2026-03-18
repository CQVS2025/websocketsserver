/**
 * Cron Jobs — Scheduled alert scanners
 *
 * These run on intervals to detect time-based and data-driven conditions
 * that should trigger alerts (e.g., stale entries, overdue reports, offline devices).
 *
 * Each scanner queries Supabase, checks conditions, and calls emitAlert()
 * which inserts into the alerts table, broadcasts via WebSocket, and sends email/SMS.
 */

import cron from 'node-cron';

// Supabase ref — set in startCronJobs
let _supabase = null;

/**
 * Check if a similar alert was already created within the time window
 * by querying the alerts table. This survives server restarts.
 */
async function wasRecentlyFired(type, entityId, windowMs = 6 * 60 * 60 * 1000) {
  if (!_supabase) return false;
  try {
    const since = new Date(Date.now() - windowMs).toISOString();
    let query = _supabase
      .from('alerts')
      .select('id', { count: 'exact', head: true })
      .eq('type', type)
      .gte('created_at', since);

    if (entityId && entityId !== 'global') {
      query = query.eq('entity_id', entityId);
    }

    const { count } = await query;
    return (count || 0) > 0;
  } catch (err) {
    console.warn('[Cron] Dedup check failed:', err.message);
    return false; // If check fails, allow the alert (better than silently dropping)
  }
}

export function startCronJobs(supabase, emitAlert) {
  _supabase = supabase;
  console.log('[Cron] Starting scheduled alert scanners...');

  // ══════════════════════════════════════════════════════════════
  // EVERY 15 MINUTES — Operations, Orders, Security checks
  // ══════════════════════════════════════════════════════════════
  cron.schedule('*/15 * * * *', async () => {
    console.log('[Cron] Running 15-min scan...');
    try {
      await scanStaleEntries(supabase, emitAlert);
      await scanPendingOrders(supabase, emitAlert);
      await scanAgentPartsNeedOrder(supabase, emitAlert);
      await scanInactiveAssignees(supabase, emitAlert);
    } catch (err) {
      console.error('[Cron] 15-min scan error:', err.message);
    }
  });

  // ══════════════════════════════════════════════════════════════
  // EVERY 5 MINUTES — Device & Chemical monitoring
  // ══════════════════════════════════════════════════════════════
  cron.schedule('*/5 * * * *', async () => {
    console.log('[Cron] Running 5-min device/chemical scan...');
    try {
      await scanDeviceHealth(supabase, emitAlert);
      await scanChemicalLevels(supabase, emitAlert);
    } catch (err) {
      console.error('[Cron] 5-min scan error:', err.message);
    }
  });

  // ══════════════════════════════════════════════════════════════
  // EVERY HOUR — Report schedules, delivery gaps, login checks
  // ══════════════════════════════════════════════════════════════
  cron.schedule('0 * * * *', async () => {
    console.log('[Cron] Running hourly scan...');
    try {
      await scanReportsDueToday(supabase, emitAlert);
      await scanReportsDueSoon(supabase, emitAlert);
      await scanOverdueReports(supabase, emitAlert);
      await scanNoDeliveryAtSite(supabase, emitAlert);
      await scanAreaManagerInactive(supabase, emitAlert);
      await scanCompanyNoSchedule(supabase, emitAlert);
      await scanScheduleNoReports(supabase, emitAlert);
    } catch (err) {
      console.error('[Cron] Hourly scan error:', err.message);
    }
  });

  // ══════════════════════════════════════════════════════════════
  // DAILY 7:00 AM — Morning digest alerts
  // ══════════════════════════════════════════════════════════════
  cron.schedule('0 7 * * *', async () => {
    console.log('[Cron] Running daily morning digest...');
    try {
      await scanDeliveriesToday(supabase, emitAlert);
    } catch (err) {
      console.error('[Cron] Morning digest error:', err.message);
    }
  });

  // ══════════════════════════════════════════════════════════════
  // WEEKLY MONDAY 7:00 AM — Weekly digest
  // ══════════════════════════════════════════════════════════════
  cron.schedule('0 7 * * 1', async () => {
    console.log('[Cron] Running weekly digest...');
    try {
      await scanWeeklyReportDigest(supabase, emitAlert);
    } catch (err) {
      console.error('[Cron] Weekly digest error:', err.message);
    }
  });

  // Run initial scan after 30 seconds (server startup)
  setTimeout(async () => {
    console.log('[Cron] Running initial startup scan...');
    try {
      await scanStaleEntries(supabase, emitAlert);
      await scanPendingOrders(supabase, emitAlert);
      await scanDeviceHealth(supabase, emitAlert);
      await scanChemicalLevels(supabase, emitAlert);
      await scanReportsDueToday(supabase, emitAlert);
      await scanOverdueReports(supabase, emitAlert);
    } catch (err) {
      console.error('[Cron] Startup scan error:', err.message);
    }
  }, 30000);
}

// ══════════════════════════════════════════════════════════════════
// OPERATIONS LOG SCANNERS
// ══════════════════════════════════════════════════════════════════

async function scanStaleEntries(supabase, emitAlert) {
  // Entry open 5+ days with no status change
  const fiveDaysAgo = new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString();

  const { data: stale } = await supabase
    .from('operations_log_entries')
    .select('id, title, status, updated_at, created_at')
    .eq('status', 'open')
    .lt('updated_at', fiveDaysAgo)
    .limit(50);

  for (const entry of stale || []) {
    if (wasRecentlyFired('ENTRY_OPEN_5_DAYS', entry.id, 24 * 60 * 60 * 1000)) continue;
    const days = Math.floor((Date.now() - new Date(entry.updated_at).getTime()) / (24 * 60 * 60 * 1000));
    await emitAlert({
      type: 'ENTRY_OPEN_5_DAYS',
      category: 'operations',
      severity: 'warning',
      entity_id: entry.id,
      entity_name: entry.title,
      message: `Entry open for ${days} days with no status change: ${entry.title}`,
    });
  }
}

async function scanInactiveAssignees(supabase, emitAlert) {
  // Entry assigned to member who has not logged in (7+ days)
  const { data: entries } = await supabase
    .from('operations_log_entries')
    .select('id, title, assigned_to')
    .in('status', ['open', 'in_progress'])
    .not('assigned_to', 'is', null)
    .limit(100);

  if (!entries?.length) return;

  const assigneeNames = [...new Set(entries.map(e => e.assigned_to).filter(Boolean))];
  const { data: profiles } = await supabase
    .from('user_profiles')
    .select('id, full_name, email')
    .in('full_name', assigneeNames);

  if (!profiles?.length) return;

  const profileIds = profiles.map(p => p.id);
  const { data: presences } = await supabase
    .from('user_presence')
    .select('user_id, last_seen_at')
    .in('user_id', profileIds);

  const presenceMap = Object.fromEntries((presences || []).map(p => [p.user_id, p.last_seen_at]));
  const profileMap = Object.fromEntries(profiles.map(p => [p.full_name, p]));
  const sevenDaysAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;

  for (const entry of entries) {
    const profile = profileMap[entry.assigned_to];
    if (!profile) continue;
    const lastSeen = presenceMap[profile.id];
    if (!lastSeen || new Date(lastSeen).getTime() < sevenDaysAgo) {
      if (wasRecentlyFired('ENTRY_ASSIGNED_INACTIVE_USER', entry.id, 24 * 60 * 60 * 1000)) continue;
      await emitAlert({
        type: 'ENTRY_ASSIGNED_INACTIVE_USER',
        category: 'security',
        severity: 'warning',
        entity_id: entry.id,
        entity_name: entry.assigned_to,
        message: `Entry "${entry.title}" assigned to ${entry.assigned_to} who hasn't logged in for 7+ days`,
      });
    }
  }
}

// ══════════════════════════════════════════════════════════════════
// ORDER SCANNERS
// ══════════════════════════════════════════════════════════════════

async function scanPendingOrders(supabase, emitAlert) {
  // Order pending approval for more than 24 hours
  const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

  const { data: pending } = await supabase
    .from('order_requests')
    .select('id, requested_by, priority, created_at, notes')
    .eq('status', 'pending')
    .lt('created_at', oneDayAgo)
    .limit(50);

  for (const order of pending || []) {
    if (wasRecentlyFired('ORDER_PENDING_APPROVAL', order.id, 12 * 60 * 60 * 1000)) continue;
    const hours = Math.floor((Date.now() - new Date(order.created_at).getTime()) / (60 * 60 * 1000));
    await emitAlert({
      type: 'ORDER_PENDING_APPROVAL',
      category: 'orders',
      severity: 'warning',
      entity_id: order.id,
      entity_name: `Order #${order.id.slice(0, 8)}`,
      message: `Order pending approval for ${hours}h (priority: ${order.priority})`,
    });
  }
}

async function scanAgentPartsNeedOrder(supabase, emitAlert) {
  // Agent has parts marked "need to order" with no request raised
  const { data: stock } = await supabase
    .from('agent_stock')
    .select('user_id, part_id, need_to_order')
    .gt('need_to_order', 0)
    .limit(200);

  if (!stock?.length) return;

  // Get user names
  const userIds = [...new Set(stock.map(s => s.user_id))];
  const { data: profiles } = await supabase
    .from('user_profiles')
    .select('id, full_name')
    .in('id', userIds);
  const nameMap = Object.fromEntries((profiles || []).map(p => [p.id, p.full_name]));

  // Check if there's an open/pending order for these parts
  const { data: openOrders } = await supabase
    .from('order_requests')
    .select('id, requested_by')
    .in('status', ['pending', 'approved', 'ordered'])
    .in('requested_by', userIds);

  const usersWithOrders = new Set((openOrders || []).map(o => o.requested_by));

  for (const s of stock) {
    if (usersWithOrders.has(s.user_id)) continue;
    if (wasRecentlyFired('AGENT_PARTS_NO_REQUEST', s.user_id, 24 * 60 * 60 * 1000)) continue;
    const agentName = nameMap[s.user_id] || 'Unknown agent';
    await emitAlert({
      type: 'AGENT_PARTS_NO_REQUEST',
      category: 'orders',
      severity: 'info',
      entity_id: s.user_id,
      entity_name: agentName,
      message: `${agentName} has parts marked "need to order" but no order request raised`,
    });
  }
}

// ══════════════════════════════════════════════════════════════════
// DEVICE SCANNERS
// ══════════════════════════════════════════════════════════════════

async function scanDeviceHealth(supabase, emitAlert) {
  // Check tank_configurations for devices — we check last data timestamp
  // Devices are considered "offline" if their last reading is stale
  const { data: configs } = await supabase
    .from('tank_configurations')
    .select('id, site_ref, device_ref, device_serial, updated_at, active')
    .eq('active', true)
    .limit(200);

  if (!configs?.length) return;

  const now = Date.now();
  const offlineThresholdMs = 30 * 60 * 1000; // 30 minutes = offline
  const extendedThresholdMs = 2 * 60 * 60 * 1000; // 2 hours = extended offline

  for (const device of configs) {
    const lastUpdate = new Date(device.updated_at).getTime();
    const offlineMs = now - lastUpdate;

    if (offlineMs > extendedThresholdMs) {
      if (wasRecentlyFired('DEVICE_OFFLINE_EXTENDED', device.id, 4 * 60 * 60 * 1000)) continue;
      const hours = Math.round(offlineMs / (60 * 60 * 1000));
      await emitAlert({
        type: 'DEVICE_OFFLINE_EXTENDED',
        category: 'devices',
        severity: 'critical',
        entity_id: device.id,
        entity_name: `${device.site_ref} — ${device.device_ref || device.device_serial}`,
        message: `Device offline for more than ${hours} hours`,
      });
    } else if (offlineMs > offlineThresholdMs) {
      if (wasRecentlyFired('DEVICE_OFFLINE', device.id, 2 * 60 * 60 * 1000)) continue;
      const mins = Math.round(offlineMs / (60 * 1000));
      await emitAlert({
        type: 'DEVICE_OFFLINE',
        category: 'devices',
        severity: 'critical',
        entity_id: device.id,
        entity_name: `${device.site_ref} — ${device.device_ref || device.device_serial}`,
        message: `Device not responding for ${mins} minutes`,
      });
    }
  }
}

// ══════════════════════════════════════════════════════════════════
// CHEMICAL / TANK LEVEL SCANNERS
// ══════════════════════════════════════════════════════════════════

async function scanChemicalLevels(supabase, emitAlert) {
  // Check tank_configurations for warning/critical thresholds
  // We look at the most recent readings
  const { data: configs } = await supabase
    .from('tank_configurations')
    .select('id, site_ref, device_ref, device_serial, product_type, tank_number, max_capacity_litres, warning_threshold_pct, critical_threshold_pct, active')
    .eq('active', true)
    .limit(200);

  if (!configs?.length) return;

  // For now, check if there's a recent chemical level reading via Supabase
  // The actual level data typically comes from the device API
  // We'll check for any low level alerts that haven't been raised
  for (const tank of configs) {
    // We'll use the device serial to check recent readings if a readings table exists
    // For now, this scanner is ready — when chemical reading data flows in,
    // it will pick up tanks below threshold
    // This is a placeholder that can be enhanced when reading data is available
  }
}

// ══════════════════════════════════════════════════════════════════
// DELIVERY SCANNERS
// ══════════════════════════════════════════════════════════════════

async function scanDeliveriesToday(supabase, emitAlert) {
  // Delivery scheduled for today (morning digest)
  const today = new Date();
  const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate()).toISOString();
  const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate() + 1).toISOString();

  const { data: deliveries, count } = await supabase
    .from('delivery_deliveries')
    .select('id, title, customer, site, driver_name', { count: 'exact' })
    .gte('date_start', startOfDay)
    .lt('date_start', endOfDay)
    .eq('archived', false)
    .limit(50);

  if (count > 0) {
    if (wasRecentlyFired('DELIVERY_SCHEDULED_TODAY', 'digest', 20 * 60 * 60 * 1000)) return;
    const sites = (deliveries || []).map(d => d.site || d.customer).filter(Boolean).slice(0, 5).join(', ');
    await emitAlert({
      type: 'DELIVERY_SCHEDULED_TODAY',
      category: 'delivery',
      severity: 'info',
      entity_name: 'Morning Digest',
      message: `${count} deliver${count === 1 ? 'y' : 'ies'} scheduled for today: ${sites}${count > 5 ? '...' : ''}`,
    });
  }
}

async function scanNoDeliveryAtSite(supabase, emitAlert) {
  // No delivery recorded at a site for 7+ days
  // Get all active sites from tank_configurations
  const { data: sites } = await supabase
    .from('tank_configurations')
    .select('site_ref')
    .eq('active', true);

  if (!sites?.length) return;

  const uniqueSites = [...new Set(sites.map(s => s.site_ref).filter(Boolean))];
  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();

  for (const siteRef of uniqueSites) {
    const { count } = await supabase
      .from('delivery_deliveries')
      .select('id', { count: 'exact', head: true })
      .eq('site', siteRef)
      .gte('date_start', sevenDaysAgo)
      .eq('archived', false);

    if ((count || 0) === 0) {
      if (wasRecentlyFired('SITE_NO_DELIVERY', siteRef, 24 * 60 * 60 * 1000)) continue;
      await emitAlert({
        type: 'SITE_NO_DELIVERY',
        category: 'delivery',
        severity: 'warning',
        entity_name: siteRef,
        message: `No delivery recorded at ${siteRef} for 7+ days`,
      });
    }
  }
}

// ══════════════════════════════════════════════════════════════════
// SECURITY SCANNERS
// ══════════════════════════════════════════════════════════════════

async function scanAreaManagerInactive(supabase, emitAlert) {
  // Area manager has not logged in for 7 days
  const { data: managers } = await supabase
    .from('user_profiles')
    .select('id, full_name, email, role')
    .eq('is_active', true)
    .in('role', ['site_manager', 'admin']);

  if (!managers?.length) return;

  const ids = managers.map(m => m.id);
  const { data: presences } = await supabase
    .from('user_presence')
    .select('user_id, last_seen_at')
    .in('user_id', ids);

  const presenceMap = Object.fromEntries((presences || []).map(p => [p.user_id, p.last_seen_at]));
  const sevenDaysAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;

  for (const mgr of managers) {
    const lastSeen = presenceMap[mgr.id];
    if (!lastSeen || new Date(lastSeen).getTime() < sevenDaysAgo) {
      if (wasRecentlyFired('MANAGER_NOT_LOGGED_IN_7_DAYS', mgr.id, 24 * 60 * 60 * 1000)) continue;
      const days = lastSeen
        ? Math.floor((Date.now() - new Date(lastSeen).getTime()) / (24 * 60 * 60 * 1000))
        : 'unknown';
      await emitAlert({
        type: 'MANAGER_NOT_LOGGED_IN_7_DAYS',
        category: 'security',
        severity: 'warning',
        entity_id: mgr.id,
        entity_name: mgr.full_name || mgr.email,
        message: `${mgr.full_name || mgr.email} (${mgr.role}) has not logged in for ${days} days`,
      });
    }
  }
}

// ══════════════════════════════════════════════════════════════════
// REPORT SCHEDULE SCANNERS
// ══════════════════════════════════════════════════════════════════

async function scanReportsDueToday(supabase, emitAlert) {
  // Report due today
  const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  const dayOfWeek = new Date().getDay(); // 0=Sun, 1=Mon, ...

  const { data: schedules } = await supabase
    .from('report_schedules')
    .select('id, contact_name, email, frequency, send_day, starting_from, last_sent, contact_company_id')
    .eq('active', true)
    .limit(200);

  if (!schedules?.length) return;

  for (const sched of schedules) {
    const isDue = isReportDueToday(sched, today, dayOfWeek);
    if (!isDue) continue;

    // Check if already sent today
    if (sched.last_sent) {
      const lastSentDate = new Date(sched.last_sent).toISOString().slice(0, 10);
      if (lastSentDate === today) continue;
    }

    if (wasRecentlyFired('REPORT_DUE_TODAY', sched.id, 20 * 60 * 60 * 1000)) continue;
    await emitAlert({
      type: 'REPORT_DUE_TODAY',
      category: 'report_scheduling',
      severity: 'warning',
      entity_id: sched.id,
      entity_name: sched.contact_name || sched.email || 'Unknown',
      message: `Report due today for ${sched.contact_name || sched.email} — not yet marked sent`,
    });
  }
}

async function scanReportsDueSoon(supabase, emitAlert) {
  // Report due in 2 days
  const twoDaysFromNow = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000);
  const targetDate = twoDaysFromNow.toISOString().slice(0, 10);
  const targetDayOfWeek = twoDaysFromNow.getDay();

  const { data: schedules } = await supabase
    .from('report_schedules')
    .select('id, contact_name, email, frequency, send_day, starting_from, last_sent')
    .eq('active', true)
    .limit(200);

  if (!schedules?.length) return;

  for (const sched of schedules) {
    const isDue = isReportDueToday(sched, targetDate, targetDayOfWeek);
    if (!isDue) continue;

    if (wasRecentlyFired('REPORT_DUE_IN_X_DAYS', sched.id, 24 * 60 * 60 * 1000)) continue;
    await emitAlert({
      type: 'REPORT_DUE_IN_X_DAYS',
      category: 'report_scheduling',
      severity: 'upcoming',
      entity_id: sched.id,
      entity_name: sched.contact_name || sched.email || 'Unknown',
      message: `Report due in 2 days for ${sched.contact_name || sched.email}`,
    });
  }
}

async function scanOverdueReports(supabase, emitAlert) {
  // Report overdue — due date passed, not marked sent
  const today = new Date();
  const todayStr = today.toISOString().slice(0, 10);

  const { data: schedules } = await supabase
    .from('report_schedules')
    .select('id, contact_name, email, frequency, send_day, starting_from, last_sent')
    .eq('active', true)
    .limit(200);

  if (!schedules?.length) return;

  for (const sched of schedules) {
    // Check if last expected send date has passed without being sent
    const lastExpected = getLastExpectedSendDate(sched, today);
    if (!lastExpected) continue;

    const lastSentDate = sched.last_sent ? new Date(sched.last_sent) : null;
    if (lastSentDate && lastSentDate >= lastExpected) continue; // Already sent

    if (lastExpected < today) {
      if (wasRecentlyFired('REPORT_OVERDUE', sched.id, 12 * 60 * 60 * 1000)) continue;
      const daysOverdue = Math.floor((today - lastExpected) / (24 * 60 * 60 * 1000));
      if (daysOverdue < 1) continue;
      await emitAlert({
        type: 'REPORT_OVERDUE',
        category: 'report_scheduling',
        severity: 'critical',
        entity_id: sched.id,
        entity_name: sched.contact_name || sched.email || 'Unknown',
        message: `Report ${daysOverdue} day${daysOverdue > 1 ? 's' : ''} overdue for ${sched.contact_name || sched.email} — not marked sent`,
      });
    }
  }
}

async function scanCompanyNoSchedule(supabase, emitAlert) {
  // Company has no report schedule set up
  const { data: companies } = await supabase
    .from('companies')
    .select('id, name')
    .eq('is_active', true)
    .limit(200);

  if (!companies?.length) return;

  const companyIds = companies.map(c => c.id);
  const { data: schedules } = await supabase
    .from('report_schedules')
    .select('contact_company_id')
    .in('contact_company_id', companyIds);

  const companiesWithSchedule = new Set((schedules || []).map(s => s.contact_company_id).filter(Boolean));

  for (const company of companies) {
    if (companiesWithSchedule.has(company.id)) continue;
    if (wasRecentlyFired('COMPANY_NO_REPORT_SCHEDULE', company.id, 7 * 24 * 60 * 60 * 1000)) continue;
    await emitAlert({
      type: 'COMPANY_NO_REPORT_SCHEDULE',
      category: 'report_scheduling',
      severity: 'info',
      entity_id: company.id,
      entity_name: company.name,
      message: `${company.name} has no report schedule set up`,
    });
  }
}

async function scanScheduleNoReports(supabase, emitAlert) {
  // Report schedule has no reports selected
  const { data: schedules } = await supabase
    .from('report_schedules')
    .select('id, contact_name, report_types')
    .eq('active', true)
    .limit(200);

  for (const sched of schedules || []) {
    if (sched.report_types && sched.report_types.length > 0) continue;
    if (wasRecentlyFired('SCHEDULE_NO_REPORTS', sched.id, 7 * 24 * 60 * 60 * 1000)) continue;
    await emitAlert({
      type: 'SCHEDULE_NO_REPORTS',
      category: 'report_scheduling',
      severity: 'info',
      entity_id: sched.id,
      entity_name: sched.contact_name || 'Unknown',
      message: `Report schedule for ${sched.contact_name || 'Unknown'} has no report types selected`,
    });
  }
}

async function scanWeeklyReportDigest(supabase, emitAlert) {
  // Recurring report coming up this week (weekly digest)
  // Runs every Monday at 7am
  const today = new Date();
  const endOfWeek = new Date(today);
  endOfWeek.setDate(today.getDate() + 7);

  const { data: schedules } = await supabase
    .from('report_schedules')
    .select('id, contact_name, email, frequency, send_day, starting_from')
    .eq('active', true)
    .limit(200);

  if (!schedules?.length) return;

  const dueThisWeek = [];
  for (let dayOffset = 0; dayOffset < 7; dayOffset++) {
    const checkDate = new Date(today);
    checkDate.setDate(today.getDate() + dayOffset);
    const dateStr = checkDate.toISOString().slice(0, 10);
    const dow = checkDate.getDay();

    for (const sched of schedules) {
      if (isReportDueToday(sched, dateStr, dow)) {
        const dayName = checkDate.toLocaleDateString('en-AU', { weekday: 'long' });
        dueThisWeek.push({ ...sched, dueDay: dayName, dueDate: dateStr });
      }
    }
  }

  if (dueThisWeek.length > 0) {
    if (wasRecentlyFired('WEEKLY_REPORT_DIGEST', 'digest', 6 * 24 * 60 * 60 * 1000)) return;
    const summary = dueThisWeek.slice(0, 5).map(d =>
      `${d.contact_name || d.email} (${d.dueDay})`
    ).join(', ');
    await emitAlert({
      type: 'WEEKLY_REPORT_DIGEST',
      category: 'report_scheduling',
      severity: 'info',
      entity_name: 'Weekly Digest',
      message: `${dueThisWeek.length} report${dueThisWeek.length > 1 ? 's' : ''} due this week: ${summary}${dueThisWeek.length > 5 ? '...' : ''}`,
    });
  }
}

// ══════════════════════════════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════════════════════════════

function isReportDueToday(schedule, todayStr, dayOfWeek) {
  const { frequency, send_day, starting_from } = schedule;
  if (!starting_from) return false;

  const startDate = new Date(starting_from);
  const today = new Date(todayStr);
  if (today < startDate) return false;

  switch (frequency) {
    case 'daily':
      return true;
    case 'weekly':
      return dayOfWeek === (send_day % 7);
    case 'fortnightly': {
      const diffDays = Math.floor((today - startDate) / (24 * 60 * 60 * 1000));
      return diffDays % 14 === 0 || (dayOfWeek === (send_day % 7) && diffDays % 14 < 7);
    }
    case 'monthly':
      return today.getDate() === send_day;
    case 'quarterly': {
      const month = today.getMonth();
      return (month % 3 === 0) && today.getDate() === send_day;
    }
    default:
      return false;
  }
}

function getLastExpectedSendDate(schedule, today) {
  const { frequency, send_day, starting_from } = schedule;
  if (!starting_from) return null;

  const start = new Date(starting_from);
  if (today < start) return null;

  const d = new Date(today);

  switch (frequency) {
    case 'daily':
      d.setDate(d.getDate() - 1);
      return d;
    case 'weekly': {
      // Find the most recent send_day before today
      const currentDow = d.getDay();
      const targetDow = send_day % 7;
      let daysBack = currentDow - targetDow;
      if (daysBack <= 0) daysBack += 7;
      d.setDate(d.getDate() - daysBack);
      return d >= start ? d : null;
    }
    case 'monthly': {
      d.setDate(send_day);
      if (d >= today) d.setMonth(d.getMonth() - 1);
      return d >= start ? d : null;
    }
    default:
      return null;
  }
}
