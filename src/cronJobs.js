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
  // EVERY 5 MINUTES — Device, Chemical & Delivery/Refill monitoring
  // ══════════════════════════════════════════════════════════════
  cron.schedule('*/5 * * * *', async () => {
    console.log('[Cron] Running 5-min device/chemical/refill scan...');
    try {
      await scanDeviceHealth(supabase, emitAlert);
      // These 3 share a cached tank-level fetch (TANK_CACHE_TTL = 4 min)
      await scanChemicalLevels(supabase, emitAlert);
      await scanRefillThresholds(supabase, emitAlert);
      await scanUnusualConsumption(supabase, emitAlert);
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
      await scanRefillThresholds(supabase, emitAlert);
      await scanUnusualConsumption(supabase, emitAlert);
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
    if (await wasRecentlyFired('ENTRY_OPEN_5_DAYS', entry.id, 24 * 60 * 60 * 1000)) continue;
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
      if (await wasRecentlyFired('ENTRY_ASSIGNED_INACTIVE_USER', entry.id, 24 * 60 * 60 * 1000)) continue;
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
    if (await wasRecentlyFired('ORDER_PENDING_APPROVAL', order.id, 12 * 60 * 60 * 1000)) continue;
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
    if (await wasRecentlyFired('AGENT_PARTS_NO_REQUEST', s.user_id, 24 * 60 * 60 * 1000)) continue;
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
    const deviceLabel = `${device.site_ref} — ${device.device_ref || device.device_serial}`;

    if (offlineMs > extendedThresholdMs) {
      if (await wasRecentlyFired('DEVICE_OFFLINE_EXTENDED', device.id, 4 * 60 * 60 * 1000)) continue;
      const hours = Math.round(offlineMs / (60 * 60 * 1000));
      await emitAlert({
        type: 'DEVICE_OFFLINE_EXTENDED',
        category: 'devices',
        severity: 'critical',
        entity_id: device.id,
        entity_name: deviceLabel,
        message: `Device offline for more than ${hours} hours`,
      });
    } else if (offlineMs > offlineThresholdMs) {
      if (await wasRecentlyFired('DEVICE_OFFLINE', device.id, 2 * 60 * 60 * 1000)) continue;
      const mins = Math.round(offlineMs / (60 * 1000));
      await emitAlert({
        type: 'DEVICE_OFFLINE',
        category: 'devices',
        severity: 'critical',
        entity_id: device.id,
        entity_name: deviceLabel,
        message: `Device not responding for ${mins} minutes`,
      });
    } else {
      // Device is online — check if it was previously offline and fire DEVICE_BACK_ONLINE
      // Look for a recent DEVICE_OFFLINE or DEVICE_OFFLINE_EXTENDED alert for this device
      // that hasn't been resolved yet
      try {
        const { data: unresolvedOffline } = await supabase
          .from('alerts')
          .select('id, type')
          .eq('entity_id', device.id)
          .in('type', ['DEVICE_OFFLINE', 'DEVICE_OFFLINE_EXTENDED'])
          .eq('status', 'active')
          .order('created_at', { ascending: false })
          .limit(1);

        if (unresolvedOffline?.length > 0) {
          // Mark the offline alert as resolved
          await supabase
            .from('alerts')
            .update({ status: 'resolved', resolved_at: new Date().toISOString() })
            .eq('id', unresolvedOffline[0].id);

          // Fire back-online alert
          if (!(await wasRecentlyFired('DEVICE_BACK_ONLINE', device.id, 2 * 60 * 60 * 1000))) {
            await emitAlert({
              type: 'DEVICE_BACK_ONLINE',
              category: 'devices',
              severity: 'resolved',
              entity_id: device.id,
              entity_name: deviceLabel,
              message: `Device is back online: ${deviceLabel}`,
            });
          }
        }
      } catch (err) {
        console.warn('[Cron] Device back-online check failed:', err.message);
      }
    }
  }
}

// ══════════════════════════════════════════════════════════════════
// CHEMICAL / TANK LEVEL + DELIVERY REFILL SCANNERS
// ══════════════════════════════════════════════════════════════════

/**
 * Shared helper — fetches tank configs, refills, and scans from ELORA APIs,
 * then computes the current level for each tank. Returns an array of tank
 * level objects used by chemical, refill, and consumption scanners.
 */
async function computeAllTankLevels(supabase) {
  const { data: configs } = await supabase
    .from('tank_configurations')
    .select('id, site_ref, device_ref, device_serial, product_type, tank_number, max_capacity_litres, warning_threshold_pct, critical_threshold_pct, calibration_rate_per_60s, active')
    .eq('active', true)
    .limit(200);

  if (!configs?.length) return [];

  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!supabaseUrl || !serviceKey) return [];

  const toDate = new Date().toISOString().split('T')[0];
  const fromDateScans = new Date(Date.now() - 730 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

  let refills = [];
  let scans = [];
  try {
    const [refillsRes, scansRes] = await Promise.all([
      fetch(`${supabaseUrl}/functions/v1/elora_refills`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${serviceKey}` },
        body: JSON.stringify({ status: 'confirmed,delivered', fromDate: '2019-01-01', toDate }),
      }).then(r => r.json()).catch(() => []),
      fetch(`${supabaseUrl}/functions/v1/elora_scans`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${serviceKey}` },
        body: JSON.stringify({ fromDate: fromDateScans, toDate, status: 'success,exceeded', export: true }),
      }).then(r => r.json()).catch(() => []),
    ]);
    refills = Array.isArray(refillsRes) ? refillsRes : (refillsRes?.data ?? refillsRes?.refills ?? []);
    scans = Array.isArray(scansRes) ? scansRes : (scansRes?.data ?? []);
  } catch (err) {
    console.warn('[Cron] Tank level fetch failed:', err.message);
    return [];
  }

  if (!Array.isArray(refills)) refills = [];
  if (!Array.isArray(scans)) scans = [];

  // Filter only confirmed/delivered refills
  refills = refills.filter(r => {
    const status = (r.status || '').toLowerCase();
    const statusId = r.statusId ?? r.status_id;
    return status === 'confirmed' || status === 'delivered' || statusId === 2 || statusId === 3;
  });

  const results = [];

  for (const tank of configs) {
    try {
      const siteRef = (tank.site_ref || '').trim();
      const productType = (tank.product_type || '').toUpperCase();
      const maxCapacity = Number(tank.max_capacity_litres) || 1000;
      const calibration = Number(tank.calibration_rate_per_60s) || 0;

      if (!calibration) {
        results.push({ tank, status: 'NO_CALIBRATION' });
        continue;
      }

      // Find refills for this site + product type
      const siteRefills = refills.filter(r => {
        const rSite = (r.siteRef ?? r.site_ref ?? r.site ?? r.siteName ?? '').toString().trim();
        if (!siteRef || !rSite) return false;
        if (rSite !== siteRef && !rSite.includes(siteRef) && !siteRef.includes(rSite)) return false;
        if (productType) {
          const rProduct = (r.productName ?? r.product ?? '').toString().toUpperCase();
          const matchConc = (productType === 'ECSR' || productType === 'CONC') && (rProduct.includes('ECSR') || rProduct.includes('CONC') || rProduct.includes('ELORA-GAR'));
          const matchFoam = productType === 'FOAM' && (rProduct.includes('FOAM') || rProduct.includes('ELORA-GAR'));
          const matchTw = productType === 'TW' && (rProduct.includes('TRUCK WASH') || rProduct.includes('ETW') || rProduct.includes('TW-'));
          const matchGel = productType === 'GEL' && rProduct.includes('GEL');
          if (!matchConc && !matchFoam && !matchTw && !matchGel) return false;
        }
        return true;
      }).sort((a, b) => {
        const aDate = new Date(a.deliveredAt || a.dateTime || a.date || 0);
        const bDate = new Date(b.deliveredAt || b.dateTime || b.date || 0);
        return bDate - aDate;
      });

      const lastRefill = siteRefills[0];
      if (!lastRefill) {
        results.push({ tank, status: 'NO_REFILL' });
        continue;
      }

      const refillDate = new Date(lastRefill.deliveredAt || lastRefill.dateTime || lastRefill.date);
      const daysSinceRefill = (Date.now() - refillDate.getTime()) / (24 * 60 * 60 * 1000);
      const deviceSerial = (tank.device_serial || '').trim();

      // Scans since last refill for this device
      const scansSinceRefill = scans.filter(s => {
        const scanDate = new Date(s.createdAt || s.created_at);
        if (scanDate < refillDate) return false;
        const scanSerial = (s.computerSerialId || s.deviceSerial || s.device_serial || '').toString().trim();
        return scanSerial && deviceSerial && scanSerial === deviceSerial;
      });

      // Total consumption: (wash_time_seconds / 60) * calibration_rate
      const totalConsumed = scansSinceRefill.reduce((sum, s) => {
        const washTime = Number(s.washTime ?? s.wash_time) || 0;
        return sum + (washTime / 60) * calibration;
      }, 0);

      const startingLevel = Number(lastRefill.newTotalLitres ?? lastRefill.new_total_litres ?? lastRefill.deliveredLitres ?? lastRefill.delivered_litres) || maxCapacity;
      const effectiveCapacity = Math.max(maxCapacity, startingLevel);
      const currentLitres = Math.max(0, Math.min(effectiveCapacity, startingLevel - totalConsumed));
      const percentage = effectiveCapacity > 0 ? (currentLitres / effectiveCapacity) * 100 : null;

      // Average daily consumption (over the period since refill)
      const avgDailyLitres = daysSinceRefill > 0.5 ? totalConsumed / daysSinceRefill : 0;
      const daysRemaining = avgDailyLitres > 0 && currentLitres > 0 ? currentLitres / avgDailyLitres : null;

      // 7-day rolling average for anomaly detection
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
      const recentScans = scansSinceRefill.filter(s => new Date(s.createdAt || s.created_at) >= sevenDaysAgo);
      const recentConsumed = recentScans.reduce((sum, s) => {
        const washTime = Number(s.washTime ?? s.wash_time) || 0;
        return sum + (washTime / 60) * calibration;
      }, 0);
      const avgDailyLitres7d = recentConsumed / 7;

      // Previous 7-day window for comparison (day -14 to day -7)
      const fourteenDaysAgo = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000);
      const prevWindowScans = scansSinceRefill.filter(s => {
        const d = new Date(s.createdAt || s.created_at);
        return d >= fourteenDaysAgo && d < sevenDaysAgo;
      });
      const prevConsumed = prevWindowScans.reduce((sum, s) => {
        const washTime = Number(s.washTime ?? s.wash_time) || 0;
        return sum + (washTime / 60) * calibration;
      }, 0);
      const avgDailyLitresPrev7d = prevConsumed / 7;

      const tankLabel = `${siteRef} — Tank ${tank.tank_number || 1} (${productType || 'Unknown'})`;

      results.push({
        tank,
        tankLabel,
        status: 'OK',
        percentage,
        currentLitres,
        effectiveCapacity,
        startingLevel,
        totalConsumed,
        avgDailyLitres,
        avgDailyLitres7d,
        avgDailyLitresPrev7d,
        daysRemaining,
        daysSinceRefill,
        refillDate,
        scansSinceRefill,
      });
    } catch (err) {
      console.warn(`[Cron] Tank level calc failed for ${tank.id}:`, err.message);
    }
  }

  return results;
}

/**
 * Shared tank data cache — reused across chemical/refill/consumption scanners
 * within the same cron cycle. Cleared after each cycle.
 */
let _tankLevelCache = null;
let _tankLevelCacheTime = 0;
const TANK_CACHE_TTL = 4 * 60 * 1000; // 4 minutes (scanners run every 5 min)

async function getTankLevels(supabase) {
  if (_tankLevelCache && (Date.now() - _tankLevelCacheTime) < TANK_CACHE_TTL) {
    return _tankLevelCache;
  }
  _tankLevelCache = await computeAllTankLevels(supabase);
  _tankLevelCacheTime = Date.now();
  return _tankLevelCache;
}

async function scanChemicalLevels(supabase, emitAlert) {
  const levels = await getTankLevels(supabase);

  for (const entry of levels) {
    if (entry.status !== 'OK' || entry.percentage == null) continue;
    const { tank, tankLabel, percentage, currentLitres, effectiveCapacity } = entry;
    const warningPct = Number(tank.warning_threshold_pct) || 20;
    const criticalPct = Number(tank.critical_threshold_pct) || 10;

    if (percentage <= warningPct) {
      if (await wasRecentlyFired('LOW_CHEMICAL_LEVEL', tank.id, 6 * 60 * 60 * 1000)) continue;
      await emitAlert({
        type: 'LOW_CHEMICAL_LEVEL',
        category: 'chemicals',
        severity: percentage <= criticalPct ? 'critical' : 'warning',
        entity_id: tank.id,
        entity_name: tankLabel,
        message: `Chemical level at ${Math.round(percentage)}% (${Math.round(currentLitres)}L / ${effectiveCapacity}L)`,
      });
    }
  }
}

async function scanRefillThresholds(supabase, emitAlert) {
  const levels = await getTankLevels(supabase);

  for (const entry of levels) {
    if (entry.status !== 'OK' || entry.percentage == null) continue;
    const { tank, tankLabel, percentage, currentLitres, effectiveCapacity, daysRemaining } = entry;
    const warningPct = Number(tank.warning_threshold_pct) || 20;
    const criticalPct = Number(tank.critical_threshold_pct) || 10;

    // SITE_APPROACHING_REFILL — level between critical and warning threshold
    if (percentage > criticalPct && percentage <= warningPct) {
      if (await wasRecentlyFired('SITE_APPROACHING_REFILL', tank.id, 12 * 60 * 60 * 1000)) continue;
      const daysMsg = daysRemaining != null ? ` (~${Math.round(daysRemaining)} days remaining)` : '';
      await emitAlert({
        type: 'SITE_APPROACHING_REFILL',
        category: 'delivery',
        severity: 'warning',
        entity_id: tank.id,
        entity_name: tankLabel,
        message: `Tank at ${Math.round(percentage)}% (${Math.round(currentLitres)}L / ${effectiveCapacity}L)${daysMsg} — approaching refill threshold`,
      });
    }

    // SITE_OVERDUE_REFILL — level at or below critical threshold
    if (percentage <= criticalPct) {
      if (await wasRecentlyFired('SITE_OVERDUE_REFILL', tank.id, 8 * 60 * 60 * 1000)) continue;
      const daysMsg = daysRemaining != null ? ` (~${Math.round(daysRemaining)} days remaining)` : '';
      await emitAlert({
        type: 'SITE_OVERDUE_REFILL',
        category: 'delivery',
        severity: 'critical',
        entity_id: tank.id,
        entity_name: tankLabel,
        message: `Tank critically low at ${Math.round(percentage)}% (${Math.round(currentLitres)}L / ${effectiveCapacity}L)${daysMsg} — overdue for refill`,
      });
    }
  }
}

async function scanUnusualConsumption(supabase, emitAlert) {
  const levels = await getTankLevels(supabase);

  for (const entry of levels) {
    if (entry.status !== 'OK') continue;
    const { tank, tankLabel, avgDailyLitres7d, avgDailyLitresPrev7d, daysSinceRefill } = entry;

    // Need at least 10 days of data (7-day current window + 7-day prior window overlap with refill)
    if (daysSinceRefill < 10) continue;
    // Need meaningful baseline consumption (at least 1 litre/day in prior period)
    if (avgDailyLitresPrev7d < 1) continue;
    // Need current consumption to be non-trivial
    if (avgDailyLitres7d < 1) continue;

    // Flag if current 7-day average is ≥50% higher than prior 7-day average
    const deviationPct = ((avgDailyLitres7d - avgDailyLitresPrev7d) / avgDailyLitresPrev7d) * 100;

    if (deviationPct >= 50) {
      if (await wasRecentlyFired('UNUSUAL_CONSUMPTION', tank.id, 24 * 60 * 60 * 1000)) continue;
      await emitAlert({
        type: 'UNUSUAL_CONSUMPTION',
        category: 'delivery',
        severity: 'warning',
        entity_id: tank.id,
        entity_name: tankLabel,
        message: `Consumption up ${Math.round(deviationPct)}% — averaging ${Math.round(avgDailyLitres7d)}L/day vs ${Math.round(avgDailyLitresPrev7d)}L/day prior week`,
      });
    }
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
    if (await wasRecentlyFired('DELIVERY_SCHEDULED_TODAY', 'digest', 20 * 60 * 60 * 1000)) return;
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
      if (await wasRecentlyFired('SITE_NO_DELIVERY', siteRef, 24 * 60 * 60 * 1000)) continue;
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
      if (await wasRecentlyFired('MANAGER_NOT_LOGGED_IN_7_DAYS', mgr.id, 24 * 60 * 60 * 1000)) continue;
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

    if (await wasRecentlyFired('REPORT_DUE_TODAY', sched.id, 20 * 60 * 60 * 1000)) continue;
    await emitAlert({
      type: 'REPORT_DUE_TODAY',
      category: 'report_scheduling',
      severity: 'critical',
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

    if (await wasRecentlyFired('REPORT_DUE_IN_X_DAYS', sched.id, 24 * 60 * 60 * 1000)) continue;
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
      if (await wasRecentlyFired('REPORT_OVERDUE', sched.id, 12 * 60 * 60 * 1000)) continue;
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
    if (await wasRecentlyFired('COMPANY_NO_REPORT_SCHEDULE', company.id, 7 * 24 * 60 * 60 * 1000)) continue;
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
    if (await wasRecentlyFired('SCHEDULE_NO_REPORTS', sched.id, 7 * 24 * 60 * 60 * 1000)) continue;
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
    if (await wasRecentlyFired('WEEKLY_REPORT_DIGEST', 'digest', 6 * 24 * 60 * 60 * 1000)) return;
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
