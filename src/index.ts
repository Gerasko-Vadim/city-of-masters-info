import { Telegraf, Markup } from 'telegraf';
import * as dotenv from 'dotenv';
import express from 'express';
import { io as socketIO } from 'socket.io-client';
import { Pool } from 'pg';
import cron from 'node-cron';

dotenv.config({
  path: process.env.NODE_ENV === 'production' ? '.env.production' : '.env.development',
});

// ── Config ───────────────────────────────────────────────────────────────────
const BOT_TOKEN    = process.env.BOT_TOKEN!;
const BACKEND_URL  = process.env.BACKEND_URL!;
const DATABASE_URL = process.env.DATABASE_URL!;

if (!BOT_TOKEN || !BACKEND_URL || !DATABASE_URL) {
  console.error('Missing env vars: BOT_TOKEN, BACKEND_URL, DATABASE_URL');
  process.exit(1);
}

const bot  = new Telegraf(BOT_TOKEN);
const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } });

// Global error handler — prevents unhandled errors from crashing polling
bot.catch((err: any, ctx: any) => {
  console.error(`Bot error [${ctx?.updateType}]:`, err?.message ?? err);
});

// Tracks last known status per order to detect transitions
const orderStatusMap = new Map<number, string>();
let ready = false;

// ── Subscribers table ─────────────────────────────────────────────────────────

async function ensureSubscribersTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS info_subscribers (
      chat_id BIGINT PRIMARY KEY,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `);
}

async function addSubscriber(chatId: number) {
  await pool.query(
    'INSERT INTO info_subscribers (chat_id) VALUES ($1) ON CONFLICT DO NOTHING',
    [chatId],
  );
}

async function removeSubscriber(chatId: number) {
  await pool.query('DELETE FROM info_subscribers WHERE chat_id = $1', [chatId]);
}

async function getSubscribers(): Promise<number[]> {
  const { rows } = await pool.query<{ chat_id: string }>('SELECT chat_id FROM info_subscribers');
  return rows.map((r) => Number(r.chat_id));
}

// ── Broadcast ─────────────────────────────────────────────────────────────────

async function broadcast(text: string) {
  const subs = await getSubscribers();
  await Promise.allSettled(
    subs.map((chatId) =>
      bot.telegram.sendMessage(chatId, text, { parse_mode: 'HTML' }),
    ),
  );
}

// ── Notification formatters ───────────────────────────────────────────────────

function fmtSource(o: any): string {
  return (o.source ?? '') === 'ADMIN' ? '👨‍💼 Источник: Администратор' : '📱 Источник: Клиент';
}

// Extracts price range from description, e.g. "Стоимость: 400–600 сом" → "от 400–600 сом"
function fmtServiceCost(o: any): string {
  const desc: string = o.description ?? '';
  const match = desc.match(/Стоимость:\s*(.+)/);
  return match ? `💵 Стоимость услуг: <b>от ${match[1].trim()}</b>` : '';
}

function fmtNewOrder(o: any) {
  const time = new Date().toLocaleTimeString('ru-RU', {
    hour: '2-digit', minute: '2-digit', timeZone: 'Asia/Bishkek',
  });
  const costLine = fmtServiceCost(o);
  return (
    `📦 <b>Новый заказ #${o.id}</b>  •  ${time}\n\n` +
    `👤 ${o.customerName ?? o.customer_name}\n` +
    `📞 ${o.phone || '—'}\n` +
    `📍 ${o.address}\n` +
    (costLine ? `${costLine}\n` : '') +
    fmtSource(o)
  );
}

function fmtPaid(o: any) {
  const costLine = fmtServiceCost(o);
  return (
    `✅ <b>Заказ #${o.id} оплачен!</b>\n\n` +
    `👤 ${o.customerName ?? o.customer_name}\n` +
    `📞 ${o.phone || '—'}\n` +
    `📍 ${o.address}\n` +
    (costLine ? `${costLine}\n` : '') +
    `${fmtSource(o)}\n\n` +
    `🔍 Поиск мастера запущен...`
  );
}

function fmtInProgress(o: any, spec: any) {
  const costLine = fmtServiceCost(o);
  const specBlock = spec
    ? `\n\n👷 Мастер: <b>${spec.fullName ?? spec.name}</b>` +
      (spec.username ? ` (@${spec.username})` : '') +
      (spec.phone ? `\n📞 ${spec.phone}` : '')
    : o.assignedSpecialistId
      ? `\n\n👷 Мастер ID: ${o.assignedSpecialistId}`
      : '';
  return (
    `🔧 <b>Заказ #${o.id} взят в работу!</b>\n\n` +
    `👤 ${o.customerName ?? o.customer_name}\n` +
    `📍 ${o.address}\n` +
    (costLine ? `${costLine}\n` : '') +
    specBlock
  );
}

function fmtCompleted(o: any, spec: any) {
  const costLine = fmtServiceCost(o);
  return (
    `🏁 <b>Заказ #${o.id} завершён!</b>\n\n` +
    `👤 ${o.customerName ?? o.customer_name}\n` +
    `📍 ${o.address}\n` +
    (costLine ? `${costLine}\n` : '') +
    (spec ? `👷 Мастер: <b>${spec.fullName ?? spec.name}</b>` : '')
  );
}

// ── DB helpers ────────────────────────────────────────────────────────────────

async function preloadOrderStatuses() {
  const { rows } = await pool.query<{ id: number; status: string }>(
    'SELECT id, status FROM "order"',
  );
  rows.forEach((r) => orderStatusMap.set(r.id, r.status));
  console.log(`Preloaded ${rows.length} order statuses`);
}

async function getSpecialist(telegramId: string | number) {
  try {
    const { rows } = await pool.query(
      'SELECT * FROM specialists WHERE "telegramId" = $1',
      [String(telegramId)],
    );
    return rows[0] ?? null;
  } catch {
    return null;
  }
}

// ── Report ────────────────────────────────────────────────────────────────────

function periodStart(period: 'today' | 'week' | 'month'): Date {
  // Bishkek = UTC+6
  const BSK = 6 * 60 * 60 * 1000;
  const bskNow = new Date(Date.now() + BSK);

  const y   = bskNow.getUTCFullYear();
  const mon = bskNow.getUTCMonth();
  const d   = bskNow.getUTCDate();
  const dow = bskNow.getUTCDay(); // 0=Sun

  if (period === 'today') {
    return new Date(Date.UTC(y, mon, d) - BSK);
  }
  if (period === 'week') {
    const daysBack = dow === 0 ? 6 : dow - 1; // back to Monday
    return new Date(Date.UTC(y, mon, d - daysBack) - BSK);
  }
  // month
  return new Date(Date.UTC(y, mon, 1) - BSK);
}

function periodLabel(period: 'today' | 'week' | 'month'): string {
  const now = new Date();
  if (period === 'today') {
    return now.toLocaleDateString('ru-RU', { day: 'numeric', month: 'long', timeZone: 'Asia/Bishkek' });
  }
  if (period === 'week') return 'эту неделю';
  return now.toLocaleDateString('ru-RU', { month: 'long', year: 'numeric', timeZone: 'Asia/Bishkek' });
}

async function safeCount(sql: string, params: any[]): Promise<number> {
  try {
    const res = await pool.query(sql, params);
    return res.rows.length;
  } catch (err: any) {
    console.error('Query failed:', sql, err.message);
    return 0;
  }
}

async function buildReport(period: 'today' | 'week' | 'month'): Promise<string> {
  const since = periodStart(period);
  console.log(`Building report [${period}] since:`, since.toISOString());

  // Orders — fetch with status and source
  let orders: any[] = [];
  try {
    const res = await pool.query(
      `SELECT status, source FROM "order" WHERE "createdAt" >= $1`, [since]
    );
    orders = res.rows;
  } catch (err: any) {
    console.error('Orders query failed:', err.message);
  }

  const total      = orders.length;
  const newCount   = orders.filter((o) => o.status === 'NEW').length;
  const paid       = orders.filter((o) => ['PAID', 'IN_PROGRESS', 'COMPLETED'].includes(o.status)).length;
  const inProgress = orders.filter((o) => o.status === 'IN_PROGRESS').length;
  const completed  = orders.filter((o) => o.status === 'COMPLETED').length;
  const fromClient = orders.filter((o) => o.source === 'CLIENT').length;
  const fromAdmin  = orders.filter((o) => o.source === 'ADMIN').length;

  // Specialists — try both column name variants
  const newSpecs = await safeCount(`SELECT id FROM specialists WHERE "createdAt" >= $1`, [since])
    || await safeCount(`SELECT id FROM specialists WHERE created_at >= $1`, [since]);

  // Registered specialists (filled fullName) in period
  const registeredSpecs = await safeCount(
    `SELECT id FROM specialists WHERE "fullName" IS NOT NULL AND "fullName" != '' AND "createdAt" >= $1`, [since]
  ) || await safeCount(
    `SELECT id FROM specialists WHERE "fullName" IS NOT NULL AND "fullName" != '' AND created_at >= $1`, [since]
  );

  // Total registered specialists (all time)
  let totalRegistered = 0;
  try {
    const res = await pool.query(`SELECT COUNT(*) FROM specialists WHERE "fullName" IS NOT NULL AND "fullName" != ''`);
    totalRegistered = Number(res.rows[0]?.count ?? 0);
  } catch {}

  // Clients — try both table names and both column names
  let newClients = 0;
  for (const tbl of ['client', 'clients']) {
    for (const col of ['"createdAt"', 'created_at']) {
      try {
        const res = await pool.query(`SELECT id FROM ${tbl} WHERE ${col} >= $1`, [since]);
        newClients = res.rows.length;
        break;
      } catch { /* try next */ }
    }
    if (newClients > 0) break;
  }

  return (
    `📊 <b>Отчёт за ${periodLabel(period)}</b>\n\n` +
    `📦 Заказов получено:  <b>${total}</b>\n` +
    `🆕 Без оплаты:         <b>${newCount}</b>\n` +
    `✅ Оплачено:           <b>${paid}</b>\n` +
    `🔧 В работе:           <b>${inProgress}</b>\n` +
    `🏁 Завершено:          <b>${completed}</b>\n\n` +
    `📱 С мини-апп (клиент): <b>${fromClient}</b>\n` +
    `👨‍💼 С админки:            <b>${fromAdmin}</b>\n\n` +
    `👷 Новых мастеров:           <b>${newSpecs}</b>\n` +
    `✅ Зарегистрировались:       <b>${registeredSpecs}</b>\n` +
    `👥 Всего зарег. мастеров:   <b>${totalRegistered}</b>\n` +
    `👤 Новых клиентов:           <b>${newClients}</b>`
  );
}

async function sendScheduledReports() {
  // Current date in Bishkek
  const BSK = 6 * 60 * 60 * 1000;
  const bskNow = new Date(Date.now() + BSK);
  const dow       = bskNow.getUTCDay();   // 0=Sun
  const dayOfMonth = bskNow.getUTCDate();
  const lastDayOfMonth = new Date(Date.UTC(
    bskNow.getUTCFullYear(), bskNow.getUTCMonth() + 1, 0
  )).getUTCDate();

  const isLastDayOfMonth = dayOfMonth === lastDayOfMonth;
  const isSunday         = dow === 0;

  // Monthly report — last day of month (takes priority, sent instead of weekly)
  if (isLastDayOfMonth) {
    try {
      const msg = await buildReport('month');
      await broadcast(msg);
      console.log('Monthly report sent');
    } catch (err: any) {
      console.error('Monthly report failed:', err.message);
    }
    return; // skip weekly/daily if monthly was sent
  }

  // Weekly report — every Sunday
  if (isSunday) {
    try {
      const msg = await buildReport('week');
      await broadcast(msg);
      console.log('Weekly report sent');
    } catch (err: any) {
      console.error('Weekly report failed:', err.message);
    }
    return; // skip daily if weekly was sent
  }

  // Daily report — every other day
  try {
    const msg = await buildReport('today');
    await broadcast(msg);
    console.log('Daily report sent');
  } catch (err: any) {
    console.error('Daily report failed:', err.message);
  }
}

// ── Socket.io ─────────────────────────────────────────────────────────────────

function connectSocket() {
  const socket = socketIO(BACKEND_URL, {
    transports: ['websocket', 'polling'],
    reconnection: true,
    reconnectionDelay: 5000,
  });

  socket.on('connect', () => console.log('✅ Socket.io connected'));
  socket.on('disconnect', (reason) => console.log('⚡ Socket.io disconnected:', reason));
  socket.on('connect_error', (err) => console.error('Socket.io error:', err.message));

  socket.on('specialistUpdate', async (spec: any) => {
    if (!ready || spec._event !== 'registered') return;
    try {
      const time = new Date().toLocaleTimeString('ru-RU', {
        hour: '2-digit', minute: '2-digit', timeZone: 'Asia/Bishkek',
      });
      const msg =
        `👷 <b>Новый мастер зарегистрирован!</b>  •  ${time}\n\n` +
        `👤 ${spec.fullName}\n` +
        `📞 ${spec.phone || '—'}\n` +
        (spec.username ? `🔗 @${spec.username}\n` : '') +
        `🆔 Telegram ID: ${spec.telegramId}`;
      await broadcast(msg);
    } catch (err: any) {
      console.error('specialistUpdate broadcast error:', err.message);
    }
  });

  socket.on('orderUpdate', async (order: any) => {
    if (!ready) return;

    const prevStatus = orderStatusMap.get(order.id);
    const newStatus: string = order.status;
    orderStatusMap.set(order.id, newStatus);

    // No change
    if (prevStatus === newStatus) return;

    try {
      if (prevStatus === undefined) {
        // Brand new order (not in preloaded map = just created)
        if (newStatus === 'NEW') {
          // Fetch full order from DB for complete details
          try {
            const { rows } = await pool.query('SELECT * FROM "order" WHERE id = $1', [order.id]);
            await broadcast(fmtNewOrder(rows[0] ?? order));
          } catch {
            await broadcast(fmtNewOrder(order));
          }
        }
        return;
      }

      // Fetch full order from DB to get description, assignedSpecialistId etc.
      let fullOrder = order;
      try {
        const { rows } = await pool.query('SELECT * FROM "order" WHERE id = $1', [order.id]);
        if (rows[0]) fullOrder = rows[0];
      } catch { /* fallback to socket data */ }

      // Known order — status transition
      if (newStatus === 'PAID') {
        await broadcast(fmtPaid(fullOrder));
      } else if (newStatus === 'IN_PROGRESS') {
        const specialistId = fullOrder.assignedSpecialistId ?? fullOrder.assigned_specialist_id;
        const spec = specialistId ? await getSpecialist(specialistId) : null;
        await broadcast(fmtInProgress(fullOrder, spec));
      } else if (newStatus === 'COMPLETED') {
        const specialistId = fullOrder.assignedSpecialistId ?? fullOrder.assigned_specialist_id;
        const spec = specialistId ? await getSpecialist(specialistId) : null;
        await broadcast(fmtCompleted(fullOrder, spec));
      }
    } catch (err: any) {
      console.error('Broadcast error:', err.message);
    }
  });
}

// ── Keyboards ─────────────────────────────────────────────────────────────────

const mainKeyboard = Markup.keyboard([
  ['📊 Отчёт'],
  ['🔕 Отписаться'],
]).resize();

const reportKeyboard = Markup.keyboard([
  ['📅 Сегодня', '📆 Неделя', '🗓 Месяц'],
  ['← Назад'],
]).resize();

// ── Bot commands ──────────────────────────────────────────────────────────────

bot.start(async (ctx) => {
  try {
    await ctx.reply(
      `👋 <b>Добро пожаловать в Город Мастеров — Info Bot!</b>\n\n` +
      `Этот бот держит вас в курсе всего что происходит на платформе.\n\n` +
      `<b>🔔 Автоматические уведомления:</b>\n` +
      `• 📦 Новый заказ — сразу после создания\n` +
      `• ✅ Заказ оплачен — клиент подтвердил оплату\n` +
      `• 🔧 Мастер взял заказ — с именем специалиста\n` +
      `• 🏁 Заказ завершён\n\n` +
      `<b>📊 Отчёты по расписанию (20:00 Бишкек):</b>\n` +
      `• Каждый день — итоги за сегодня\n` +
      `• Каждое воскресенье — итоги за неделю\n` +
      `• Последний день месяца — итоги за месяц\n\n` +
      `<b>Кнопки ниже:</b>\n` +
      `📊 <b>Отчёт</b> — запросить отчёт прямо сейчас за любой период\n` +
      `🔕 <b>Отписаться</b> — отключить уведомления`,
      { parse_mode: 'HTML', ...mainKeyboard },
    );
  } catch (err: any) {
    console.error('/start reply error:', err.message);
    return;
  }
  try {
    await addSubscriber(ctx.chat.id);
    console.log(`Subscriber added: ${ctx.chat.id}`);
  } catch (err: any) {
    console.error('Failed to save subscriber:', err.message);
  }
});

async function handleReport(ctx: any, period: 'today' | 'week' | 'month') {
  await ctx.reply('⏳ Генерирую отчёт...');
  try {
    const msg = await buildReport(period);
    await ctx.reply(msg, { parse_mode: 'HTML', ...reportKeyboard });
  } catch (err: any) {
    console.error('Report error:', err.message);
    await ctx.reply('❌ Не удалось сформировать отчёт. Попробуйте позже.');
  }
}

bot.on('text', async (ctx) => {
  const text = ctx.message.text;
  console.log('Received text:', JSON.stringify(text));

  if (text.includes('Отчёт') || text.includes('Отчет')) {
    await ctx.reply('📊 Выберите период:', reportKeyboard);

  } else if (text.includes('Отписаться')) {
    await ctx.reply('🔕 Вы отписались от уведомлений.\n\nНапишите /start чтобы подписаться снова.', Markup.removeKeyboard());
    try { await removeSubscriber(ctx.chat.id); } catch {}

  } else if (text.includes('Назад')) {
    await ctx.reply('Главное меню:', { ...mainKeyboard });

  } else if (text.includes('Сегодня')) {
    await handleReport(ctx, 'today');

  } else if (text.includes('Неделя') || text.includes('неделя')) {
    await handleReport(ctx, 'week');

  } else if (text.includes('Месяц') || text.includes('месяц')) {
    await handleReport(ctx, 'month');
  }
});

// ── Bootstrap ─────────────────────────────────────────────────────────────────

async function main() {
  console.log('Starting City of Masters Info Bot...');

  // 1. Start Express FIRST so Railway healthcheck passes immediately
  const app = express();
  app.use(express.json());
  app.get('/health', (_, res) => res.json({ status: 'ok', ready, orders: orderStatusMap.size }));

  // Internal webhook — called by backend when master registers
  app.post('/internal/master-registered', async (req, res) => {
    const secret = req.headers['x-internal-secret'];
    if (secret !== process.env.INTERNAL_SECRET) {
      res.status(403).json({ error: 'Forbidden' });
      return;
    }
    const spec = req.body;
    res.json({ ok: true });
    try {
      const time = new Date().toLocaleTimeString('ru-RU', {
        hour: '2-digit', minute: '2-digit', timeZone: 'Asia/Bishkek',
      });
      const msg =
        `👷 <b>Новый мастер зарегистрирован!</b>  •  ${time}\n\n` +
        `👤 ${spec.fullName}\n` +
        `📞 ${spec.phone || '—'}\n` +
        (spec.username ? `🔗 @${spec.username}\n` : '') +
        `🆔 Telegram ID: ${spec.telegramId}`;
      await broadcast(msg);
    } catch (err: any) {
      console.error('master-registered broadcast error:', err.message);
    }
  });

  const PORT = process.env.PORT || 3005;
  await new Promise<void>((resolve) => app.listen(PORT, () => {
    console.log(`Health check on :${PORT}`);
    resolve();
  }));

  // 2. Delete webhook then start polling with auto-restart on timeout
  try {
    await bot.telegram.deleteWebhook({ drop_pending_updates: true });
    console.log('Webhook deleted');
  } catch (err: any) {
    console.error('deleteWebhook error:', err.message);
  }

  function launchBot() {
    bot.launch().catch((err: any) => {
      console.error('Bot polling error:', err?.message ?? err);
      console.log('Restarting polling in 3s...');
      setTimeout(launchBot, 3000);
    });
  }
  launchBot();
  console.log('Info Bot is running 🚀');

  // 3. Async init in background — does not block healthcheck
  (async () => {
    try {
      await ensureSubscribersTable();
      await preloadOrderStatuses();
      ready = true;
      connectSocket();
      // 20:00 Bishkek every day — decides which report to send automatically
  cron.schedule('0 20 * * *', sendScheduledReports, { timezone: 'Asia/Bishkek' });
      console.log('✅ Init complete');
    } catch (err: any) {
      console.error('Init error:', err.message);
    }
  })();

  process.once('SIGINT', () => { bot.stop('SIGINT'); process.exit(0); });
  process.once('SIGTERM', () => { bot.stop('SIGTERM'); process.exit(0); });
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
