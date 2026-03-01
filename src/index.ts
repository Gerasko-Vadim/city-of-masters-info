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

function fmtNewOrder(o: any) {
  const time = new Date().toLocaleTimeString('ru-RU', {
    hour: '2-digit', minute: '2-digit', timeZone: 'Asia/Bishkek',
  });
  return (
    `📦 <b>Новый заказ #${o.id}</b>  •  ${time}\n\n` +
    `👤 ${o.customerName ?? o.customer_name}\n` +
    `📞 ${o.phone || '—'}\n` +
    `📍 ${o.address}\n` +
    `💰 Сумма: <b>${o.totalAmount ?? o.total_amount} сом</b>`
  );
}

function fmtPaid(o: any) {
  return (
    `✅ <b>Заказ #${o.id} оплачен!</b>\n\n` +
    `👤 ${o.customerName ?? o.customer_name}\n` +
    `📞 ${o.phone || '—'}\n` +
    `📍 ${o.address}\n` +
    `💰 ${o.totalAmount ?? o.total_amount} сом  •  комиссия ${o.commission} сом\n\n` +
    `🔍 Поиск мастера запущен...`
  );
}

function fmtInProgress(o: any, spec: any) {
  const specLine = spec
    ? `\n\n👷 <b>${spec.name}</b>` +
      (spec.username ? ` (@${spec.username})` : '') +
      (spec.phone ? `\n📞 ${spec.phone}` : '')
    : (o.assignedSpecialistId ? `\n\n👷 Мастер ID: ${o.assignedSpecialistId}` : '');
  return (
    `🔧 <b>Заказ #${o.id} взят в работу!</b>\n\n` +
    `👤 ${o.customerName ?? o.customer_name}\n` +
    `📍 ${o.address}\n` +
    `💰 ${o.totalAmount ?? o.total_amount} сом` +
    specLine
  );
}

function fmtCompleted(o: any, spec: any) {
  return (
    `🏁 <b>Заказ #${o.id} завершён!</b>\n\n` +
    `👤 ${o.customerName ?? o.customer_name}\n` +
    `📍 ${o.address}\n` +
    `💰 ${o.totalAmount ?? o.total_amount} сом  •  комиссия ${o.commission} сом` +
    (spec ? `\n👷 Мастер: ${spec.name}` : '')
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
  const now = new Date();
  // Get current date in Bishkek timezone
  const bskStr = now.toLocaleString('en-US', { timeZone: 'Asia/Bishkek' });
  const bsk = new Date(bskStr);

  if (period === 'today') {
    bsk.setHours(0, 0, 0, 0);
  } else if (period === 'week') {
    bsk.setHours(0, 0, 0, 0);
    bsk.setDate(bsk.getDate() - bsk.getDay() + (bsk.getDay() === 0 ? -6 : 1)); // Mon
  } else {
    bsk.setHours(0, 0, 0, 0);
    bsk.setDate(1);
  }

  // Convert Bishkek time back to UTC
  const utcOffset = now.getTime() - new Date(bskStr).getTime();
  return new Date(bsk.getTime() + utcOffset);
}

function periodLabel(period: 'today' | 'week' | 'month'): string {
  const now = new Date();
  if (period === 'today') {
    return now.toLocaleDateString('ru-RU', { day: 'numeric', month: 'long', timeZone: 'Asia/Bishkek' });
  }
  if (period === 'week') return 'эту неделю';
  return now.toLocaleDateString('ru-RU', { month: 'long', year: 'numeric', timeZone: 'Asia/Bishkek' });
}

async function buildReport(period: 'today' | 'week' | 'month'): Promise<string> {
  const since = periodStart(period);

  const [ordersRes, specsRes, clientsRes] = await Promise.all([
    pool.query(`SELECT status FROM "order" WHERE "createdAt" >= $1`, [since]),
    pool.query(`SELECT id FROM specialists WHERE "createdAt" >= $1`, [since]),
    pool.query(`SELECT id FROM client WHERE "createdAt" >= $1`, [since]),
  ]);

  const orders     = ordersRes.rows;
  const total      = orders.length;
  const newCount   = orders.filter((o) => o.status === 'NEW').length;
  const paid       = orders.filter((o) => ['PAID', 'IN_PROGRESS', 'COMPLETED'].includes(o.status)).length;
  const inProgress = orders.filter((o) => o.status === 'IN_PROGRESS').length;
  const completed  = orders.filter((o) => o.status === 'COMPLETED').length;
  const newSpecs   = specsRes.rows.length;
  const newClients = clientsRes.rows.length;

  return (
    `📊 <b>Отчёт за ${periodLabel(period)}</b>\n\n` +
    `📦 Заказов получено:  <b>${total}</b>\n` +
    `🆕 Без оплаты:         <b>${newCount}</b>\n` +
    `✅ Оплачено:           <b>${paid}</b>\n` +
    `🔧 В работе:           <b>${inProgress}</b>\n` +
    `🏁 Завершено:          <b>${completed}</b>\n\n` +
    `👷 Новых мастеров:  <b>${newSpecs}</b>\n` +
    `👤 Новых клиентов:  <b>${newClients}</b>`
  );
}

async function sendDailyReport() {
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

  // New order — emitted by backend after create
  socket.on('admin_notification', async (data: any) => {
    if (data.type !== 'NEW_ORDER' || !ready) return;
    if (orderStatusMap.has(Number(data.orderId))) return; // already known
    try {
      const { rows } = await pool.query('SELECT * FROM "order" WHERE id = $1', [data.orderId]);
      const o = rows[0];
      if (o) {
        orderStatusMap.set(o.id, o.status);
        await broadcast(fmtNewOrder(o));
      }
    } catch {
      orderStatusMap.set(Number(data.orderId), 'NEW');
      await broadcast(`📦 <b>Новый заказ #${data.orderId}</b>\n👤 ${data.customerName}`);
    }
  });

  // Status transitions
  socket.on('orderUpdate', async (order: any) => {
    if (!ready) return;
    const prevStatus = orderStatusMap.get(order.id);
    const newStatus: string = order.status;
    orderStatusMap.set(order.id, newStatus);

    if (!prevStatus || prevStatus === newStatus) return;

    try {
      if (newStatus === 'PAID') {
        await broadcast(fmtPaid(order));
      } else if (newStatus === 'IN_PROGRESS') {
        const spec = order.assignedSpecialistId
          ? await getSpecialist(order.assignedSpecialistId)
          : null;
        await broadcast(fmtInProgress(order, spec));
      } else if (newStatus === 'COMPLETED') {
        const spec = order.assignedSpecialistId
          ? await getSpecialist(order.assignedSpecialistId)
          : null;
        await broadcast(fmtCompleted(order, spec));
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
  await ctx.reply(
    `👋 <b>Город Мастеров — Info Bot</b>\n\nВы будете получать уведомления о новых заказах, оплатах и назначениях мастеров.`,
    { parse_mode: 'HTML', ...mainKeyboard },
  );
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
    await ctx.reply('❌ Ошибка при формировании отчёта. Попробуйте позже.');
    console.error('Report error:', err.message);
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
  app.get('/health', (_, res) => res.json({ status: 'ok', ready, orders: orderStatusMap.size }));
  const PORT = process.env.PORT || 3005;
  await new Promise<void>((resolve) => app.listen(PORT, () => {
    console.log(`Health check on :${PORT}`);
    resolve();
  }));

  // 2. Delete webhook (in case it was set before), then launch polling
  try {
    await bot.telegram.deleteWebhook({ drop_pending_updates: true });
    console.log('Webhook deleted');
  } catch (err: any) {
    console.error('deleteWebhook error:', err.message);
  }
  bot.launch().catch((err) => console.error('Bot launch error:', err.message));
  console.log('Info Bot is running 🚀');

  // 3. Async init in background — does not block healthcheck
  (async () => {
    try {
      await ensureSubscribersTable();
      await preloadOrderStatuses();
      ready = true;
      connectSocket();
      cron.schedule('55 23 * * *', sendDailyReport, { timezone: 'Asia/Bishkek' });
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
