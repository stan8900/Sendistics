import hmac
import logging
import os
import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from aiohttp import web
from dotenv import load_dotenv

try:
    from .runtime_config import BASE_DIR, WEB_DIR, create_storage_from_env
except ImportError:  # pragma: no cover - allows `python web_admin/main.py`
    from runtime_config import BASE_DIR, WEB_DIR, create_storage_from_env


load_dotenv(BASE_DIR / ".env")
load_dotenv(WEB_DIR / ".env", override=True)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PAYMENT_AMOUNT = int(os.getenv("PAYMENT_AMOUNT", "100000"))
PAYMENT_CURRENCY = os.getenv("PAYMENT_CURRENCY", "UZS")
PAYMENT_VALID_DAYS = int(os.getenv("PAYMENT_VALID_DAYS", "30"))
SESSION_COOKIE = "sendertistics_admin"


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def period_since(period: str) -> tuple[str, Optional[datetime], int]:
    now = datetime.utcnow()
    if period == "week":
        return "7 дней", now - timedelta(days=7), 7
    if period == "month":
        return "30 дней", now - timedelta(days=30), 30
    if period == "all":
        return "всё время", None, 30
    return "сегодня", now.replace(hour=0, minute=0, second=0, microsecond=0), 1


def money(amount: int) -> str:
    return f"{amount:,}".replace(",", " ") + f" {PAYMENT_CURRENCY}"


def day_key(value: Optional[datetime]) -> Optional[str]:
    return value.date().isoformat() if value else None


def build_series(
    *,
    days: int,
    payments: List[Dict[str, Any]],
    campaign_events: List[Dict[str, Any]],
    delivery_events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    start = datetime.utcnow().date() - timedelta(days=max(0, days - 1))
    buckets: Dict[str, Dict[str, Any]] = {}
    for offset in range(days):
        key = (start + timedelta(days=offset)).isoformat()
        buckets[key] = {"date": key, "payments": 0, "subscriptions": 0, "campaigns": 0, "deliveries": 0}

    for payment in payments:
        created = day_key(parse_iso(payment.get("created_at")))
        resolved = day_key(parse_iso(payment.get("resolved_at")))
        if created in buckets:
            buckets[created]["payments"] += 1
        if payment.get("status") == "approved" and resolved in buckets:
            buckets[resolved]["subscriptions"] += 1
    for event in campaign_events:
        key = day_key(parse_iso(event.get("started_at")))
        if key in buckets:
            buckets[key]["campaigns"] += 1
    for event in delivery_events:
        key = day_key(parse_iso(event.get("delivered_at")))
        if key in buckets:
            buckets[key]["deliveries"] += int(event.get("sent_count") or 0)
    return list(buckets.values())


def public_payment(payment: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "request_id": payment.get("request_id"),
        "user_id": payment.get("user_id"),
        "username": payment.get("username"),
        "full_name": payment.get("full_name"),
        "status": payment.get("status"),
        "created_at": payment.get("created_at"),
        "resolved_at": payment.get("resolved_at"),
        "expires_at": (
            (parse_iso(payment.get("resolved_at")) + timedelta(days=PAYMENT_VALID_DAYS)).isoformat()
            if payment.get("status") == "approved" and parse_iso(payment.get("resolved_at"))
            else None
        ),
    }


async def require_auth(request: web.Request) -> Optional[web.Response]:
    token = request.cookies.get(SESSION_COOKIE)
    expected = request.app["session_token"]
    if token and hmac.compare_digest(token, expected):
        return None
    if request.path.startswith("/api/"):
        return web.json_response({"error": "unauthorized"}, status=401)
    raise web.HTTPFound("/login")


async def login_page(request: web.Request) -> web.Response:
    if request.cookies.get(SESSION_COOKIE) == request.app["session_token"]:
        raise web.HTTPFound("/")
    return web.Response(text=LOGIN_HTML, content_type="text/html")


async def login_api(request: web.Request) -> web.Response:
    data = await request.json()
    password = str(data.get("password") or "")
    expected = request.app["admin_password"]
    if not expected or not hmac.compare_digest(password, expected):
        return web.json_response({"error": "bad_password"}, status=403)
    response = web.json_response({"ok": True})
    response.set_cookie(
        SESSION_COOKIE,
        request.app["session_token"],
        httponly=True,
        samesite="Strict",
        max_age=60 * 60 * 12,
    )
    return response


async def logout_api(request: web.Request) -> web.Response:
    response = web.json_response({"ok": True})
    response.del_cookie(SESSION_COOKIE)
    return response


async def index(request: web.Request) -> web.Response:
    auth = await require_auth(request)
    if auth:
        return auth
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def analytics_api(request: web.Request) -> web.Response:
    auth = await require_auth(request)
    if auth:
        return auth
    period = request.query.get("period", "day")
    if period not in {"day", "week", "month", "all"}:
        period = "day"
    title, since, chart_days = period_since(period)
    storage = request.app["storage"]

    payments = await storage.get_all_payments()
    campaign_events = await storage.list_auto_campaign_events(since=since)
    delivery_events = await storage.list_auto_delivery_events(since=since)

    period_payments = []
    approved = []
    pending = []
    declined = []
    active_users = set()
    active_threshold = datetime.utcnow() - timedelta(days=PAYMENT_VALID_DAYS)
    for payment in payments:
        created = parse_iso(payment.get("created_at"))
        resolved = parse_iso(payment.get("resolved_at"))
        status = payment.get("status")
        if since is None or (created and created >= since):
            period_payments.append(payment)
            if status == "pending":
                pending.append(payment)
            elif status == "declined":
                declined.append(payment)
        if status == "approved" and resolved:
            if since is None or resolved >= since:
                approved.append(payment)
            if resolved >= active_threshold:
                active_users.add(int(payment.get("user_id")))

    deliveries = sum(int(event.get("sent_count") or 0) for event in delivery_events)
    active_campaigns = await storage.count_active_auto_campaigns()
    latest = await storage.latest_payment_timestamp()
    latest_due = (latest + timedelta(days=PAYMENT_VALID_DAYS)).isoformat() if latest else None
    recent_payments = [public_payment(payment) for payment in payments[:50]]

    return web.json_response(
        {
            "period": period,
            "period_title": title,
            "currency": PAYMENT_CURRENCY,
            "cards": {
                "payment_requests": len(period_payments),
                "subscriptions": len(approved),
                "pending": len(pending),
                "declined": len(declined),
                "active_subscriptions": len(active_users),
                "revenue": len(approved) * PAYMENT_AMOUNT,
                "revenue_text": money(len(approved) * PAYMENT_AMOUNT),
                "campaign_starts": len(campaign_events),
                "deliveries": deliveries,
                "active_campaigns": active_campaigns,
                "latest_global_payment_due": latest_due,
            },
            "series": build_series(
                days=chart_days,
                payments=payments,
                campaign_events=campaign_events,
                delivery_events=delivery_events,
            ),
            "payments": recent_payments,
        }
    )


async def health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


def create_app() -> web.Application:
    password = (
        os.getenv("ADMIN_WEB_PASSWORD")
        or os.getenv("WEB_DASHBOARD_PASSWORD")
        or os.getenv("ADMIN_CODE")
    )
    if not password:
        raise RuntimeError(
            "Set ADMIN_WEB_PASSWORD, WEB_DASHBOARD_PASSWORD, or ADMIN_CODE to protect the web admin panel."
        )
    app = web.Application()
    app["storage"] = create_storage_from_env()
    app["admin_password"] = password
    app["session_token"] = os.getenv("WEB_DASHBOARD_SECRET") or secrets.token_urlsafe(32)
    app.router.add_get("/health", health)
    app.router.add_get("/login", login_page)
    app.router.add_post("/api/login", login_api)
    app.router.add_post("/api/logout", logout_api)
    app.router.add_get("/api/analytics", analytics_api)
    app.router.add_get("/", index)
    return app


LOGIN_HTML = r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sendertistics Admin</title>
  <style>
    :root { color-scheme: light; font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    body { margin: 0; min-height: 100vh; display: grid; place-items: center; background: #eef2f6; color: #17202a; }
    main { width: min(380px, calc(100vw - 32px)); background: #fff; border: 1px solid #d7dee8; border-radius: 8px; padding: 24px; box-shadow: 0 18px 50px rgba(23,32,42,.10); }
    h1 { margin: 0 0 6px; font-size: 24px; letter-spacing: 0; }
    p { margin: 0 0 20px; color: #667085; }
    label { display: block; font-size: 13px; font-weight: 700; margin-bottom: 8px; }
    input { width: 100%; box-sizing: border-box; border: 1px solid #b9c4d0; border-radius: 6px; padding: 11px 12px; font-size: 15px; }
    button { width: 100%; margin-top: 14px; border: 0; border-radius: 6px; padding: 12px; font-weight: 800; color: white; background: #1565c0; cursor: pointer; }
    .error { min-height: 20px; color: #b42318; font-size: 13px; margin-top: 10px; }
  </style>
</head>
<body>
  <main>
    <h1>Sendertistics Admin</h1>
    <p>Вход в веб-панель аналитики</p>
    <form id="form">
      <label for="password">Пароль администратора</label>
      <input id="password" name="password" type="password" autocomplete="current-password" autofocus>
      <button type="submit">Войти</button>
      <div class="error" id="error"></div>
    </form>
  </main>
  <script>
    document.querySelector('#form').addEventListener('submit', async (event) => {
      event.preventDefault();
      const error = document.querySelector('#error');
      error.textContent = '';
      const response = await fetch('/api/login', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({password: document.querySelector('#password').value})
      });
      if (response.ok) location.href = '/';
      else error.textContent = 'Неверный пароль.';
    });
  </script>
</body>
</html>"""


DASHBOARD_HTML = r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sendertistics Analytics</title>
  <style>
    :root {
      font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: #182230; background: #eef2f6;
    }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; }
    header { position: sticky; top: 0; z-index: 5; display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 14px 24px; background: rgba(255,255,255,.94); border-bottom: 1px solid #d9e1ea; backdrop-filter: blur(10px); }
    h1 { margin: 0; font-size: 22px; letter-spacing: 0; }
    .subtitle { margin: 2px 0 0; color: #667085; font-size: 13px; }
    .actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
    button { border: 1px solid #c5cfdb; background: #fff; color: #233044; border-radius: 6px; padding: 9px 11px; font-weight: 800; cursor: pointer; }
    button.active { background: #1565c0; border-color: #1565c0; color: #fff; }
    button.icon { width: 38px; height: 38px; display: grid; place-items: center; padding: 0; }
    main { width: min(1440px, 100%); margin: 0 auto; padding: 20px 24px 28px; }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
    .card { background: #fff; border: 1px solid #d9e1ea; border-radius: 8px; padding: 16px; min-width: 0; }
    .metric { display: grid; gap: 6px; min-height: 104px; }
    .metric span { color: #667085; font-size: 13px; font-weight: 700; }
    .metric strong { font-size: 28px; line-height: 1.05; letter-spacing: 0; overflow-wrap: anywhere; }
    .metric small { color: #667085; }
    .layout { display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(360px, .65fr); gap: 12px; margin-top: 12px; align-items: start; }
    .panel-title { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 10px; }
    h2 { margin: 0; font-size: 16px; letter-spacing: 0; }
    canvas { width: 100%; height: 320px; display: block; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 10px 8px; border-bottom: 1px solid #edf1f5; text-align: left; vertical-align: top; }
    th { color: #667085; font-size: 12px; }
    .status { display: inline-flex; align-items: center; border-radius: 999px; padding: 3px 8px; font-size: 12px; font-weight: 800; }
    .approved { background: #dcfae6; color: #067647; }
    .pending { background: #fef0c7; color: #93370d; }
    .declined { background: #fee4e2; color: #b42318; }
    .muted { color: #667085; }
    .split { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
    .mini { padding: 12px; background: #f7f9fc; border: 1px solid #e3e9f1; border-radius: 8px; }
    .mini span { display: block; color: #667085; font-size: 12px; font-weight: 700; }
    .mini strong { display: block; margin-top: 5px; font-size: 20px; }
    @media (max-width: 1050px) { .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } .layout { grid-template-columns: 1fr; } }
    @media (max-width: 680px) { header { align-items: flex-start; flex-direction: column; padding: 14px 16px; } main { padding: 14px 16px 22px; } .grid, .split { grid-template-columns: 1fr; } .actions { justify-content: flex-start; } canvas { height: 260px; } table { font-size: 12px; } }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Sendertistics Analytics</h1>
      <p class="subtitle" id="subtitle">Загрузка данных</p>
    </div>
    <div class="actions">
      <button data-period="day" class="active">День</button>
      <button data-period="week">Неделя</button>
      <button data-period="month">Месяц</button>
      <button data-period="all">Всё</button>
      <button class="icon" id="refresh" title="Обновить" aria-label="Обновить">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M20 12a8 8 0 1 1-2.34-5.66M20 4v6h-6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      </button>
      <button class="icon" id="logout" title="Выйти" aria-label="Выйти">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4M16 17l5-5-5-5M21 12H9" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      </button>
    </div>
  </header>
  <main>
    <section class="grid">
      <div class="card metric"><span>Выручка</span><strong id="revenue">0</strong><small>по подтверждённым оплатам</small></div>
      <div class="card metric"><span>Подписки</span><strong id="subscriptions">0</strong><small id="activeSubs">активных сейчас: 0</small></div>
      <div class="card metric"><span>Запуски рассылок</span><strong id="campaigns">0</strong><small id="activeCampaigns">активно сейчас: 0</small></div>
      <div class="card metric"><span>Сообщения</span><strong id="deliveries">0</strong><small>успешно отправлено</small></div>
    </section>
    <section class="layout">
      <div class="card">
        <div class="panel-title"><h2>Динамика</h2><span class="muted" id="chartLabel"></span></div>
        <canvas id="chart" width="1000" height="360"></canvas>
      </div>
      <div class="card">
        <div class="panel-title"><h2>Сводка периода</h2></div>
        <div class="split">
          <div class="mini"><span>Заявки</span><strong id="requests">0</strong></div>
          <div class="mini"><span>Ожидают</span><strong id="pending">0</strong></div>
          <div class="mini"><span>Отклонено</span><strong id="declined">0</strong></div>
        </div>
        <p class="muted" id="globalDue" style="margin:14px 0 0;"></p>
      </div>
    </section>
    <section class="card" style="margin-top:12px;">
      <div class="panel-title"><h2>Последние оплаты</h2><span class="muted">до 50 записей</span></div>
      <table>
        <thead><tr><th>Пользователь</th><th>Статус</th><th>Создано</th><th>Активна до</th></tr></thead>
        <tbody id="payments"></tbody>
      </table>
    </section>
  </main>
  <script>
    const state = { period: 'day', data: null };
    const fmt = new Intl.NumberFormat('ru-RU');
    const dateFmt = new Intl.DateTimeFormat('ru-RU', {day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit'});
    const dayFmt = new Intl.DateTimeFormat('ru-RU', {day:'2-digit', month:'2-digit'});

    function parseDate(value) { return value ? new Date(value) : null; }
    function safeDate(value) { const d = parseDate(value); return d && !Number.isNaN(d) ? dateFmt.format(d) : '—'; }
    function statusLabel(status) {
      return {approved: 'Оплачено', pending: 'Ожидает', declined: 'Отклонено'}[status] || status || '—';
    }
    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>"']/g, char => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
      }[char]));
    }
    function setText(id, text) { document.getElementById(id).textContent = text; }

    async function load() {
      const response = await fetch(`/api/analytics?period=${state.period}`);
      if (response.status === 401) { location.href = '/login'; return; }
      state.data = await response.json();
      render();
    }

    function render() {
      const data = state.data;
      const cards = data.cards;
      setText('subtitle', `Период: ${data.period_title}. Обновлено ${new Date().toLocaleTimeString('ru-RU', {hour:'2-digit', minute:'2-digit'})}`);
      setText('revenue', cards.revenue_text);
      setText('subscriptions', fmt.format(cards.subscriptions));
      setText('activeSubs', `активных сейчас: ${fmt.format(cards.active_subscriptions)}`);
      setText('campaigns', fmt.format(cards.campaign_starts));
      setText('activeCampaigns', `активно сейчас: ${fmt.format(cards.active_campaigns)}`);
      setText('deliveries', fmt.format(cards.deliveries));
      setText('requests', fmt.format(cards.payment_requests));
      setText('pending', fmt.format(cards.pending));
      setText('declined', fmt.format(cards.declined));
      setText('globalDue', cards.latest_global_payment_due ? `Общая оплата активна до ${safeDate(cards.latest_global_payment_due)}` : 'Общая оплата не найдена');
      setText('chartLabel', data.period === 'day' ? 'сегодня' : 'по дням');
      drawChart(data.series);
      renderPayments(data.payments);
    }

    function drawChart(series) {
      const canvas = document.getElementById('chart');
      const ctx = canvas.getContext('2d');
      const w = canvas.width, h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      const pad = {l: 46, r: 18, t: 24, b: 42};
      const plotW = w - pad.l - pad.r, plotH = h - pad.t - pad.b;
      const max = Math.max(1, ...series.flatMap(d => [d.subscriptions, d.campaigns, d.deliveries]));
      ctx.strokeStyle = '#d9e1ea'; ctx.lineWidth = 1;
      ctx.font = '12px system-ui';
      ctx.fillStyle = '#667085';
      for (let i = 0; i <= 4; i++) {
        const y = pad.t + plotH * i / 4;
        ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(w - pad.r, y); ctx.stroke();
        ctx.fillText(String(Math.round(max * (4 - i) / 4)), 8, y + 4);
      }
      const keys = [
        ['subscriptions', '#067647', 'Подписки'],
        ['campaigns', '#1565c0', 'Запуски'],
        ['deliveries', '#c11574', 'Сообщения']
      ];
      keys.forEach(([key, color]) => {
        ctx.strokeStyle = color; ctx.lineWidth = key === 'deliveries' ? 3 : 2;
        ctx.beginPath();
        series.forEach((d, i) => {
          const x = pad.l + (series.length === 1 ? plotW / 2 : plotW * i / (series.length - 1));
          const y = pad.t + plotH - (Number(d[key]) / max) * plotH;
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        });
        ctx.stroke();
      });
      const step = Math.max(1, Math.ceil(series.length / 8));
      ctx.fillStyle = '#667085';
      series.forEach((d, i) => {
        if (i % step !== 0 && i !== series.length - 1) return;
        const x = pad.l + (series.length === 1 ? plotW / 2 : plotW * i / (series.length - 1));
        ctx.fillText(dayFmt.format(new Date(d.date)), Math.max(4, x - 16), h - 14);
      });
      let legendX = pad.l;
      keys.forEach(([, color, label]) => {
        ctx.fillStyle = color; ctx.fillRect(legendX, 8, 10, 10);
        ctx.fillStyle = '#344054'; ctx.fillText(label, legendX + 14, 17);
        legendX += ctx.measureText(label).width + 42;
      });
    }

    function renderPayments(payments) {
      const tbody = document.getElementById('payments');
      tbody.innerHTML = payments.map(payment => {
        const userName = escapeHtml(payment.full_name || '—');
        const username = payment.username ? ' @' + escapeHtml(payment.username) : '';
        const userId = escapeHtml(payment.user_id);
        const user = `${userName}${username}<br><span class="muted">ID ${userId}</span>`;
        const statusClass = ['approved', 'pending', 'declined'].includes(payment.status) ? payment.status : '';
        return `<tr>
          <td>${user}</td>
          <td><span class="status ${statusClass}">${escapeHtml(statusLabel(payment.status))}</span></td>
          <td>${safeDate(payment.created_at)}</td>
          <td>${safeDate(payment.expires_at)}</td>
        </tr>`;
      }).join('') || '<tr><td colspan="4" class="muted">Оплат пока нет</td></tr>';
    }

    document.querySelectorAll('[data-period]').forEach(button => {
      button.addEventListener('click', () => {
        state.period = button.dataset.period;
        document.querySelectorAll('[data-period]').forEach(item => item.classList.toggle('active', item === button));
        load();
      });
    });
    document.getElementById('refresh').addEventListener('click', load);
    document.getElementById('logout').addEventListener('click', async () => {
      await fetch('/api/logout', {method: 'POST'});
      location.href = '/login';
    });
    load();
  </script>
</body>
</html>"""


if __name__ == "__main__":
    port = int(os.getenv("PORT", os.getenv("ADMIN_WEB_PORT", "8080")))
    host = os.getenv("ADMIN_WEB_HOST", os.getenv("WEB_DASHBOARD_HOST", "0.0.0.0"))
    web.run_app(create_app(), host=host, port=port)
