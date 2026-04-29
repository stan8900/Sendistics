import html
import hmac
import logging
import secrets
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any, Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

from aiohttp import web

from .auto_sender import AUTO_DAILY_MESSAGE_LIMIT
from .storage import Storage


TASHKENT_TZ = ZoneInfo("Asia/Tashkent")
SESSION_COOKIE = "dashboard_session"


AutoSenderGetter = Callable[[], Any]


def _escape(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _status_label(status: str) -> str:
    return {
        "approved": "Оплачено",
        "pending": "Ожидает",
        "declined": "Отклонено",
    }.get(status, status or "—")


def _format_datetime(value: Optional[str]) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(value).strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return value


def _sign(secret: str, token: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), token.encode("utf-8"), sha256).hexdigest()
    return f"{token}.{digest}"


def _verify(secret: str, signed_token: str) -> bool:
    try:
        token, digest = signed_token.rsplit(".", 1)
    except ValueError:
        return False
    expected = hmac.new(secret.encode("utf-8"), token.encode("utf-8"), sha256).hexdigest()
    return hmac.compare_digest(digest, expected)


def _base_page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_escape(title)}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f8fb;
      --panel: #ffffff;
      --text: #17202a;
      --muted: #667085;
      --line: #d9dee8;
      --accent: #116a7b;
      --accent-2: #2f7d32;
      --danger: #b42318;
      --warn: #9a6700;
      --shadow: 0 10px 28px rgba(25, 38, 58, .08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    header {{
      position: sticky;
      top: 0;
      z-index: 10;
      background: rgba(247, 248, 251, .92);
      backdrop-filter: blur(12px);
      border-bottom: 1px solid var(--line);
    }}
    .bar {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 14px 18px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
    }}
    .brand {{
      display: flex;
      flex-direction: column;
      gap: 2px;
      min-width: 0;
    }}
    h1, h2, p {{ margin: 0; }}
    h1 {{ font-size: 19px; font-weight: 720; }}
    h2 {{ font-size: 17px; margin-bottom: 12px; }}
    .sub {{ color: var(--muted); font-size: 13px; }}
    main {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 22px 18px 38px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }}
    .card, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }}
    .card {{ padding: 16px; min-height: 104px; }}
    .label {{ color: var(--muted); font-size: 13px; }}
    .value {{ font-size: 30px; font-weight: 760; margin-top: 8px; }}
    .hint {{ color: var(--muted); font-size: 12px; margin-top: 8px; }}
    .panel {{ padding: 16px; margin-top: 14px; overflow: hidden; }}
    .actions {{ display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }}
    button, .button {{
      border: 1px solid transparent;
      border-radius: 7px;
      padding: 9px 12px;
      font: inherit;
      font-weight: 650;
      background: var(--accent);
      color: white;
      cursor: pointer;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 38px;
    }}
    button.secondary {{ background: #eef3f6; color: var(--text); border-color: var(--line); }}
    button.danger {{ background: var(--danger); }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      padding: 11px 10px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }}
    th {{ color: var(--muted); font-size: 12px; font-weight: 700; text-transform: uppercase; }}
    tr:last-child td {{ border-bottom: 0; }}
    .badge {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 4px 8px;
      font-size: 12px;
      font-weight: 700;
      background: #edf7f0;
      color: var(--accent-2);
    }}
    .badge.off {{ background: #f2f4f7; color: var(--muted); }}
    .badge.warn {{ background: #fff7df; color: var(--warn); }}
    .login {{
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 18px;
    }}
    .login form {{
      width: min(420px, 100%);
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      padding: 22px;
    }}
    input {{
      width: 100%;
      min-height: 42px;
      margin: 14px 0 12px;
      border: 1px solid var(--line);
      border-radius: 7px;
      padding: 10px 12px;
      font: inherit;
    }}
    .error {{ color: var(--danger); font-size: 13px; margin-top: 10px; }}
    .table-wrap {{ overflow-x: auto; }}
    @media (max-width: 860px) {{
      .grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .bar {{ align-items: flex-start; flex-direction: column; }}
    }}
    @media (max-width: 560px) {{
      main {{ padding-inline: 12px; }}
      .grid {{ grid-template-columns: 1fr; }}
      .value {{ font-size: 26px; }}
      th, td {{ padding: 10px 8px; }}
      button, .button {{ width: 100%; }}
      .actions {{ width: 100%; }}
    }}
  </style>
</head>
<body>{body}</body>
</html>"""


class Dashboard:
    def __init__(
        self,
        *,
        storage: Storage,
        password: str,
        secret: str,
        auto_sender_getter: AutoSenderGetter,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._storage = storage
        self._password = password
        self._secret = secret
        self._auto_sender_getter = auto_sender_getter
        self._logger = logger or logging.getLogger(__name__)

    def app(self) -> web.Application:
        app = web.Application(middlewares=[self._auth_middleware])
        app.add_routes(
            [
                web.get("/", self.redirect_dashboard),
                web.get("/dashboard", self.dashboard),
                web.get("/dashboard/login", self.login),
                web.post("/dashboard/login", self.login_post),
                web.post("/dashboard/logout", self.logout),
                web.post("/dashboard/stop-all", self.stop_all),
                web.post("/dashboard/stop-user/{user_id}", self.stop_user),
            ]
        )
        return app

    @web.middleware
    async def _auth_middleware(self, request: web.Request, handler: Callable[[web.Request], Any]) -> web.StreamResponse:
        if request.path == "/dashboard/login":
            return await handler(request)
        token = request.cookies.get(SESSION_COOKIE)
        if not token or not _verify(self._secret, token):
            raise web.HTTPFound("/dashboard/login")
        return await handler(request)

    async def redirect_dashboard(self, request: web.Request) -> web.Response:
        raise web.HTTPFound("/dashboard")

    async def login(self, request: web.Request) -> web.Response:
        return web.Response(text=self._login_page(), content_type="text/html")

    async def login_post(self, request: web.Request) -> web.Response:
        form = await request.post()
        password = str(form.get("password") or "")
        if not hmac.compare_digest(password, self._password):
            return web.Response(text=self._login_page(error="Неверный пароль."), content_type="text/html", status=401)
        token = _sign(self._secret, secrets.token_urlsafe(24))
        response = web.HTTPFound("/dashboard")
        response.set_cookie(
            SESSION_COOKIE,
            token,
            httponly=True,
            secure=False,
            samesite="Lax",
            max_age=60 * 60 * 12,
        )
        raise response

    async def logout(self, request: web.Request) -> web.Response:
        response = web.HTTPFound("/dashboard/login")
        response.del_cookie(SESSION_COOKIE)
        raise response

    async def dashboard(self, request: web.Request) -> web.Response:
        data = await self._build_snapshot()
        return web.Response(text=self._dashboard_page(data), content_type="text/html")

    async def stop_all(self, request: web.Request) -> web.Response:
        disabled_count = await self._storage.disable_all_auto()
        auto_sender = self._auto_sender_getter()
        if auto_sender:
            await auto_sender.stop_all()
        self._logger.info("Dashboard stopped all mailings (%s active configs).", disabled_count)
        raise web.HTTPFound("/dashboard")

    async def stop_user(self, request: web.Request) -> web.Response:
        try:
            user_id = int(request.match_info["user_id"])
        except (KeyError, ValueError):
            raise web.HTTPBadRequest(text="Invalid user id")
        await self._storage.set_auto_enabled(user_id, False)
        auto_sender = self._auto_sender_getter()
        if auto_sender:
            await auto_sender.stop_user(user_id)
        self._logger.info("Dashboard stopped mailing for user %s.", user_id)
        raise web.HTTPFound("/dashboard")

    async def _build_snapshot(self) -> Dict[str, Any]:
        data = await self._storage.get_data()
        autos = list((data.get("auto") or {}).values())
        payments = list((data.get("payments") or {}).values())
        sessions = data.get("sessions") or {}
        known_chats = data.get("known_chats") or {}
        today_key = datetime.now(TASHKENT_TZ).date().isoformat()
        daily_counts = await self._storage.list_auto_daily_counts(today_key)
        active = [auto for auto in autos if auto.get("is_enabled")]
        pending_payments = [payment for payment in payments if payment.get("status") == "pending"]
        approved_payments = [payment for payment in payments if payment.get("status") == "approved"]
        return {
            "today_key": today_key,
            "autos": sorted(autos, key=lambda item: int(item.get("user_id") or 0)),
            "active": active,
            "payments": sorted(payments, key=lambda item: item.get("created_at") or "", reverse=True),
            "pending_payments": pending_payments,
            "approved_payments": approved_payments,
            "sessions": sessions,
            "known_chats": known_chats,
            "daily_counts": daily_counts,
            "daily_total": sum(daily_counts.values()),
        }

    def _login_page(self, *, error: Optional[str] = None) -> str:
        error_html = f'<p class="error">{_escape(error)}</p>' if error else ""
        body = f"""
<main class="login">
  <form method="post" action="/dashboard/login">
    <h1>Sendertistics Dashboard</h1>
    <p class="sub">Админ-доступ к рассылкам и оплатам</p>
    <input type="password" name="password" placeholder="Пароль администратора" autocomplete="current-password" required>
    <button type="submit">Войти</button>
    {error_html}
  </form>
</main>"""
        return _base_page("Вход", body)

    def _dashboard_page(self, data: Dict[str, Any]) -> str:
        active_count = len(data["active"])
        total_autos = len(data["autos"])
        pending_count = len(data["pending_payments"])
        approved_count = len(data["approved_payments"])
        now_text = datetime.now(TASHKENT_TZ).strftime("%d.%m.%Y %H:%M")
        rows = self._auto_rows(data["autos"], data["daily_counts"])
        payment_rows = self._payment_rows(data["payments"][:12])
        body = f"""
<header>
  <div class="bar">
    <div class="brand">
      <h1>Sendertistics Dashboard</h1>
      <p class="sub">Ташкент: {now_text} · окно рассылки 08:00-20:00 · лимит {AUTO_DAILY_MESSAGE_LIMIT}/день</p>
    </div>
    <form method="post" action="/dashboard/logout">
      <button class="secondary" type="submit">Выйти</button>
    </form>
  </div>
</header>
<main>
  <section class="grid" aria-label="Метрики">
    <div class="card"><p class="label">Активные рассылки</p><p class="value">{active_count}</p><p class="hint">из {total_autos} настроек</p></div>
    <div class="card"><p class="label">Отправлено сегодня</p><p class="value">{data["daily_total"]}</p><p class="hint">дата {data["today_key"]}</p></div>
    <div class="card"><p class="label">Ожидают оплату</p><p class="value">{pending_count}</p><p class="hint">нужна проверка админа</p></div>
    <div class="card"><p class="label">Подтверждённые оплаты</p><p class="value">{approved_count}</p><p class="hint">за всё время</p></div>
  </section>

  <section class="panel">
    <h2>Управление</h2>
    <div class="actions">
      <form method="post" action="/dashboard/stop-all">
        <button class="danger" type="submit">Остановить все рассылки</button>
      </form>
      <a class="button" href="/dashboard">Обновить</a>
    </div>
  </section>

  <section class="panel">
    <h2>Авторассылки</h2>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Пользователь</th><th>Статус</th><th>Группы</th><th>Интервал</th><th>Сегодня</th><th>Последняя отправка</th><th>Ошибки</th><th></th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
  </section>

  <section class="panel">
    <h2>Последние оплаты</h2>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Пользователь</th><th>Статус</th><th>Карта</th><th>Создано</th><th>Обновлено</th></tr></thead>
        <tbody>{payment_rows}</tbody>
      </table>
    </div>
  </section>
</main>"""
        return _base_page("Sendertistics Dashboard", body)

    def _auto_rows(self, autos: List[Dict[str, Any]], daily_counts: Dict[int, int]) -> str:
        if not autos:
            return '<tr><td colspan="8">Рассылки ещё не настроены.</td></tr>'
        rows = []
        for auto in autos:
            user_id = int(auto.get("user_id") or 0)
            enabled = bool(auto.get("is_enabled"))
            stats = auto.get("stats") or {}
            errors = stats.get("last_error") or "—"
            if len(errors) > 160:
                errors = errors[:157] + "..."
            status_class = "" if enabled else " off"
            status_text = "Активна" if enabled else "Выключена"
            today_count = daily_counts.get(user_id, 0)
            today_badge = "warn" if today_count >= AUTO_DAILY_MESSAGE_LIMIT else ""
            action = (
                f'<form method="post" action="/dashboard/stop-user/{user_id}">'
                '<button class="secondary" type="submit">Стоп</button></form>'
                if enabled
                else ""
            )
            rows.append(
                "<tr>"
                f"<td><code>{user_id}</code></td>"
                f'<td><span class="badge{status_class}">{status_text}</span></td>'
                f"<td>{len(auto.get('target_chat_ids') or [])}</td>"
                f"<td>{_escape(auto.get('interval_minutes') or 0)} мин</td>"
                f'<td><span class="badge {today_badge}">{today_count}/{AUTO_DAILY_MESSAGE_LIMIT}</span></td>'
                f"<td>{_escape(_format_datetime(stats.get('last_sent_at')))}</td>"
                f"<td>{_escape(errors)}</td>"
                f"<td>{action}</td>"
                "</tr>"
            )
        return "\n".join(rows)

    def _payment_rows(self, payments: List[Dict[str, Any]]) -> str:
        if not payments:
            return '<tr><td colspan="5">Заявок на оплату пока нет.</td></tr>'
        rows = []
        for payment in payments:
            user = payment.get("full_name") or payment.get("username") or payment.get("user_id")
            username = payment.get("username")
            if username:
                user = f"{user} (@{username})"
            status = payment.get("status") or ""
            status_class = "warn" if status == "pending" else ("" if status == "approved" else " off")
            rows.append(
                "<tr>"
                f"<td>{_escape(user)}</td>"
                f'<td><span class="badge {status_class}">{_escape(_status_label(status))}</span></td>'
                f"<td><code>{_escape(payment.get('card_number') or '—')}</code></td>"
                f"<td>{_escape(_format_datetime(payment.get('created_at')))}</td>"
                f"<td>{_escape(_format_datetime(payment.get('resolved_at')))}</td>"
                "</tr>"
            )
        return "\n".join(rows)


async def start_dashboard(
    *,
    storage: Storage,
    password: str,
    secret: str,
    host: str,
    port: int,
    auto_sender_getter: AutoSenderGetter,
    logger: Optional[logging.Logger] = None,
) -> web.AppRunner:
    dashboard = Dashboard(
        storage=storage,
        password=password,
        secret=secret,
        auto_sender_getter=auto_sender_getter,
        logger=logger,
    )
    runner = web.AppRunner(dashboard.app())
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner

