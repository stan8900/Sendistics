from web_admin.main import create_app


if __name__ == "__main__":
    import os

    from aiohttp import web

    port = int(os.getenv("PORT", os.getenv("ADMIN_WEB_PORT", "8080")))
    host = os.getenv("ADMIN_WEB_HOST", os.getenv("WEB_DASHBOARD_HOST", "0.0.0.0"))
    web.run_app(create_app(), host=host, port=port)
