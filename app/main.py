import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.database import init_db, async_session
from app.services.auth_service import ensure_admin_exists
from app.services.cvm_service import run_full_sync
from app.routers import auth, api, admin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def scheduled_sync():
    """Job diario de sincronizacao de dados da CVM."""
    logger.info("Executando sincronizacao agendada...")
    async with async_session() as db:
        await run_full_sync(db)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db()
    async with async_session() as db:
        await ensure_admin_exists(db)
    # Agendar sync diario as 6h
    scheduler.add_job(scheduled_sync, "cron", hour=6, minute=0, id="daily_sync")
    scheduler.start()
    logger.info("Visao 360 iniciado com sucesso")
    yield
    # Shutdown
    scheduler.shutdown()


app = FastAPI(
    title="Visao 360 - Empresas Abertas",
    description="Plataforma de analise 360 de empresas abertas brasileiras para especialistas de credito corporativo",
    version="1.0.0",
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# Routers
app.include_router(auth.router)
app.include_router(api.router)
app.include_router(admin.router)


# --- Paginas HTML ---

@app.get("/", response_class=HTMLResponse)
async def page_home(request: Request):
    return templates.TemplateResponse(request, "login.html")


@app.get("/register", response_class=HTMLResponse)
async def page_register(request: Request):
    return templates.TemplateResponse(request, "register.html")


@app.get("/dashboard", response_class=HTMLResponse)
async def page_dashboard(request: Request):
    return templates.TemplateResponse(request, "dashboard.html")


@app.get("/admin", response_class=HTMLResponse)
async def page_admin(request: Request):
    return templates.TemplateResponse(request, "admin/panel.html")
