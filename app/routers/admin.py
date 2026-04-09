from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc

from app.database import get_db
from app.models.models import User, UserQuery, AnalysisPrompt
from app.services.auth_service import approve_user, deactivate_user, get_all_users
from app.services.cvm_service import run_full_sync
from app.routers.auth import require_admin
from pydantic import BaseModel

router = APIRouter(prefix="/api/admin", tags=["admin"])


class PromptUpdate(BaseModel):
    nome: str
    descricao: str | None = None
    prompt_template: str
    categoria: str | None = None
    is_active: bool = True


@router.get("/users")
async def admin_list_users(db: AsyncSession = Depends(get_db), admin: User = Depends(require_admin)):
    users = await get_all_users(db)
    return [
        {
            "id": u.id, "email": u.email, "name": u.name,
            "is_active": u.is_active, "is_admin": u.is_admin,
            "created_at": u.created_at.isoformat() if u.created_at else None,
            "approved_at": u.approved_at.isoformat() if u.approved_at else None,
        }
        for u in users
    ]


@router.post("/users/{user_id}/approve")
async def admin_approve_user(user_id: int, db: AsyncSession = Depends(get_db), admin: User = Depends(require_admin)):
    user = await approve_user(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Usuario nao encontrado")
    return {"message": f"Usuario {user.email} aprovado", "user_id": user.id}


@router.post("/users/{user_id}/deactivate")
async def admin_deactivate_user(user_id: int, db: AsyncSession = Depends(get_db), admin: User = Depends(require_admin)):
    user = await deactivate_user(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Usuario nao encontrado ou e admin")
    return {"message": f"Usuario {user.email} desativado"}


@router.get("/queries")
async def admin_list_queries(
    page: int = 1, per_page: int = 50,
    db: AsyncSession = Depends(get_db), admin: User = Depends(require_admin),
):
    offset = (page - 1) * per_page
    result = await db.execute(
        select(UserQuery, User.email, User.name)
        .join(User, UserQuery.user_id == User.id)
        .order_by(desc(UserQuery.created_at))
        .offset(offset).limit(per_page)
    )
    rows = result.all()

    total_result = await db.execute(select(func.count(UserQuery.id)))
    total = total_result.scalar()

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "queries": [
            {
                "id": q.id, "user_email": email, "user_name": name,
                "company_name": q.company_name, "query_text": q.query_text,
                "feedback_rating": q.feedback_rating,
                "feedback_comment": q.feedback_comment,
                "created_at": q.created_at.isoformat() if q.created_at else None,
            }
            for q, email, name in rows
        ],
    }


@router.get("/stats")
async def admin_stats(db: AsyncSession = Depends(get_db), admin: User = Depends(require_admin)):
    total_users = (await db.execute(select(func.count(User.id)))).scalar()
    active_users = (await db.execute(select(func.count(User.id)).where(User.is_active == True))).scalar()
    pending_users = (await db.execute(select(func.count(User.id)).where(User.is_active == False))).scalar()
    total_queries = (await db.execute(select(func.count(UserQuery.id)))).scalar()
    avg_rating = (await db.execute(
        select(func.avg(UserQuery.feedback_rating)).where(UserQuery.feedback_rating.isnot(None))
    )).scalar()

    # Queries por dia (ultimos 7 dias)
    return {
        "total_users": total_users,
        "active_users": active_users,
        "pending_users": pending_users,
        "total_queries": total_queries,
        "avg_rating": round(avg_rating, 1) if avg_rating else None,
    }


@router.post("/sync")
async def admin_trigger_sync(db: AsyncSession = Depends(get_db), admin: User = Depends(require_admin)):
    results = await run_full_sync(db)
    return {"message": "Sincronizacao concluida", "results": results}


@router.get("/prompts")
async def admin_list_prompts(db: AsyncSession = Depends(get_db), admin: User = Depends(require_admin)):
    result = await db.execute(select(AnalysisPrompt).order_by(AnalysisPrompt.nome))
    prompts = result.scalars().all()
    return [
        {
            "id": p.id, "nome": p.nome, "descricao": p.descricao,
            "prompt_template": p.prompt_template, "categoria": p.categoria,
            "is_active": p.is_active,
        }
        for p in prompts
    ]


@router.post("/prompts")
async def admin_create_prompt(req: PromptUpdate, db: AsyncSession = Depends(get_db), admin: User = Depends(require_admin)):
    prompt = AnalysisPrompt(
        nome=req.nome, descricao=req.descricao,
        prompt_template=req.prompt_template, categoria=req.categoria,
        is_active=req.is_active,
    )
    db.add(prompt)
    await db.commit()
    await db.refresh(prompt)
    return {"message": "Prompt criado", "id": prompt.id}


@router.put("/prompts/{prompt_id}")
async def admin_update_prompt(prompt_id: int, req: PromptUpdate, db: AsyncSession = Depends(get_db), admin: User = Depends(require_admin)):
    result = await db.execute(select(AnalysisPrompt).where(AnalysisPrompt.id == prompt_id))
    prompt = result.scalar_one_or_none()
    if not prompt:
        raise HTTPException(status_code=404, detail="Prompt nao encontrado")
    prompt.nome = req.nome
    prompt.descricao = req.descricao
    prompt.prompt_template = req.prompt_template
    prompt.categoria = req.categoria
    prompt.is_active = req.is_active
    await db.commit()
    return {"message": "Prompt atualizado"}
