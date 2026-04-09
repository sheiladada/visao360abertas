from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel

from app.database import get_db
from app.models.models import User, UserQuery
from app.services.analysis_service import (
    search_companies, generate_analysis_360,
    get_company_documents, get_default_prompts,
)
from app.services.ai_service import generate_ai_analysis
from app.routers.auth import get_current_user

router = APIRouter(prefix="/api", tags=["api"])


class SearchRequest(BaseModel):
    query: str


class AnalysisRequest(BaseModel):
    empresa: str
    prompt_id: str | None = None
    custom_prompt: str | None = None


class FeedbackRequest(BaseModel):
    query_id: int
    rating: int
    comment: str | None = None


@router.get("/companies/search")
async def api_search_companies(q: str, db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    companies = await search_companies(db, q)
    return [
        {
            "cod_cvm": c.cod_cvm, "nome": c.nome, "nome_pregao": c.nome_pregao,
            "cnpj": c.cnpj, "setor": c.setor, "situacao": c.situacao,
        }
        for c in companies
    ]


@router.post("/analysis")
async def api_analysis(req: AnalysisRequest, db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    # Buscar dados estruturados da CVM
    result = await generate_analysis_360(db, req.empresa)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    # Gerar analise narrativa com IA
    ai_analysis = await generate_ai_analysis(
        data=result,
        prompt_id=req.prompt_id,
        custom_prompt=req.custom_prompt,
    )
    result["ai_analysis"] = ai_analysis

    # Salvar query do usuario
    query = UserQuery(
        user_id=user.id,
        company_name=req.empresa,
        query_text=req.custom_prompt or req.prompt_id or "visao_geral",
        response_text=ai_analysis[:2000] if ai_analysis else "",
    )
    db.add(query)
    await db.commit()
    await db.refresh(query)

    result["query_id"] = query.id
    return result


@router.get("/documents/{cod_cvm}")
async def api_documents(cod_cvm: str, tipo: str = None, db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    docs = await get_company_documents(db, cod_cvm, tipo)
    return [
        {
            "tipo": d.tipo, "descricao": d.descricao,
            "data_referencia": d.data_referencia, "data_entrega": d.data_entrega,
            "link": d.link_documento, "versao": d.versao,
        }
        for d in docs
    ]


@router.get("/prompts")
async def api_prompts(user: User = Depends(get_current_user)):
    return await get_default_prompts()


@router.post("/feedback")
async def api_feedback(req: FeedbackRequest, db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    result = await db.execute(select(UserQuery).where(UserQuery.id == req.query_id, UserQuery.user_id == user.id))
    query = result.scalar_one_or_none()
    if not query:
        raise HTTPException(status_code=404, detail="Consulta nao encontrada")
    query.feedback_rating = req.rating
    query.feedback_comment = req.comment
    await db.commit()
    return {"message": "Feedback registrado com sucesso"}
