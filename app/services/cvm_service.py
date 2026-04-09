"""
Servico de coleta de dados do Portal Dados Abertos da CVM.
Fontes:
  - Cadastro de Cias Abertas: https://dados.cvm.gov.br/dataset/cia_aberta-cad
  - ITR: https://dados.cvm.gov.br/dataset/cia_aberta-doc-itr
  - DFP: https://dados.cvm.gov.br/dataset/cia_aberta-doc-dfp
  - IPE (Fatos Relevantes): https://dados.cvm.gov.br/dataset/cia_aberta-doc-ipe
  - FRE: https://dados.cvm.gov.br/dataset/cia_aberta-doc-fre
  - DFP Demonstracoes: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/
  - ITR Demonstracoes: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/
"""
import io
import logging
from datetime import datetime, date

import httpx
import pandas as pd
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.models import Company, CompanyDocument, FinancialData

logger = logging.getLogger(__name__)

# URLs base dos dados abertos da CVM
BASE_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA"
CAD_URL = f"{BASE_URL}/CAD/DADOS/cad_cia_aberta.csv"

# Ano corrente para buscar dados mais recentes
CURRENT_YEAR = date.today().year


def _build_urls():
    """Constroi URLs para os datasets mais recentes."""
    year = CURRENT_YEAR
    return {
        "itr_docs": f"{BASE_URL}/DOC/ITR/DADOS/itr_cia_aberta_{year}.csv",
        "itr_docs_prev": f"{BASE_URL}/DOC/ITR/DADOS/itr_cia_aberta_{year - 1}.csv",
        "dfp_docs": f"{BASE_URL}/DOC/DFP/DADOS/dfp_cia_aberta_{year}.csv",
        "dfp_docs_prev": f"{BASE_URL}/DOC/DFP/DADOS/dfp_cia_aberta_{year - 1}.csv",
        "ipe_docs": f"{BASE_URL}/DOC/IPE/DADOS/ipe_cia_aberta_{year}.csv",
        "ipe_docs_prev": f"{BASE_URL}/DOC/IPE/DADOS/ipe_cia_aberta_{year - 1}.csv",
        # Demonstracoes financeiras detalhadas (BPA, BPP, DRE, DFC)
        "dfp_bpa": f"{BASE_URL}/DOC/DFP/DADOS/dfp_cia_aberta_BPA_con_{year - 1}.csv",
        "dfp_bpp": f"{BASE_URL}/DOC/DFP/DADOS/dfp_cia_aberta_BPP_con_{year - 1}.csv",
        "dfp_dre": f"{BASE_URL}/DOC/DFP/DADOS/dfp_cia_aberta_DRE_con_{year - 1}.csv",
        "dfp_dfc": f"{BASE_URL}/DOC/DFP/DADOS/dfp_cia_aberta_DFC_MI_con_{year - 1}.csv",
        "itr_bpa": f"{BASE_URL}/DOC/ITR/DADOS/itr_cia_aberta_BPA_con_{year}.csv",
        "itr_bpa_prev": f"{BASE_URL}/DOC/ITR/DADOS/itr_cia_aberta_BPA_con_{year - 1}.csv",
        "itr_dre": f"{BASE_URL}/DOC/ITR/DADOS/itr_cia_aberta_DRE_con_{year}.csv",
        "itr_dre_prev": f"{BASE_URL}/DOC/ITR/DADOS/itr_cia_aberta_DRE_con_{year - 1}.csv",
    }


async def _fetch_csv(url: str, encoding: str = "latin-1", sep: str = ";") -> pd.DataFrame | None:
    """Faz download de CSV do portal da CVM."""
    try:
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                content = resp.content.decode(encoding, errors="replace")
                df = pd.read_csv(io.StringIO(content), sep=sep, on_bad_lines="skip")
                logger.info(f"Baixado {url}: {len(df)} registros")
                return df
            else:
                logger.warning(f"HTTP {resp.status_code} para {url}")
                return None
    except Exception as e:
        logger.error(f"Erro ao baixar {url}: {e}")
        return None


async def sync_companies(db: AsyncSession):
    """Sincroniza cadastro de companhias abertas."""
    logger.info("Sincronizando cadastro de companhias abertas...")
    df = await _fetch_csv(CAD_URL)
    if df is None:
        return 0

    count = 0
    for _, row in df.iterrows():
        cod_cvm = str(row.get("CD_CVM", "")).strip()
        if not cod_cvm:
            continue

        existing = await db.execute(select(Company).where(Company.cod_cvm == cod_cvm))
        company = existing.scalar_one_or_none()

        if company:
            company.nome = str(row.get("DENOM_SOCIAL", ""))
            company.nome_pregao = str(row.get("DENOM_COMERC", ""))
            company.cnpj = str(row.get("CNPJ_CIA", ""))
            company.setor = str(row.get("SETOR_ATIV", ""))
            company.situacao = str(row.get("SIT", ""))
            company.data_registro = str(row.get("DT_REG", ""))
            company.updated_at = datetime.utcnow()
        else:
            company = Company(
                cod_cvm=cod_cvm,
                nome=str(row.get("DENOM_SOCIAL", "")),
                nome_pregao=str(row.get("DENOM_COMERC", "")),
                cnpj=str(row.get("CNPJ_CIA", "")),
                setor=str(row.get("SETOR_ATIV", "")),
                situacao=str(row.get("SIT", "")),
                data_registro=str(row.get("DT_REG", "")),
            )
            db.add(company)
        count += 1

    await db.commit()
    logger.info(f"Sincronizadas {count} companhias")
    return count


async def sync_documents(db: AsyncSession, doc_type: str, url: str):
    """Sincroniza documentos (ITR, DFP, IPE) da CVM."""
    df = await _fetch_csv(url)
    if df is None:
        return 0

    count = 0
    for _, row in df.iterrows():
        cod_cvm = str(row.get("CD_CVM", "")).strip()
        if not cod_cvm:
            continue

        # Monta link para o documento original
        link = str(row.get("LINK_DOC", "")) if "LINK_DOC" in df.columns else ""

        # Para IPE (fatos relevantes), usar campos especificos
        if doc_type == "IPE":
            descricao = str(row.get("DS_ASSUNTO", ""))
            data_ref = str(row.get("DT_REFER", row.get("DT_INI_SITAM", "")))
            data_entrega = str(row.get("DT_ENTREGA", ""))
        else:
            descricao = str(row.get("DENOM_CIA", ""))
            data_ref = str(row.get("DT_REFER", ""))
            data_entrega = str(row.get("DT_RECEB", row.get("DT_ENTREGA", "")))

        doc = CompanyDocument(
            cod_cvm=cod_cvm,
            tipo=doc_type,
            descricao=descricao,
            data_referencia=data_ref,
            data_entrega=data_entrega,
            link_documento=link,
            versao=str(row.get("VERSAO", "1")),
        )
        db.add(doc)
        count += 1

    await db.commit()
    logger.info(f"Sincronizados {count} documentos tipo {doc_type}")
    return count


async def sync_financial_data(db: AsyncSession, tipo: str, url: str):
    """Sincroniza dados financeiros detalhados (BPA, BPP, DRE, DFC)."""
    df = await _fetch_csv(url)
    if df is None:
        return 0

    count = 0
    for _, row in df.iterrows():
        cod_cvm = str(row.get("CD_CVM", "")).strip()
        if not cod_cvm:
            continue

        fin = FinancialData(
            cod_cvm=cod_cvm,
            tipo_documento=tipo,
            data_referencia=str(row.get("DT_REFER", "")),
            conta=str(row.get("CD_CONTA", "")),
            descricao_conta=str(row.get("DS_CONTA", "")),
            valor=float(row.get("VL_CONTA", 0)) if pd.notna(row.get("VL_CONTA")) else None,
            escala=str(row.get("ESCALA_MOEDA", "")),
            moeda=str(row.get("MOEDA", "")),
            ordem_exercicio=str(row.get("ORDEM_EXERC", "")),
        )
        db.add(fin)
        count += 1

    await db.commit()
    logger.info(f"Sincronizados {count} dados financeiros tipo {tipo}")
    return count


async def run_full_sync(db: AsyncSession):
    """Executa sincronizacao completa de todos os dados."""
    logger.info("=== INICIANDO SINCRONIZACAO COMPLETA ===")
    results = {}

    # 1. Cadastro de companhias
    results["companies"] = await sync_companies(db)

    urls = _build_urls()

    # 2. Limpar documentos antigos antes de reimportar
    await db.execute(delete(CompanyDocument))
    await db.execute(delete(FinancialData))
    await db.commit()

    # 3. Documentos ITR
    for key in ["itr_docs", "itr_docs_prev"]:
        r = await sync_documents(db, "ITR", urls[key])
        results[key] = r

    # 4. Documentos DFP
    for key in ["dfp_docs", "dfp_docs_prev"]:
        r = await sync_documents(db, "DFP", urls[key])
        results[key] = r

    # 5. IPE (Fatos Relevantes)
    for key in ["ipe_docs", "ipe_docs_prev"]:
        r = await sync_documents(db, "IPE", urls[key])
        results[key] = r

    # 6. Dados financeiros detalhados
    fin_map = {
        "dfp_bpa": "DFP_BPA", "dfp_bpp": "DFP_BPP",
        "dfp_dre": "DFP_DRE", "dfp_dfc": "DFP_DFC",
        "itr_bpa": "ITR_BPA", "itr_bpa_prev": "ITR_BPA",
        "itr_dre": "ITR_DRE", "itr_dre_prev": "ITR_DRE",
    }
    for key, tipo in fin_map.items():
        r = await sync_financial_data(db, tipo, urls[key])
        results[key] = r

    logger.info(f"=== SINCRONIZACAO COMPLETA: {results} ===")
    return results
