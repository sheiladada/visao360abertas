"""
Servico de coleta de dados do Portal Dados Abertos da CVM.
Fontes oficiais (arquivos ZIP):
  - Cadastro: https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv
  - ITR: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_{ano}.zip
  - DFP: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{ano}.zip
  - IPE (Fatos Relevantes): https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{ano}.zip
"""
import io
import logging
import zipfile
from datetime import datetime, date

import httpx
import pandas as pd
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.models import Company, CompanyDocument, FinancialData

logger = logging.getLogger(__name__)

BASE_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA"
CAD_URL = f"{BASE_URL}/CAD/DADOS/cad_cia_aberta.csv"

CURRENT_YEAR = date.today().year


async def _fetch_bytes(url: str) -> bytes | None:
    """Download de arquivo (CSV ou ZIP) do portal da CVM."""
    try:
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                logger.info(f"Baixado {url}: {len(resp.content)} bytes")
                return resp.content
            else:
                logger.warning(f"HTTP {resp.status_code} para {url}")
                return None
    except Exception as e:
        logger.error(f"Erro ao baixar {url}: {e}")
        return None


def _read_csv_from_zip(content: bytes, encoding: str = "latin-1") -> pd.DataFrame | None:
    """Extrai e le o primeiro CSV de um arquivo ZIP."""
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            csv_files = [n for n in z.namelist() if n.endswith(".csv")]
            if not csv_files:
                return None
            with z.open(csv_files[0]) as f:
                raw = f.read().decode(encoding, errors="replace")
                df = pd.read_csv(io.StringIO(raw), sep=";", on_bad_lines="skip")
                logger.info(f"CSV extraido do ZIP: {len(df)} linhas, colunas: {list(df.columns)}")
                return df
    except Exception as e:
        logger.error(f"Erro ao ler ZIP: {e}")
        return None


def _read_csv_direct(content: bytes, encoding: str = "latin-1") -> pd.DataFrame | None:
    """Le CSV diretamente (sem ZIP)."""
    try:
        raw = content.decode(encoding, errors="replace")
        df = pd.read_csv(io.StringIO(raw), sep=";", on_bad_lines="skip")
        logger.info(f"CSV direto: {len(df)} linhas")
        return df
    except Exception as e:
        logger.error(f"Erro ao ler CSV: {e}")
        return None


async def sync_companies(db: AsyncSession) -> int:
    """Sincroniza cadastro de companhias abertas."""
    logger.info("Sincronizando cadastro de companhias abertas...")
    content = await _fetch_bytes(CAD_URL)
    if not content:
        return 0

    df = _read_csv_direct(content)
    if df is None:
        return 0

    logger.info(f"Colunas cadastro: {list(df.columns)}")

    count = 0
    for _, row in df.iterrows():
        cod_cvm = str(row.get("CD_CVM", "")).strip()
        if not cod_cvm or cod_cvm == "nan":
            continue

        existing = await db.execute(select(Company).where(Company.cod_cvm == cod_cvm))
        company = existing.scalar_one_or_none()

        nome = str(row.get("DENOM_SOCIAL", ""))
        nome_pregao = str(row.get("DENOM_COMERC", ""))
        cnpj = str(row.get("CNPJ_CIA", ""))
        setor = str(row.get("SETOR_ATIV", ""))
        situacao = str(row.get("SIT", ""))
        data_registro = str(row.get("DT_REG", ""))

        if company:
            company.nome = nome
            company.nome_pregao = nome_pregao
            company.cnpj = cnpj
            company.setor = setor
            company.situacao = situacao
            company.data_registro = data_registro
            company.updated_at = datetime.utcnow()
        else:
            company = Company(
                cod_cvm=cod_cvm, nome=nome, nome_pregao=nome_pregao,
                cnpj=cnpj, setor=setor, situacao=situacao, data_registro=data_registro,
            )
            db.add(company)
        count += 1

    await db.commit()
    logger.info(f"Sincronizadas {count} companhias")
    return count


async def sync_ipe(db: AsyncSession, ano: int) -> int:
    """Sincroniza fatos relevantes (IPE) de um ano."""
    url = f"{BASE_URL}/DOC/IPE/DADOS/ipe_cia_aberta_{ano}.zip"
    content = await _fetch_bytes(url)
    if not content:
        return 0

    df = _read_csv_from_zip(content)
    if df is None:
        return 0

    # Colunas: CNPJ_Companhia, Nome_Companhia, Codigo_CVM, Data_Referencia,
    #          Categoria, Tipo, Especie, Assunto, Data_Entrega, Link_Download
    count = 0
    for _, row in df.iterrows():
        cod_cvm = str(row.get("Codigo_CVM", "")).strip()
        if not cod_cvm or cod_cvm == "nan":
            continue

        link = str(row.get("Link_Download", ""))
        if link == "nan":
            link = ""

        doc = CompanyDocument(
            cod_cvm=cod_cvm,
            tipo="IPE",
            descricao=str(row.get("Assunto", row.get("Categoria", ""))),
            data_referencia=str(row.get("Data_Referencia", "")),
            data_entrega=str(row.get("Data_Entrega", "")),
            link_documento=link,
            versao=str(row.get("Versao", "1")),
        )
        db.add(doc)
        count += 1

    await db.commit()
    logger.info(f"IPE {ano}: {count} documentos")
    return count


async def sync_itr_dfp(db: AsyncSession, tipo: str, ano: int) -> int:
    """Sincroniza documentos ITR ou DFP de um ano."""
    url = f"{BASE_URL}/DOC/{tipo}/DADOS/{tipo.lower()}_cia_aberta_{ano}.zip"
    content = await _fetch_bytes(url)
    if not content:
        return 0

    df = _read_csv_from_zip(content)
    if df is None:
        return 0

    logger.info(f"Colunas {tipo} {ano}: {list(df.columns)}")

    # Detectar colunas (podem variar entre ITR e DFP)
    col_cvm = next((c for c in df.columns if "CVM" in c.upper() or "COD" in c.upper()), None)
    col_ref = next((c for c in df.columns if "REFER" in c.upper() or "DT_REF" in c.upper()), None)
    col_link = next((c for c in df.columns if "LINK" in c.upper() or "DOWNLOAD" in c.upper()), None)
    col_entrega = next((c for c in df.columns if "ENTREGA" in c.upper() or "RECEB" in c.upper()), None)
    col_versao = next((c for c in df.columns if "VERSAO" in c.upper() or "VERS" in c.upper()), None)

    if not col_cvm:
        logger.warning(f"Coluna CVM nao encontrada em {tipo} {ano}")
        return 0

    count = 0
    for _, row in df.iterrows():
        cod_cvm = str(row.get(col_cvm, "")).strip()
        if not cod_cvm or cod_cvm == "nan":
            continue

        link = str(row.get(col_link, "")) if col_link else ""
        if link == "nan":
            link = ""

        doc = CompanyDocument(
            cod_cvm=cod_cvm,
            tipo=tipo,
            descricao=str(row.get("Nome_Companhia", row.get("DENOM_CIA", ""))),
            data_referencia=str(row.get(col_ref, "")) if col_ref else "",
            data_entrega=str(row.get(col_entrega, "")) if col_entrega else "",
            link_documento=link,
            versao=str(row.get(col_versao, "1")) if col_versao else "1",
        )
        db.add(doc)
        count += 1

    await db.commit()
    logger.info(f"{tipo} {ano}: {count} documentos")
    return count


def _read_named_csv_from_zip(zip_content: bytes, filename: str, encoding: str = "latin-1") -> pd.DataFrame | None:
    """Le um CSV especifico de dentro de um ZIP pelo nome."""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            if filename not in z.namelist():
                return None
            with z.open(filename) as f:
                raw = f.read().decode(encoding, errors="replace")
                return pd.read_csv(io.StringIO(raw), sep=";", on_bad_lines="skip")
    except Exception as e:
        logger.error(f"Erro ao ler {filename} do ZIP: {e}")
        return None


async def sync_financial_zip(db: AsyncSession, tipo_doc: str, subtipo: str, ano: int) -> int:
    """Sincroniza dados financeiros detalhados (BPA, BPP, DRE, DFC).
    Os arquivos estao dentro do ZIP principal do tipo_doc.
    Ex: dfp_cia_aberta_2025.zip contem dfp_cia_aberta_BPA_con_2025.csv
    """
    url = f"{BASE_URL}/DOC/{tipo_doc}/DADOS/{tipo_doc.lower()}_cia_aberta_{ano}.zip"
    content = await _fetch_bytes(url)
    if not content:
        return 0

    # Nome do CSV dentro do ZIP: tipo_cia_aberta_SUBTIPO_con_ano.csv
    tipo_lower = tipo_doc.lower()
    filename = f"{tipo_lower}_cia_aberta_{subtipo}_con_{ano}.csv"
    df = _read_named_csv_from_zip(content, filename)
    if df is None:
        # Tentar individual se consolidado nao existir
        filename = f"{tipo_lower}_cia_aberta_{subtipo}_ind_{ano}.csv"
        df = _read_named_csv_from_zip(content, filename)
    if df is None:
        logger.warning(f"Arquivo {filename} nao encontrado no ZIP {tipo_doc} {ano}")
        return 0

    logger.info(f"Colunas {tipo_doc} {subtipo} {ano}: {list(df.columns)}")

    col_cvm = next((c for c in df.columns if c in ["CD_CVM", "Codigo_CVM"]), None)
    if not col_cvm:
        return 0

    count = 0
    for _, row in df.iterrows():
        cod_cvm = str(row.get(col_cvm, "")).strip()
        if not cod_cvm or cod_cvm == "nan":
            continue

        try:
            valor = float(row.get("VL_CONTA", 0))
        except (ValueError, TypeError):
            valor = None

        fin = FinancialData(
            cod_cvm=cod_cvm,
            tipo_documento=f"{tipo_doc}_{subtipo}",
            data_referencia=str(row.get("DT_REFER", "")),
            conta=str(row.get("CD_CONTA", "")),
            descricao_conta=str(row.get("DS_CONTA", "")),
            valor=valor,
            escala=str(row.get("ESCALA_MOEDA", "")),
            moeda=str(row.get("MOEDA", "")),
            ordem_exercicio=str(row.get("ORDEM_EXERC", "")),
        )
        db.add(fin)
        count += 1

    await db.commit()
    logger.info(f"{tipo_doc}_{subtipo} {ano}: {count} registros")
    return count


async def run_full_sync(db: AsyncSession) -> dict:
    """Executa sincronizacao completa de todos os dados."""
    logger.info("=== INICIANDO SINCRONIZACAO COMPLETA ===")
    results = {}

    # 1. Cadastro de companhias
    results["companies"] = await sync_companies(db)

    # 2. Limpar documentos e dados financeiros anteriores
    await db.execute(delete(CompanyDocument))
    await db.execute(delete(FinancialData))
    await db.commit()

    ano = CURRENT_YEAR
    ano_ant = ano - 1

    # 3. IPE (Fatos Relevantes) - ano atual e anterior
    results["ipe_atual"] = await sync_ipe(db, ano)
    results["ipe_anterior"] = await sync_ipe(db, ano_ant)

    # 4. ITR - ano atual e anterior
    results["itr_atual"] = await sync_itr_dfp(db, "ITR", ano)
    results["itr_anterior"] = await sync_itr_dfp(db, "ITR", ano_ant)

    # 5. DFP - ano anterior (DFP anual, mais completo)
    results["dfp_anterior"] = await sync_itr_dfp(db, "DFP", ano_ant)
    results["dfp_atual"] = await sync_itr_dfp(db, "DFP", ano)

    # 6. Dados financeiros detalhados - DFP ano anterior (BPA, BPP, DRE, DFC)
    # Os CSVs estao dentro do ZIP principal: dfp_cia_aberta_{ano}.zip
    for subtipo in ["BPA", "BPP", "DRE", "DFC_MI", "DVA"]:
        key = f"dfp_{subtipo.lower()}"
        results[key] = await sync_financial_zip(db, "DFP", subtipo, ano_ant)

    # 7. Dados financeiros ITR - ano atual e anterior
    for subtipo in ["BPA", "DRE"]:
        results[f"itr_{subtipo.lower()}_atual"] = await sync_financial_zip(db, "ITR", subtipo, ano)
        results[f"itr_{subtipo.lower()}_ant"] = await sync_financial_zip(db, "ITR", subtipo, ano_ant)

    total_docs = sum(v for k, v in results.items() if k != "companies")
    logger.info(f"=== SINCRONIZACAO COMPLETA: {results} ===")
    logger.info(f"Total: {results['companies']} empresas, {total_docs} documentos/registros")
    return results
