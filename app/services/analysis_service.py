"""
Servico de analise de empresas abertas.
Gera analise 360 baseada nos dados coletados da CVM.
Todas as informacoes sao estritamente baseadas nas fontes oficiais.
"""
import logging
from datetime import datetime

from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.models import Company, CompanyDocument, FinancialData, AnalysisPrompt

logger = logging.getLogger(__name__)

# Links de referencia oficiais
REFS = {
    "cadastro": "https://dados.cvm.gov.br/dataset/cia_aberta-cad",
    "itr": "https://dados.cvm.gov.br/dataset/cia_aberta-doc-itr",
    "dfp": "https://dados.cvm.gov.br/dataset/cia_aberta-doc-dfp",
    "ipe": "https://dados.cvm.gov.br/dataset/cia_aberta-doc-ipe",
    "portal_cvm": "https://dados.cvm.gov.br/",
    "sistemas_cvm": "https://sistemas.cvm.gov.br/?CiaDoc",
}


def _format_brl(value):
    """Formata valor em BRL."""
    if value is None:
        return "N/D"
    if abs(value) >= 1_000_000_000:
        return f"R$ {value / 1_000_000_000:.2f} bi"
    if abs(value) >= 1_000_000:
        return f"R$ {value / 1_000_000:.2f} mi"
    if abs(value) >= 1_000:
        return f"R$ {value / 1_000:.2f} mil"
    return f"R$ {value:.2f}"


async def get_company_by_name(db: AsyncSession, search: str) -> Company | None:
    """Busca empresa por nome (parcial)."""
    result = await db.execute(
        select(Company).where(
            Company.nome.ilike(f"%{search}%")
        ).limit(1)
    )
    return result.scalar_one_or_none()


async def search_companies(db: AsyncSession, search: str, limit: int = 20):
    """Busca empresas por nome."""
    result = await db.execute(
        select(Company).where(
            Company.nome.ilike(f"%{search}%")
        ).order_by(Company.nome).limit(limit)
    )
    return result.scalars().all()


async def get_company_documents(db: AsyncSession, cod_cvm: str, tipo: str = None, limit: int = 50):
    """Busca documentos de uma empresa."""
    query = select(CompanyDocument).where(CompanyDocument.cod_cvm == cod_cvm)
    if tipo:
        query = query.where(CompanyDocument.tipo == tipo)
    query = query.order_by(CompanyDocument.data_referencia.desc()).limit(limit)
    result = await db.execute(query)
    return result.scalars().all()


async def get_financial_summary(db: AsyncSession, cod_cvm: str):
    """Busca resumo financeiro de uma empresa."""
    # Buscar dados mais recentes de DRE (receita, lucro)
    contas_chave = {
        "3.01": "Receita Liquida",
        "3.11": "Lucro/Prejuizo do Periodo",
        "1": "Ativo Total",
        "2": "Passivo Total",
        "2.03": "Patrimonio Liquido",
        "1.01": "Ativo Circulante",
        "2.01": "Passivo Circulante",
        "3.05": "EBIT (Resultado antes dos tributos)",
    }

    summary = {}
    for conta_prefix, label in contas_chave.items():
        # Buscar sem filtrar por ordem_exercicio (encoding pode variar)
        result = await db.execute(
            select(FinancialData)
            .where(
                and_(
                    FinancialData.cod_cvm == cod_cvm,
                    FinancialData.conta == conta_prefix,
                )
            )
            .order_by(FinancialData.data_referencia.desc())
            .limit(8)
        )
        rows = result.scalars().all()
        if rows:
            # Filtrar apenas o exercicio mais recente (ultimo ou penultimo)
            vistos = set()
            dedup = []
            for r in rows:
                key = r.data_referencia
                if key not in vistos:
                    vistos.add(key)
                    dedup.append(r)
                if len(dedup) >= 4:
                    break
            summary[label] = [
                {"data": r.data_referencia, "valor": r.valor, "valor_fmt": _format_brl(r.valor)}
                for r in dedup
            ]

    return summary


async def get_peer_companies(db: AsyncSession, setor: str, exclude_cod: str, limit: int = 5):
    """Busca empresas do mesmo setor para comparacao."""
    result = await db.execute(
        select(Company).where(
            and_(
                Company.setor == setor,
                Company.cod_cvm != exclude_cod,
                Company.situacao == "ATIVO",
            )
        ).limit(limit)
    )
    return result.scalars().all()


async def generate_analysis_360(db: AsyncSession, company_name: str) -> dict:
    """Gera analise 360 completa de uma empresa."""
    company = await get_company_by_name(db, company_name)
    if not company:
        return {"error": f"Empresa '{company_name}' nao encontrada. Tente buscar pelo nome oficial registrado na CVM."}

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    # Dados cadastrais
    cadastro = {
        "nome": company.nome,
        "nome_pregao": company.nome_pregao,
        "cnpj": company.cnpj,
        "setor": company.setor,
        "situacao": company.situacao,
        "data_registro_cvm": company.data_registro,
        "cod_cvm": company.cod_cvm,
        "fonte": REFS["cadastro"],
        "data_consulta": now,
    }

    # Documentos recentes
    itrs = await get_company_documents(db, company.cod_cvm, "ITR", 8)
    dfps = await get_company_documents(db, company.cod_cvm, "DFP", 4)
    ipes = await get_company_documents(db, company.cod_cvm, "IPE", 20)

    docs = {
        "itrs": [
            {
                "data_referencia": d.data_referencia,
                "data_entrega": d.data_entrega,
                "link": d.link_documento if d.link_documento else REFS["itr"],
                "versao": d.versao,
            }
            for d in itrs
        ],
        "dfps": [
            {
                "data_referencia": d.data_referencia,
                "data_entrega": d.data_entrega,
                "link": d.link_documento if d.link_documento else REFS["dfp"],
                "versao": d.versao,
            }
            for d in dfps
        ],
        "fatos_relevantes": [
            {
                "data_referencia": d.data_referencia,
                "descricao": d.descricao,
                "data_entrega": d.data_entrega,
                "link": d.link_documento if d.link_documento else REFS["ipe"],
            }
            for d in ipes
        ],
        "fonte_itr": REFS["itr"],
        "fonte_dfp": REFS["dfp"],
        "fonte_ipe": REFS["ipe"],
    }

    # Dados financeiros
    financials = await get_financial_summary(db, company.cod_cvm)

    # Indicadores calculados
    indicators = _calculate_indicators(financials)

    # Empresas pares
    peers_list = []
    if company.setor:
        peers = await get_peer_companies(db, company.setor, company.cod_cvm)
        for p in peers:
            peer_fin = await get_financial_summary(db, p.cod_cvm)
            peers_list.append({
                "nome": p.nome,
                "cod_cvm": p.cod_cvm,
                "financials": {
                    k: v[0]["valor_fmt"] if v else "N/D"
                    for k, v in peer_fin.items()
                },
            })

    return {
        "cadastro": cadastro,
        "documentos": docs,
        "financeiros": financials,
        "indicadores": indicators,
        "pares": peers_list,
        "fontes": REFS,
        "data_analise": now,
    }


def _calculate_indicators(financials: dict) -> dict:
    """Calcula indicadores financeiros a partir dos dados."""
    indicators = {}

    def _get_latest(label):
        vals = financials.get(label, [])
        return vals[0]["valor"] if vals else None

    receita = _get_latest("Receita Liquida")
    lucro = _get_latest("Lucro/Prejuizo do Periodo")
    ativo = _get_latest("Ativo Total")
    passivo = _get_latest("Passivo Total")
    pl = _get_latest("Patrimonio Liquido")
    ativo_circ = _get_latest("Ativo Circulante")
    passivo_circ = _get_latest("Passivo Circulante")
    ebit = _get_latest("EBIT (Resultado antes dos tributos)")

    if receita and lucro:
        indicators["Margem Liquida"] = f"{(lucro / receita) * 100:.1f}%"
    if ativo and lucro:
        indicators["ROA"] = f"{(lucro / ativo) * 100:.1f}%"
    if pl and lucro:
        indicators["ROE"] = f"{(lucro / pl) * 100:.1f}%"
    if ativo_circ and passivo_circ and passivo_circ != 0:
        indicators["Liquidez Corrente"] = f"{ativo_circ / passivo_circ:.2f}"
    if ativo and pl:
        divida_total = ativo - pl
        if pl != 0:
            indicators["Divida/PL"] = f"{divida_total / pl:.2f}"
    if receita and ebit:
        indicators["Margem EBIT"] = f"{(ebit / receita) * 100:.1f}%"

    return indicators


async def get_default_prompts():
    """Retorna prompts padrao para analise."""
    return [
        {
            "id": "visao_geral",
            "nome": "Visao Geral da Empresa",
            "categoria": "geral",
            "prompt": (
                "Com base nos dados cadastrais e documentos publicos da CVM, "
                "faca uma visao geral da empresa {empresa}, incluindo setor de atuacao, "
                "situacao do registro, e historico de entregas regulatorias. "
                "Cite as fontes e datas de referencia."
            ),
        },
        {
            "id": "analise_financeira",
            "nome": "Analise Financeira",
            "categoria": "financeiro",
            "prompt": (
                "Analise a performance financeira da empresa {empresa} com base nos dados "
                "das demonstracoes financeiras (DFP/ITR) disponiveis no portal da CVM. "
                "Inclua receita liquida, lucro/prejuizo, margens, ROE, ROA, liquidez corrente "
                "e endividamento. Compare evolucao trimestral. Cite fontes e datas."
            ),
        },
        {
            "id": "analise_credito",
            "nome": "Analise de Credito",
            "categoria": "credito",
            "prompt": (
                "Faca uma analise de credito corporativo da empresa {empresa} considerando: "
                "capacidade de pagamento (cobertura de juros, geracao de caixa), "
                "alavancagem (divida/PL, divida/EBITDA), liquidez, "
                "e qualidade dos ativos. Use dados das DFP/ITR da CVM. "
                "Identifique riscos e pontos de atencao. Cite fontes e datas."
            ),
        },
        {
            "id": "fatos_relevantes",
            "nome": "Fatos Relevantes Recentes",
            "categoria": "estrategico",
            "prompt": (
                "Liste e analise os fatos relevantes mais recentes da empresa {empresa} "
                "publicados na CVM (IPE). Identifique possiveis impactos em: "
                "estrategia, aquisicoes, mudancas de gestao, eventos corporativos, "
                "e riscos de credito. Cite fontes e datas de cada documento."
            ),
        },
        {
            "id": "comparacao_pares",
            "nome": "Comparacao com Pares",
            "categoria": "comparativo",
            "prompt": (
                "Compare a empresa {empresa} com seus pares do mesmo setor "
                "usando dados financeiros da CVM (DFP/ITR). Inclua indicadores como "
                "receita, margens, ROE, liquidez e endividamento. "
                "Identifique vantagens e desvantagens competitivas. Cite fontes e datas."
            ),
        },
    ]
