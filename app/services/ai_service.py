"""
Servico de analise com IA (Claude API).
Gera analise narrativa especialista a partir dos dados da CVM + prompt parametrizavel.
Toda informacao citada e estritamente baseada nos dados oficiais da CVM.
"""
import logging
from datetime import datetime

import anthropic

from app.config import settings

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Voce e um analista especialista em credito corporativo e mercado de capitais brasileiro.
Sua funcao e analisar dados oficiais de empresas abertas brasileiras coletados do Portal Dados Abertos da CVM.

REGRAS FUNDAMENTAIS:
1. Os dados ja foram fornecidos na mensagem do usuario - NUNCA peca mais dados
2. Analise SEMPRE com base no que foi fornecido, mesmo que seja parcial
3. Cite APENAS informacoes presentes nos dados - NUNCA invente numeros ou fatos
4. Se um indicador especifico nao estiver disponivel, diga "nao disponivel nos dados" e continue a analise
5. Use linguagem tecnica e precisa adequada a analistas de credito
6. Formate a resposta em Markdown com secoes claras e objetivas
7. Sempre inclua uma secao "## Fontes e Referencias" ao final com links da CVM
8. Conclua sempre com uma secao "## Conclusao de Credito" mesmo com dados parciais
"""

PROMPT_TEMPLATES = {
    "visao_geral": """Com base nos dados abaixo da empresa {empresa} (fonte: CVM), faca uma visao geral executiva incluindo:
- Perfil da empresa (setor, situacao registral, historico de entregas)
- Documentos mais recentes entregues (ITR, DFP, Fatos Relevantes)
- Principais eventos corporativos recentes
- Pontos de atencao para analise de credito

Dados disponíveis:
{dados}""",

    "analise_financeira": """Com base nos dados financeiros abaixo da empresa {empresa} (fonte: DFP/ITR CVM), faca uma analise financeira detalhada incluindo:
- Evolucao da receita liquida e margens (com datas de referencia)
- Analise de rentabilidade: ROE, ROA, EBITDA estimado
- Estrutura de capital e endividamento
- Geracao de caixa (DFC quando disponivel)
- Evolucao dos indicadores ao longo dos periodos disponiveis
- Tendencias e alertas

Dados disponíveis:
{dados}""",

    "analise_credito": """Com base nos dados abaixo da empresa {empresa} (fonte: CVM), elabore uma analise de credito corporativo completa incluindo:
- Capacidade de pagamento: cobertura de juros estimada, geracao de caixa livre
- Alavancagem: divida/PL, divida/EBITDA (com base nos dados disponiveis)
- Liquidez: indices de liquidez corrente e imediata
- Qualidade dos ativos e composicao do passivo
- Risco de refinanciamento e concentracao
- Rating interno sugerido (AAA a D) com justificativa baseada nos dados
- Principais covenants potenciais recomendados
- Conclusao de credito e limites sugeridos

Dados disponíveis:
{dados}""",

    "fatos_relevantes": """Com base nos fatos relevantes (IPE) abaixo da empresa {empresa} publicados na CVM, faca uma analise estrategica incluindo:
- Resumo cronologico dos principais eventos
- Identificacao de: aquisicoes, desinvestimentos, mudancas de controle/gestao
- Impacto potencial em: estrategia, estrutura de capital, risco de credito
- Eventos com maior materialidade para analise de credito
- Alertas e pontos de atencao

Dados disponíveis:
{dados}""",

    "comparacao_pares": """Com base nos dados financeiros abaixo, compare a empresa {empresa} com seus pares do mesmo setor (fonte: CVM). Inclua:
- Tabela comparativa de indicadores-chave (receita, margens, rentabilidade, liquidez)
- Posicionamento competitivo da empresa no setor
- Vantagens e desvantagens em relacao aos pares
- Implicacoes para o risco de credito relativo
- Conclusao: melhor ou pior credito que os pares e por que

Dados disponíveis:
{dados}""",
}


def _format_data_for_ai(data: dict) -> str:
    """Formata os dados da CVM em texto estruturado para o prompt."""
    lines = []
    data_analise = data.get("data_analise", datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))

    # Cadastro
    c = data.get("cadastro", {})
    lines.append("=== DADOS CADASTRAIS (Fonte: CVM - Cadastro Cias Abertas) ===")
    lines.append(f"Nome: {c.get('nome', 'N/D')}")
    lines.append(f"Nome de Pregao: {c.get('nome_pregao', 'N/D')}")
    lines.append(f"CNPJ: {c.get('cnpj', 'N/D')}")
    lines.append(f"Setor CVM: {c.get('setor', 'N/D')}")
    lines.append(f"Situacao: {c.get('situacao', 'N/D')}")
    lines.append(f"Data de Registro na CVM: {c.get('data_registro_cvm', 'N/D')}")
    lines.append(f"Codigo CVM: {c.get('cod_cvm', 'N/D')}")
    lines.append(f"Fonte: {c.get('fonte', 'https://dados.cvm.gov.br')}")
    lines.append(f"Data da Consulta: {data_analise}")
    lines.append("")

    # Indicadores calculados
    ind = data.get("indicadores", {})
    if ind:
        lines.append("=== INDICADORES FINANCEIROS CALCULADOS ===")
        for k, v in ind.items():
            lines.append(f"{k}: {v}")
        lines.append("(Calculados a partir dos dados DFP/ITR da CVM)")
        lines.append("")

    # Dados financeiros
    fin = data.get("financeiros", {})
    if fin:
        lines.append("=== DADOS FINANCEIROS (Fonte: DFP/ITR CVM) ===")
        for conta, registros in fin.items():
            for reg in registros:
                lines.append(f"{conta} | Periodo: {reg['data']} | Valor: {reg['valor_fmt']}")
        lines.append("")

    # Fatos relevantes
    docs = data.get("documentos", {})
    fatos = docs.get("fatos_relevantes", [])
    if fatos:
        lines.append(f"=== FATOS RELEVANTES (Fonte: IPE CVM) - {len(fatos)} eventos ===")
        for f in fatos[:30]:  # Limitar para nao exceder contexto
            lines.append(f"Data: {f.get('data_referencia', 'N/D')} | Assunto: {f.get('descricao', 'N/D')}")
            if f.get('link'):
                lines.append(f"  Link: {f['link']}")
        lines.append("")

    # ITRs
    itrs = docs.get("itrs", [])
    if itrs:
        lines.append(f"=== ITR - INFORMACOES TRIMESTRAIS (Fonte: CVM) - {len(itrs)} entregas ===")
        for d in itrs[:8]:
            lines.append(f"Referencia: {d.get('data_referencia', 'N/D')} | Entrega: {d.get('data_entrega', 'N/D')}")
        lines.append("")

    # DFPs
    dfps = docs.get("dfps", [])
    if dfps:
        lines.append(f"=== DFP - DEMONSTRACOES FINANCEIRAS (Fonte: CVM) - {len(dfps)} entregas ===")
        for d in dfps[:4]:
            lines.append(f"Referencia: {d.get('data_referencia', 'N/D')} | Entrega: {d.get('data_entrega', 'N/D')}")
        lines.append("")

    # Pares
    pares = data.get("pares", [])
    if pares:
        lines.append("=== EMPRESAS PARES DO MESMO SETOR (Fonte: CVM) ===")
        for p in pares:
            fin_str = " | ".join(f"{k}: {v}" for k, v in p.get("financials", {}).items())
            lines.append(f"{p['nome']}: {fin_str}")
        lines.append("")

    # Aviso de disponibilidade de dados
    fin = data.get("financeiros", {})
    docs = data.get("documentos", {})
    fatos = docs.get("fatos_relevantes", [])
    itrs = docs.get("itrs", [])
    dfps = docs.get("dfps", [])

    lines.append("=== DISPONIBILIDADE DE DADOS ===")
    lines.append(f"Dados financeiros detalhados: {'SIM - ' + str(len(fin)) + ' indicadores' if fin else 'NAO DISPONIVEL (aguardando sincronizacao ou empresa sem dados)'}")
    lines.append(f"Fatos relevantes: {len(fatos)} eventos")
    lines.append(f"ITR (trimestrais): {len(itrs)} entregas")
    lines.append(f"DFP (anuais): {len(dfps)} entregas")
    lines.append("Fonte de todos os dados: Portal Dados Abertos CVM (dados.cvm.gov.br)")
    lines.append("")

    return "\n".join(lines)


async def generate_ai_analysis(data: dict, prompt_id: str = None, custom_prompt: str = None) -> str:
    """Gera analise narrativa usando Claude API com dados da CVM."""
    if not settings.ANTHROPIC_API_KEY:
        return "_Analise por IA nao configurada. Adicione ANTHROPIC_API_KEY nas variaveis de ambiente._"

    empresa = data.get("cadastro", {}).get("nome", "empresa")
    dados_formatados = _format_data_for_ai(data)

    # Selecionar template de prompt
    if custom_prompt:
        template = custom_prompt
    elif prompt_id and prompt_id in PROMPT_TEMPLATES:
        template = PROMPT_TEMPLATES[prompt_id]
    else:
        template = PROMPT_TEMPLATES["visao_geral"]

    user_prompt = template.format(empresa=empresa, dados=dados_formatados)

    try:
        client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)
        message = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        return message.content[0].text
    except anthropic.AuthenticationError:
        logger.error("ANTHROPIC_API_KEY invalida")
        return "_Chave de API da Anthropic invalida. Verifique a variavel ANTHROPIC_API_KEY._"
    except Exception as e:
        logger.error(f"Erro na chamada Claude API: {e}")
        return f"_Erro ao gerar analise por IA: {str(e)}_"
