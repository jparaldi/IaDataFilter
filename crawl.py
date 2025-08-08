import asyncio
import os
from pydantic import BaseModel, Field
# from google.colab import userdata
import json
import re
import sqlite3

# Imports das bibliotecas necessárias
from crawl4ai import (
    AsyncWebCrawler,
    CrawlerRunConfig
)
import litellm
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("GROQ_API_KEY")


#URL A SER ESTUDADA, MODIFICAR AQUI
TARGET_URL = "https://www.dados.ms.gov.br/"


# ESTRUTURAS DE DADOS E TOKENS

# Schema para a coleta de evidências dos 13 critérios principais pela IA
class EvidenceCollection(BaseModel):
    provide_api_reference: bool = Field(default=False, description="Verdadeiro se a página menciona uma API.")
    provide_api_reference_reasoning: str = Field(default="", description="Citação direta do texto que comprova a existência de uma API.")
    provide_metadata: bool = Field(default=False, description="Verdadeiro se a página contém metadados estruturados.")
    provide_metadata_reasoning: str = Field(default="", description="Citação direta do texto que descreve os metadados.")
    provide_descriptive_metadata: bool = Field(default=False, description="Verdadeiro se a página possui título e descrição claros.")
    provide_descriptive_metadata_reasoning: str = Field(default="", description="Citação direta do título ou descrição.")
    provide_data_license_information: bool = Field(default=False, description="Verdadeiro se a página menciona a licença de uso dos dados.")
    provide_data_license_information_reasoning: str = Field(default="", description="Citação direta da menção à licença.")
    provide_data_provenance_information: bool = Field(default=False, description="Verdadeiro se a página informa a origem dos dados.")
    provide_data_provenance_information_reasoning: str = Field(default="", description="Citação direta da informação de origem.")
    provide_a_version_indicator: bool = Field(default=False, description="Verdadeiro se há um indicador de versão ou data de atualização.")
    provide_a_version_indicator_reasoning: str = Field(default="", description="Citação direta da data ou versão.")
    gather_feedback_from_data_consumers: bool = Field(default=False, description="Verdadeiro se existe um mecanismo de feedback.")
    gather_feedback_from_data_consumers_reasoning: str = Field(default="", description="Citação direta do mecanismo de feedback.")
    provide_bulk_download: bool = Field(default=False, description="Verdadeiro se há opção de download em lote.")
    provide_bulk_download_reasoning: str = Field(default="", description="Citação direta da opção de download em lote.")
    provide_data_up_to_date: bool = Field(default=False, description="Verdadeiro se os dados parecem ser recentes e atualizados?")
    provide_data_up_to_date_reasoning: str = Field(default="", description="Citação direta que indique a recente atualização dos dados.")
    use_persistent_URIs_as_identifiers_of_datasets: bool = Field(default=False, description="Verdadeiro se as URLs parecem ser estáveis e persistentes.")
    use_persistent_URIs_as_identifiers_of_datasets_reasoning: str = Field(default="", description="Justificativa da análise sobre a persistência das URIs.")
    use_machine_readable_standardized_data_formats: bool = Field(default=False, description="Verdadeiro se os dados são oferecidos em formatos como CSV, JSON, XML.")
    use_machine_readable_standardized_data_formats_reasoning: str = Field(default="", description="Citação direta dos formatos disponíveis.")
    provide_data_in_multiple_formats: bool = Field(default=False, description="Verdadeiro se os dados estão disponíveis em mais de um formato.")
    provide_data_in_multiple_formats_reasoning: str = Field(default="", description="Citação direta que comprove a existência de múltiplos formatos.")
    cite_the_original_publication: bool = Field(default=False, description="Verdadeiro se os dados citam a publicação original ou a fonte.")
    cite_the_original_publication_reasoning: str = Field(default="", description="Citação direta da referência à publicação original.")

# Schema para o julgamento final do 14º critério pela IA "sênior"
class FalsePositiveJudgement(BaseModel):
    possible_false_positive: bool = Field(description="Julgamento final: Verdadeiro se a página é um 'falso positivo'.")
    possible_false_positive_reasoning: str = Field(description="A justificativa concisa para o julgamento de falso positivo, baseada no resumo da análise.")

# Mapa de tokens completo, derivado sua documentação
TOKEN_MAP = {
    "provide_api_reference": ["api", "apis", "documentacao api", "webservice", "rest", "restful", "get", "headers", "post", "delete", "put", "swagger", "apigility", "restify", "restlet"],
    "provide_metadata": ["metadados", "informacoes adicionais", "dicionario", "dicionarios", "dicionario dados", "taxonomia", "criterio", "criterios", "descricao conjunto dados", "titulo", "uri", "palavra chave", "palavras chave", "data publicacao", "data criacao", "criacao", "frequencia", "atualizacao", "data atualizacao", "contato", "granularidade", "referencia", "referencias", "responsavel", "responsaveis", "idioma", "fonte", "fontes", "versao", "mantenedor", "mantenedores", "tema", "formato data", "metadado estrutural", "metadados estruturais", "campo", "campos", "tipo dados", "metrica", "ultima modificacao", "ultima atualizacao", "descricao", "cobertura geografica", "cobertura temporal", "escopo geopolitico", "autor", "autores", "criado", "entidade responsavel", "ponto contato", "periodo temporal", "data ultima modificacao", "temas", "categorias", "formato", "formato midia", "licenca", "identificador", "relacao", "tipo conteudo", "recursos"],
    "provide_descriptive_metadata": ["title", "og:description", "publicação", "modificação", "e-mail", "email"],
    "provide_data_license_information": ["licença", "tipo licença", "termos licença", "restrições", "restrição", "licenças", "creative commons", "cc by", "cc by-sa", "cc by sa", "cco", "ec 0", "open database license", "odbl", "general public license", "gnu gpl", "gpl", "crel", "odrl", "odrs"],
    "provide_data_provenance_information": ["fonte", "fontes", "criador", "criadores", "responsavel", "responsaveis", "area responsavel", "mantenedor", "mantenedores", "autor", "autores", "editor", "editores", "editoras", "publicado", "publicador", "proveniencia"],
    "provide_a_version_indicator": ["modificação", "modificado", "atualização", "atualizado", "atualizados", "revisão"],
    "gather_feedback_from_data_consumers": ["contato", "feedback", "formulario", "rank", "ranqueamento", "esperado", "avaliacao", "avaliacoes", "ajuda", "duvidas", "duvida", "comunique", "qualidade dados", "qualidade", "comentario", "questionamento", "classifica", "classificacao", "correcao", "revisao", "compartilhar", "compartilhe", "informe", "fale", "entre", "sugestoes", "telefone"],
    "provide_bulk_download": [".zip", ".rar", ".7z", ".tar", ".gz", "data", "atualizacao", "ultima", "frequencia", "criado", "criacao", "atualizado", "cobertura", "temporal", "validade"],
    "provide_data_up_to_date": ["modificação", "modificado", "atualização", "atualizado", "atualizados", "revisão"],
    "use_persistent_URIs_as_identifiers_of_datasets": ["uri", "persistente", "identificador único", "link permanente", "permalink", "doi", "https://", "http://"],
    "use_machine_readable_standardized_data_formats": ["dataset", "dump:", "datastore", "dados abertos", "dados", "conjunto de dados", "conjunto dados", ".csv", ".json", ".xml", ".rdf", ".xlsx", ".pdf", ".ods", ".zip", ".dat", ".id", ".ind", ".map", ".tab", ".jsonld", ".ttl", ".tsv", ".xls"],
    "provide_data_in_multiple_formats": [".csv", ".json", ".xml", ".rdf", ".xlsx", "ods", ".zip", ".dat", ".id", ".ind", ".map", ".tab", "formatos"],
    "cite_the_original_publication": ["fonte", "procedencia", "citacao", "publicador", "disponivel", "referencia", "referencias"]
}

# FUNÇÕES AUXILIARES

def chunk_text(text, chunk_size_chars):
    """Divide um texto grande em pedaços de um tamanho máximo de caracteres."""
    return [text[i:i + chunk_size_chars] for i in range(0, len(text), chunk_size_chars)]

# FUNÇÃO PRINCIPAL

async def main():

    # URL ALVO:

    url_alvo = TARGET_URL
    print(f"🚀 Iniciando pipeline ROBUSTO com processamento em lotes para: {url_alvo}")

    try:
        # --- ETAPA 1: COLETA DE EVIDÊNCIAS ---
        print("\n--- Etapa 1: Coletando e analisando evidências para os 13 critérios... ---")

        async with AsyncWebCrawler() as crawler:
            result_crawl = await crawler.arun(url=url_alvo, config=CrawlerRunConfig())
        full_cleaned_text = result_crawl.markdown.lower()
        evidence_dossier = ""
        sentences = re.split(r'(?<!\w\w.)(?<![A-Z][a-z].)(?<=\.|\?)\s', full_cleaned_text)
        found_tokens_map = {topic: set() for topic in TOKEN_MAP.keys()}

        for topic, keywords in TOKEN_MAP.items():
            for sentence in sentences:
                matching_keywords = [keyword for keyword in keywords if keyword in sentence]
                if matching_keywords:
                    evidence_dossier += f"- {sentence.strip()}\n"
                    for kw in matching_keywords:
                        found_tokens_map[topic].add(kw)

        TOKEN_LIMIT_CHARS = 8000
        text_chunks = chunk_text(evidence_dossier, TOKEN_LIMIT_CHARS)

        partial_results = []
        print(f"  > Dossiê dividido em {len(text_chunks)} pacote(s) para análise...")
        for i, chunk in enumerate(text_chunks):
            print(f"  > Analisando evidências no pacote {i + 1} de {len(text_chunks)}...")
            try:
                # ### REFINAMENTO FINAL DO PROMPT ###
                user_prompt_evidence = f"""
                Analise o dossiê de evidências abaixo para preencher os campos da ferramenta 'EvidenceCollection'.

                **Regras Estritas:**
                1. Para cada critério, avalie se as evidências no dossiê o comprovam.
                2. Se comprovado, defina o campo booleano como `True` e o campo `_reasoning` com a CITAÇÃO DIRETA da evidência.
                3. Se não houver evidência, defina o campo booleano como `False` e o campo `_reasoning` como uma string vazia.
                4. Sua resposta deve ser **exclusivamente** uma chamada à ferramenta `EvidenceCollection` no formato JSON. Não inclua texto introdutório, explicações ou resumos. Apenas o JSON.

                ### DOSSIÊ DE EVIDÊNCIAS ###
                {chunk}
                """

                response = await litellm.acompletion(
                    model="groq/llama3-70b-8192",
                    messages=[{"role": "user", "content": user_prompt_evidence}],
                    tools=[{"type": "function", "function": {"name": "EvidenceCollection", "description": "Formulário para coletar evidências de 13 princípios de dados abertos.", "parameters": EvidenceCollection.model_json_schema()}}],
                    tool_choice={"type": "function", "function": {"name": "EvidenceCollection"}},

                    api_key=API_KEY

                )
                arguments = json.loads(response.choices[0].message.tool_calls[0].function.arguments)
                partial_results.append(EvidenceCollection.model_validate(arguments))
                print(f"  > Pacote {i + 1} analisado com sucesso.")
            except Exception as e:
                print(f"  > ERRO ao processar o pacote {i + 1}: {e}")
                # Adicionamos uma pausa antes de continuar, pode ajudar em erros de API momentâneos
                await asyncio.sleep(20)
                continue

        # Verifica se algum pacote foi processado com sucesso
        if not partial_results:
            print("\n❌ Nenhum pacote de evidências pôde ser processado. Análise abortada.")
            return

        # Junção dos resultados dos 13 critérios
        evidence_analysis = EvidenceCollection()
        unique_reasonings = {field: set() for field in EvidenceCollection.model_fields if field.endswith("_reasoning")}
        for partial in partial_results:
            for field, value in partial.model_dump().items():
                if field.endswith("_reasoning") and value:
                    unique_reasonings[field].add(value.strip())
                elif isinstance(value, bool) and value:
                    setattr(evidence_analysis, field.replace("_reasoning", ""), True)
        for field, reason_set in unique_reasonings.items():
            if reason_set:
                setattr(evidence_analysis, field, " | ".join(sorted(list(reason_set))))

        print("✅ Coleta de evidências concluída.")

        # ETAPA 2: JULGAMENTO FINAL 
        print("\n--- Etapa 2: Solicitando julgamento final do 'Auditor Sênior'... ---")

        detailed_report_for_judgement = "RELATÓRIO DE ANÁLISE DETALHADA PARA JULGAMENTO:\n\n"
        for field in EvidenceCollection.model_fields:
            if not field.endswith("_reasoning"):
                status = "SIM" if getattr(evidence_analysis, field) else "NÃO"
                evidence = getattr(evidence_analysis, f"{field}_reasoning") or "Nenhuma evidência encontrada."
                detailed_report_for_judgement += f"- {field}: {status}\n  Evidência(s) da IA: {evidence}\n"
        rule_keys = ["provide_metadata", "provide_descriptive_metadata", "cite_the_original_publication", "use_machine_readable_standardized_data_formats"]
        num_indicators = sum(1 for key in rule_keys if getattr(evidence_analysis, key))
        detailed_report_for_judgement += f"\nRegra de Referência do Documento: um portal é considerado fraco ('falso positivo') se menos de 3 de 4 indicadores chave forem positivos. Resultado desta análise: {num_indicators} de 4 indicadores foram positivos."
        await asyncio.sleep(20)
        final_judgement_response = await litellm.acompletion(
            model="groq/llama3-70b-8192",
            messages=[
                {"role": "system", "content": "Você é um auditor de dados sênior. Sua tarefa é fazer um julgamento final sobre a qualidade de um portal de dados com base em um relatório detalhado que inclui as evidências encontradas."},
                {"role": "user", "content": f"Com base no relatório detalhado a seguir, faça um julgamento final e justificado: esta página deve ser considerada um 'falso positivo' (um portal que parece bom na superfície, mas cujas evidências são fracas, genéricas ou contraditórias)?\n\n### RELATÓRIO DETALHADO ###\n{detailed_report_for_judgement}"}
            ],
            tools=[{"type": "function", "function": {"name": "FalsePositiveJudgement", "description": "Formulário para o julgamento final de falso positivo.", "parameters": FalsePositiveJudgement.model_json_schema()}}],
            tool_choice={"type": "function", "function": {"name": "FalsePositiveJudgement"}},
            
            api_key= API_KEY
        )
        judgement_arguments = json.loads(final_judgement_response.choices[0].message.tool_calls[0].function.arguments)
        final_judgement = FalsePositiveJudgement.model_validate(judgement_arguments)
        print("✅ Julgamento final recebido.")

        # ETAPA 3: APRESENTAÇÃO DOS RESULTADOS 
        print("\n\n--- RESULTADO FINAL DA AUDITORIA ---")
        for field in EvidenceCollection.model_fields:
            if not field.endswith("_reasoning"):
                value = getattr(evidence_analysis, field)
                reasoning = getattr(evidence_analysis, f"{field}_reasoning")
                status = "✅ SIM" if value else "❌ NÃO"
                tokens_encontrados = sorted(list(found_tokens_map.get(field, [])))
                print(f"\n- {field.replace('_', ' ').capitalize()}: {status}")
                if tokens_encontrados:
                    print(f"  Tokens Encontrados (Filtro): {tokens_encontrados}")
                print(f"  Evidência (IA): {reasoning if reasoning else 'Nenhuma evidência direta encontrada.'}")
        print("\n--- VEREDITO FINAL (AUDITOR SÊNIOR) ---")
        status = "🚨 SIM, RISCO DE FALSO POSITIVO" if final_judgement.possible_false_positive else "✅ NÃO, PORTAL CONSISTENTE"
        print(f"\n- Possible false positive: {status}")
        print(f"  Justificativa do Auditor: {final_judgement.possible_false_positive_reasoning}")
        insert_results_into_db(url_alvo, evidence_analysis, final_judgement, found_tokens_map)

    except Exception as e:
        print(f"🚨 Erro inesperado durante a execução: {e}")

    
def insert_results_into_db(url, evidence_analysis: EvidenceCollection, final_judgement: FalsePositiveJudgement, found_tokens_map: dict):
    conn = None
    cursor = None
    try:
        conn = sqlite3.connect('opendata.db') 
        cursor = conn.cursor()

        # Cria tabelas se não existirem
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tokens (
            site_id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            provide_api_reference BOOLEAN,
            provide_metadata BOOLEAN,
            provide_descriptive_metadata BOOLEAN,
            provide_data_license_information BOOLEAN,
            provide_data_provenance_information BOOLEAN,
            provide_a_version_indicator BOOLEAN,
            gather_feedback_from_data_consumers BOOLEAN,
            provide_bulk_download BOOLEAN,
            provide_data_up_to_date BOOLEAN,
            use_persistent_uris_as_identifiers_of_datasets BOOLEAN,
            use_machine_readable_standardized_data_formats BOOLEAN,
            provide_data_in_multiple_formats BOOLEAN,
            cite_the_original_publication BOOLEAN,
            possible_false_positive BOOLEAN,
            possible_false_positive_reasoning TEXT
        );
        """)

        # Inserir na tabela tokens
        insert_tokens = """
        INSERT INTO tokens (
            url,
            provide_api_reference,
            provide_metadata,
            provide_descriptive_metadata,
            provide_data_license_information,
            provide_data_provenance_information,
            provide_a_version_indicator,
            gather_feedback_from_data_consumers,
            provide_bulk_download,
            provide_data_up_to_date,
            use_persistent_uris_as_identifiers_of_datasets,
            use_machine_readable_standardized_data_formats,
            provide_data_in_multiple_formats,
            cite_the_original_publication,
            possible_false_positive,
            possible_false_positive_reasoning
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        values_tokens = (
            url,
            evidence_analysis.provide_api_reference,
            evidence_analysis.provide_metadata,
            evidence_analysis.provide_descriptive_metadata,
            evidence_analysis.provide_data_license_information,
            evidence_analysis.provide_data_provenance_information,
            evidence_analysis.provide_a_version_indicator,
            evidence_analysis.gather_feedback_from_data_consumers,
            evidence_analysis.provide_bulk_download,
            evidence_analysis.provide_data_up_to_date,
            evidence_analysis.use_persistent_URIs_as_identifiers_of_datasets,
            evidence_analysis.use_machine_readable_standardized_data_formats,
            evidence_analysis.provide_data_in_multiple_formats,
            evidence_analysis.cite_the_original_publication,
            final_judgement.possible_false_positive,
            final_judgement.possible_false_positive_reasoning
        )
        cursor.execute(insert_tokens, values_tokens)
        site_id = cursor.lastrowid

        conn.commit()
        print(f"✅ Resultados armazenados no banco local com site_id = {site_id}")

    except Exception as e:
        print(f"🚨 ERRO ao inserir no banco de dados local: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("🔒 Conexão com o banco de dados fechada.")

# Executa a função principal
if __name__ == "__main__":
    asyncio.run(main())