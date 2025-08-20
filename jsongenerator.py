import asyncio
import os
from pydantic import BaseModel, Field
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMExtractionStrategy, LLMConfig
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Definindo o schema para os dados.
class DatasetMetadata(BaseModel):
    is_opendata_page: bool = Field(..., description="Indica se a página é de dados abertos.")
    last_updated_date: str = Field(..., description="Data da última atualização do dataset, ou 'Não encontrado'")
    version_info: str = Field(..., description="Informações de versão do dataset, ou 'Não encontrado'")
    license_info: str = Field(..., description="Informações sobre a licença (ex: Creative Commons), ou 'Não encontrado'")
    data_formats: list[str] = Field(..., description="Lista de formatos de arquivo disponíveis (ex: CSV, XLSX), ou ['Não encontrado']")
    main_page_url: str = Field(..., description="URL da página principal do dataset, ou 'Não encontrado'")
    url_download_data: str = Field(..., description="URL para download dos dados, ou 'Não encontrado'")
    possible_api: str = Field(..., description="Possíveis URLs de API para acesso aos dados, ou 'Não encontrado'")
    presencaURLsArquivosMetadadosAnexos: str = Field(..., description="URLs de arquivos, metadados ou anexos presentes na página, ou 'Não encontrado'")
    URLsArquivosMetadadosAnexos: list[str] = Field(..., description="Lista de URLs de arquivos, metadados ou anexos presentes na página, ou ['Não encontrado']")
    title: str = Field(..., description="Título do dataset, ou 'Não encontrado'")
    descricao: str = Field(..., description="Descrição do dataset, ou 'Não encontrado'")
    emails: list[str] = Field(..., description="Lista de e-mails de contato, ou ['Não encontrado']")
    responsable_organizacao: str = Field(..., description="Nome da organização responsável pelo dataset, ou 'Não encontrado'")


async def main():
    groq_api_key = os.getenv("GROQ_API_KEY")
    if not groq_api_key:
        print(" GROQ_API_KEY não encontrado no .env")
        return
    
    url = input("Insira a URL da página de dados: ").strip()
    if not url:
        print(" URL inválida.")
        return
    
    # Configuração da extração com LLM
    crawler_config = CrawlerRunConfig(
        cache_mode="bypass",
        extraction_strategy=LLMExtractionStrategy(
            llm_config=LLMConfig(
                provider="groq/llama3-8b-8192",
                api_token=groq_api_key,
            ),
            schema=DatasetMetadata.model_json_schema(),
            extraction_type="schema",
            instruction=(
                "Retorne exclusivamente um JSON válido, sem explicações, sem comentários. Extraia da página as seguintes informações:\n"
                "0. Indica se a página é de dados abertos. Se não for, pode parar por aqui o crawl.\n"
                "1. Data da última atualização do dataset.\n"
                "2. Informações de versão.\n"
                "3. Tipo de licença.\n"
                "4. Lista de formatos de arquivo disponíveis.\n"
                "5. Url da página principal.\n"
                "6. Url para download dos dados.\n"
                "7. Possíveis URLs de API para acesso aos dados.\n"
                "8. URLs de arquivos, metadados ou anexos presentes na página.\n"
                "9. Título do dataset.\n"
                "10. Descrição do dataset.\n"
                "11. Lista de e-mails de contato.\n"
                "12. Nome da organização responsável pelo dataset.\n"
                "Se algum item não for encontrado, retorne 'Não encontrado'."
            ),
            extra_args={"temperature": 0, "max_tokens": 1000},
        ),
    )

    print(f"\n🌐 Iniciando crawl em {url}\n")
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=url, config=crawler_config)

        if result.success:
            print("Extração concluída com sucesso:")
            print(result.extracted_content) 
        else:
            print("Falha no crawl:", result.error_message)

if __name__ == "__main__":
    asyncio.run(main())
