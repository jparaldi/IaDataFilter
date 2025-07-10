import asyncio
from crawl4ai import AsyncWebCrawler # type: ignore

async def main():
    async with AsyncWebCrawler() as crawler:
        url_alvo = "https://dados.gov.br/dados/conjuntos-dados/patrimonio-genetico-nacional-que-foram-envolvidas-em-pesquisas-realizadas-na-ufam-por-ano"
        print(f"[FETCHING] Buscando conteúdo de: {url_alvo}")

        result = await crawler.arun(url_alvo)

        # Verifica se o token "licença de uso" está presente no conteúdo Markdown
        token_alvo = "licença de uso"
        if token_alvo.lower() in result.markdown.lower():
            print(f"\n[SUCESSO] O token '{token_alvo}' foi encontrado na página!")
            #imprimir um trecho do markdown onde o token aparece para contexto
            print("\n--- Trecho relevante ---")
            # Encontra a posição do token e imprime um pedaço ao redor
            start_index = result.markdown.lower().find(token_alvo.lower())
            if start_index != -1:
                end_index = min(start_index + len(token_alvo) + 50, len(result.markdown))
                print(result.markdown[max(0, start_index - 50):end_index] + "...")
            print("----------------------")
        else:
            print(f"\n[ALERTA] O token '{token_alvo}' NÃO foi encontrado na página.")

        # Opcional: Imprimir a saída completa em Markdown para referência
        # print("\n--- Saída Markdown Completa ---")
        # print(result.markdown)
        # print("----------------------------")

asyncio.run(main())