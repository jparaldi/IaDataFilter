import asyncio
from crawl4ai import AsyncWebCrawler # type: ignore

async def main():
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun("https://dados.gov.br/dados/conjuntos-dados/patrimonio-genetico-nacional-que-foram-envolvidas-em-pesquisas-realizadas-na-ufam-por-ano")
        print(result.markdown)

asyncio.run(main())
