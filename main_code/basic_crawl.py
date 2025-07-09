import asyncio
from crawl4ai import AsyncWebCrawler

async def main():
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun("https://dados.gov.br/dados/conjuntos-dados")
        print(result.markdown)

asyncio.run(main())
