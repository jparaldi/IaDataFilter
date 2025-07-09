from crawl4ai import WebCrawler

#criar uma instância pro webCrawler
crawler = WebCawler()

crawler.warmup()

#rodar o crawler numa url
result = crawler.run(url="https://dados.gov.br/dados/conjuntos-dados")

#printar os resultados
print(result.markdown)