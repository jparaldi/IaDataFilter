# IA4noCKAN

Este projeto utiliza o framework **Crawl4AI** em conjunto com a **API da Groq** para automatizar a auditoria e a avaliação da qualidade de portais de dados abertos. Ele verifica 13 critérios de qualidade com o apoio de inteligência artificial e gera um veredito final sobre a consistência dos dados, armazenando os resultados em um banco de dados local.

Além disso, inclui o script **`jsongenerator.py`**, que permite extrair de qualquer página de dados abertos um **JSON estruturado** com informações essenciais do dataset, como formatos de arquivo, URLs de download, licença, versão, título, descrição, contatos e organização responsável. Esse script serve como um gerador automático de metadados a partir de páginas web.

---

### Funcionalidades

* **Web Scraping Robusto**: Coleta e processa o conteúdo de páginas web de forma eficiente.
* **Extração de Metadados**: O `jsongenerator.py` transforma o conteúdo da página em um JSON estruturado, com campos como `is_opendata_page`, `last_updated_date`, `version_info`, `license_info`, `data_formats`, `main_page_url`, `url_download_data`, `possible_api`, `title`, `descricao`, `emails` e `responsable_organizacao`.
* **Análise de 13 Critérios**: Utiliza um LLM (Large Language Model) para analisar o conteúdo do site e extrair evidências para 13 critérios de dados abertos.
* **Processamento em Lotes**: Divide o conteúdo da página em pacotes, otimizando o uso da API e garantindo que análises grandes sejam processadas sem problemas de limite de tokens.
* **Veredito Final**: Um "auditor sênior" da IA faz uma avaliação final sobre a qualidade da página, identificando possíveis "falsos positivos".
* **Armazenamento Local**: Salva todos os resultados da auditoria em um banco de dados (`opendata.db`).

---

### Instalação e Configuração

Siga estes passos para configurar e executar o projeto:

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/jparaldi/IA4noCKAN.git
    cd IA4noCKAN
    ```

2.  **Crie e ative um ambiente virtual:**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

    Instale a biblioteca do crawl4AI com pip.

3.  **Configure sua Chave de API da Groq:**
    Para usar a API da Groq, é necessário obter uma chave de API.
    * Vá para o **[Groq Console](https://console.groq.com/)** e faça login ou crie uma conta.
    * No menu lateral, navegue até `API Keys`.
    * Clique em `Create API Key` e dê um nome a ela.
    * Copie a chave gerada. **Importante**: Por questões de segurança, você não conseguirá vê-la novamente.
    * No diretório principal do seu projeto, crie um arquivo chamado `.env`.
    * Dentro do arquivo `.env`, adicione a sua chave no seguinte formato:
        ```bash
        GROQ_API_KEY=sua_chave_aqui
        ```
    * Substitua `sua_chave_aqui` pela chave que você copiou do Groq Console.

---

### Como Usar

Para iniciar a auditoria completa do portal de dados, execute:

```bash
python crawl.py
```

Para gerar um JSON estruturado de metadados a partir de uma página específica de dados abertos, execute:

```bash
python jsongenerator.py
```

O script pedirá a URL da página e retornará um JSON com os principais campos do dataset, pronto para uso ou integração com outras ferramentas.