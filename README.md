### Spark - Receita Federal - CNPJ - Empresas do Brasil

Este projeto tem como objetivo, ler os arquivos de dados de todas as empresas do Brasil, estruturar as informações em datasets e Armazenar os dados em qualquer Datawarehouse como o Google Big Query ou qualquer outro Data Warehouse

#### Stack
* *Linguagem:* Scala
* *Cloud Provider:* Google Cloud
* *Data Processing Engine:* Spark 2.4
* *Spark-runtime:* Google DataProc
* *Script:* Bash

#### Estrutura

A solução é composta por 2 componentes principais:

##### 1. Download e preparação de dados:
O script [execute-load.sh](bin/execute-load.sh) executa o download dos arquivos de dados do site da receita federal: [http://200.152.38.155/CNPJ/DADOS_ABERTOS_CNPJ](http://200.152.38.155/CNPJ)
  
Após o download os arquivos de dados são enviados para uma pasta do google storage definida na variável:

`GS_PATH=gs://<your-googlecloud-project-name>/public `

Substitua o `<your-googlecloud-project-name>` pelo nome do seu projeto do google.

##### 2. Ler os arquivos, estruturar , separar e armazenar os dados:

A Classe [Starter](src/main/scala/br/com/bruno/data/ingestion/receitafederal/Starter.scala) recebe o caminho dos arquivos de dados baixados e executa o tratamento e armazenamento dos dados.

#### Execução
1. Para executar em produção, gere o arquivo jar com o comando abaixo:
` sbt clean assembly`

2. Salve o arquivo jar gerado na pasta do google storage `gs://<your-googlecloud-project-name>/lib/receita_federal.jar`

3. Altere o arquivo [dataproc-receita-federal.yml](workflows/dataproc-receita-federal.yml) substituindo `your-googlecloud-project-name>` pelo nome do seu projeto do google.

4. Crie um banco de dado no bigquery como o nome: stg_receita_federal

5. Execute o comando `gcloud auth login`

6. Execute o script [execute-load.sh](bin/execute-load.sh)

#### Fontes externas

Para armazenar os dados no google Big Query, utilizo o projeto do spotify: https://github.com/spotify/spark-bigquery
Fiz alterações mínimas para rodar de acordo com a minha necessidade.

#### Licença

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0




