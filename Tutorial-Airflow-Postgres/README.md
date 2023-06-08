
## Tutorial - Airflow com Docker usando banco de dados Postgres Local

Neste tutorial, coloquei em prática o uso do airflow com docker, criando uma conexão com o banco de dados postgres localmente, visualizando uma tabela criada como teste no pgAdmin através das DAGs.

- Instale o docker e o docker compose
- Instale o airflow no docker
- No arquivo docker-compose.yaml foram feitas algumas modificações para vincular o postgres que está instaldo localmente na sua máquina para visualizar o banco de dados criado no docker com airflow.
- No restante foram criados o arquivo dag_executa_sql.py e a pasta sql com os aquivos sql da criacao da tabela e a insercao de dados.

[https://www.youtube.com/watch?v=mMqaiNbeeUU&t=188s](https://www.youtube.com/watch?v=mMqaiNbeeUU)


