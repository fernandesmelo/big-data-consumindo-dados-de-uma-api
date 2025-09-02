# ETL Universidades (Hipolabs API)

Script em Python para realizar ETL da API pública: http://universities.hipolabs.com/

## Estrutura das Tabelas

```
countries (id INTEGER PK AUTOINCREMENT, name TEXT UNIQUE NOT NULL)
universities (
  id INTEGER PK AUTOINCREMENT,
  name TEXT NOT NULL,
  country_id INTEGER NOT NULL FK -> countries(id),
  alpha_two_code TEXT,
  state_province TEXT,
  domains TEXT,     -- lista separada por ;
  web_pages TEXT    -- lista separada por ;
)
```

Índices: `idx_universities_country`, `idx_universities_name`.

## Requisitos

```
python 3.9+
requests
```

Instale dependências:

```
pip install -r requirements.txt
```

## Execução

```
python etl_universities.py
```

O script irá:

1. Criar (se não existir) o banco SQLite `universities.db`.
2. Criar as tabelas e índices.
3. Consultar a API para uma lista padrão de países.
4. Inserir os registros (evitando duplicações simples por nome+país+estado).
5. Executar consultas exemplo.

## Consultas SQL Exemplo

Total de universidades por país:

```sql
SELECT c.name, COUNT(u.id) as total
FROM countries c
LEFT JOIN universities u ON u.country_id = c.id
GROUP BY c.id, c.name
ORDER BY total DESC;
```

Listagem de universidades de um país específico (ex: Brasil):

```sql
SELECT u.name
FROM universities u
JOIN countries c ON c.id = u.country_id
WHERE c.name = 'Brazil'
ORDER BY u.name;
```

Busca por termo no nome (ex: 'Tech'):

```sql
SELECT u.name, c.name AS country
FROM universities u
JOIN countries c ON c.id = u.country_id
WHERE u.name LIKE '%Tech%'
ORDER BY u.name;
```

## Personalização da Lista de Países

Altere a constante `DEFAULT_COUNTRIES` em `etl_universities.py` ou chame a função `etl_load(["Brazil", "Argentina"])`.

## Observações

- API não possui endpoint direto para listar todos os países; utilizamos lista predefinida.
- Campos de lista (domains, web_pages) armazenados como texto separado por `;` para simplicidade.
- Possível evoluir para tabela normalizada de domínios / páginas se necessário.

## Próximos Passos Sugeridos

- Adicionar logging estruturado.
- Criar testes unitários.
- Implementar atualização incremental (comparar registros já existentes por domain principal).
# big-data-consumindo-dados-de-uma-api
