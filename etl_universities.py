import requests
import sqlite3
import time
from typing import List, Dict, Any

BASE_URL = "http://universities.hipolabs.com/search"
DB_NAME = "universities.db"

# Tabela alvo (modelo sugerido):
# countries (id INTEGER PK, name TEXT UNIQUE)
# universities (id INTEGER PK, name TEXT, country_id INTEGER FK, alpha_two_code TEXT, state_province TEXT, domains TEXT, web_pages TEXT)
# Índices auxiliares para consulta por país e nome

CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS countries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS universities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        country_id INTEGER NOT NULL,
        alpha_two_code TEXT,
        state_province TEXT,
        domains TEXT,         -- lista separada por ;
        web_pages TEXT,        -- lista separada por ;
        FOREIGN KEY(country_id) REFERENCES countries(id)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_universities_country ON universities(country_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_universities_name ON universities(name);
    """
]

# Algumas listas de países podem ser derivadas ao consultar sem filtro (mas a API exige parâmetro). 
# Estratégia: usar uma lista de países conhecida (ISO / exemplos) ou carregar de fonte externa. 
# Para simplicidade, definimos uma lista padrão de países comuns; pode ser ampliada conforme necessário.
DEFAULT_COUNTRIES = [
    "Brazil", "United States", "Canada", "Argentina", "Chile", "Colombia", "Mexico", "Peru",
    "United Kingdom", "France", "Germany", "Spain", "Italy", "Portugal", "Netherlands", "Belgium",
    "Australia", "New Zealand", "China", "Japan", "South Korea", "India", "South Africa",
    "Nigeria", "Egypt", "Kenya", "Ghana", "Sweden", "Norway", "Finland", "Denmark", "Poland"
]


def fetch_country_universities(country: str, retry: int = 3, backoff: float = 1.5) -> List[Dict[str, Any]]:
    params = {"country": country}
    for attempt in range(1, retry + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            # Garante que só retornamos registros coerentes
            cleaned = [r for r in data if r.get("country") and r.get("name")]
            return cleaned
        except (requests.RequestException, ValueError) as e:
            if attempt == retry:
                print(f"Falha ao obter dados de {country}: {e}")
                return []
            sleep_for = backoff ** attempt
            print(f"Erro na tentativa {attempt} para {country}. Retentando em {sleep_for:.1f}s...")
            time.sleep(sleep_for)
    return []


def ensure_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    for sql in CREATE_TABLES_SQL:
        cur.execute(sql)
    conn.commit()


def get_or_create_country_id(conn: sqlite3.Connection, country_name: str) -> int:
    cur = conn.cursor()
    cur.execute("SELECT id FROM countries WHERE name = ?", (country_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO countries (name) VALUES (?)", (country_name,))
    conn.commit()
    return cur.lastrowid


def insert_university(conn: sqlite3.Connection, uni: Dict[str, Any]):
    country_id = get_or_create_country_id(conn, uni["country"].strip())
    domains = ";".join(uni.get("domains", []) or [])
    web_pages = ";".join(uni.get("web_pages", []) or [])
    alpha_two_code = uni.get("alpha_two_code")
    state_province = (uni.get("state-province") or uni.get("state_province") or None)

    cur = conn.cursor()
    # Evitar duplicações básicas: checar por nome + país + state_province
    cur.execute(
        """
        SELECT id FROM universities 
        WHERE name = ? AND country_id = ? AND IFNULL(state_province,'') = IFNULL(?, '')
        """,
        (uni["name"].strip(), country_id, state_province)
    )
    if cur.fetchone():
        return  # já existe

    cur.execute(
        """
        INSERT INTO universities (name, country_id, alpha_two_code, state_province, domains, web_pages)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            uni["name"].strip(),
            country_id,
            alpha_two_code,
            state_province,
            domains,
            web_pages,
        )
    )


def etl_load(countries: List[str] = None):
    countries = countries or DEFAULT_COUNTRIES
    conn = sqlite3.connect(DB_NAME)
    try:
        ensure_tables(conn)
        total_inserted = 0
        for country in countries:
            records = fetch_country_universities(country)
            print(f"{country}: {len(records)} registros brutos")
            for uni in records:
                insert_university(conn, uni)
            conn.commit()
            # contar temporariamente
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM universities")
            total_inserted = cur.fetchone()[0]
        print(f"Total acumulado de universidades na base: {total_inserted}")
    finally:
        conn.commit()
        conn.close()


def example_queries():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    print("Total de universidades por país:")
    for row in cur.execute(
        """
        SELECT c.name, COUNT(u.id) as total
        FROM countries c
        LEFT JOIN universities u ON u.country_id = c.id
        GROUP BY c.id, c.name
        ORDER BY total DESC;
        """
    ):
        print(row)

    print("\nUniversidades do Brasil:")
    for row in cur.execute(
        """
        SELECT u.name FROM universities u
        JOIN countries c ON c.id = u.country_id
        WHERE c.name = ?
        ORDER BY u.name
        LIMIT 20;
        """,
        ("Brazil",)
    ):
        print(row)

    termo = "%Tech%"  # exemplo de busca
    print(f"\nBusca por universidades contendo 'Tech':")
    for row in cur.execute(
        """
        SELECT u.name, c.name as country
        FROM universities u
        JOIN countries c ON c.id = u.country_id
        WHERE u.name LIKE ?
        ORDER BY u.name
        LIMIT 20;
        """,
        (termo,)
    ):
        print(row)
    conn.close()


if __name__ == "__main__":
    etl_load()
    example_queries()
