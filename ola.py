import praw
import sqlite3
import datetime
import requests
from bs4 import BeautifulSoup
import random
import time
import pandas as pd
from googlesearch import search  # Libreria: googlesearch-python
import json

# ==========================================
# CONFIGURACIÓN
# ==========================================

# 1. CONFIGURACIÓN DE SQLITE (Base de datos local tipo NoSQL)
# Se creará automáticamente un archivo .db en tu carpeta
DB_FILE = "metro_cdmx_monitor.db"

# 2. CONFIGURACIÓN DE REDDIT (Necesitas crear una app en https://www.reddit.com/prefs/apps)
# Es gratuito para uso moderado.
REDDIT_CLIENT_ID = 'TU_CLIENT_ID'
REDDIT_CLIENT_SECRET = 'TU_CLIENT_SECRET'
REDDIT_USER_AGENT = 'MetroCDMXMonitor/0.1'

# 3. PALABRAS CLAVE DE BÚSQUEDA
KEYWORDS = [
    "Metro CDMX", "Metro de la Ciudad de México", "retraso metro cdmx",
    "linea 12 metro", "linea 3 metro", "linea 7 metro",
    "humo metro", "lento metro cdmx", "inseguridad metro cdmx"
]

class DataMiner:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.connect_to_db()
        self.create_table()
        
    def connect_to_db(self):
        """Establece conexión con SQLite (se crea automáticamente si no existe)"""
        try:
            self.connection = sqlite3.connect(DB_FILE)
            self.cursor = self.connection.cursor()
            print(f"[*] Conectado a SQLite: {DB_FILE}")
        except sqlite3.Error as e:
            print(f"[!] Error conectando a SQLite: {e}")
            raise

    def create_table(self):
        """Crea la tabla si no existe - Estructura tipo documento (NoSQL style)"""
        try:
            # Tabla con estructura flexible para almacenar documentos tipo JSON
            create_table_query = """
            CREATE TABLE IF NOT EXISTS raw_opinions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            self.cursor.execute(create_table_query)
            
            # Crear índices para búsquedas rápidas
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_created 
                ON raw_opinions(created_at)
            """)
            
            self.connection.commit()
            print(f"[*] Tabla creada/verificada correctamente")
            
        except sqlite3.Error as e:
            print(f"[!] Error creando tabla: {e}")
            raise

    def save_to_db(self, source, text, author, url, timestamp):
        """
        Guarda el documento en SQLite como JSON (estilo NoSQL)
        Cada registro es un documento completo similar a MongoDB
        """
        try:
            # Crear documento tipo NoSQL (JSON)
            document = {
                "source": source,
                "text": text,
                "author": author,
                "url": url,
                "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime.datetime) else str(timestamp),
                "processed_by_llm": False,
                "metadata": {
                    "text_length": len(text),
                    "has_emoji": any(char for char in text if ord(char) > 127),
                    "extraction_date": datetime.datetime.now().isoformat()
                }
            }
            
            # Verificar si ya existe la URL (evitar duplicados)
            check_query = "SELECT id FROM raw_opinions WHERE json_extract(document, '$.url') = ?"
            self.cursor.execute(check_query, (url,))
            
            if self.cursor.fetchone():
                print(f"[.] Dato duplicado omitido de {source}")
                return
            
            # Insertar nuevo registro como JSON
            insert_query = "INSERT INTO raw_opinions (document) VALUES (?)"
            self.cursor.execute(insert_query, (json.dumps(document, ensure_ascii=False),))
            self.connection.commit()
            
            print(f"[+] Nuevo dato guardado de {source}")
            
        except sqlite3.Error as e:
            print(f"[!] Error guardando en base de datos: {e}")

    def mine_reddit(self):
        """Extrae datos reales de Reddit usando PRAW"""
        print("\n--- Iniciando Minería en Reddit ---")
        try:
            reddit = praw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent=REDDIT_USER_AGENT
            )
            
            # Buscamos en subreddits relevantes
            for query in KEYWORDS:
                print(f"Buscando '{query}' en Reddit...")
                for submission in reddit.subreddit("all").search(query, sort="new", limit=10):
                    self.save_to_db(
                        source="Reddit",
                        text=f"{submission.title} \n {submission.selftext}",
                        author=submission.author.name if submission.author else "Unknown",
                        url=submission.url,
                        timestamp=datetime.datetime.fromtimestamp(submission.created_utc)
                    )
                    time.sleep(0.5)  # Pequeña pausa para no saturar
        except Exception as e:
            print(f"[!] Error en Reddit (Revisa tus credenciales): {e}")

    def mine_web_news(self):
        """Busca noticias recientes en Google y extrae el contenido"""
        print("\n--- Iniciando Minería Web (Noticias) ---")
        try:
            # Usamos googlesearch para encontrar URLs recientes
            for query in KEYWORDS[:3]: # Limitamos para no saturar
                search_query = f"{query} site:.mx" # Filtramos sitios de México
                print(f"Googleando: {search_query}")
                
                for url in search(search_query, num_results=5, advanced=True):
                    try:
                        # Basic Web Scraping
                        response = requests.get(url.url, timeout=10)
                        if response.status_code == 200:
                            soup = BeautifulSoup(response.content, 'html.parser')
                            
                            # Intenta obtener el texto principal (esto varia por sitio)
                            # Generalmente está en etiquetas <p>
                            paragraphs = soup.find_all('p')
                            text_content = " ".join([p.get_text() for p in paragraphs if len(p.get_text()) > 50])
                            
                            if len(text_content) > 200: # Solo guardar si hay contenido sustancial
                                title = soup.title.string if soup.title else "Sin titulo"
                                self.save_to_db(
                                    source="WebNews",
                                    text=f"{title} \n {text_content[:1000]}...", # Cortamos para no saturar
                                    author=url.netloc, # Dominio como autor
                                    url=url.url,
                                    timestamp=datetime.datetime.now()
                                )
                        time.sleep(1)  # Pausa entre requests
                    except Exception as e:
                        print(f"Error scrapeando {url.url}: {e}")
                        
        except Exception as e:
            print(f"[!] Error en Web Mining: {e}")

    def simulate_twitter_facebook(self):
        """
        IMPORTANTE:
        Las APIs de Twitter (X) y FB son de pago o muy restrictivas.
        Para tu prototipo, generaremos datos sintéticos que imitan tweets/posts.
        Esto permite probar tu LLM sin gastar dinero ni ser bloqueado.
        """
        print("\n--- Simulando Datos de Twitter/Facebook (Mock Data) ---")
        
        quejas_comunes = [
            "Llevo 20 minutos esperando en la linea 7 y no pasa nada #MetroCDMX",
            "Huele a quemado en la estación Hidalgo, cuidado gente.",
            "Increíble que suban el precio y las escaleras eléctricas no sirvan.",
            "La linea 12 está super lenta hoy, tomen precauciones.",
            "Vagoneros peleandose en la linea B, no hay policias.",
            "Todo fluido en la linea 3 hoy, milagro!",
            "¿Alguien sabe si ya abrió el tramo elevado?"
        ]
        
        plataformas = ["Twitter (X)", "Facebook Groups"]
        
        for _ in range(10): # Generar 10 posts falsos
            texto = random.choice(quejas_comunes)
            source = random.choice(plataformas)
            
            # Añadimos variabilidad
            if random.random() > 0.5:
                texto += " 😡"
            
            self.save_to_db(
                source=source,
                text=texto,
                author=f"usuario_{random.randint(1000,9999)}",
                url=f"http://mock-social-media.com/{random.randint(10000,99999)}",
                timestamp=datetime.datetime.now()
            )

    def get_statistics(self):
        """Muestra estadísticas de los datos recopilados (consultas estilo NoSQL)"""
        try:
            print("\n--- Estadísticas de Datos Recopilados ---")
            
            # Total de registros
            self.cursor.execute("SELECT COUNT(*) FROM raw_opinions")
            total = self.cursor.fetchone()[0]
            print(f"Total de registros: {total}")
            
            # Registros por fuente (extrayendo del JSON)
            self.cursor.execute("""
                SELECT json_extract(document, '$.source') as source, COUNT(*) as cantidad 
                FROM raw_opinions 
                GROUP BY json_extract(document, '$.source')
            """)
            print("\nRegistros por fuente:")
            for row in self.cursor.fetchall():
                print(f"  - {row[0]}: {row[1]}")
            
            # Registros pendientes de procesar
            self.cursor.execute("""
                SELECT COUNT(*) 
                FROM raw_opinions 
                WHERE json_extract(document, '$.processed_by_llm') = 'false'
            """)
            pendientes = self.cursor.fetchone()[0]
            print(f"\nRegistros pendientes de procesar por LLM: {pendientes}")
            
        except sqlite3.Error as e:
            print(f"[!] Error obteniendo estadísticas: {e}")

    def get_all_documents(self, limit=None):
        """
        Obtiene todos los documentos (estilo NoSQL)
        Útil para procesamiento posterior con LLM
        """
        try:
            query = "SELECT id, document FROM raw_opinions"
            if limit:
                query += f" LIMIT {limit}"
            
            self.cursor.execute(query)
            documents = []
            
            for row in self.cursor.fetchall():
                doc_id = row[0]
                doc_data = json.loads(row[1])
                doc_data['_id'] = doc_id  # Agregar ID al documento
                documents.append(doc_data)
            
            return documents
            
        except sqlite3.Error as e:
            print(f"[!] Error obteniendo documentos: {e}")
            return []

    def query_by_source(self, source_name):
        """Consulta documentos por fuente (estilo NoSQL)"""
        try:
            query = """
                SELECT document 
                FROM raw_opinions 
                WHERE json_extract(document, '$.source') = ?
            """
            self.cursor.execute(query, (source_name,))
            
            documents = []
            for row in self.cursor.fetchall():
                documents.append(json.loads(row[0]))
            
            return documents
            
        except sqlite3.Error as e:
            print(f"[!] Error en consulta: {e}")
            return []

    def export_to_json(self, filename="export_data.json"):
        """Exporta todos los datos a un archivo JSON"""
        try:
            documents = self.get_all_documents()
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(documents, f, ensure_ascii=False, indent=2)
            print(f"[*] Datos exportados a {filename}")
        except Exception as e:
            print(f"[!] Error exportando datos: {e}")

    def close_connection(self):
        """Cierra la conexión a SQLite"""
        if self.connection:
            self.connection.close()
            print("\n[*] Conexión a SQLite cerrada")

if __name__ == "__main__":
    miner = None
    try:
        miner = DataMiner()
        
        # 1. Ejecutar Reddit (Requiere API Key real, si no la tienes, comenta esta linea)
        # miner.mine_reddit() 
        
        # 2. Ejecutar Web News (Funciona sin API key, usa Google search público)
        miner.mine_web_news()
        
        # 3. Simular Twitter/FB (Para llenar la DB y probar el LLM)
        miner.simulate_twitter_facebook()
        
        # 4. Mostrar estadísticas
        miner.get_statistics()
        
        # 5. Exportar datos a JSON (opcional)
        miner.export_to_json()
        
        print("\n[***] Ciclo de minería terminado. Datos listos para el LLM.")
        
        # Ejemplo: Ver algunos documentos
        print("\n--- Ejemplo de documentos almacenados ---")
        docs = miner.get_all_documents(limit=2)
        for doc in docs:
            print(json.dumps(doc, indent=2, ensure_ascii=False))
        
    except Exception as e:
        print(f"\n[!!!] Error fatal: {e}")
    finally:
        if miner:
            miner.close_connection()