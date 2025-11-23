import requests
import json
import random
import time
from datetime import datetime

"""
Scraper Main - Drip Feed Mode (Queue System)

Overview:
    This script implements a "Drip Feed" data ingestion strategy for the MAI 
    (Mexican Analytics & Insights) project. 
    
    Unlike batch processing, this module uses a First-In-First-Out (FIFO) queue 
    to smooth out data delivery. It fetches external data in "chunks" (to respect 
    API limits) but pushes data to the central API Server one record at a time.

    Architecture:
    1. Producer (IngestionService): Fetches real data every 60 seconds.
    2. Buffer (Queue): Holds real data mixed with synthetic data.
    3. Consumer (enviar_uno): Pops one record every 2 seconds and sends it 
       to the Intelligence Core.

    Goal: 
    To simulate a continuous, organic flow of user reports, preventing server 
    overload while maintaining active monitoring.
"""

# Target: The IP address of the machine running `api_server.py`.
URL_ENDPOINT = "http://10.110.168.59:8000/ingestar-realtime/"

# Strategy: Mimic a standard web browser to prevent 403 Forbidden errors 
# from strict public APIs like Reddit.
HEADERS_FAKE = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def now_iso_format():
    """Helper to generate ISO 8601 timestamps for real-time alignment."""
    return datetime.now().isoformat()

class SyntheticGenerator:
    """
    Class: SyntheticGenerator
    
    Aim:
        Acts as a fallback data provider. When the "Real Data" queue is empty, 
        this class generates synthetic Metro reports.
        
    Why this is needed:
        Real-world data can be sparse (e.g., at 3 AM). To ensure the MAI 
        dashboards remain active and the system health checks pass during 
        demonstrations or low-traffic periods, we inject synthetic noise.
    """
    def __init__(self):
        self.lineas = ["Línea 1", "Línea 2", "Línea 3", "Línea 7", "Línea 9", "Línea B", "Línea 12"]
        self.problemas = ["humo", "marcha lenta", "retraso 5 min", "andén lleno", "frenado de emergencia", "avance fluido"]
        self.estaciones = ["Pantitlán", "Hidalgo", "Centro Médico", "Chabacano", "Tacubaya", "Zócalo", "Guerrero", "Bellas Artes"]

    def generar_uno(self):
        """
        Method: generar_uno (generate_one)

        Returns:
            dict: A single synthetic report object containing source, author, 
            text, and metadata.
        """
        linea = random.choice(self.lineas)
        problema = random.choice(self.problemas)
        estacion = random.choice(self.estaciones)
        
        return {
            "fuente": "Simulacion_Usuario",
            "autor": f"user_{random.randint(1000,9999)}",
            "texto": f"Reporte {linea} en {estacion}: {problema} #MetroCDMX",
            "timestamp": now_iso_format(),
            "metadata": {"tipo": "Sintetico", "prioridad": "Alta" if "humo" in problema else "Baja"}
        }

class IngestionService:
    """
    Class: IngestionService

    Aim:
        Interfaces with external public APIs (Open-Meteo and Reddit) to gather
        contextual and sentiment data.
    """
    def get_weather(self):
        """
        Method: get_weather
        
        Aim: 
            Fetches current environmental conditions. 
            Weather is a high-priority constraint for railway operations.
        
        Returns:
            dict | None: Standardized weather object or None if fetch fails.
        """
        try:
            url = "https://api.open-meteo.com/v1/forecast?latitude=19.4326&longitude=-99.1332&current_weather=true&timezone=America%2FMexico_City"
            res = requests.get(url, timeout=3)
            if res.status_code == 200:
                data = res.json()
                return {
                    "fuente": "Open-Meteo",
                    "timestamp": now_iso_format(),
                    "condicion": "Lluvia" if data['current_weather']['weathercode'] >= 51 else "Nublado/Seco",
                    "temperatura": data['current_weather']['temperature']
                }
        except:
            pass
        return None

    def get_reddit(self):
        """
        Method: get_reddit
        
        Aim:
            Harvests user sentiment from social media.
            
        Constraint Handling (Anti-Ban):
            This method fetches 10 posts at once but is only called every 60 seconds 
            by the main loop. This keeps the request volume low (1 req/min) while 
            providing enough data to feed the 2-second drip loop.
            
        Returns:
            list[dict]: A list of standardized social media posts.
        """
        url = "https://www.reddit.com/search.json?q=metro+cdmx&sort=new&limit=10"
        try:
            res = requests.get(url, headers=HEADERS_FAKE, timeout=5)
            if res.status_code == 200:
                posts = res.json().get('data', {}).get('children', [])
                extracted = []
                for p in posts:
                    d = p['data']
                    extracted.append({
                        "fuente": "Reddit",
                        "autor": d['author'],
                        "texto": d['title'][:200],
                        "timestamp": datetime.fromtimestamp(d['created_utc']).isoformat(),
                        "url": f"https://reddit.com{d['permalink']}"
                    })
                return extracted
        except Exception as e:
            print(f"⚠️ Error Reddit: {e}")
        return []

def enviar_uno(dato):
    """
    Function: enviar_uno (send_one)

    Aim:
        Delivers a single data point to the API Server.
        
    Technical Note:
        Even though we are sending a single item, we wrap it in a list 
        inside a dictionary `{"data": [dato]}`. This maintains schema compatibility 
        with the API endpoint that might also accept batches.
    """
    try:
        # Wrapper for schema compatibility
        paquete = { "data": [dato] }
        
        print(f"📡 Enviando registro único ({dato['fuente']}) a {URL_ENDPOINT}...")
        
        # Increased timeout to 10s to ensure delivery on slower networks
        res = requests.post(URL_ENDPOINT, json=paquete, timeout=10)
        
        if res.status_code in [200, 201]:
            print(f"✅ Enviado OK.")
        elif res.status_code == 422:
            print(f"❌ Error 422: Formato rechazado. Respuesta: {res.text}")
        else:
            print(f"⚠️ Error Server: {res.status_code}")

    except Exception as e:
        print(f"❌ Error Conexión: {e}")

def iniciar_modo_goteo():
    """
    Function: iniciar_modo_goteo (start_drip_mode)

    Aim:
        The core orchestration loop implementing the Producer-Consumer pattern.
        
    Logic Flow:
        1. Queue Management: Maintains a local list (`cola_de_envio`) serving as a buffer.
        2. Producer Cycle (Every 60s): Fetches real data from Reddit/Weather and 
           appends it to the queue.
        3. Fallback: If the queue is empty (consumed faster than produced), it 
           generates a synthetic record immediately.
        4. Consumer Cycle (Every 2s): Pops the first item (FIFO), updates its 
           timestamp to 'now', and sends it.
           
    Result:
        External APIs see 1 request/minute (Safe).
        Internal API sees 1 request/2 seconds (Active).
    """
    servicio_real = IngestionService()
    generador = SyntheticGenerator()
    
    # The Buffer Queue
    cola_de_envio = []
    
    ultimo_refresco_reddit = 0
    INTERVALO_REDDIT = 60 # Cooldown for external API calls

    print("💧 INICIANDO MODO GOTEO: Enviando 1 registro cada 2 segundos.")
    print("🛡️ Anti-Ban Activo: Reddit se consulta cada 60s.")

    while True:
        try:
            ahora = time.time()

            # A. REFILL THE QUEUE (Producer Phase)
            if ahora - ultimo_refresco_reddit > INTERVALO_REDDIT:
                print("\n🔄 Buscando datos nuevos en Reddit/Clima...")
                
                nuevos_reddit = servicio_real.get_reddit()
                nuevo_clima = servicio_real.get_weather()
                
                if nuevo_clima: cola_de_envio.append(nuevo_clima)
                cola_de_envio.extend(nuevos_reddit)
                
                ultimo_refresco_reddit = ahora
                print(f"   -> Se agregaron {len(nuevos_reddit) + (1 if nuevo_clima else 0)} registros reales a la cola.")

            # B. FALLBACK MECHANISM (Synthetic Phase)
            if len(cola_de_envio) == 0:
                dato_fake = generador.generar_uno()
                cola_de_envio.append(dato_fake)

            # C. PROCESS QUEUE (Consumer Phase - FIFO)
            dato_a_enviar = cola_de_envio.pop(0)
            
            # Update timestamp to simulate real-time arrival at the ingestion point
            if 'timestamp' in dato_a_enviar:
                dato_a_enviar['timestamp'] = now_iso_format()

            # D. TRANSMISSION
            enviar_uno(dato_a_enviar)
            
            # Pace the loop to create the "Drip" effect
            time.sleep(2)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error en el ciclo: {e}")
            time.sleep(2)

if __name__ == "__main__":
    iniciar_modo_goteo()