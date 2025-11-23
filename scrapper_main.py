import requests
import json
import random
import time
from datetime import datetime, timedelta

"""
Scraper Main - Data Collection & Injection Engine

Overview:
    This script acts as the active "Feeder" for the MAI system. Its primary 
    responsibility is to continuously gather data from multiple sources (Simulated, 
    Weather API, and Reddit) and push it to the central API Server for processing 
    by the Constraint Satisfaction Tracker and LLM.

    Key Features:
    1. Hybrid Data Sourcing: Combines real-world data (Weather, Reddit) with 
       synthetic data to ensure a constant stream for stress-testing the API.
    2. Turbo Mode: Implements a high-frequency loop (2s interval) to simulate 
       real-time traffic spikes typical of railway systems during rush hour.
    3. Caching Strategy: Respects external API rate limits (Reddit/Open-Meteo) 
       by caching their responses while generating fresh synthetic data every cycle.
"""

# Target: The IP address of the machine running `api_server.py`.
# This endpoint is the gateway to the MAI Intelligence Layer.
URL_ENDPOINT = "http://10.110.168.59:8000/ingestar-realtime/"

# Strategy: Many public sites (like Reddit) block requests from scripts.
# We use a 'User-Agent' string to mimic a standard Chrome browser.
HEADERS_FAKE = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def now_iso_format():
    """Helper to generate ISO 8601 timestamps for temporal data alignment."""
    return datetime.now().isoformat()

class SyntheticGenerator:
    """
    Class: SyntheticGenerator
    
    Aim:
        To ensure the MAI system always has data to process, even when external 
        sources are quiet. This is crucial for demonstrating the "Customer Happiness 
        Index" dashboard functionality during demos or stress testing.

    Logic:
        It randomly assembles reports using pre-defined lists of Metro lines, 
        common stations, and typical issues (smoke, delays, etc.).
    """
    def __init__(self):
        self.lineas = ["Línea 1", "Línea 2", "Línea 3", "Línea 7", "Línea 9", "Línea B", "Línea 12"]
        self.problemas = ["humo", "marcha lenta", "retraso 5 min", "andén lleno", "frenado de emergencia", "avance fluido"]
        self.estaciones = ["Pantitlán", "Hidalgo", "Centro Médico", "Chabacano", "Tacubaya", "Zócalo", "Guerrero", "Bellas Artes"]

    def generar_lote(self, cantidad=3):
        """
        Method: generar_lote (generate_batch)

        Args:
            cantidad (int): Number of fake reports to generate.

        Returns:
            list[dict]: A list of dictionaries mimicking the structure of real social media posts.
        """
        datos = []
        for _ in range(cantidad):
            linea = random.choice(self.lineas)
            problema = random.choice(self.problemas)
            estacion = random.choice(self.estaciones)
            
            datos.append({
                "fuente": "Simulacion_Usuario",
                "autor": f"user_{random.randint(1000,9999)}",
                "texto": f"Reporte {linea} en {estacion}: {problema} #MetroCDMX",
                "timestamp": now_iso_format(),
                "metadata": {"tipo": "Sintetico", "prioridad": "Alta" if "humo" in problema else "Baja"}
            })
        return datos

class IngestionService:
    """
    Class: IngestionService

    Aim:
        Handles connections to external, real-world APIs. This feeds the "Data Sourcing"
        component of the MAI project, providing context (Weather) and public sentiment (Reddit).
    """
    def get_weather(self):
        """
        Method: get_weather
        
        Aim: 
            Fetches current weather data for Mexico City coordinates.
            Weather is a key constraint in railway systems (e.g., rain causes slow trains).
        
        Returns:
            dict | None: A standardized dictionary with condition and temperature.
        """
        try:
            url = "https://api.open-meteo.com/v1/forecast?latitude=19.4326&longitude=-99.1332&current_weather=true&timezone=America%2FMexico_City"
            res = requests.get(url, timeout=10)
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
            Scrapes the 'r/MexicoCity' or general search for 'metro cdmx' to find
            real user complaints and reports.
        
        Technical Details:
            - Uses HEADERS_FAKE to avoid being blocked.
            - Limits results to 15 to keep payload manageable.

        Returns:
            list[dict]: A list of cleaned Reddit post objects.
        """
        url = "https://www.reddit.com/search.json?q=metro+cdmx&sort=new&limit=15"
        try:
            res = requests.get(url, headers=HEADERS_FAKE, timeout=4)
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

def enviar_datos(payload):
    """
    Function: enviar_datos (send_data)

    Aim:
        Transmits the collected batch of data to the `api_server.py`.

    Args:
        payload (list): The list of data objects (dictionaries) to send.

    Important Note on Protocol:
        The FastAPI endpoint expects a JSON body wrapped in a specific key 
        (e.g., 'data') to satisfy Pydantic validation. 
        We wrap the payload: `{ "data": payload }`.
    """
    try:
        paquete = { "data": payload }
        
        print(f"📡 Enviando {len(payload)} registros a {URL_ENDPOINT}...")
        
        res = requests.post(URL_ENDPOINT, json=paquete, timeout=2)
        
        if res.status_code in [200, 201]:
            print(f"✅ Enviado OK (Status {res.status_code})")
        elif res.status_code == 422:
            print(f"❌ Error 422: El servidor no acepta la clave 'data'. Pregunta el nombre correcto del campo JSON.")
            print(res.text)
        else:
            print(f"⚠️ Error Servidor: {res.status_code}")

    except Exception as e:
        print(f"❌ Error Conexión: {e}")

def iniciar_turbo_mode():
    """
    Function: iniciar_turbo_mode (start_turbo_mode)

    Aim:
        The main execution loop. It orchestrates the data flow.
    
    Strategy (Rate Limiting vs. Real-time):
        - Cycle Time: 2 seconds.
        - Synthetic Data: Generated EVERY cycle (high frequency).
        - Real Data (Reddit/Weather): Refreshed every 30 cycles (60 seconds).
          This prevents hitting API rate limits on Reddit/Open-Meteo while keeping
          the local API server busy with synthetic traffic.
    """
    servicio_real = IngestionService()
    generador = SyntheticGenerator()
    
    contador_ciclos = 0
    cache_reddit = []
    cache_clima = None

    print("🚀 MODO TURBO ACTIVADO: Envíos cada 2 segundos.")
    print("🛡️ Reddit/Clima se actualizarán cada 60 segundos (Ciclo 30).")

    while True:
        try:
            payload_final = []

            # Logic: 30 cycles * 2 seconds = 60 seconds refresh rate for external APIs
            if contador_ciclos % 30 == 0:
                print("\n🔄 [Ciclo 30] Refrescando datos reales de Reddit y Clima...")
                cache_clima = servicio_real.get_weather()
                cache_reddit = servicio_real.get_reddit()
            
            if cache_clima:
                # Refresh timestamp to make cached data appear current in the stream
                cache_clima_copy = cache_clima.copy()
                cache_clima_copy['timestamp'] = now_iso_format()
                payload_final.append(cache_clima_copy)
            
            if cache_reddit:
                payload_final.extend(cache_reddit)

            # Generate fresh synthetic data every cycle to maintain "Turbo" volume
            datos_fake = generador.generar_lote(cantidad=5)
            payload_final.extend(datos_fake)

            enviar_datos(payload_final)

            time.sleep(2)
            contador_ciclos += 1

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    iniciar_turbo_mode()