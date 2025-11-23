import requests
import json
import random
import time
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE DESTINO ---
URL_ENDPOINT = "http://10.110.168.59:8000/ingestar-realtime/"

# --- HEADERS (Disfraz de navegador) ---
HEADERS_FAKE = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def now_iso_format():
    return datetime.now().isoformat()

# --- 1. GENERADOR SINTÉTICO (Relleno constante) ---
class SyntheticGenerator:
    def __init__(self):
        self.lineas = ["Línea 1", "Línea 2", "Línea 3", "Línea 7", "Línea 9", "Línea B", "Línea 12"]
        self.problemas = ["humo", "marcha lenta", "retraso 5 min", "andén lleno", "frenado de emergencia", "avance fluido"]
        self.estaciones = ["Pantitlán", "Hidalgo", "Centro Médico", "Chabacano", "Tacubaya", "Zócalo", "Guerrero", "Bellas Artes"]

    def generar_lote(self, cantidad=3):
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

# --- 2. SERVICIO DE INGESTA REAL (Reddit + Clima) ---
class IngestionService:
    def get_weather(self):
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

# --- 3. FUNCIÓN DE ENVÍO (Con corrección de Diccionario) ---
def enviar_datos(payload):
    try:
        # IMPORTANTE: Envolvemos la lista en un diccionario "data"
        # Si sigue fallando con 422, cambia "data" por "items" o "registros"
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

# --- 4. BUCLE RÁPIDO (2 SEGUNDOS) ---
def iniciar_turbo_mode():
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

            # LÓGICA MATEMÁTICA: 30 ciclos * 2 segundos = 60 segundos
            if contador_ciclos % 30 == 0:
                print("\n🔄 [Ciclo 30] Refrescando datos reales de Reddit y Clima...")
                cache_clima = servicio_real.get_weather()
                cache_reddit = servicio_real.get_reddit()
            
            # Agregamos datos reales cacheados (si existen)
            if cache_clima:
                # Actualizamos hora para que parezca nuevo
                cache_clima_copy = cache_clima.copy()
                cache_clima_copy['timestamp'] = now_iso_format()
                payload_final.append(cache_clima_copy)
            
            if cache_reddit:
                payload_final.extend(cache_reddit)

            # Agregamos datos sintéticos NUEVOS (Generamos 5 cada vez para no saturar tanto)
            datos_fake = generador.generar_lote(cantidad=5)
            payload_final.extend(datos_fake)

            # ¡FUEGO!
            enviar_datos(payload_final)

            # Descanso corto
            time.sleep(2)
            contador_ciclos += 1

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    iniciar_turbo_mode()