import requests
import json
import random
import time
from datetime import datetime

# --- CONFIGURACIÓN ---
URL_ENDPOINT = "http://10.110.168.59:8000/ingestar-realtime/"
HEADERS_FAKE = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def now_iso_format():
    return datetime.now().isoformat()

# --- 1. GENERADOR SINTÉTICO (Datos de relleno) ---
class SyntheticGenerator:
    def __init__(self):
        self.lineas = ["Línea 1", "Línea 2", "Línea 3", "Línea 7", "Línea 9", "Línea B", "Línea 12"]
        self.problemas = ["humo", "marcha lenta", "retraso 5 min", "andén lleno", "frenado de emergencia", "avance fluido"]
        self.estaciones = ["Pantitlán", "Hidalgo", "Centro Médico", "Chabacano", "Tacubaya", "Zócalo", "Guerrero", "Bellas Artes"]

    def generar_uno(self):
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

# --- 2. SERVICIO REAL (Reddit + Clima) ---
class IngestionService:
    def get_weather(self):
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
        # Pedimos 10 posts. Como se enviarán 1 a 1 cada 2 seg, 
        # tardaremos 20 segundos en consumirlos. Perfecto para el anti-ban.
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

# --- 3. FUNCIÓN DE ENVÍO INDIVIDUAL ---
def enviar_uno(dato):
    try:
        # Mantenemos el wrapper {"data": [dato]} por compatibilidad con lo que ya te funcionó,
        # pero la lista solo lleva 1 elemento.
        paquete = { "data": [dato] }
        
        print(f"📡 Enviando registro único ({dato['fuente']}) a {URL_ENDPOINT}...")
        
        # Timeout subido a 10s por seguridad
        res = requests.post(URL_ENDPOINT, json=paquete, timeout=10)
        
        if res.status_code in [200, 201]:
            print(f"✅ Enviado OK.")
        elif res.status_code == 422:
            print(f"❌ Error 422: Formato rechazado. Respuesta: {res.text}")
        else:
            print(f"⚠️ Error Server: {res.status_code}")

    except Exception as e:
        print(f"❌ Error Conexión: {e}")

# --- 4. BUCLE DE GOTEO (QUEUE SYSTEM) ---
def iniciar_modo_goteo():
    servicio_real = IngestionService()
    generador = SyntheticGenerator()
    
    # Esta es nuestra "Fila de Espera"
    cola_de_envio = []
    
    ultimo_refresco_reddit = 0
    INTERVALO_REDDIT = 60 # Segundos entre llamadas a Reddit

    print("💧 INICIANDO MODO GOTEO: Enviando 1 registro cada 2 segundos.")
    print("🛡️ Anti-Ban Activo: Reddit se consulta cada 60s.")

    while True:
        try:
            ahora = time.time()

            # A. RELLENAR LA COLA (Si toca)
            # Verificamos si ya pasaron 60 segundos para ir por datos reales
            if ahora - ultimo_refresco_reddit > INTERVALO_REDDIT:
                print("\n🔄 Buscando datos nuevos en Reddit/Clima...")
                
                nuevos_reddit = servicio_real.get_reddit()
                nuevo_clima = servicio_real.get_weather()
                
                # Agregamos a la cola
                if nuevo_clima: cola_de_envio.append(nuevo_clima)
                cola_de_envio.extend(nuevos_reddit)
                
                ultimo_refresco_reddit = ahora
                print(f"   -> Se agregaron {len(nuevos_reddit) + (1 if nuevo_clima else 0)} registros reales a la cola.")

            # B. SI LA COLA ESTÁ VACÍA, USAMOS RELLENO (Sintético)
            if len(cola_de_envio) == 0:
                dato_fake = generador.generar_uno()
                cola_de_envio.append(dato_fake)

            # C. TOMAR EL PRIMERO DE LA FILA (FIFO)
            dato_a_enviar = cola_de_envio.pop(0)
            
            # Actualizamos timestamp justo antes de enviar para que sea "tiempo real"
            if 'timestamp' in dato_a_enviar:
                dato_a_enviar['timestamp'] = now_iso_format()

            # D. ENVIAR Y ESPERAR
            enviar_uno(dato_a_enviar)
            
            time.sleep(2) # Pausa obligatoria de 2 segundos

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error en el ciclo: {e}")
            time.sleep(2)

if __name__ == "__main__":
    iniciar_modo_goteo()