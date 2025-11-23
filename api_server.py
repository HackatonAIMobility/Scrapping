from fastapi import FastAPI, Request
import uvicorn

app = FastAPI()

# Este es el ENDPOINT (la puerta de entrada)
@app.post("/ingest/metro-data")
async def receive_data(request: Request):
    # 1. Recibimos el JSON que manda tu scraper
    datos = await request.json()
    
    print(f"⚡ [API] Recibidos {len(datos)} reportes del Metro.")
    
    # --- AQUÍ CONECTAS TU LLM ---
    # En lugar de guardar archivo, aquí pasas 'datos' a tu función de IA
    # Ejemplo: respuesta_llm = mi_modelo.analizar(datos)
    
    # Por ahora solo imprimimos el primero para verificar
    if datos:
        print(f"   Ejemplo: {datos[0]['texto']}")

    return {"status": "exito", "mensaje": "Datos recibidos correctamente"}

if __name__ == "__main__":
    # host="0.0.0.0" es CRUCIAL para que funcione en red local (Hotspot)
    print("🚀 Servidor escuchando en toda la red local...")
    uvicorn.run(app, host="0.0.0.0", port=8000)