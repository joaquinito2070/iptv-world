import requests
import aiohttp
import asyncio
import gzip
import json
from lxml import etree
from m3u_parser import M3uParser
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import os
import logging

# --- Configuración ---
SOURCE_M3U_URL = "https://tv.piperagossip.org/all.m3u"
SOURCE_EPG_URL = "https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz"

# --- Archivos de Salida ---
OUTPUT_M3U_FILE = "filtered.m3u"
OUTPUT_JSON_FILE = "epg.json"
OUTPUT_XMLTV_FILE = "epg.xml"

# --- Constantes ---
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
# Timeout en segundos para verificar cada canal
CHANNEL_TIMEOUT = 8

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_epg_time(time_str):
    """Convierte el formato de tiempo de XMLTV (ej: 20251117163000 +0100) a un objeto datetime UTC."""
    try:
        # Asume que el formato siempre incluye timezone
        return datetime.strptime(time_str, '%Y%m%d%H%M%S %z').astimezone(timezone.utc)
    except ValueError:
        # Fallback si no hay timezone (aunque debería)
        return datetime.strptime(time_str, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)

async def check_stream_status(session, stream_url):
    """
    Verifica el estado de una URL de stream.
    Retorna True si el estado es 2xx o 3xx, False en caso contrario.
    """
    try:
        # Usamos HEAD para ser más rápidos, permitimos redirecciones
        async with session.head(stream_url, timeout=CHANNEL_TIMEOUT, allow_redirects=True) as response:
            if 200 <= response.status < 400:
                logging.info(f"ÉXITO (HTTP {response.status}) - {stream_url}")
                return True
            else:
                logging.warning(f"FALLO (HTTP {response.status}) - {stream_url}")
                return False
    except asyncio.TimeoutError:
        logging.warning(f"FALLO (Timeout) - {stream_url}")
        return False
    except Exception as e:
        logging.error(f"FALLO (Error: {e}) - {stream_url}")
        return False

async def filter_m3u_streams(parser):
    """
    Filtra la lista de streams M3U concurrentemente.
    """
    logging.info(f"Iniciando verificación de {len(parser.get_list())} canales...")
    valid_streams = []
    tasks = []
    
    # Creamos una sesión de aiohttp
    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
        for stream in parser.get_list():
            stream_url = stream['url']
            # Añadimos la tarea de verificación
            tasks.append(asyncio.create_task(check_stream_status(session, stream_url)))
        
        # Esperamos a que todas las tareas terminen
        results = await asyncio.gather(*tasks)
        
        # Recogemos los streams que devolvieron True
        for stream, is_valid in zip(parser.get_list(), results):
            if is_valid:
                valid_streams.append(stream)
                
    logging.info(f"Verificación completada. Canales válidos: {len(valid_streams)} de {len(parser.get_list())}")
    return valid_streams

def generate_filtered_m3u(valid_streams):
    """
    Genera el contenido del archivo M3U filtrado.
    """
    logging.info(f"Generando archivo {OUTPUT_M3U_FILE}...")
    content = ["#EXTM3U"]
    for stream in valid_streams:
        # Reconstruimos la línea #EXTINF
        extinf = f"#EXTINF:{stream['duration']}"
        attrs = []
        if stream.get('attributes'):
            for key, val in stream['attributes'].items():
                attrs.append(f'{key}="{val}"')
        if attrs:
            extinf += " " + " ".join(attrs)
        
        extinf += f",{stream['name']}"
        content.append(extinf)
        content.append(stream['url'])
        
    with open(OUTPUT_M3U_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(content))
    logging.info(f"Archivo {OUTPUT_M3U_FILE} guardado.")

def process_epg(valid_channel_ids):
    """
    Descarga, descomprime y procesa el EPG.
    Retorna el árbol XML parseado y un diccionario de programas por canal.
    """
    logging.info(f"Descargando EPG desde {SOURCE_EPG_URL}...")
    try:
        headers = {"User-Agent": USER_AGENT}
        response = requests.get(SOURCE_EPG_URL, headers=headers, timeout=30)
        response.raise_for_status()
        
        logging.info("Descomprimiendo EPG...")
        xml_content = gzip.decompress(response.content)
        
        logging.info("Parseando EPG XML...")
        # Usamos 'recover=True' para ignorar posibles errores de XML mal formado
        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(xml_content, parser=parser)
        
        programs_by_channel = defaultdict(list)
        now_utc = datetime.now(timezone.utc)
        
        # Límites de tiempo para JSON y XMLTV
        json_limit_3h = now_utc + timedelta(hours=3)
        xmltv_limit_6h = now_utc + timedelta(hours=6)
        
        all_programs = root.findall('programme')
        logging.info(f"Procesando {len(all_programs)} programas del EPG...")

        for prog in all_programs:
            channel_id = prog.get('channel')
            if channel_id not in valid_channel_ids:
                continue # Ignoramos EPG de canales que no están en nuestra M3U válida

            try:
                start_dt = parse_epg_time(prog.get('start'))
                stop_dt = parse_epg_time(prog.get('stop'))
            except Exception as e:
                logging.warning(f"Error parseando tiempo de programa: {e}")
                continue

            # --- Condición: No incluir programas ya emitidos ---
            if stop_dt <= now_utc:
                continue
                
            program_data = {
                "title": prog.findtext('title'),
                "desc": prog.findtext('desc'),
                "start": start_dt.isoformat(),
                "stop": stop_dt.isoformat()
            }
            
            # Añadimos a la lista para el JSON (si cumple límite de 3h)
            if start_dt < json_limit_3h:
                programs_by_channel[channel_id].append(program_data)
        
        logging.info("EPG parseado y filtrado para JSON.")
        return root, programs_by_channel, now_utc, xmltv_limit_6h

    except Exception as e:
        logging.error(f"Error fatal procesando el EPG: {e}")
        return None, defaultdict(list), datetime.now(timezone.utc), datetime.now(timezone.utc) + timedelta(hours=6)

def generate_filtered_xmltv(root, valid_channel_ids, now_utc, limit_6h):
    """
    Genera el archivo XMLTV filtrado (max 6 horas, solo canales válidos).
    """
    if root is None:
        logging.warning("No se generará XMLTV porque el EPG original falló.")
        return
        
    logging.info(f"Generando archivo {OUTPUT_XMLTV_FILE} (límite 6 horas)...")
    
    # Crear un nuevo árbol XML
    new_root = etree.Element('tv')
    
    # 1. Filtrar Canales
    for channel in root.findall('channel'):
        if channel.get('id') in valid_channel_ids:
            new_root.append(channel)
            
    # 2. Filtrar Programas
    program_count = 0
    for prog in root.findall('programme'):
        channel_id = prog.get('channel')
        if channel_id not in valid_channel_ids:
            continue
            
        try:
            start_dt = parse_epg_time(prog.get('start'))
            stop_dt = parse_epg_time(prog.get('stop'))
        except Exception:
            continue # Ignorar programas con tiempo mal formado

        # Condiciones:
        # 1. El programa aún no ha terminado (stop > now)
        # 2. El programa comienza antes del límite de 6 horas (start < limit_6h)
        if stop_dt > now_utc and start_dt < limit_6h:
            new_root.append(prog)
            program_count += 1

    # Guardar el nuevo XML
    tree = etree.ElementTree(new_root)
    tree.write(OUTPUT_XMLTV_FILE, pretty_print=True, encoding='UTF-8', xml_declaration=True)
    logging.info(f"Archivo {OUTPUT_XMLTV_FILE} guardado con {program_count} programas.")

def generate_json_output(valid_streams, programs_by_channel):
    """
    Genera el archivo JSON final con streams y EPG integrado (max 3h, no emitidos).
    """
    logging.info(f"Generando archivo {OUTPUT_JSON_FILE} (límite 3 horas, no emitidos)...")
    
    output_data = []
    for stream in valid_streams:
        channel_id = stream['attributes'].get('tvg-id')
        
        channel_data = {
            "id": channel_id,
            "name": stream['name'],
            "group": stream['attributes'].get('group-title'),
            "logo": stream['attributes'].get('tvg-logo'),
            "url": stream['url'],
            "epg": programs_by_channel.get(channel_id, []) # Ya filtrado por tiempo
        }
        output_data.append(channel_data)
        
    with open(OUTPUT_JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    logging.info(f"Archivo {OUTPUT_JSON_FILE} guardado.")

async def main():
    logging.info("--- Iniciando proceso de actualización ---")
    
    # 1. Descargar y parsear M3U fuente
    try:
        parser = M3uParser(timeout=10, useragent=USER_AGENT)
        parser.parse_m3u(SOURCE_M3U_URL)
    except Exception as e:
        logging.error(f"Error fatal: No se pudo descargar o parsear la M3U fuente: {e}")
        return

    # 2. Filtrar streams (verificar estado online)
    valid_streams = await filter_m3u_streams(parser)
    if not valid_streams:
        logging.warning("No se encontraron canales válidos. Abortando.")
        return
        
    # 3. Generar M3U filtrado
    generate_filtered_m3u(valid_streams)
    
    # 4. Obtener IDs de canales válidos para el EPG
    valid_channel_ids = {s['attributes'].get('tvg-id') for s in valid_streams if s['attributes'].get('tvg-id')}
    logging.info(f"IDs de canales válidos con EPG: {len(valid_channel_ids)}")

    # 5. Procesar EPG (Descargar, parsear, filtrar para JSON)
    epg_root, programs_by_channel, now_utc, limit_6h = await asyncio.to_thread(process_epg, valid_channel_ids)
    
    # 6. Generar XMLTV filtrado (límite 6h)
    await asyncio.to_thread(generate_filtered_xmltv, epg_root, valid_channel_ids, now_utc, limit_6h)
    
    # 7. Generar JSON filtrado (límite 3h, no emitidos)
    generate_json_output(valid_streams, programs_by_channel)
    
    logging.info("--- Proceso completado exitosamente ---")

if __name__ == "__main__":
    # Asegurarse de que el directorio 'artifacts' existe si es necesario (no en este script, pero buena práctica)
    # os.makedirs("artifacts", exist_ok=True) 
    
    asyncio.run(main())
