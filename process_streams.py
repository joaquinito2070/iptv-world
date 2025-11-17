import requests
import gzip
import shutil
import json
import re
from lxml import etree
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

# --- Configuración ---

# URLs de origen
SOURCE_M3U_URL = "https://tv.piperagossip.org/all.m3u"
SOURCE_EPG_URL = "https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz"

# Límite de tiempo para comprobar cada stream (en segundos)
CHECK_TIMEOUT = 4

# Número de hilos para comprobar URLs en paralelo (¡importante para la velocidad!)
MAX_WORKERS = 50

# URL base de tu repositorio de GitHub Pages (¡CAMBIA ESTO!)
# Ejemplo: "https://tu-usuario.github.io/tu-repositorio"
BASE_URL = "https://TU_USUARIO.github.io/TU_REPOSITORIO"

# Nombres de los archivos de salida
OUTPUT_M3U = "playlist.m3u"
OUTPUT_EPG = "epg.xml"
OUTPUT_JSON = "playlist.json"
OUTPUT_INFO = "info.json"

# --- Funciones Auxiliares ---

def get_current_utc_time():
    """Devuelve la hora actual en UTC."""
    return datetime.now(timezone.utc)

def parse_epg_datetime(date_str):
    """Convierte la fecha del EPG (ej: '20230101120000 +0000') a un objeto datetime UTC."""
    try:
        # Formato de fecha de XMLTV
        return datetime.strptime(date_str, '%Y%m%d%H%M%S %z')
    except ValueError:
        return None

def check_stream_status(url):
    """
    Comprueba el estado de una URL. Devuelve True si es 2xx o 3xx, False en caso contrario.
    Usamos 'HEAD' para no descargar el stream, solo los encabezados.
    """
    try:
        response = requests.head(url, timeout=CHECK_TIMEOUT, allow_redirects=True, headers={'User-Agent': 'IPTV-Checker/1.0'})
        # Códigos 2xx (Éxito) y 3xx (Redirección) se consideran "online"
        if 200 <= response.status_code < 400:
            return True
    except requests.RequestException:
        # Ignora errores de conexión, timeouts, etc.
        pass
    return False

def download_and_parse_epg():
    """Descarga, descomprime y analiza el EPG XML."""
    print(f"Descargando EPG desde {SOURCE_EPG_URL}...")
    try:
        response = requests.get(SOURCE_EPG_URL, stream=True)
        response.raise_for_status()
        
        # Guardar el .gz temporalmente
        with open('epg.xml.gz', 'wb') as f:
            shutil.copyfileobj(response.raw, f)
        
        # Descomprimir y analizar el XML
        print("Descomprimiendo y analizando EPG...")
        with gzip.open('epg.xml.gz', 'rb') as f_in:
            xml_content = f_in.read()
            # Usamos 'recover=True' para ignorar posibles errores de parseo en el XML
            parser = etree.XMLParser(recover=True, encoding='utf-8')
            return etree.fromstring(xml_content, parser=parser)
            
    except Exception as e:
        print(f"Error al descargar o analizar el EPG: {e}")
        return None

def parse_m3u(content):
    """Analiza el contenido M3U y devuelve una lista de diccionarios de canales."""
    channels = []
    lines = content.splitlines()
    i = 0
    while i < len(lines):
        if lines[i].startswith('#EXTINF:'):
            try:
                # Extrae info de la línea #EXTINF
                info_line = lines[i]
                tvg_id = re.search(r'tvg-id="([^"]*)"', info_line, re.IGNORECASE)
                tvg_name = re.search(r'tvg-name="([^"]*)"', info_line, re.IGNORECASE)
                tvg_logo = re.search(r'tvg-logo="([^"]*)"', info_line, re.IGNORECASE)
                group_title = re.search(r'group-title="([^"]*)"', info_line, re.IGNORECASE)
                channel_name = info_line.split(',')[-1].strip()
                
                # La siguiente línea (o líneas) es la URL
                i += 1
                url = ""
                while i < len(lines) and not lines[i].startswith('#EXT'):
                    url += lines[i].strip()
                    i += 1
                
                if url:
                    channels.append({
                        'info': info_line,
                        'url': url,
                        'tvg_id': tvg_id.group(1) if tvg_id else "",
                        'tvg_name': tvg_name.group(1) if tvg_name else channel_name,
                        'tvg_logo': tvg_logo.group(1) if tvg_logo else "",
                        'group_title': group_title.group(1) if group_title else "general",
                        'name': channel_name
                    })
                    continue # El 'i' ya fue incrementado
            except Exception as e:
                print(f"Error analizando línea M3U: {lines[i]}. Error: {e}")
                
        i += 1
    return channels

# --- Lógica Principal ---

def main():
    print("Iniciando proceso de actualización de IPTV...")
    
    # --- 1. Descargar M3U de origen ---
    print(f"Descargando M3U desde {SOURCE_M3U_URL}...")
    try:
        m3u_response = requests.get(SOURCE_M3U_URL)
        m3u_response.raise_for_status()
        m3u_content = m3u_response.text
    except Exception as e:
        print(f"Error fatal: No se pudo descargar el M3U de origen. {e}")
        return

    # --- 2. Analizar M3U ---
    all_channels = parse_m3u(m3u_content)
    print(f"Total de canales encontrados en M3U: {len(all_channels)}")

    # --- 3. Comprobar estados de los streams en paralelo ---
    print(f"Comprobando estado de los streams (usando {MAX_WORKERS} hilos)...")
    live_channels = []
    live_tvg_ids = set()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Creamos un mapa de 'future' a 'channel' para saber qué canal corresponde a cada resultado
        future_to_channel = {executor.submit(check_stream_status, channel['url']): channel for channel in all_channels}
        
        for future in future_to_channel:
            channel = future_to_channel[future]
            is_live = future.result()
            
            if is_live:
                live_channels.append(channel)
                if channel['tvg_id']:
                    live_tvg_ids.add(channel['tvg_id'])

    print(f"Canales online (vivos): {len(live_channels)} / {len(all_channels)}")

    # --- 4. Descargar y analizar EPG ---
    epg_root = download_and_parse_epg()
    if epg_root is None:
        print("Error: No se pudo procesar el EPG. Se generarán archivos sin EPG.")
        # Podríamos optar por parar aquí o continuar sin EPG.
        # Por ahora, continuaremos para que al menos el M3U filtrado se genere.
    
    # --- 5. Preparar datos de EPG filtrados ---
    now = get_current_utc_time()
    limit_3h = now + timedelta(hours=3)
    limit_6h = now + timedelta(hours=6)
    
    epg_data_3h = {} # { 'tvg_id': [ {prog}, {prog} ] }
    epg_data_6h = {} # { 'tvg_id': [ {prog}, {prog} ] }
    
    filtered_epg_channels = [] # Canales EPG para el XML de 6h
    filtered_epg_programmes = [] # Programas EPG para el XML de 6h

    if epg_root is not None:
        print("Filtrando EPG...")
        
        # Filtrar canales del EPG
        for channel_elem in epg_root.findall("channel"):
            channel_id = channel_elem.get('id')
            if channel_id in live_tvg_ids:
                filtered_epg_channels.append(channel_elem)

        # Filtrar programas
        for prog_elem in epg_root.findall("programme"):
            prog_channel = prog_elem.get('channel')
            
            # Solo nos interesan programas de canales que están vivos
            if prog_channel in live_tvg_ids:
                start_time_str = prog_elem.get('start')
                stop_time_str = prog_elem.get('stop')
                
                start_time = parse_epg_datetime(start_time_str)
                stop_time = parse_epg_datetime(stop_time_str)
                
                if not start_time or not stop_time:
                    continue # Ignorar programa si las fechas son inválidas

                # Regla: No mostrar programas ya emitidos (stop_time > now)
                if stop_time > now:
                    
                    prog_data = {
                        'title': prog_elem.findtext('title'),
                        'desc': prog_elem.findtext('desc'),
                        'start': start_time.isoformat(),
                        'stop': stop_time.isoformat()
                    }
                    
                    # Regla JSON: Máximo 3 horas
                    if start_time < limit_3h:
                        if prog_channel not in epg_data_3h:
                            epg_data_3h[prog_channel] = []
                        epg_data_3h[prog_channel].append(prog_data)

                    # Regla XMLTV: Máximo 6 horas
                    if start_time < limit_6h:
                        if prog_channel not in epg_data_6h:
                            epg_data_6h[prog_channel] = []
                        # Añadimos el elemento XML original para el archivo epg.xml
                        filtered_epg_programmes.append(prog_elem)
        
        print(f"Encontrados {len(filtered_epg_channels)} canales y {len(filtered_epg_programmes)} programas en el EPG filtrado (6h).")

    # --- 6. Generar archivo M3U (playlist.m3u) ---
    print(f"Generando {OUTPUT_M3U}...")
    with open(OUTPUT_M3U, 'w', encoding='utf-8') as f:
        # Encabezado M3U. Apunta al EPG que también generaremos.
        f.write(f'#EXTM3U url-tvg="{BASE_URL}/{OUTPUT_EPG}"\n')
        for channel in live_channels:
            f.write(f"{channel['info']}\n")
            f.write(f"{channel['url']}\n")

    # --- 7. Generar archivo XMLTV (epg.xml) ---
    print(f"Generando {OUTPUT_EPG}...")
    if epg_root is not None:
        # Crear un nuevo árbol XML solo con los canales y programas filtrados (6h)
        new_epg_root = etree.Element("tv")
        
        for channel_elem in filtered_epg_channels:
            new_epg_root.append(channel_elem)
            
        for prog_elem in filtered_epg_programmes:
            new_epg_root.append(prog_elem)
            
        # Guardar el nuevo XML
        tree = etree.ElementTree(new_epg_root)
        tree.write(OUTPUT_EPG, pretty_print=True, xml_declaration=True, encoding='UTF-8')
    else:
        # Crear un XML vacío si el EPG falló
        with open(OUTPUT_EPG, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?><tv></tv>')

    # --- 8. Generar archivo JSON (playlist.json) ---
    print(f"Generando {OUTPUT_JSON}...")
    json_output = []
    for channel in live_channels:
        tvg_id = channel['tvg_id']
        json_output.append({
            'name': channel['name'],
            'url': channel['url'],
            'tvg_id': tvg_id,
            'tvg_name': channel['tvg_name'],
            'tvg_logo': channel['tvg_logo'],
            'group_title': channel['group_title'],
            # Integramos el EPG filtrado de 3 horas
            'epg': epg_data_3h.get(tvg_id, []) 
        })
        
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(json_output, f, indent=4)

    # --- 9. Generar archivo de información (info.json) ---
    print(f"Generando {OUTPUT_INFO}...")
    info_data = {
        'm3u_url': f"{BASE_URL}/{OUTPUT_M3U}",
        'json_url': f"{BASE_URL}/{OUTPUT_JSON}",
        'xmltv_url': f"{BASE_URL}/{OUTPUT_EPG}",
        'last_updated': now.isoformat()
    }
    with open(OUTPUT_INFO, 'w', encoding='utf-8') as f:
        json.dump(info_data, f, indent=4)

    print("\n¡Proceso completado exitosamente!")
    print(f" - M3U: {OUTPUT_M3U} ({len(live_channels)} canales)")
    print(f" - XMLTV: {OUTPUT_EPG}")
    print(f" - JSON: {OUTPUT_JSON}")
    print(f" - INFO: {OUTPUT_INFO}")


if __name__ == "__main__":
    main()
