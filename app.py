import os
import re
import gc
import sqlite3
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from datetime import datetime

app = Flask(__name__)
app.secret_key = "clave-secreta"

# Directorios para cargar, limpiar y guardar resultados
UPLOAD_FOLDER = 'uploads'
CLEANED_FOLDER = 'cleaned'
RESULTADO_FOLDER = 'resultado'
DB_PATH = 'database.db'

# Crear carpetas si no existen
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(CLEANED_FOLDER, exist_ok=True)
os.makedirs(RESULTADO_FOLDER, exist_ok=True)

# Ruta del último reporte generado para descarga
ULTIMO_REPORTE = None

# Archivos requeridos para que la consulta JOIN funcione
ARCHIVOS_ESPERADOS = ['tabla_detaller', 'tabla_pedidos', 'tabla_remision']

# Limpieza de texto por línea para remover caracteres problemáticos
def limpiar_contenido(texto):
    texto = texto.replace(',', '.').replace(':', '').replace('"', '')
    texto = texto.replace('“', '').replace('”', '').replace('\t', '').strip()
    return texto

# Obtener nombre de tabla SQLite a partir del nombre del archivo
def nombre_tabla_valido(nombre_archivo):
    base = os.path.splitext(nombre_archivo)[0].lower()
    base = re.sub(r'\W+', '_', base).strip('_')
    return base

# Limpiar archivo y convertirlo a CSV limpio listo para cargar
def limpiar_y_guardar(nombre, archivo):
    ruta_original = os.path.join(UPLOAD_FOLDER, nombre)
    ruta_limpia_txt = os.path.join(CLEANED_FOLDER, f"limpio_txt_{nombre}")
    ruta_final_csv = os.path.join(CLEANED_FOLDER, f"limpio_{nombre}")

    archivo.save(ruta_original)  # Guardar archivo original

    # Leer archivo línea por línea y limpiar
    with open(ruta_original, 'r', encoding='utf-8', errors='ignore') as f:
        lineas_limpias = [limpiar_contenido(linea) + '\n' for linea in f if linea.strip() != '']

    # Guardar archivo intermedio limpio en TXT
    with open(ruta_limpia_txt, 'w', encoding='utf-8') as f:
        f.writelines(lineas_limpias)

    # Convertir el TXT limpio a CSV separando por |
    try:
        df_iter = pd.read_csv(ruta_limpia_txt, sep='|', header=None, dtype=str, on_bad_lines='skip', engine='python', chunksize=10000)
        with open(ruta_final_csv, 'w', encoding='utf-8') as out:
            for i, chunk in enumerate(df_iter):
                chunk = chunk.dropna(how='all')
                num_cols = chunk.shape[1]
                chunk.columns = [f"col_{j+1}" for j in range(num_cols)]
                chunk.to_csv(out, sep=',', index=False, header=(i == 0))
        return ruta_final_csv
    except Exception as e:
        raise Exception(f"Error limpiando {nombre}: {e}")

# Cargar CSV limpio a SQLite por lotes para reducir uso de memoria
def cargar_csv_a_sqlite(nombre_archivo, ruta_csv, conn):
    tabla = nombre_tabla_valido(nombre_archivo)
    try:
        df_iter = pd.read_csv(ruta_csv, chunksize=10000, dtype=str)
        for i, chunk in enumerate(df_iter):
            # Normalizar nombres de columnas para evitar errores
            chunk.columns = [re.sub(r'\W+', '_', c).lower() for c in chunk.columns]
            chunk.to_sql(tabla, conn, if_exists='replace' if i == 0 else 'append', index=False)
        return tabla
    except Exception as e:
        raise Exception(f"{nombre_archivo} no se pudo procesar correctamente: {e}")
    finally:
        gc.collect()

# Página principal para cargar archivos uno a uno
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        archivo = request.files.get('archivo')
        if not archivo or archivo.filename == '':
            flash("⚠️ No se seleccionó ningún archivo.")
            return redirect(url_for('index'))

        nombre_base = os.path.splitext(archivo.filename)[0].lower()
        if nombre_base not in ARCHIVOS_ESPERADOS:
            flash(f"⚠️ El archivo '{archivo.filename}' no es uno de los requeridos: {', '.join(ARCHIVOS_ESPERADOS)}")
            return redirect(url_for('index'))

        # Limpiar y guardar archivo, convertirlo a CSV limpio
        ruta_final_csv = limpiar_y_guardar(archivo.filename, archivo)

        conn = sqlite3.connect(DB_PATH)
        try:
            cargar_csv_a_sqlite(archivo.filename, ruta_final_csv, conn)
            flash(f"✅ Archivo '{archivo.filename}' limpio y cargado con éxito.")
        except Exception as e:
            flash(str(e))
        finally:
            conn.close()
            gc.collect()

        return redirect(url_for('index'))

    # Mostrar archivos faltantes al usuario para obligar carga ordenada
    archivos_cargados = [f.lower().split('.')[0] for f in os.listdir(CLEANED_FOLDER)]
    faltan = [a for a in ARCHIVOS_ESPERADOS if a not in archivos_cargados]
    return render_template("index.html", faltan=faltan)

# Página de consulta con SQL libre o predefinida
@app.route('/consultar', methods=['GET', 'POST'])
def consultar():
    global ULTIMO_REPORTE
    resultado_preview = None
    error = None
    tablas = []
    consulta_sql = None

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = [fila[0] for fila in cursor.fetchall()]

        # Si usuario envió una consulta personalizada
        if request.method == 'POST':
            consulta_sql = request.form.get("consulta_sql", "").strip()
            if not consulta_sql:
                raise Exception("La consulta SQL está vacía.")
        else:
            # Consulta JOIN predefinida de las tres tablas
            consulta_sql = """
           SELECT DISTINCT 
            Re.col_1 as re_numero_remision, 
            Re.col_2 as re_numero_pedido,
            PE.COL_1 as pe_posicion, 
            PE.COL_5 as pe_cliente, 
            pe.col_6 as pe_fecha, 
            pe.col_8 as pe_fecha_referencia, 
            pe.col_9 as pe_decripcion_ref,
            pe.col_10 as pe_unidad, 
            pe.col_11 as pe_cantidad, 
            pe.col_12 as pe_costo, 
            pe.col_13 as pe_estado,
            TD.COL_6 as de_cantidad, 
            td.col_19 as td_fecha_vencimiento
            FROM tabla_remision RE
            INNER JOIN tabla_pedidos PE ON (PE.col_4 = RE.col_2)
            inner join tabla_detaller td on (td.col_1 = re.col_1);
            """

        # Ejecutar consulta SQL y guardar resultados
        resultado_completo = pd.read_sql_query(consulta_sql, conn)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        nombre_archivo = f"reporte_resultado_{timestamp}.csv"
        ruta_reporte = os.path.join(RESULTADO_FOLDER, nombre_archivo)

        resultado_completo.to_csv(ruta_reporte, index=False, encoding='utf-8')
        ULTIMO_REPORTE = ruta_reporte

        resultado_preview = resultado_completo.head(10)  # Solo primeras filas

    except Exception as e:
        error = f"⚠️ Error al ejecutar la consulta: {e}"
    finally:
        conn.close()
        gc.collect()

    return render_template(
        "consultar.html",
        resultado=resultado_preview,
        error=error,
        tablas=tablas,
        consulta_actual=consulta_sql or ""
    )

# Ruta para descargar último reporte generado
@app.route('/descargar')
def descargar():
    global ULTIMO_REPORTE
    if ULTIMO_REPORTE and os.path.exists(ULTIMO_REPORTE):
        return send_file(ULTIMO_REPORTE, as_attachment=True)
    else:
        flash("⚠️ No se encontró el último archivo generado para descargar.")
        return redirect(url_for('consultar'))

# Ejecutar la app en modo debug (solo desarrollo)
if __name__ == '__main__':
    app.run(debug=True)
