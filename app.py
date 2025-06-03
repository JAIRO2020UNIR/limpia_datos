import os
import re
import sqlite3
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from datetime import datetime
import gc

app = Flask(__name__)
app.secret_key = "clave-secreta"

# Carpetas temporales (para Render u otros entornos que solo permiten escritura en /tmp)
UPLOAD_FOLDER = '/tmp/uploads'
CLEANED_FOLDER = '/tmp/cleaned'
RESULTADO_FOLDER = '/tmp/resultado'
DB_PATH = '/tmp/database.db'

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(CLEANED_FOLDER, exist_ok=True)
os.makedirs(RESULTADO_FOLDER, exist_ok=True)

ULTIMO_REPORTE = None

def limpiar_contenido(texto):
    texto = texto.replace(',', '.') \
                 .replace(':', '') \
                 .replace('"', '') \
                 .replace('“', '') \
                 .replace('”', '') \
                 .replace('\t', '') \
                 .strip()
    return texto

NOMBRES_VALIDOS = {'tabla_detaller', 'tabla_pedidos', 'tabla_remision'}

def nombre_tabla_valido(nombre_archivo):
    base = os.path.splitext(nombre_archivo)[0].lower()
    base = re.sub(r'\W+', '_', base).strip('_')
    return base

def cargar_csv_a_sqlite(nombre_archivo, ruta_csv, conn):
    tabla = nombre_tabla_valido(nombre_archivo)

    try:
        with open(ruta_csv, 'r', encoding='utf-8') as f:
            encabezado = f.readline().strip().split(',')
            num_columnas = len(encabezado)

        df = pd.read_csv(ruta_csv, header=0, encoding='utf-8', dtype=str, on_bad_lines='skip')

        col_vistos = {}
        nuevas_columnas = []
        for i, col in enumerate(df.columns):
            if not isinstance(col, str) or col.strip() == '':
                nuevo_nombre = f"col_{i+1}"
            else:
                nuevo_nombre = col.strip().replace(' ', '_')
            contador = col_vistos.get(nuevo_nombre, 0)
            if contador > 0:
                nuevo_nombre = f"{nuevo_nombre}_{contador}"
            col_vistos[nuevo_nombre] = contador + 1
            nuevas_columnas.append(nuevo_nombre)

        df.columns = nuevas_columnas
        df = df[df.apply(lambda x: len(x) == num_columnas, axis=1)]
        df.to_sql(tabla, conn, if_exists='replace', index=False)

    except Exception as e:
        raise Exception(f"{nombre_archivo} no se pudo procesar correctamente: {e}")

    finally:
        del df
        gc.collect()

    return tabla

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        archivos = request.files.getlist('archivos')
        if not archivos or archivos[0].filename == '':
            flash("⚠️ No se seleccionó ningún archivo.")
            return redirect(url_for('index'))

        nombres_subidos = {os.path.splitext(a.filename)[0].lower() for a in archivos}
        if not NOMBRES_VALIDOS.issubset(nombres_subidos):
            faltantes = NOMBRES_VALIDOS - nombres_subidos
            flash(f"⚠️ Faltan archivos requeridos, se deben cargar 3 archivos: {', '.join(faltantes)}")
            return redirect(url_for('index'))

        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        conn = sqlite3.connect(DB_PATH)

        for archivo in archivos:
            nombre = archivo.filename
            ruta_original = os.path.join(UPLOAD_FOLDER, nombre)
            ruta_limpia_txt = os.path.join(CLEANED_FOLDER, f"limpio_txt_{nombre}")
            ruta_final_csv = os.path.join(CLEANED_FOLDER, f"limpio_{nombre}")
            archivo.save(ruta_original)

            with open(ruta_original, 'r', encoding='utf-8', errors='ignore') as f:
                lineas = (linea for linea in f)
                lineas_limpias = []
                for linea in lineas:
                    linea_limpia = limpiar_contenido(linea)
                    if linea_limpia.strip() != '':
                        lineas_limpias.append(linea_limpia + '\n')

            with open(ruta_limpia_txt, 'w', encoding='utf-8') as f:
                f.writelines(lineas_limpias)

            try:
                df = pd.read_csv(ruta_limpia_txt, sep='|', header=None, dtype=str, on_bad_lines='skip', engine='python')
                num_cols = df.shape[1]
                df.columns = [f"col_{i+1}" for i in range(num_cols)]
                df.to_csv(ruta_final_csv, sep=',', index=False)
                del df
                gc.collect()

                cargar_csv_a_sqlite(nombre, ruta_final_csv, conn)
            except Exception as e:
                flash(f"Error cargando {nombre}: {e}")

        conn.close()
        flash("✅ Archivos limpios y cargados correctamente a SQLite.")
        return redirect(url_for('consultar'))

    return render_template("index.html")

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

        if request.method == 'POST':
            consulta_sql = request.form.get("consulta_sql", "").strip()
            if not consulta_sql:
                raise Exception("La consulta SQL está vacía.")
        else:
            consulta_sql = """
            SELECT DISTINCT Re.col_1, Re.col_20, PE.COL_2, PE.COL_20, TD.COL_6 
            FROM tabla_remision RE
            INNER JOIN tabla_pedidos PE ON (PE.col_4 = RE.col_2)
            INNER JOIN tabla_detaller TD ON TD.COL_1 = RE.col_6
            """

        resultado_completo = pd.read_sql_query(consulta_sql, conn)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        nombre_archivo = f"reporte_resultado_{timestamp}.csv"
        ruta_reporte = os.path.join(RESULTADO_FOLDER, nombre_archivo)

        resultado_completo.to_csv(ruta_reporte, index=False, encoding='utf-8')
        ULTIMO_REPORTE = ruta_reporte

        resultado_preview = resultado_completo.head(10)

    except Exception as e:
        error = f"⚠️ Error al ejecutar la consulta: {e}"

    finally:
        conn.close()

    return render_template(
        "consultar.html",
        resultado=resultado_preview,
        error=error,
        tablas=tablas,
        consulta_actual=consulta_sql or ""
    )

@app.route('/descargar')
def descargar():
    global ULTIMO_REPORTE
    if ULTIMO_REPORTE and os.path.exists(ULTIMO_REPORTE):
        return send_file(ULTIMO_REPORTE, as_attachment=True)
    else:
        flash("⚠️ No se encontró el último archivo generado para descargar.")
        return redirect(url_for('consultar'))

if __name__ == '__main__':
    app.run(debug=True)
