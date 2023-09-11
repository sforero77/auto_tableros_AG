from flask import Flask, render_template, request, send_file
from data_process.data_proceesing_tab7 import *
from data_process.data_processing_tab1 import *
from werkzeug.utils import secure_filename
from db_utils.db_process import *
from db_utils.db_process_tab1 import *

import pandas as pd 
import os

app = Flask(__name__)

app.config['UPLOAD_FOLDER'] = 'static/archivos'  # Carpeta donde se guardarán los archivos subidos
app.config['ALLOWED_EXTENSIONS'] = {'xls', 'xlsx'}  # Extensiones permitidas

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


@app.route("/upload_tablero7", methods=["POST"])
def upload_and_process7():
    # Obtén la fecha final del formulario
    end_date = request.form['end_date']
    partes = end_date.split("-")

    # Obtener el año, mes y día como enteros
    año = int(partes[0])
    mes = int(partes[1])
    dia = int(partes[2])

    if request.method == "POST":
        file = request.files["file"]
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            path_bd_pn = './static/archivos/BD_PN_.csv'
            
            df = process_excel_file(file_path, path_bd_pn)  # Llama a la función de procesamiento           
            
            path_bd_pn = "./static/archivos/tab7/resultado_fusion.csv"
            path_bd_dane = "./static/archivos/BD_DANE.csv"

            process_db(path_bd_pn,path_bd_dane,año,mes,dia)
            
            path_periodo = './static/archivos/tab7/resultados/Periodo.csv'
            path_proy = './static/archivos/PROY.csv'
            path_pdet = './static/archivos/PDET_PLAN.csv'
            
            process_periodo(path_periodo,path_proy,path_pdet)

            path_TCP_PN = './static/archivos/tab7/resultados/TCP_PN_2022_2023.csv'
            path_proy = './static/archivos/PROY.csv'
            path_pdet = './static/archivos/PDET_PLAN.csv'
                        
            process_TCP_PN(path_TCP_PN,path_proy,path_pdet)
            
            path_Variables = './static/archivos/tab7/resultados/Variables_PN_2022_2023.csv'
            process_Variables(path_Variables)
            pass
        return render_template('tab7download.html')

@app.route("/upload_tablero1", methods=["POST"])
def upload_and_process1():
    if request.method == "POST":
        file = request.files["file"]
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            path_bd_pn = './static/archivos/BD_PN_.csv'
            df = process_excel_file_tab1(file_path, path_bd_pn)
            
            path_bd_pn = "./static/archivos/tab1/resultado_fusion.csv"
            path_bd_dane = "./static/archivos/BD_DANE.csv"

            process_db_tb1(path_bd_pn,path_bd_dane)
            
            path_CTP = './static/archivos/tab1/resultados/Tabla_1.csv'
            process_CTP_POLICIA(path_CTP)
            
            path_TCP_CAT = './static/archivos/tab1/resultados/Tabla_2.csv'
            path_CTP = './static/archivos/tab1/resultados/Tabla_1.csv'
            path_pdet = './static/archivos/PDET_PLAN.csv'
            
            process_TCP_CAT_PDET(path_TCP_CAT,path_CTP,path_pdet)
            
            path_TCP_DEP = './static/archivos/tab1/resultados/Tabla_3.csv'
            process_TCP_DEPT(path_TCP_DEP)
            
            path_Variables = './static/archivos/tab1/resultados/Tabla_5.csv'
            process_HOMICIDIOS_Variables(path_Variables)
            
            pass
        return render_template('tab1download.html')

@app.route('/tab1')
def show_page_tab1():
    return render_template('tab1.html')

@app.route('/tab1/download')
def show_download_page_tab1():
    return render_template('tab1download.html')

@app.route('/tab1/download/ctp_policia')
def download_ctp_policia():
    PATH = './static/archivos/tab1/resultados/final/CTP_POLICIA.csv'
    return send_file(PATH, as_attachment=True)

@app.route('/tab1/download/homicidios_variables')
def download_homicidios_variables():
    PATH = './static/archivos/tab1/resultados/final/HOMICIDIOS_Variables_2010_2021_POLICIA__A.csv'
    return send_file(PATH, as_attachment=True)

@app.route('/tab1/download/tcp_categoria_pdet')
def download_tcp_categoria_pdet():
    PATH = './static/archivos/tab1/resultados/final/TCP_CATEGORIA_PDET_A.csv'
    return send_file(PATH, as_attachment=True)

@app.route('/tab1/download/tcp_departamento')
def download_tcp_departamento():
    PATH = './static/archivos/tab1/resultados/final/TCP_Departamento.csv'
    return send_file(PATH, as_attachment=True)

@app.route('/tab7')
def show_page_tab7():
    return render_template('tab7.html')

@app.route('/tab7/download')
def show_download_page_tab7():
    return render_template('tab7download.html')

@app.route('/tab7/download/variables')
def download_variables():
    PATH = './static/archivos/tab7/resultados/final/Variables_process.csv'
    return send_file(PATH, as_attachment=True)

@app.route('/tab7/download/tcp')
def download_tcp():
    PATH = './static/archivos/tab7/resultados/final/TCP_PN_process.csv'
    return send_file(PATH, as_attachment=True)

@app.route('/tab7/download/periodo')
def download_periodo():
    PATH = './static/archivos/tab7/resultados/final/Periodo_process.csv'
    return send_file(PATH, as_attachment=True)


@app.route("/")
def index():
    titulo = "sebastian"
    lista = ["hola", "sebastian", 2]
    return render_template("index.html", titulo=titulo, lista = lista)

if __name__=="__main__":
    app.run(debug = True)