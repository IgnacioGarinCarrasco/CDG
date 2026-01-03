import streamlit as st
import pandas as pd
from io import BytesIO
from preprocesamiento import preprocesamiento
from procesamiento import procesamiento

st.set_page_config(layout="wide")
st.title("Automatización Consolidado CDG")

st.write("---")
st.subheader("Carga de la data")
# -------------------------
# 1. Entrada de mes y año
# -------------------------
col1, col2 = st.columns(2)
with col1:
    mes = st.selectbox("Mes", 
                       ["01","02","03","04","05","06","07","08","09","10","11","12"])
with col2:
    año = st.number_input("Año", min_value=2020, max_value=2100, step=1)

# Construir fecha estándar 01/mes/año
fecha_manual = f"01/{mes}/{año}"

# Convertir a datetime
fecha_dt = pd.to_datetime(fecha_manual, dayfirst=True)


# -------------------------
# 2. Carga de archivo
# -------------------------
archivo = st.file_uploader("Cargar archivo Excel", type=["xlsx", "xls"])

if archivo is not None:

    st.write("---")
    st.subheader("⚠️ Consideraciones")

    # Leer todas las hojas del archivo
    hojas_excel = pd.read_excel(archivo, sheet_name=None)

    # Verificar si las hojas existen en el archivo
    # Lista de hojas esperadas
    hojas_esperadas = [
        'UDM General',
        'Consolidado Anterior',
        'Caso Analistas',
        'Reasignaciones',
        'Base Gerencias',
        'Ceco Activables',
        'UDM col - Dot. Mes',
        'UDM col - Dot. Egresos',
        'Masivos',
        'FARR_FRR - Base',
        'IDs Filtrar TD DP',
        'Control Interinatos',
        'Discapacidad',
        'Egresos del mes',
        'Consolidado PF - Solo CDG'
    ]
    
    # Obtener los nombres de las hojas que están presentes en el archivo
    hojas_actuales = hojas_excel.keys()
    
    # Verificar si todas las hojas esperadas están presentes en el archivo
    hojas_faltantes = [hoja for hoja in hojas_esperadas if hoja not in hojas_actuales]
    
    # Si faltan hojas, mostrar un mensaje de error
    if hojas_faltantes:
        st.error(f"Las siguientes hojas faltan en el archivo de Excel: {', '.join(hojas_faltantes)}")
        st.stop()  # Detener la ejecución si faltan hojas
    else:
        st.success("✔️ Todas las hojas necesarias están presentes.")


    # Lectura de los Excels
    df_udm = pd.read_excel(archivo, sheet_name="UDM General", header=2)
    df_consolidado_historico = hojas_excel["Consolidado Anterior"]
    df_caso_analistas = hojas_excel["Caso Analistas"]
    df_reasignaciones = hojas_excel["Reasignaciones"]
    df_base_gerencias = hojas_excel["Base Gerencias"]
    df_ceco_activable = hojas_excel["Ceco Activables"]
    df_udm_colores = hojas_excel["UDM col - Dot. Mes"]
    df_udm_colores_egresos = hojas_excel["UDM col - Dot. Egresos"]
    df_masivos = hojas_excel["Masivos"]
    df_far_frr = hojas_excel["FARR_FRR - Base"]
    df_ids_filtrar_td_dp = hojas_excel["IDs Filtrar TD DP"]
    df_control_interinatos = hojas_excel["Control Interinatos"]
    df_info_discapacidad = hojas_excel["Discapacidad"]
    df_egresos_excel = hojas_excel["Egresos del mes"]
    df_consolidado_pf = hojas_excel["Consolidado PF - Solo CDG"]
    
    df_udm, df_consolidado_historico, df_caso_analistas, df_reasignaciones, df_base_gerencias, df_ceco_activable, df_udm_colores, df_udm_colores_egresos, df_masivos, df_far_frr, df_ids_filtrar_td_dp, df_control_interinatos, df_info_discapacidad, df_egresos_excel, df_consolidado_pf, df_udm_general, df_udm_egresos, df_consolidado_anterior_sin_dup, df_far_td, df_far_td_filtrado = preprocesamiento(fecha_dt, df_udm, df_consolidado_historico, df_caso_analistas, df_reasignaciones, df_base_gerencias, df_ceco_activable, df_udm_colores, df_udm_colores_egresos, df_masivos, df_far_frr, df_ids_filtrar_td_dp, df_control_interinatos, df_info_discapacidad, df_egresos_excel, df_consolidado_pf)

    # -------------------------
    # 5. Procesar archivo
    # -------------------------
    st.write("---")
    st.subheader("Procesamiento")
    if st.button("Procesar archivo"):
        st.success("Procesando...")

        # Procesar archivo
        df_cdg = procesamiento(fecha_dt, df_udm, df_consolidado_historico, df_caso_analistas, df_reasignaciones, df_base_gerencias, df_ceco_activable, df_udm_colores, df_udm_colores_egresos, df_masivos, df_far_frr, df_ids_filtrar_td_dp, df_control_interinatos, df_info_discapacidad, df_egresos_excel, df_consolidado_pf, df_udm_general, df_udm_egresos, df_consolidado_anterior_sin_dup, df_far_td, df_far_td_filtrado)

        # Guardar el df_cdg como archivo Excel en memoria
        output = BytesIO()  # Crea un buffer en memoria
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Escribir el df_cdg en una hoja llamada 'CDG'
            df_cdg.to_excel(writer, index=False, sheet_name="CDG")
            
            # Acceder al objeto workbook y worksheet
            workbook  = writer.book
            worksheet = writer.sheets['CDG']
            
            # Definir el formato para la primera fila (títulos)
            formato_titulo = workbook.add_format({
                'bold': True,
                'color': 'white',          # Color del texto
                'bg_color': '#4F81BD',     # Fondo azul (puedes cambiar el color)
                'border': 1,               # Borde de las celdas
                'align': 'center',         # Alinear al centro
                'valign': 'vcenter'        # Alineación vertical al centro
            })
            
            # Definir formato de la cuadrícula
            formato_cuadricula = workbook.add_format({
                'border': 1,               # Borde de las celdas
                'align': 'center',         # Alinear al centro
                'valign': 'vcenter'        # Alineación vertical al centro
            })
            
            # Aplicar el formato a la primera fila (títulos) solo si la celda no está vacía
            for col_num, value in enumerate(df_cdg.columns.values):
                if pd.notna(value):  # Solo aplicar formato si la celda no está vacía
                    worksheet.write(0, col_num, value, formato_titulo)  # Aplicar formato a la primera fila

            # Aplicar la cuadrícula para todas las celdas de la tabla (estilo básico)
            # Calcular el rango dinámicamente basado en el número de columnas
            last_column = chr(65 + df_cdg.shape[1] - 1)  # Para un máximo de 26 columnas
            worksheet.set_column(f'A:{last_column}', None, formato_cuadricula)  # Ajusta el rango según el número de columnas

        # Mover el puntero al inicio del archivo Excel en memoria
        output.seek(0)

        # Botón de descarga
        st.download_button(
            label="⬇️ Descargar archivo procesado",
            data=output,
            file_name=f"CDG_{mes}_{año}.xlsx",  # Asegúrate de definir 'año' y 'mes' antes
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


        st.markdown("---")
        st.caption("Si desea usar nuevamente el programa, presione **Ctrl + R** para reiniciar.")