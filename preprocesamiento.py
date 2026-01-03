import pandas as pd
import numpy as np
import streamlit as st

def preprocesamiento(fecha_dt, df_udm, df_consolidado_historico, df_caso_analistas, df_reasignaciones, df_base_gerencias, df_ceco_activable, df_udm_colores, df_udm_colores_egresos, df_masivos, df_far_frr, df_ids_filtrar_td_dp, df_control_interinatos, df_info_discapacidad, df_egresos_excel, df_consolidado_pf):

    # Verificación de columnas esperadas ----------------------------------------------------------------------------------------------------------

    columnas_esperadas = {'df_udm': ['ID Sap', 'Rut', 'Fecha Ingreso', 'Gerencia', 'Subgerencia', 'Apellido Paterno', 'Apellido Materno', 'Nombre', 'Id Centro Costo', 'Centro Costo', 'Id Unidad', 'Unidad', 'Id Puesto', 'Puesto', 'Estamento', 'Hrs Semanales', 'Calidad Jurídica'],
                          'df_consolidado_historico': ['Hojas.Data.ID Trabajador', 'Hojas.Data.Mes', 'Hojas.Data.Clasificación Dotación', 'Hojas.Data.Gerencia', 'Hojas.Data.Puesto', 'Hojas.Data.Masivo', 'Hojas.Data.Posición', 'Hojas.Data.Jornada', 'Hojas.Data.Tipo Contrato', 'Hojas.Data.Comentario Sobredotación', 'Hojas.Data.Estamento'], 'df_caso_analistas': ['ID Trabajador','Clasificación Dotación'],
                          'df_reasignaciones': ['ID Trabajador','Gerencia de Origen','Gerencia Nueva','Tipo Reasignación','Estado','Fecha Inicio', 'Fecha de Término Inicial', 'Motivo Flexibilidad'], 'df_base_gerencias': ['Gerencia UDM', 'Gerencia'], 'df_ceco_activable': ['Centro de Costo', 'Tipo de Ceco'],'df_udm_colores': ['ID personal', 'Responsable', 'Id Posición', 'Tipo Jornada'],
                          'df_udm_colores_egresos': ['ID personal', 'Id Resp'], 'df_masivos': ['Gerencia Cargo'], 'df_far_frr': ['RUT', '%'], 'df_ids_filtrar_td_dp': ['ID Filtrar'],
                          'df_control_interinatos': ['ID Trabajador', 'Clasificación', 'Cargo que Interina'], 'df_info_discapacidad': ['ID personal'], 'df_egresos_excel': ['ID personal','Fecha de terminación'], 'df_consolidado_pf': ['ID personal', 'A quien Reemplaza', 'Cometario  CN']}
    
    dfs = {
        "df_udm": df_udm,
        "df_consolidado_historico": df_consolidado_historico,
        "df_caso_analistas": df_caso_analistas,
        "df_reasignaciones": df_reasignaciones,
        "df_base_gerencias": df_base_gerencias,
        "df_ceco_activable": df_ceco_activable,
        "df_udm_colores": df_udm_colores,
        "df_udm_colores_egresos": df_udm_colores_egresos,
        "df_masivos": df_masivos,
        "df_far_frr": df_far_frr,
        "df_ids_filtrar_td_dp": df_ids_filtrar_td_dp,
        "df_control_interinatos": df_control_interinatos,
        "df_info_discapacidad": df_info_discapacidad,
        "df_egresos_excel": df_egresos_excel,
        "df_consolidado_pf": df_consolidado_pf
    }

    nombres_de_hojas = {
        "df_udm": 'UDM General',
        "df_consolidado_historico": 'Consolidado Anterior',
        "df_caso_analistas": 'Caso Analistas',
        "df_reasignaciones": 'Reasignaciones',
        "df_base_gerencias": 'Base Gerencias',
        "df_ceco_activable": 'Ceco Activables',
        "df_udm_colores": 'UDM col - Dot. Mes',
        "df_udm_colores_egresos": 'UDM col - Dot. Egresos',
        "df_masivos": 'Masivos',
        "df_far_frr": 'FARR_FRR - Base',
        "df_ids_filtrar_td_dp": 'IDs Filtrar TD DP',
        "df_control_interinatos": 'Control Interinatos',
        "df_info_discapacidad": 'Discapacidad',
        "df_egresos_excel": 'Egresos del mes',
        "df_consolidado_pf": 'Consolidado PF - Solo CDG'
    }

    # Diccionario donde guardaremos las columnas faltantes
    columnas_faltantes = {}

    # Recorrer cada dataframe y sus columnas esperadas
    for nombre_df, columnas in columnas_esperadas.items():
        df = dfs[nombre_df]  # dataframe correspondiente
        columnas_no_encontradas = [col for col in columnas if col not in df.columns]

        if columnas_no_encontradas:
            columnas_faltantes[nombre_df] = columnas_no_encontradas

    # Si faltan columnas, mostrar el error en Streamlit
    if columnas_faltantes:
        # Formato amigable con los nombres de las hojas
        mensaje_error = "Las siguientes columnas faltan en la plantilla ingresada:\n\n"
        
        for nombre_df, columnas in columnas_faltantes.items():
            hoja = nombres_de_hojas[nombre_df]  # Obtener el nombre de la hoja
            mensaje_error += f"**{hoja}:**\n"
            mensaje_error += "   • " + " - ".join(columnas) + "\n\n"

        st.error(mensaje_error)  # Mostrar error en Streamlit
        st.stop()  # Detener la ejecución para que el usuario corrija
    else:
        st.success("✔️ Se han encontrado todas las columnas según su nombre esperado.")





    # Preprocesamiento -------------------------------------------------------------------------------------------------------------------
    # Leemos la tabla Egresos

    # Encuentra las posiciones donde aparece 'ID Sap' en la primera columna
    id_sap_indices = df_udm[df_udm.iloc[:, 0] == 'ID Sap'].index

    # Verificamos que haya al menos una aparición
    if len(id_sap_indices) > 0:
        # La primera aparición es en la posición id_sap_indices[0]
        start_index_egresos = id_sap_indices[0]

        # Ahora leemos desde la primera aparición hasta que encontremos un NaN o se termine el DataFrame
        end_index_egresos = df_udm.iloc[start_index_egresos:, 0].isna()

        # Buscamos el primer NaN después de la primera tabla
        nan_index_egresos = df_udm.iloc[start_index_egresos:, 0][end_index_egresos].index

        # Si no hay NaN, tomamos hasta el final del DataFrame
        if nan_index_egresos.empty:
            df_udm_egresos = df_udm.iloc[start_index_egresos:, :].reset_index(drop=True)
        else:
            df_udm_egresos = df_udm.iloc[start_index_egresos:nan_index_egresos[0], :].reset_index(drop=True)

        # Usamos la primera fila de df_udm_egresos como nuevos nombres de columna
        df_udm_egresos.columns = df_udm_egresos.iloc[0]
        df_udm_egresos = df_udm_egresos.drop(0).reset_index(drop=True)

        # Eliminar las columnas completamente vacías (NaN)
        df_udm_egresos = df_udm_egresos.dropna(axis=1, how='all')

        # Imprimir el resultado final
        #print(df_udm_egresos)
    else:
        print("No se encontró la aparición de 'ID Sap'.")

    df_udm_egresos = df_udm_egresos.dropna(subset=['ID Sap'])
    df_udm_egresos['ID Sap'] = df_udm_egresos['ID Sap'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    df_udm_egresos

    """df_udm_dot_actual"""

    # Generar df_udm_dot_actual a partir de df_udm en donde se debe obtener hasta que encuentre un nan

    # Obtener nombre de la primera columna
    first_col = df_udm.columns[0]

    # Ubicar el índice del primer NaN en esa columna
    idx_nan = df_udm[first_col].isna().idxmax()

    # Filtrar hasta antes del NaN
    df_udm_dot_actual = df_udm.loc[:idx_nan - 1]

    df_udm_dot_actual

    """**df_udm_general:** Concatenación de las dos tablas df_udm_dot_actual y df_udm_egresos. Pasamos los Ids a string y eliminamos los duplicados."""

    # Unir ambos dataframes en un nuevo dataframe llamado df_udm_general (uno debajo del otro)
    # Asegurar que las columnas de df_udm_egresos coincidan con las de df_udm_dot_actual
    # Rellenamos las columnas faltantes en df_udm_egresos con valores NaN
    for col in df_udm_dot_actual.columns:
        if col not in df_udm_egresos.columns:
            df_udm_egresos[col] = np.nan  # np.nan requiere import numpy as np

    # Reordenar las columnas de df_udm_egresos para que coincidan con el orden de df_udm_dot_actual
    df_udm_egresos = df_udm_egresos[df_udm_dot_actual.columns]

    # Unir los dataframes
    df_udm_general = pd.concat([df_udm_dot_actual, df_udm_egresos], ignore_index=True)

    # Identificar los IDs duplicados antes de eliminarlos
    ids_duplicados = df_udm_general[df_udm_general.duplicated(subset=['ID Sap'], keep=False)].sort_values('ID Sap')

    # Mostrar los duplicados
    print(ids_duplicados[['ID Sap']])

    # (Opcional) Ver cuántas veces aparece cada ID duplicado
    conteo_duplicados = ids_duplicados['ID Sap'].value_counts()
    print(conteo_duplicados)

    # Eliminar duplicados basados en la columna ID_SAP
    df_udm_general['ID Sap'] = df_udm_general['ID Sap'].astype(str)
    df_udm_general = df_udm_general.drop_duplicates(subset=['ID Sap'], keep='first')
    df_udm_general

    #df_udm_general.to_excel("df_udm_general.xlsx")

    """Para que este código muestre algo, habría que comentar la línea donde se eliminan los duplicados de la celda anterior."""

    ejecutar = True

    if ejecutar:
        # 6. DETECTAR DUPLICADOS ANTES DE ELIMINARLOS
        # ==============================================================================
        df_dups = df_udm_general[
            df_udm_general.duplicated(subset=["ID Sap"], keep=False)
        ].sort_values("ID Sap")

        print("\n=== IDS DUPLICADOS DETECTADOS ===")
        print(df_dups["ID Sap"])

        # Conteo por ID
        print("\n=== CUÁNTAS VECES APARECE CADA DUPLICADO ===")
        print(df_dups["ID Sap"].value_counts())

        # Obtener el conteo de duplicados por ID
        duplicate_counts = df_dups["ID Sap"].value_counts()

        # Imprimir los registros duplicados en bloques
        print("\n=== IDS DUPLICADOS DETECTADOS ===")
        for id_sap, count in duplicate_counts.items():
            # Imprimir el conjunto de registros con ese ID duplicado
            print(f"\nID Sap: {id_sap} (Aparece {count} veces)")
            print(df_dups[df_dups["ID Sap"] == id_sap])
            print("\n" + "-"*50)  # Separador entre bloques de duplicados

        # También puedes imprimir el conteo de los duplicados por ID
        print("\n=== CUÁNTAS VECES APARECE CADA DUPLICADO ===")
        print(duplicate_counts)

        # Detectar duplicados antes de eliminarlos
        df_dups = df_udm_general[
            df_udm_general.duplicated(subset=["ID Sap"], keep=False)
        ].sort_values("ID Sap")

        # Obtener el conteo de duplicados por ID
        duplicate_counts = df_dups["ID Sap"].value_counts()

        print("\n=== DIFERENCIAS ENTRE REGISTROS DUPLICADOS ===")

        # Iterar sobre cada grupo de duplicados
        for id_sap, count in duplicate_counts.items():
            # Seleccionar los registros con el mismo ID Sap
            duplicates_group = df_dups[df_dups["ID Sap"] == id_sap]

            print(f"\nID Sap: {id_sap} (Aparece {count} veces)")

            # Comparar las columnas de los duplicados
            # Crear un DataFrame de diferencias
            differences = (duplicates_group.iloc[0] != duplicates_group.iloc[1:]).any(axis=0)

            # Filtrar las columnas donde hay diferencias
            diff_columns = differences[differences].index.tolist()

            if diff_columns:
                print(f"Columnas con diferencias para el ID Sap {id_sap}: {', '.join(diff_columns)}")

                # Mostrar las diferencias por columna
                for col in diff_columns:
                    diff_values = duplicates_group[[col, 'ID Sap']].drop_duplicates()
                    print(f"\nColumna: {col}")
                    print(diff_values)
            else:
                print(f"No hay diferencias en las columnas para el ID Sap {id_sap}")

            print("\n" + "-"*50)  # Separador entre bloques de duplicados

    """df_consolidado_historico"""

    # Lectura de la data y renombre del ID
    df_consolidado_historico = df_consolidado_historico.rename(columns={'Hojas.Data.ID Trabajador': 'ID Sap'})

    # Eliminar filas sin ID
    df_consolidado_historico = df_consolidado_historico.dropna(subset=['ID Sap'])

    # Convertir a string y eliminar solo espacios iniciales y finales
    df_consolidado_historico['ID Sap'] = (
        df_consolidado_historico['ID Sap']
        .astype(str)
        .str.strip().str.replace(r'\.0$', '', regex=True)
    )

    """df_consolidado_anterior"""

    mes_anterior = (fecha_dt - pd.DateOffset(months=1)).strftime('%Y-%m')

    df_consolidado_historico['Hojas.Data.Mes'] = pd.to_datetime(
        df_consolidado_historico['Hojas.Data.Mes'], errors='coerce'
    )

    df_consolidado_anterior = df_consolidado_historico[
        df_consolidado_historico['Hojas.Data.Mes'].dt.strftime('%Y-%m') == mes_anterior
    ].copy()

    df_consolidado_anterior['ID Sap'] = df_consolidado_anterior['ID Sap'].astype(str).str.strip()

    """df_consolidado_anterior_sin_dup"""

    df_consolidado_anterior_sin_dup = (
        df_consolidado_anterior
        .drop_duplicates(subset=['ID Sap'], keep='first')
        .copy()
    )

    """df_caso_analista"""

    df_caso_analistas = df_caso_analistas.rename(columns={'ID Trabajador': 'ID Sap'})
    df_caso_analistas = df_caso_analistas.dropna(subset=['ID Sap'])
    df_caso_analistas['ID Sap'] = df_caso_analistas['ID Sap'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    df_caso_analistas

    """df_reasignaciones"""

    df_reasignaciones = df_reasignaciones.rename(columns={'ID Trabajador': 'ID Sap'})
    df_reasignaciones['ID Sap'] = df_reasignaciones['ID Sap'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_reasignaciones

    """df_base_gerencias"""
    df_base_gerencias

    """df_ceco_activable"""
    # Cambiamos el nombre de la columna "Centro de Costo a "Id Centro Costo"
    df_ceco_activable = df_ceco_activable.rename(columns={'Centro de Costo': 'Id Centro Costo'})
    df_ceco_activable

    """df_udm_colores dot del mes"""
    df_udm_colores = df_udm_colores.rename(columns={'ID personal': 'ID Sap'})
    df_udm_colores['ID Sap'] = df_udm_colores['ID Sap'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_udm_colores

    """df_udm_colores_egresos


    """

    df_udm_colores_egresos = df_udm_colores_egresos.rename(columns={'ID personal': 'ID Sap'})
    df_udm_colores_egresos['ID Sap'] = df_udm_colores_egresos['ID Sap'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_udm_colores_egresos['Id Resp'] = df_udm_colores_egresos['Id Resp'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    df_udm_colores_egresos

    """df_masivos"""
    df_masivos

    """df_far_frr"""

    # Leemo el excel
    # Asegurar que todos los ruts que terminen con k, su k sea con mayuscula
    df_far_frr['RUT'] = df_far_frr['RUT'].str.upper()

    df_far_frr

    df_ids_filtrar_td_dp

    # A partir del df_far_frr, generamos un df_far_td que agrupe la columna "Rut" y sume la columna "%"
    df_far_td = df_far_frr.groupby('RUT')['%'].sum().reset_index()

    # Renombramos la columna RUT a Rut
    df_far_td = df_far_td.rename(columns={'RUT': 'Rut'})

    # Imprimimos el resultado
    df_far_td

    """df_far_td_filtrado"""

    # =========================================================
    # FILTRAR df_far_frr SEGÚN IDs PERMITIDOS
    # =========================================================

    # Lista de IDs válidos
    ids_validos = df_ids_filtrar_td_dp["ID Filtrar"]

    # Filtramos el dataframe original
    df_far_frr_filtrado = df_far_frr[
        df_far_frr["Centro de Costo Proyecto"].isin(ids_validos)
    ]

    # =========================================================
    # AGRUPAR POR RUT Y SUMAR %
    # =========================================================

    df_far_td_filtrado = (
        df_far_frr_filtrado
            .groupby("RUT", as_index=False)["%"]
            .sum()
    )

    # =========================================================
    # RENOMBRAR COLUMNA
    # =========================================================

    df_far_td_filtrado = df_far_td_filtrado.rename(columns={"RUT": "Rut"})
    df_far_td_filtrado = df_far_td_filtrado.rename(columns={"%": "% Activación DP"})


    # =========================================================
    # RESULTADO
    # =========================================================

    df_far_td_filtrado


    """df_control_interinatos"""


    # Renombramos la columna RUT a Rut
    df_control_interinatos = df_control_interinatos.rename(columns={'ID Trabajador': 'ID Sap'})
    df_control_interinatos['ID Sap'] = df_control_interinatos['ID Sap'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    df_control_interinatos

    """df_info_discapacidad"""

    df_info_discapacidad = df_info_discapacidad.rename(columns={'ID personal': 'ID Sap'})
    df_info_discapacidad['ID Sap'] = df_info_discapacidad['ID Sap'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_info_discapacidad

    """df_egresos_excel"""

    # renombrar columna ID personal a ID Sap
    df_egresos_excel = df_egresos_excel.rename(columns={'ID personal': 'ID Sap'})
    df_egresos_excel['ID Sap'] = df_egresos_excel['ID Sap'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    df_egresos_excel

    """df_consolidado_pf"""


    # renombramos ID personal a ID Sap
    df_consolidado_pf = df_consolidado_pf.rename(columns={'ID personal': 'ID Sap'})
    df_consolidado_pf['ID Sap'] = df_consolidado_pf['ID Sap'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_consolidado_pf


   # Advertencias de ID's repetidos ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
       # Función para verificar duplicados en la columna 'ID Sap'
    def verificar_duplicados(df, nombre_df):
        if 'ID Sap' in df.columns:
            duplicados = df[df.duplicated(subset='ID Sap', keep=False)]  # Filtra filas duplicadas
            if not duplicados.empty:
                # Mostrar advertencia en Streamlit con los detalles de los duplicados
                st.warning(f"⚠️ Se han encontrado IDs duplicados en la hoja: {nombre_df}.")
                st.write(f"Los siguientes IDs de 'ID Sap' están duplicados:")
                st.write(duplicados[['ID Sap']])
                st.write("Se ha mantenido el primer id que aparece en la hoja para el procesamiento.")

    # Verificar duplicados en cada DataFrame
    verificar_duplicados(df_udm, 'UDM General')
    verificar_duplicados(df_consolidado_anterior, 'Consolidado Anterior')
    verificar_duplicados(df_caso_analistas, 'Caso Analistas')
    verificar_duplicados(df_reasignaciones, 'Reasignaciones')
    verificar_duplicados(df_base_gerencias, 'Base Gerencias')
    verificar_duplicados(df_ceco_activable, 'Ceco Activables')
    verificar_duplicados(df_udm_colores, 'UDM col - Dot. Mes')
    verificar_duplicados(df_udm_colores_egresos, 'UDM col - Dot. Egresos')
    verificar_duplicados(df_masivos, 'Masivos')
    verificar_duplicados(df_far_frr, 'FARR_FRR - Base')
    verificar_duplicados(df_ids_filtrar_td_dp, 'IDs Filtrar TD DP')
    verificar_duplicados(df_control_interinatos, 'Control Interinatos')
    verificar_duplicados(df_info_discapacidad, 'Discapacidad')
    verificar_duplicados(df_egresos_excel, 'Egresos del mes')
    verificar_duplicados(df_consolidado_pf, 'Consolidado PF - Solo CDG')

    return df_udm, df_consolidado_historico, df_caso_analistas, df_reasignaciones, df_base_gerencias, df_ceco_activable, df_udm_colores, df_udm_colores_egresos, df_masivos, df_far_frr, df_ids_filtrar_td_dp, df_control_interinatos, df_info_discapacidad, df_egresos_excel, df_consolidado_pf, df_udm_general, df_udm_egresos, df_consolidado_anterior_sin_dup, df_far_td, df_far_td_filtrado