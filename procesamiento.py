import pandas as pd
import numpy as np

def procesamiento(fecha_dt, df_udm, df_consolidado_historico, df_caso_analistas, df_reasignaciones, df_base_gerencias, df_ceco_activable, df_udm_colores, df_udm_colores_egresos, df_masivos, df_far_frr, df_ids_filtrar_td_dp, df_control_interinatos, df_info_discapacidad, df_egresos_excel, df_consolidado_pf, df_udm_general, df_udm_egresos, df_consolidado_anterior_sin_dup, df_far_td, df_far_td_filtrado):

    """## Construcción cdg

    Agregamos Mes e ID Sap
    """

    # Generamos el df_cdg
    df_cdg = pd.DataFrame()
    df_cdg["ID Sap"] = df_udm_general['ID Sap'].astype(str)  # asegurar tipo

    # Agregar fecha del mes
    df_cdg['Mes'] = fecha_dt
    df_cdg = df_cdg[['Mes', 'ID Sap']]

    # Convertir mes a string
    df_cdg['Mes'] = df_cdg['Mes'].dt.strftime('%d/%m/%Y')

    """Agregamos Rut en función del ID"""

    # Realizamos el cruce (merge) entre df_cdg y df_udm_general usando 'ID Sap' como clave
    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Rut']], on='ID Sap', how='left')

    # Ahora df_cdg tiene la columna 'Rut' agregada, junto con la columna 'ID Sap'
    df_cdg

    """Agregamos columna 'Ingreso/Egreso'"""

    # Convertimos la columna 'Fecha Ingreso' a formato datetime, si no lo está
    df_udm_general['Fecha Ingreso'] = pd.to_datetime(df_udm_general['Fecha Ingreso'], errors='coerce')

    # Extraemos mes y año del objeto fecha_dt
    mes_comparar = fecha_dt.month
    anio_comparar = fecha_dt.year

    # Función para determinar el valor de la columna "Ingreso / Egreso"
    def determinar_estado(row):
        id_sap = row['ID Sap']

        # Verificamos si el ID está en df_udm_general
        if id_sap in df_udm_general['ID Sap'].values:
            ingreso_fecha = df_udm_general.loc[
                df_udm_general['ID Sap'] == id_sap, 'Fecha Ingreso'
            ].iloc[0]

            # Si coincide mes y año con la fecha de comparación
            if ingreso_fecha.month == mes_comparar and ingreso_fecha.year == anio_comparar:
                # Si también aparece en egresos => "Ingreso / Egreso"
                if id_sap in df_udm_egresos['ID Sap'].values:
                    return 'Ingreso / Egreso'
                # Si no, solo "Ingreso"
                return 'Ingreso'

        # Si está en egresos pero no cumple condición anterior
        if id_sap in df_udm_egresos['ID Sap'].values:
            return 'Egreso'

        # Caso por defecto: devolvemos el RUT
        rut_trabajador = df_udm_general.loc[
            df_udm_general['ID Sap'] == id_sap, 'Rut'
        ].iloc[0]
        return rut_trabajador

    # Aplicamos la función
    df_cdg['Ingreso / Egreso'] = df_cdg.apply(determinar_estado, axis=1)

    """**Agregamos Clasificación Dotación:**

    1. Se trae caso análistas
    2. Los ids restantes los llama a df_consolidado_anterior_sin_dup.
    3. Se reclasifican los sobredotación y los pf sobredotación a real pf dotación respectivamente.
    """

    # === 1. MERGE con df_caso_analistas ===
    df_cdg = df_cdg.merge(
        df_caso_analistas[['ID Sap', 'Clasificación Dotación']],
        on='ID Sap',
        how='left',
        suffixes=('', '_caso')
    )

    # Si existiera nombre distinto en df_caso_analistas, reemplaza arriba.
    # Por ejemplo: ['ID Sap', 'Clasificación']

    # === 2. COMPLETAR VACÍOS con el consolidado anterior ===
    df_cdg = df_cdg.merge(
        df_consolidado_anterior_sin_dup[['ID Sap', 'Hojas.Data.Clasificación Dotación']],
        on='ID Sap',
        how='left'
    )

    # Llenar valores faltantes
    df_cdg['Clasificación Dotación'] = df_cdg['Clasificación Dotación'].fillna(
        df_cdg['Hojas.Data.Clasificación Dotación']
    )

    # Eliminar la columna auxiliar
    df_cdg.drop(columns=['Hojas.Data.Clasificación Dotación'], inplace=True)

    # === 3. APLICAR REGLAS DE NEGOCIO ===
    # Preguntas PF sobredotación
    df_cdg['Clasificación Dotación'] = df_cdg['Clasificación Dotación'].replace({
        'Sobredotación': 'Real'
    })

    # Asegurar que cada primera letra de cada palabra sea en mayuscula
    def capitalizar_primera_letra(texto):
        if pd.isna(texto):
            return texto
        palabras = texto.split()
        resultado = []
        for p in palabras:
            if len(p) > 1:
                resultado.append(p[0].upper() + p[1:])
            else:
                resultado.append(p.upper())
        return " ".join(resultado)

    df_cdg['Clasificación Dotación'] = df_cdg['Clasificación Dotación'].apply(capitalizar_primera_letra)

    """**Agregamos Gerencia UDM**

    1. Cruce simple con df_udm_general
    """

    # Agregamos la columnas "Gerencia" al df_cdg mediante el cruce de ID Sap en df_udm_general
    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Gerencia']], on='ID Sap', how='left')
    df_cdg

    # Cambiaremos el nombre de la columna "Gerencia" a "Gerencia UDM"
    df_cdg = df_cdg.rename(columns={'Gerencia': 'Gerencia UDM'})

    df_cdg

    """**Gerencia:**

    1. Cruce simple trayendomme Gerencia Nueva
    2. Demás celdas vacías tienen el mismo nombre que Gerencia UDM.
    3. Reemplazamos los nombres con su abreviatura desde df_base_gerencia.

    consideraciones adicionales: estoy usando la columna B para reemplazar el nombre de la gerencia.
    """

    # Cruzamos ID Sap

    df_reasignaciones_cruce = df_reasignaciones[['ID Sap', 'Gerencia de Origen']]

    df_cdg = df_cdg.merge(
        df_reasignaciones_cruce[['ID Sap', 'Gerencia de Origen']],
        on='ID Sap',
        how='left'
    )

    # Cambiamos el nombre de Gerencia Nueva por Gerencia
    df_cdg = df_cdg.rename(columns={'Gerencia de Origen': 'Gerencia'})

    # Las demás celdas vacias tendrán el mismo valor que la columna "Gerencia UDM"
    df_cdg['Gerencia'] = df_cdg['Gerencia'].fillna(df_cdg['Gerencia UDM'])

    # Reemplazamos los nombres con su abreviatura desde df_base_gerencia.
    df_cdg['Gerencia'] = df_cdg['Gerencia'].replace(df_base_gerencias.set_index('Gerencia UDM')['Gerencia'])

    df_cdg

    """**Gerencia**

    Modificación hecha
    """



    """**Gerencia Mes Anterior**

    1.	Se trae Hojas.Data.Gerencia desde cdg mes anterior mediante ID Sap.
    2.	Las celdas vacías restantes se les rellena con “S/Gerencia Mes Anterior”.
    """

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_consolidado_anterior_sin_dup[['ID Sap', 'Hojas.Data.Gerencia']].rename(columns={'Hojas.Data.Gerencia': 'Gerencia Mes Anterior'}),
        on='ID Sap',
        how='left'
    )

    # Marcamos trabajadores nuevos
    df_cdg['Gerencia Mes Anterior'] = df_cdg['Gerencia Mes Anterior'].fillna('S/Gerencia Mes Anterior')

    df_cdg

    """**Check Gerencia**"""

    df_cdg["Check Gerencia"] = df_cdg.apply(
        lambda x:
            "" if x["Gerencia Mes Anterior"] == "S/Gerencia Mes Anterior"
            else ("VERDADERO" if x["Gerencia Mes Anterior"] == x["Gerencia"] else "FALSO"),
        axis=1
    )

    df_cdg

    """**Subgerencia**"""

    # Agregamos la columnas "Subgerencia" al df_cdg mediante el cruce de ID Sap en df_udm_general
    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Subgerencia']], on='ID Sap', how='left')

    """**Apellido Paterno**"""

    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Apellido Paterno']], on='ID Sap', how='left')

    """**Apellido Materno**"""

    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Apellido Materno']], on='ID Sap', how='left')

    """**Nombre**"""

    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Nombre']], on='ID Sap', how='left')

    """**Centro de Costo**"""

    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Id Centro Costo']], on='ID Sap', how='left')

    df_cdg

    """**Tipo de Ceco**"""

    # Nos traemo Tipo de Ceco mediante cruce simple entre la columna "Id Centro Costo" de df_ceco_actible y del mismo df_cdg
    df_cdg = df_cdg.merge(df_ceco_activable[['Id Centro Costo', 'Tipo de Ceco']], on='Id Centro Costo', how='left')

    # Los NaN se rellenan con la palabra "Gasto"
    df_cdg['Tipo de Ceco'] = df_cdg['Tipo de Ceco'].fillna('Gasto')

    df_cdg

    """**Nombre CCO**"""

    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Centro Costo']], on='ID Sap', how='left')

    """**Unidad**"""

    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Id Unidad']], on='ID Sap', how='left')

    """**Nombre Unidad**"""

    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Unidad']], on='ID Sap', how='left')

    """**Fecha Ingreso**"""

    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Fecha Ingreso']], on='ID Sap', how='left')

    """**Id Puesto**"""

    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Id Puesto']], on='ID Sap', how='left')

    """**Puesto**"""

    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Puesto']], on='ID Sap', how='left')

    """**Puesto Anterior**"""

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_consolidado_anterior_sin_dup[['ID Sap', 'Hojas.Data.Puesto']].rename(columns={'Hojas.Data.Puesto': 'Puesto Anterior'}),
        on='ID Sap',
        how='left'
    )

    # Marcamos trabajadores nuevos
    df_cdg['Puesto Anterior'] = df_cdg['Puesto Anterior'].fillna('S/Puesto Mes Anterior')

    """**Check Puesto**"""

    df_cdg["Check Puesto"] = df_cdg.apply(
        lambda x:
            "" if x["Puesto Anterior"] == "S/Puesto Anterior"
            else ("VERDADERO" if x["Puesto Anterior"] == x["Puesto"] else "FALSO"),
        axis=1
    )

    df_cdg

    """**ID Responsable**"""

    df_udm_colores

    """O esta"""

    df_udm_colores_egresos.columns

    # Crear diccionarios para mapear
    map_egresos = df_udm_colores_egresos.set_index('ID Sap')['Id Resp'].to_dict()
    map_dotacion = df_udm_colores.set_index('ID Sap')['Responsable'].to_dict()

    # Traer los datos desde cada origen sin modificar df_cdg
    df_cdg['ID Resp_temp'] = df_cdg['ID Sap'].map(map_egresos)
    df_cdg['Responsable_temp'] = df_cdg['ID Sap'].map(map_dotacion)

    # Aplicar la lógica de la fórmula Excel
    df_cdg['ID Responsable'] = np.where(
        df_cdg['Ingreso / Egreso'].isin(['Egreso', 'Ingreso/Egreso']),
        df_cdg['ID Resp_temp'],         # si es egreso o ingreso/egreso
        df_cdg['Responsable_temp']      # si no lo es
    )

    # Eliminar columnas temporales
    df_cdg.drop(columns=['ID Resp_temp', 'Responsable_temp'], inplace=True)

    df_cdg

    """**Responsable**

    El nombre se coloca inicialmente por df_udm_colores pero yo lo hago con el df_udm_general
    """

    # Generamos un diccionario que tenga como llave el ID Sap y como valor la concatenacion "Nombre" + " " + "Apellido Paterno"a partir del df_udm_general
    diccionario_id_nombre = dict(zip(df_udm_general['ID Sap'], df_udm_general['Apellido Paterno'] + ' ' + df_udm_general['Nombre']))
    print(diccionario_id_nombre)

    # Ahora agregamos una columna al df_cdg llamada Responsable que muestre el nombre bajo la lógica del cruce del id del diccionario_id_nombre e ID Responsable de df_cdg
    df_cdg['Responsable'] = df_cdg['ID Responsable'].map(diccionario_id_nombre)
    df_cdg

    # ================================
    # 1. Crear claves limpias auxiliares (sin modificar columnas reales)
    # ================================

    df_udm_general['_key_clean'] = df_udm_general['ID Sap'].astype(str).str.strip()
    df_cdg['_key_clean_resp']   = df_cdg['ID Responsable'].astype(str).str.strip()


    # ================================
    # 2. Crear diccionario ID → Nombre Completo usando clave limpia
    # ================================

    diccionario_id_nombre = dict(
        zip(
            df_udm_general['_key_clean'],
            df_udm_general['Apellido Paterno'] + ' ' + df_udm_general['Nombre']
        )
    )


    # ================================
    # 3. Mapear nombre del responsable usando clave limpia
    # ================================

    df_cdg['Responsable'] = df_cdg['_key_clean_resp'].map(diccionario_id_nombre)


    # ================================
    # 4. Reestablecer NO_MANAGER como nombre del responsable
    # ================================

    df_cdg.loc[df_cdg['ID Responsable'] == 'NO_MANAGER', 'Responsable'] = ''


    # ================================
    # 5. Eliminar columnas auxiliares
    # ================================

    df_cdg.drop(columns=['_key_clean_resp'], inplace=True)
    df_udm_general.drop(columns=['_key_clean'], inplace=True)

    """**Estamento**"""

    # Cruce simple mediante el ID Sap de las tablas df_udm_general y df_cdg, trayendonos desde df_udm_general la columna Estamento
    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Estamento']], on='ID Sap', how='left')
    df_cdg

    """**Masivo**"""

    # Consturimos un diccionario desde df_masivo
    diccionario_puesto_masivo = dict(zip(df_masivos['Gerencia Cargo'], df_masivos['Masivo']))
    print(diccionario_puesto_masivo)

    # Hacer un cruce mediante la llave "Gerencia" y "Puesto" del df_cdg y las llaves que coincidin con la llave diccionario, poner le valor del diccionario de la llave asociada
    df_cdg['Masivo'] = df_cdg['Gerencia'] + '' + df_cdg['Puesto']
    df_cdg['Masivo'] = df_cdg['Masivo'].map(diccionario_puesto_masivo)
    df_cdg

    """**Masivo Mes anterior**"""

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_consolidado_anterior_sin_dup[['ID Sap', 'Hojas.Data.Masivo']].rename(columns={'Hojas.Data.Puesto': 'Puesto Anterior'}),
        on='ID Sap',
        how='left'
    )

    # Marcamos trabajadores nuevos
    #df_cdg['Puesto Anterior'] = df_cdg['Puesto Anterior'].fillna('S/Puesto Mes Anterior')

    # renombramos la columna de Masivo a Masivo Mes Anterior
    df_cdg = df_cdg.rename(columns={'Hojas.Data.Masivo': 'Masivo Mes Anterior'})

    df_cdg

    """**Check Masivo**"""

    # Si Masivo es igual a Masivo Mes Anterior en el cdg imprimir "VERDADERO", de lo contarrio "FALSO"
    df_cdg["Check Masivo"] = df_cdg.apply(
        lambda x: "VERDADERO" if x["Masivo"] == x["Masivo Mes Anterior"] else "FALSO",
        axis=1
    )

    df_cdg

    """**Posición**"""

    # --- 1. Prepara los diccionarios de búsqueda para evitar merges grandes ---

    dict_consolidado = (
        df_consolidado_anterior_sin_dup
        .set_index("ID Sap")["Hojas.Data.Posición"]
        .to_dict()
    )

    dict_udm = (
        df_udm_colores
        .set_index("ID Sap")["Id Posición"]
        .to_dict()
    )

    # --- 2. Aplica la misma lógica que Excel ---

    def obtener_posicion(row):
        idsap = row["ID Sap"]
        estado = row["Ingreso / Egreso"]

        if estado in ["Egreso", "Ingreso/Egreso"]:
            return dict_consolidado.get(idsap, np.nan)
        else:
            return dict_udm.get(idsap, np.nan)

    df_cdg["Posición"] = df_cdg.apply(obtener_posicion, axis=1)

    df_cdg

    """**Jornada**"""

    # --- 1. Prepara los diccionarios de búsqueda para evitar merges grandes ---

    dict_consolidado = (
        df_consolidado_anterior_sin_dup
        .set_index("ID Sap")["Hojas.Data.Jornada"]
        .to_dict()
    )

    dict_udm = (
        df_udm_colores
        .set_index("ID Sap")["Tipo Jornada"]
        .to_dict()
    )

    # --- 2. Aplica la misma lógica que Excel ---

    def obtener_posicion(row):
        idsap = row["ID Sap"]
        estado = row["Ingreso / Egreso"]

        if estado in ["Egreso", "Ingreso/Egreso"]:
            return dict_consolidado.get(idsap, np.nan)
        else:
            return dict_udm.get(idsap, np.nan)

    df_cdg["Jornada"] = df_cdg.apply(obtener_posicion, axis=1)

    """**Hrs Semanales**"""

    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Hrs Semanales']], on='ID Sap', how='left')

    """**Tipo Contrato**"""

    df_cdg = df_cdg.merge(df_udm_general[['ID Sap', 'Calidad Jurídica']], on='ID Sap', how='left')

    # renombramos la columna calidad juridica a "Tipo Contrato"
    df_cdg = df_cdg.rename(columns={'Calidad Jurídica': 'Tipo Contrato'})

    df_cdg

    """**Tipo Contrato Mes Anterior**"""

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_consolidado_anterior_sin_dup[['ID Sap', 'Hojas.Data.Tipo Contrato']],
        on='ID Sap',
        how='left'
    )

    # Marcamos trabajadores nuevos
    #df_cdg['Puesto Anterior'] = df_cdg['Puesto Anterior'].fillna('S/Puesto Mes Anterior')

    # renombramos la columna de Masivo a Masivo Mes Anterior
    df_cdg = df_cdg.rename(columns={'Hojas.Data.Tipo Contrato': 'Tipo Contrato Mes Anterior'})

    # Las columnas vacías se rellenan con 0
    df_cdg['Tipo Contrato Mes Anterior'] = df_cdg['Tipo Contrato Mes Anterior'].fillna(0)

    df_cdg

    """**Check Tipo Contrato**"""

    # Si Masivo es igual a Masivo Mes Anterior en el cdg imprimir "VERDADERO", de lo contarrio "FALSO"
    df_cdg["Check Tipo Contrato"] = df_cdg.apply(
        lambda x: "VERDADERO" if x["Tipo Contrato"] == x["Tipo Contrato Mes Anterior"] else "FALSO",
        axis=1
    )

    """**PF Reemplazo**"""

    # Si el valor de la columna "Clasificación Dotación" es "PF Reemplazo" del df_cdg entonces se le asigna 1 a la celda y de lo contrario 0
    df_cdg["PF Reemplazo"] = df_cdg["Clasificación Dotación"].apply(lambda x: 1 if x == "PF Reemplazo" else 0)

    df_cdg[df_cdg['ID Sap']=='10009060']

    """**PF Dotación**"""

    df_cdg["PF Dotación"] = df_cdg["Clasificación Dotación"].apply(lambda x: 1 if x == "PF Dotación" else 0)

    """**Sobredotación aprobada**"""

    df_cdg["Sobredotación Aprobada"] = df_cdg["Clasificación Dotación"].apply(lambda x: 1 if x == "Sobredotación Aprobada" else 0)

    """**Comentario Sobredotación**"""

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_consolidado_anterior_sin_dup[['ID Sap', 'Hojas.Data.Comentario Sobredotación']],
        on='ID Sap',
        how='left'
    )

    # Marcamos trabajadores nuevos
    #df_cdg['Puesto Anterior'] = df_cdg['Puesto Anterior'].fillna('S/Puesto Mes Anterior')

    # renombramos la columna de Masivo a Masivo Mes Anterior
    df_cdg = df_cdg.rename(columns={'Hojas.Data.Comentario Sobredotación': 'Comentario Sobredotación'})

    """**Activable / No Activable**

    Primero se crea y en celdas posteriores se desarrolla la lógica. Esto se hace para que la columna esté en la posición correcta.
    """

    df_cdg["Activable / No Activable"] = np.nan

    """**Matricial**"""

    df_cdg["Matricial"] = 0

    """**% Activación**"""

    df_cdg["% Activación"] = np.nan

    """**FARR**"""

    df_far_td[df_far_td['Rut']=='13444908-K']

    df_cdg[df_cdg['Rut']=='13444908-K']

    # Hacemos el cruce mediante la columna Rut de df_cdg y df_far_td trayendonos "%" del df_far_td a df_cdg
    df_cdg = df_cdg.merge(df_far_td[['Rut', '%']], on='Rut', how='left')

    # Los NaN los rellenamos con 0
    df_cdg['%'] = df_cdg['%'].fillna(0)

    # Renombramos la columna "%" a "FARR"
    df_cdg = df_cdg.rename(columns={'%': 'FARR'})
    df_cdg

    """**Aplicamos la lógica a las columnas creadas Activable / No Activable y % de Activación**

    % Activación
    """

    # Cuando la columna Tipo de Ceco es "Activable", se pone 1, de lo contrario se coloca el valor de la columna "FARR"
    df_cdg['% Activación'] = np.where(
        df_cdg['Tipo de Ceco'] == 'Activable',
        1,
        df_cdg['FARR']
    )

    df_cdg

    """**Activable / No Activable**"""

    # Es 1 cuando "% Activación" es mayor a 0.009, de lo contrario, 0
    df_cdg['Activable / No Activable'] = np.where(
        df_cdg['% Activación'] > 0.009,
        1,
        0
    )

    """**% Activación DP**

    cómo se cuáles ids se suman y cuáles no?
    """

    # =========================================================
    # MERGE df_cdg CON df_far_td_filtrado
    # =========================================================

    df_cdg = df_cdg.merge(
        df_far_td_filtrado,
        on="Rut",
        how="left"
    )

    # =========================================================
    # SI LA COLUMNA NO EXISTE, CREARLA
    # =========================================================

    if "% Activación DP" not in df_cdg.columns:
        df_cdg["% Activación DP"] = 0.0

    # =========================================================
    # REEMPLAZAR NaN POR 0.0 Y ASEGURAR FLOAT
    # =========================================================

    df_cdg["% Activación DP"] = (
        df_cdg["% Activación DP"]
            .fillna(0.0)
            .astype(float)
    )

    # =========================================================
    # RESULTADO
    # =========================================================

    df_cdg

    """**Gerencia reasignación**"""

    df_reasignaciones

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_reasignaciones[['ID Sap', 'Gerencia Nueva']],
        on='ID Sap',
        how='left'
    )

    # renombramos la columna de Masivo a Masivo Mes Anterior
    df_cdg = df_cdg.rename(columns={'Gerencia Nueva': 'Gerencia Reasignación'})

    # Las columnas vacías se rellenan con 0
    df_cdg['Gerencia Reasignación'] = df_cdg['Gerencia Reasignación'].fillna("")

    df_cdg

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_reasignaciones[['ID Sap', 'Tipo Reasignación']],
        on='ID Sap',
        how='left'
    )

    # Las columnas vacías se rellenan con 0
    #df_cdg['Tipo Reasignación'] = df_cdg['Tipo Reasignación'].fillna("")

    df_cdg

    """**Estado Reasignación**"""

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_reasignaciones[['ID Sap', 'Estado']],
        on='ID Sap',
        how='left'
    )

    # renombramos la columna de Masivo a Masivo Mes Anterior
    df_cdg = df_cdg.rename(columns={'Estado': 'Estado Reasignación'})

    # Las columnas vacías se rellenan con 0
    #df_cdg['Estado Reasignación'] = df_cdg['Estado Reasignación'].fillna("")

    df_cdg

    """**Fecha inicio**"""

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_reasignaciones[['ID Sap', 'Fecha Inicio']],
        on='ID Sap',
        how='left'
    )

    # renombramos la columna de Masivo a Masivo Mes Anterior
    df_cdg = df_cdg.rename(columns={'Fecha Inicio': 'Fecha inicio'})

    # Las columnas vacías se rellenan con 0
    #df_cdg['Fecha inicio'] = df_cdg['Fecha inicio'].fillna("")

    df_cdg

    """**Fecha de Termino**

    Consultar esta formula pq el si hace lo mismo

    =SI.ERROR(SI(BUSCARV(B4;Reasignaciones!B:AD;18;0)>=FECHA(2025;6;30);BUSCARV(B4;Reasignaciones!B:AF;18;0);BUSCARV(B4;Reasignaciones!B:AF;18;0));"")
    """

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_reasignaciones[['ID Sap', 'Fecha de Término Inicial']],
        on='ID Sap',
        how='left'
    )

    # renombramos la columna de Masivo a Masivo Mes Anterior
    df_cdg = df_cdg.rename(columns={'Fecha de Término Inicial': 'Fecha de Termino'})

    # Las columnas vacías se rellenan con 0
    #df_cdg['Fecha de Termino'] = df_cdg['Fecha de Termino'].fillna("")

    df_cdg

    """**Interinato/reemplazo**"""

    df_control_interinatos

    # --- 1. Hacer el merge para traer la columna "Clasificación" ---
    df_cdg = df_cdg.merge(
        df_control_interinatos[['ID Sap', 'Clasificación']],
        on='ID Sap',
        how='left'
    )

    # --- 2. Replicar la lógica del SI + O + SI.ERROR ---
    def clasificacion_flag(x):
        if pd.isna(x):          # Equivalente al SI.ERROR → no encontrado
            return 0
        elif x in ["Reemplazo", "Interinato", "Interinato ", "Reemplazo "]:
            return 1
        else:
            return ""

    # renombramos la columna "Clasificación"
    df_cdg = df_cdg.rename(columns={'Clasificación': 'Interinato/reemplazo'})

    df_cdg["Interinato/reemplazo"] = df_cdg["Interinato/reemplazo"].apply(clasificacion_flag)

    """**puesto al que interina**"""

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_control_interinatos[['ID Sap', 'Cargo que Interina']],
        on='ID Sap',
        how='left'
    )

    # renombramos la columna de Masivo a Masivo Mes Anterior
    df_cdg = df_cdg.rename(columns={'Cargo que Interina': 'puesto al que interina'})

    # Las columnas vacías se rellenan con 0
    #df_cdg['puesto al que interina'] = df_cdg['puesto al que interina'].fillna("")

    df_cdg

    """**Niveles de crecimiento**"""

    df_cdg["Niveles de crecimiento"] = np.nan

    """**% Incremento SB**"""

    df_cdg["% Incremento SB"] = np.nan

    """**% Incremento Líq. Mensual**"""

    df_cdg["% Incremento Líq. Mensual"] = np.nan

    """**% Incremento Líq. Anualizado**"""

    df_cdg["% Incremento Líq. Anualizado"] = np.nan

    """**Inclusión**"""

    # Si el ID Sap de de df_info_discapacidad está también en df_cdg se pone 1 en la columna del df_cdg a crear llamada "Inclusión"
    df_cdg["Inclusión"] = df_cdg["ID Sap"].isin(df_info_discapacidad["ID Sap"]).astype(int)

    """**Egresos**"""

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_egresos_excel[['ID Sap', 'Fecha de terminación']],
        on='ID Sap',
        how='left'
    )

    # renombramos la columna de Masivo a Masivo Mes Anterior
    df_cdg = df_cdg.rename(columns={'Fecha de terminación': 'Egresos'})

    # Las columnas vacías se rellenan con 0
    #df_cdg['Egresos'] = df_cdg['Egresos'].fillna("")

    df_cdg

    """**Gerencia Origen**"""

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_reasignaciones[['ID Sap', 'Gerencia de Origen']],
        on='ID Sap',
        how='left'
    )

    # renombramos la columna de Masivo a Masivo Mes Anterior
    #df_cdg = df_cdg.rename(columns={'Fecha de terminación': 'Egresos'})

    # Las columnas vacías se rellenan con 0
    df_cdg['Gerencia de Origen'] = df_cdg['Gerencia de Origen'].fillna("No Aplica")

    df_cdg

    """**Nivel**"""

    df_cdg['Nivel'] = np.nan

    """**A quien reemplaza PF**

    Al parecer acá, tenemos datos duplicados.
    """

    # Ver los duplicados de df_consolidado_pf de la columna ID Sap
    df_consolidado_pf[df_consolidado_pf.duplicated(subset=['ID Sap'], keep=False)]

    # Comprobar si los valores de las celdas de cada id duplicado son los mismo:

    # Ver los duplicados en la columna 'ID Sap'
    duplicados = df_consolidado_pf[df_consolidado_pf.duplicated(subset=['ID Sap'], keep=False)]

    # Comprobar si las celdas para cada ID duplicado son iguales
    # Agrupar por 'ID Sap' y comparar si todas las demás columnas son iguales
    # Se asumirá que queremos verificar todas las columnas excepto 'ID Sap'

    cols_a_comparar = df_consolidado_pf.columns.difference(['ID Sap'])  # Las columnas a comparar

    # Verificar si los valores en todas las demás columnas son iguales dentro de cada ID duplicado
    valores_iguales = duplicados.groupby('ID Sap')[cols_a_comparar].nunique().eq(1).all(axis=1)

    # Mostrar los resultados
    valores_iguales[valores_iguales == False]

    # Mantenemos solo el primer ID duplicado:

    df_consolidado_pf = df_consolidado_pf.drop_duplicates(subset=['ID Sap'], keep='first')

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_consolidado_pf[['ID Sap', 'A quien Reemplaza']],
        on='ID Sap',
        how='left'
    )

    # renombramos la columna de Masivo a Masivo Mes Anterior
    df_cdg = df_cdg.rename(columns={'A quien Reemplaza': 'A quien Reemplaza PF'})

    # Las columnas vacías se rellenan con 0
    df_cdg['A quien Reemplaza PF'] = df_cdg['A quien Reemplaza PF'].fillna("No Aplica")

    df_cdg

    """**Justificación PF**

    ojo acá con cometario (está mal escrito y tiene muchos espacios)
    """

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_consolidado_pf[['ID Sap', 'Cometario  CN']],
        on='ID Sap',
        how='left'
    )

    # renombramos la columna de Masivo a Masivo Mes Anterior
    df_cdg = df_cdg.rename(columns={'Cometario  CN': 'Justificación PF'})

    # Las columnas vacías se rellenan con 0
    df_cdg['Justificación PF'] = df_cdg['Justificación PF'].fillna("No Aplica")

    df_cdg

    """**Motivo Flexibilidad**"""

    # Realizamos el merge
    df_cdg = df_cdg.merge(
        df_reasignaciones[['ID Sap', 'Motivo Flexibilidad']],
        on='ID Sap',
        how='left'
    )

    # renombramos la columna de Masivo a Masivo Mes Anterior
    #df_cdg = df_cdg.rename(columns={'Fecha de terminación': 'Egresos'})

    # Las columnas vacías se rellenan con 0
    #df_cdg['Motivo Flexibilidad'] = df_cdg['Motivo Flexibilidad'].fillna("")

    df_cdg

    """**MASIVO**"""

    df_cdg.columns

    df_consolidado_anterior_sin_dup.columns

    GRUPOS_VALIDOS = ['GOS', 'GSEG', 'GMAN']

    for idx, row in df_cdg.iterrows():

        if row["Gerencia"] in GRUPOS_VALIDOS:

            # Caso 1: REASIGNADO
            if row["Estado Reasignación"] == "REASIGNADO":

                mask = df_consolidado_anterior_sin_dup["ID Sap"] == row["ID Sap"]
                if mask.any():
                    df_cdg.at[idx, "Masivo"] = df_consolidado_anterior_sin_dup.loc[
                        mask, "Hojas.Data.Masivo"
                    ].iloc[0]
                else:
                    df_cdg.at[idx, "Masivo"] = np.nan

            # Caso 2: Interno en práctica
            elif row["Puesto"] == "Interno en Práctica" or row["Puesto"] == "Interno en práctica":

                mask = df_consolidado_anterior_sin_dup["ID Sap"] == row["ID Sap"]
                if mask.any():
                    df_cdg.at[idx, "Masivo"] = df_consolidado_anterior_sin_dup.loc[
                        mask, "Hojas.Data.Masivo"
                    ].iloc[0]
                else:
                    df_cdg.at[idx, "Masivo"] = np.nan

            # Caso 3: lookup en Masivos
            else:
                clave = row["Gerencia"] + row["Puesto"]
                mask = df_masivos["Gerencia Cargo"] == clave

                if mask.any():
                    df_cdg.at[idx, "Masivo"] = df_masivos.loc[mask, "Masivo"].iloc[0]
                else:
                    df_cdg.at[idx, "Masivo"] = np.nan

        else:
            df_cdg.at[idx, "Masivo"] = np.nan

    """**Algunos Cambios generales**"""

    # Convertimos 'ID Sap' e 'ID Responsable' a formato numerico del df_cdg
    df_cdg['ID Sap'] = pd.to_numeric(df_cdg['ID Sap'], errors='coerce')
    df_cdg['ID Responsable'] = df_cdg['ID Responsable'].apply(
        lambda x: pd.to_numeric(x, errors='coerce') if str(x).isdigit() else x
    )

    # Cambiamos los nombres de las columnas para que sea identico a lo que tiene en los consolidados de compensaciones
    df_cdg = df_cdg.rename(columns={'ID Sap': 'ID Trabajador', 'Id Centro Costo': 'Centro de Costo', 'Centro Costo': 'Nombre CCO', 'Id Unidad': 'Unidad', 'Unidad': 'Nombre Unidad'})


    return df_cdg