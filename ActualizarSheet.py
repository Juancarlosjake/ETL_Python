import gspread #libreria para el manejo de google drive
from oauth2client.service_account import ServiceAccountCredentials #libreria para el manejo de credenciales y autenticacion
import datetime #libreia para manejo de formatos de fecha
from conexionpostgress import PostgressConecction #Clase donde se conecta a las Bd de postgress
from conexionDB2 import DB2Connection #clase donde se conecta a las Bd de IBM DB2
import pandas as pd #libreria para el manejo de datos
import json #libreria para leer el archivo Json con los querys a ejecutar

class DataUploader:
    def __init__(self,db_connection):
        self.db_connection = db_connection
    
    def execute_sql_query(self, query,default_value=0):
        try:
            #verifica si es una conexion de postgress o de SQL, dependiendo de cual sea realiza la conexion con la clase correspondiente
            if isinstance(self.db_connection, PostgressConecction):
                connection = self.db_connection.connect()
            elif isinstance(self.db_connection, DB2Connection):
                connection = self.db_connection.connect()
            else:
                raise ValueError('Tipo de conexion no valida')

            if connection: #crea la conexion y un objeto cursor para ejecutar los querys, retorna el resultado
                cursor = connection.cursor()
                cursor.execute(query)
                result = cursor.fetchall()
                cursor.close()
                return result
            
        except Exception as e: #si el query falla Agrega un 0 al valor por defecto
            print(f'Error al ejecutar consulta SQL: {e}')
            return [[default_value]]  # Retorna una lista con el valor por defecto en una lista
        finally:
            if isinstance(self.db_connection,PostgressConecction) or isinstance(self.db_connection,DB2Connection): #cierra la conexion dependiendo del tipo de clase
                self.db_connection.close()

    def read_queries_from_json(self, json_file):
        """Metodo para leer el Json en el cual estan los querys de DB2

              --Recibe la ruta del JSON

              --Return: Retorna, los querys correxpondientes de cada etiqueta JSON
        """
        with open(json_file, "r") as file:
            queries_data = json.load(file)

        Ofertas_Activas_coppel = queries_data.get("Ofertas_Activas_coppel", [])
        Escenarios_productos = queries_data.get("Escenarios_Productos",[])
        Escenarios_ofertas = queries_data.get("Escenarios_ofertas",[])
        Escenarios_Negocio = queries_data.get("Escenarios_Para_Negocio",[])

        return Ofertas_Activas_coppel,Escenarios_productos,Escenarios_ofertas,Escenarios_Negocio

    def concatenar_consultas(self, queries):
        """
            Metodo Para concatenar los resultados de los Querys y Formar la 
            Fila que se enviara al excel

            --Recibe: una lista de querys

            -- Retorna: Un dataFrame con los resultados en forma de tabla        
        """
        concatenated_data = []

        for query in queries:
            results = self.execute_sql_query(query)
            if results:
                # Agregar los resultados directamente a la lista
                concatenated_data.append(results)

        if concatenated_data:
            # Concatenar los resultados en un solo DataFrame
            concatenated_df = pd.concat([pd.DataFrame(data) for data in concatenated_data], axis=1)
            return concatenated_df
        else:
            return None  

    def upload_google_sheet(self,data,sheet):
        """
            Metodo para Subir los Datos a Google Sheet

            --Recibe: Informacion del query ejecutado

            --Retorna: un mensaje de Exito si se subieron los datos o un mensaje de error en caso de fallo       
        """
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

            #Json de la llave privada de la cuenta de servicio con la Api de Google Drive
            creds = ServiceAccountCredentials.from_json_keyfile_name("C:/Users/Carlos.romerop/Downloads/proyecto-396216-e6aaef0024ca.json", scope) 
            
             # Creación del cliente de Google
            client = gspread.authorize(creds)

            spreadsheet_id = '1grnz1tb3046XNYVauXzl0DeT7N1NEoSvlM0kK5Mf7H0'

             # Accede a la hoja que deseas actualizar
            worksheet = client.open_by_key(spreadsheet_id).worksheet(sheet) 

            df = pd.DataFrame(data)

             # Itera a través de las filas del DataFrame y actualiza la hoja
            for index, row in df.iterrows():
                list_of_values = row.tolist()
                for i,value in enumerate(list_of_values):
                    if isinstance(value,datetime.date):
                        list_of_values[i] = value.strftime('%Y/%m/%d %H:%M:%S')
                worksheet.append_row(list_of_values, value_input_option='USER_ENTERED')
                    
            print(f'Resultados cargados con exito a la Hoja: {sheet}')
        
        except Exception as e:
            print(f'Error al cargar los datos en google sheet {e}')


if __name__ == '__main__':
########Consultas para llenado de hojas de la Bd de postgreSQL TiendaVirtual#####
#################################################################################
    db = PostgressConecction(xml_file='Conf/XML_BASE.xml')

    sheetEscenarioNegocio = "ESCENARIOS DE PRODCUTOS PARA NEGOCIO (Tiendavirtual)"
    sheetTiposDePago = "TIPOS DE PAGOS"
    sheetTipoDePagoFact = "TIPOS DE PAGOS FACTURADOS"

    data_uploader = DataUploader(
        db_connection=db
    ) 

    Ofertas_Activas_coppel_querys,Escenarios_productos_query,Escenarios_ofertas_query,Escenarios_Negocio_query = data_uploader.read_queries_from_json('Conf/Querys.json')
    Escenarios_Negocio_df = data_uploader.concatenar_consultas(Escenarios_Negocio_query)

    if Escenarios_Negocio_df is not None:
        data_uploader.upload_google_sheet(Escenarios_Negocio_df,sheetEscenarioNegocio)

    query_tipos_de_pago = """select count(1) as conteo, 
                                p.desc_canalventa,ctp.tipopago, (select CURRENT_DATE-1) as fecha from 
                                ctl_pedidos P
                                inner join ctl_formadepago fp on p.idu_pedido = fp.idu_pedido 
                                inner join cat_tipopagos ctp on ctp.id = fp.num_tipopago where p.imp_marketplace > 0 
                                and p.fec_orden = (select CURRENT_DATE-1)
                                group by fp.num_tipopago, ctp.tipopago,p.desc_canalventa;"""
                                
    query_tiposDe_pago_df = data_uploader.execute_sql_query(query_tipos_de_pago)
    if query_tiposDe_pago_df:
        data_uploader.upload_google_sheet(query_tiposDe_pago_df,sheetTiposDePago)

    query_tipos_de_pagofact = """SELECT count(1),p.desc_canalventa, ctp.tipopago, CAST((mov.fec_movimiento AT TIME ZONE 'utc') AT TIME ZONE 'utc-2' AS DATE) AS DATE
                                FROM (
                                    select * from ctl_pedidos where imp_marketplace != 0 and fec_orden >= (select CURRENT_DATE-10)
                                    ) P 
                                INNER JOIN ctl_formadepago fp ON p.idu_pedido = fp.idu_pedido
                                INNER JOIN mov_estatuspedido mov ON p.num_folio = mov.num_folio and mov.idu_estatuspedido = 6 and 
                                    CAST((mov.fec_movimiento AT TIME ZONE 'utc') AT TIME ZONE 'utc-2' AS DATE) = CURRENT_DATE-1 
                                INNER JOIN cat_tipopagos ctp ON ctp.id = fp.num_tipopago
                                INNER JOIN ctl_pedidosmarketplace cp ON p.num_folio = cp.num_folio
                                INNER JOIN ctl_detallepedidosmarketplace dp ON dp.idu_ordenlogistica = cp.idu_ordenlogistica and dp.idu_factura != 0
                                GROUP BY p.desc_canalventa,fp.num_tipopago, ctp.tipopago,DATE;"""
    
    query_tiposDe_pagofact_df = data_uploader.execute_sql_query(query_tipos_de_pagofact)

    if query_tiposDe_pagofact_df:
        data_uploader.upload_google_sheet(query_tiposDe_pagofact_df,sheetTipoDePagoFact)

    ###consulta para la base de datos IBM db2###########################################
    ####################################################################################
    db2 = DB2Connection(xml_file='Conf/XML_BASE.xml')
    sheetOfertasActivasCoppel = "OFERTAS ACTIVAS COPPEL.COM"
    sheetEscenariosProducto = "ESCENARIOS PRODUCTOS"
    sheetEscenariosOfertas = "ESCENARIOS OFERTAS"

    data_uploaderdb2 = DataUploader(
        db_connection=db2
    )

    OfertasActivasCoppel_df = data_uploaderdb2.concatenar_consultas(Ofertas_Activas_coppel_querys)
    EscenariosProductos_df = data_uploaderdb2.concatenar_consultas(Escenarios_productos_query)
    EscenariosOfertas_df = data_uploaderdb2.concatenar_consultas(Escenarios_ofertas_query)

    if OfertasActivasCoppel_df is not None:
        data_uploaderdb2.upload_google_sheet(OfertasActivasCoppel_df,sheetOfertasActivasCoppel)
    
    if EscenariosProductos_df is not None:
        data_uploaderdb2.upload_google_sheet(EscenariosProductos_df,sheetEscenariosProducto)
    
    if EscenariosOfertas_df is not None:
        data_uploaderdb2.upload_google_sheet(EscenariosOfertas_df,sheetEscenariosOfertas)