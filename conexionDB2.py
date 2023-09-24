import os #Se debe indicar la ubicación de los dll de la libreria de IBM en versiones actuales de Python
os.add_dll_directory(r'C:/Users/Carlos.romerop/AppData/Local/Programs/Python/Python311/Lib/site-packages/clidriver/bin')  

import xml.etree.ElementTree as ET #libreria para leer archivos XML
import ibm_db_dbi as dbi #Libreria para conexiones con Bases de datos IBM 

class DB2Connection:
    def __init__(self,xml_file):
        """Constructos de la clase

            --Recibe: un archivo de XML        
        """
        self.xml_file = xml_file
        self.conn = None
        self.read_credentials()
        
    
    def read_credentials(self):
        """Metodo para leer las credenciales desde el archivo XML
        
            Encuentra las credenciales mediante los Tags del XML,
            en caso de no encontrar algun tag pinta la excepcion por consola
        """
        try:
            tree = ET.parse(self.xml_file)
            root = tree.getroot()

            self.host = root.find('ibm_db2_database/hostdb2').text
            self.port = root.find('ibm_db2_database/portdb2').text
            self.database  = root.find('ibm_db2_database/database_namedb2').text
            self.user = root.find('ibm_db2_database/userdb2').text
            self.password = root.find('ibm_db2_database/passworddb2').text

        except Exception as e:
            print(f'credenciales invalidas: {str(e)}')

    def connect(self):
        """
            Metodo para realizar la conexion con la base de datos de IBM DB2

            --Retorna: una conexion establecida con las credeciales del XML        
        """
        try:
            conn_str = (
                f'DATABASE = {self.database};'
                f'HOSTNAME = {self.host};'
                f'PORT = {self.port};'
                f'UID = {self.user};'
                f'PWD = {self.password};'
            )
            self.conn = dbi.connect(conn_str,"","")
            return self.conn
        except Exception as e:
            print(f'Error al conectarse a la BD de DB2 {str(e)}')
            return None
    
    def cursor(self):
        """Creacion de un cursor para ejecutar los querys
        
            --Retorna: un cursor
        """
        return self.conn.cursor()
    
    def close(self):
        """
        Metodo para cerrar la conexion con la Base de datos de IBM DB2
        """
        if self.conn:
            self.conn.close()

