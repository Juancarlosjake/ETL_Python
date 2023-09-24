import psycopg2 #libreria para establecer conexion con postgress
import xml.etree.ElementTree as ET #libreria para leer archivos XML

class PostgressConecction:
    
    def __init__(self,xml_file):
        """Constructos de la Clase
        
            --Recibe: una ruta donde estaran las credenciales a las BD en un XML
        """
        self.xml_file = xml_file
        self.connection = None
        self.read_credentials()

    def read_credentials(self):
        """Metodo para leer las credenciales desde el archivo XML
        
            Encuentra las credenciales mediante los Tags del XML,
            en caso de no encontrar algun tag pinta la excepcion por consola
        """
        try:
             tree = ET.parse(self.xml_file)
             root = tree.getroot()

             self.host = root.find('postgresql_database/host').text
             self.port = root.find('postgresql_database/port').text
             self.database_name = root.find('postgresql_database/database_name').text
             self.user = root.find('postgresql_database/user').text
             self.password = root.find('postgresql_database/password').text
            

        except Exception as e:
            print(f'error al leer el archivo XML: {str(e)}')

    def connect(self):
        """
            Metodo para realizar la conexion con la base de datos de postgress

            Regresa: una conexion establecida con las credeciales del XML        
        """
        try:
            self.connection = psycopg2.connect(
                host = self.host,
                port = self.port,
                database = self.database_name,
                user = self.user,
                password = self.password
            )
            return self.connection
        except Exception as e:
            print(f'error al conectarse a la base de datos: {str(e)}')
    
    def close(self):
        """
        Metodo para cerrar la conexion con la Base de datos de postgress
        """
        if self.connection:
            self.connection.close()