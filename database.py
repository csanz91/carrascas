import logging
import sqlite3 #Conexion con la base de datos

class Database():
    """Esta clase gestiona todas las tareas relacionadas con la base de datoss

    Args:
        dbPath: la ruta del fichero de la base de datos"""
        
    def __init__(self, dbPath):
        #Ruta de la base de datoss
        self.dbPath = dbPath

        #Nos conectamos con la base de datos
        self.connect()
        
    def connect(self):
        """Inicializa la conexion con la base de datos
        """
        try:
            logging.debug("Database connect: Starting the database...")
            self.conn = sqlite3.connect(self.dbPath, check_same_thread=False)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA busy_timeout = 30000")   # 30 s
            #Configuramos el conector para que devuelva cada fila como un diccionario
            self.conn.row_factory = sqlite3.Row

            self.cursor = self.conn.cursor()
            logging.debug("Database connect: Connected to the database.")

        except Exception as e:
            logging.error("Database connect: Could not start the database")
            #Atencion datos sensibles. NO mostar en produccion.
            logging.debug("Database connect: Exception: %s" % e)

    def insert(self, query, values, getKey=0):
        """Realiza una operacion de insert/update sobre la base de datos configurada

        Args:
            query: un string con la query a ejecutar
            values: un array con la lista de valores a pasar como parametros a la query. Pasar un array vacio si no hay parametros
            getKey: parametro opcional. Si es true la funcion devuelve el ultimo indice de la base de datos de la operacion realizada.
        Returns:
            devulve true si la funcion se ha ejecutado de forma correcta.
            si getKey=1 -> Devuelve el ultimo indice de la base de datos de la operacion realizada.

        """
        if query:
            try:
                # Insert data
                self.cursor.execute(query, values)

                # Save (commit) the changes
                self.conn.commit()

                #Return the inserted key
                if getKey:
                    return self.cursor.lastrowid

                return 1

            #Si la base de datos esta cerrada la abrimos:
            except sqlite3.ProgrammingError as e:
                logging.debug("insert: Exception: %s" % e)
                if "Cannot operate on a closed database" in str(e):
                    self.connect()

            except Exception as e:

                logging.error('insert: Exception when trying to perform an insert')
                #Importante: nunca mostrar trazas de debug en entorno de produccion, estamos exponiendo datos potencialmente sensibles.
                logging.debug('insert: Detalles de la excepcion: ' + str(e))

        return 0


    def executescript(self, query):
        """ Execute more than one statment

        Args:
            query: a string containing the query to execute
        Returns:
            devulve true si la funcion se ha ejecutado de forma correcta.

        """
        if query:
            try:
                # Insert data
                self.cursor.executescript(query)

                # Save (commit) the changes
                self.conn.commit()

                return 1

            #Si la base de datos esta cerrada la abrimos:
            except sqlite3.ProgrammingError as e:
                logging.debug("executescript: Exception: %s" % e)
                if "Cannot operate on a closed database" in str(e):
                    self.connect()

            except Exception as e:

                logging.error('executescript: Exception when trying to executescript')
                #Importante: nunca mostrar trazas de debug en entorno de produccion, estamos exponiendo datos potencialmente sensibles.
                logging.debug('executescript: Exception details: ' + str(e))

        return 0

    def select(self, query, values, scalar=0):
        """Realiza una operacion de select sobre la base de datos configurada

        Args:
            query: un string con la query a ejecutar
            values: un array con la lista de valores a pasar como parametros a la query. Pasar un array vacio si no hay parametros
            scalar: parametro opcional. Si es true la funcion devuelve la primera columna de la primera fila
        Returns:
            devulve el conjunto de datos devuelto por la query
            si scalar=1 -> Devuelve la primera columna de la primera fila

        """
        if query:
            try:
                # Insert data
                self.cursor.execute(query, values)

                #In case we want to return a single result
                if scalar:
                    result = self.cursor.fetchone()
                    if result is not None:
                        return result[0]
                    else:
                        return 0

                #Rerturn all the data
                r = self.cursor.fetchall()          
                return r    

            #Si la base de datos esta cerrada la abrimos:
            except sqlite3.ProgrammingError as e:
                logging.debug("insert: Exception: %s" % e)
                if "Cannot operate on a closed database" in str(e):
                    self.connect()

            except Exception as e:
                logging.error('select: Exception when trying to perform a select')
                #Importante: nunca mostrar trazas de debug en entorno de produccion, estamos exponiendo datos potencialmente sensibles.
                logging.debug('select: Detalles de la excepcion: ' + str(e))

        return 0


    def close(self):
        """Cerramos la conexion con la base de datos"""

        self.conn.close()
