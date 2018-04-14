import json  # Para serializar los datos
import logging  # Para guardar los loggings
#Esperamos de forma indefinida. Todas las tareas se ejecutan el threads en el background.
import signal
import time  # Para obtener el epoch
import threading
from database import Database
from collections import defaultdict
import requests
from datetime import datetime
import configparser

import paho.mqtt.client as mqtt  # Libreria cliente MQTT #pip install paho-mqtt


#Guaramos el fichero de logs en smappee.log, con nivel de logging en "DEBUG" y reemplazamos el fichero en cada arranque
logging.basicConfig(filename='carrascas.log', level=logging.DEBUG, filemode="w")

config = configparser.ConfigParser()
config.read('config.ini')

class Carrascas():
    """Esta clase se conecta con el broker interno del Smappee, recibe los datos, los almacena 
       y se conecta con el blockchain de forma periodica para transmitir la informacion guardada

       Args:
            MQTT_addr: host del broker MQTT
            MQTT_port: puerto del broker MQTT
    """

    def __init__(self):

        # API key for the AEMET API
        self.apiAEMET = [config['AEMET']['apiKey']]

        serverAddr = 'iothub.sytes.net'
        serverPort = 1883

         # Set the topic header
        self.topicConf = "device/+/conf"
        self.topicRealtime = "device/+/realtime"

        self.initMQTT(serverAddr, serverPort)

    def validateResponse(self, response):
        """Esta funcion se encarga de validar la respuesta recibida de la API
        Args:
           response: instancia del objeto Response de la libreria Requests. 
        Returns:
           devuelve la respuesta decodificada en un diccionario o 
           en formato de texto plano si no se ha podido decodificar el Json.
           devuelve un array con un 0 si el servidor devuelve error.

        """

        #Comprobamos el status code
        status = response.status_code
        #Si el status code empieza con 5 significa que el servidor ha devuelto un error
        if str(status).startswith('5'):
            logging.error('send: Server failure...')
            #Salimos indicando fallo
            return [0]

        #Convertimos el resultado en un diccionario
        try:
            result = response.json()
        #Si falla, devolvemos el resultado como texto plano
        except:
            result = response.text
            logging.warning("validateResponse: the response could not be json decoded. Response: %s" % result)

        return result

    def getCurrentWeather(self):
        """Get the current weather from the AEMET API
        Args:
           ---
        Returns:
           

        """

        idema = "8523X"
        url = "https://opendata.aemet.es/opendata/api/observacion/convencional/datos/estacion/"

        headers = {'cache-control': "no-cache"}
        response = requests.request("GET", url+idema, headers=headers, params=self.apiAEMET)

        jsonResponse = self.validateResponse(response)

        # Si exito
        if jsonResponse["estado"] == 200:
            datosUrl = jsonResponse["datos"]
            datosResponse = requests.request("GET", datosUrl, headers=headers)

            datos = self.validateResponse(datosResponse)
            latestData = datos[0]

            print latestData["prec"]
            print latestData["hr"]
            print latestData["ta"]
            print latestData["tamin"]
            print latestData["tamax"]

            return latestData

    def getProbPrecipitacion(self):
        """Get the rain probabilitys for the next 6 days from the AEMET API
        Args:
            ---
        Returns:
            dicc with the rain probability for the next days

        """
        municipio = "12027"
        headers = {'cache-control': "no-cache"}
        url = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/diaria/"

        response = requests.request("GET", url+municipio, headers=headers, params=self.apiAEMET)

        jsonResponse = self.validateResponse(response)

        result = {}

        # Si exito
        if jsonResponse["estado"] == 200:
            datosUrl = jsonResponse["datos"]
            datosResponse = requests.request("GET", datosUrl, headers=headers)

            datos = self.validateResponse(datosResponse)
            latestData = datos[0]

            predicciones = latestData["prediccion"]["dia"]

            format = '%Y-%m-%d'

            for prediccion in predicciones:
                dayDelta = (datetime.strptime(prediccion["fecha"], format).date()-datetime.utcnow().date()).days
                key = "pred_%s_d" % (dayDelta)
                result[key] = max(probability["value"] for probability in prediccion["probPrecipitacion"])

            return result


    def initMQTT(self, MQTT_addr, MQTT_port):
        """Initialize and connect the MQTT service

        Args:
            ---
        Returns:
           Does not return anything

        """
        logging.debug("Initializing the MQTT client...")

        # Data buffer
        self.dataBuffer = defaultdict(list)

        #Inicializamos el cliente MQTT
        self.client = mqtt.Client()

        #Creamos la llamada a las funciones de conexion y de desconexion del MQTT
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        
        #Definimos los callbacks segun los diferentes topicos
        self.client.message_callback_add(self.topicConf, self.configureDevice)
        self.client.message_callback_add(self.topicRealtime, self.realtimeData)

        #Nos conectamos al broker MQTT
        logging.debug("Connecting with the MQTT broker...")
        self.client.connect(MQTT_addr, MQTT_port)

        self.client.loop_start()


    def setupThreads(self):
        """Initialize and start all the background threads

        Args:
            ---
        Returns:
           Does not return anything

        """

         # Threads config
        self.stopThreads = threading.Event()
        self.saveBufferedDataThread = threading.Thread(target=self.saveBufferedData, args=(self.stopThreads, ))
        self.saveBufferedDataThread.start()

    def on_connect(self, client, userdata, flags, rc):
        """Esta funcion es llamada por la libreria cuando recibimos una respuesta de tipo CONNACK del servidor

        Args:
            client: the client instance for this callback
            userdata: the private user data as set in Client() or userdata_set()
            flags: response flags sent by the broker
            rc: the connection result. 0: Connection successful
                                       1: Connection refused - incorrect protocol version
                                       2: Connection refused - invalid client identifier 
                                       3: Connection refused - server unavailable 
                                       4: Connection refused - bad username or password 
                                       5: Connection refused - not authorised 
                                       6-255: Currently unused.
        Returns:
           Does not return anything

        """

        # If the connection was unsuccessful
        if rc != 0:
            logging.error("MQTT on_connect: It was not posible to connect to the broker. Connection result: %s" % rc)
            return

        # Start the database
        self.db = Database("carrascas.db")

        # Setup the threads
        self.setupThreads()

        # Subsription to the relevant topics
        client.subscribe(self.topicConf, 0)
        client.subscribe(self.topicRealtime, 0)

        logging.debug("MQTT on_connect: Connected with result code "+str(rc))

    def on_disconnect(self, client, userdata, rc):
        """Esta funcion es llamada por la libreria cuando se desconecta del servidor

        Args:
            client: the client instance for this callback
            userdata: the private user data as set in Client() or userdata_set()
            rc: the disconnection result. 0: the callback was called in response to a disconnect() call
                                          other: unexpected disconnection
        Returns:
           Does not return anything

        """
        # If the desconnection was unexpected
        if rc != 0:
            logging.error('MQTT on_disconnect: Unexpected disconnection.')

    def publish(self, topic, payload, qos=0, retain=False):
        """Publish a message, with the given parameters.

        Args:
            topic: string containing the destination topic
            payload: string with the data to send
            qos: optional integer with the quality needed to
                 publish the message
            retain: optional boolean 
           
        Returns:
           Does not return anything

        """
        # Check if the topic is not empty, otherwise exit
        if not topic:
            logging.warning('publish: a topic must be specified')
            return

        logging.debug("publish: publishing message: %s to the topic: %s" % (payload, topic))

        result, mid = self.client.publish(topic, payload, qos=qos, retain=retain)

        if result != mqtt.MQTT_ERR_SUCCESS:
            logging.error('publish: It was not possible to pusblish the payload: %s to the topic: %s. Result code: %s' % (payload, topic, result))

        return


    def configureDevice(self, client, userdata, msg):
        """This functions is called when a message is received in the topic device/+/configure

        Args:
            client: the client instance for this callback
            userdata: the private user data as set in Client() or userdata_set()
            msg: an instance of MQTTMessage. This is a class with members topic, payload, qos, retain.
           
        Returns:
           Does not return anything

        """

        # The incoming data is json encoded
        try:
            receivedData = json.loads(msg.payload)
        except ValueError:
            logging.error("saveDeviceStatus: The received data is not a valid JSON object. receivedData: %s" % msg.payload)
            return

        deviceId = receivedData["deviceId"]

        logging.debug("saveDeviceStatus: detected device : %s" % (deviceId))

        # Save the status of the device in the database
        while not self.db.insert('''--- Insert or replace the device
                                    INSERT OR REPLACE INTO devices (deviceId) ; ''', [deviceId]):
            logging.error("saveDeviceStatus: retrying to save the status for the device with serial number: %s" % deviceId)
            time.sleep(1)


    def realtimeData(self, client, userdata, msg):
        """This functions is called when a message is received in the topic device/+/realtime

        Args:
            client: the client instance for this callback
            userdata: the private user data as set in Client() or userdata_set()
            msg: an instance of MQTTMessage. This is a class with members topic, payload, qos, retain.
           
        Returns:
           Does not return anything

        """
        # The incoming data is json encoded
        try:
            receivedData = json.loads(msg.payload)
        except ValueError:
            logging.error("realtimeData: The received data is not a valid JSON object. receivedData: %s" % msg.payload)
            return

        # Check if the client has sent his indentification
        if "deviceId" not in receivedData:
            logging.error("realtimeData: Could not find the client id in the received data. receivedData: %s" % receivedData)
            return

        # Retrieve the data
        data = receivedData["data"]

        deviceId = receivedData["deviceId"]
        temperature = data["temperature"]
        humidity = data["humidity"]
        rainPulses = data["rainPulses"]

        currentTimestamp = int(time.time())

        self.dataBuffer[deviceId].append([currentTimestamp, temperature, humidity, rainPulses])


    def getDataFromBuffer(self, deviceId):
        """This function gets the data from the buffer and then calculates the weighted arithmetic mean
        Args:
            serialNumber: a string with the serial number of the device
           
        Returns:
           a list containing the timestamp and the agregated values in the same order as there were saved

        """

        # Is the serial number in the buffer data
        if not deviceId in self.dataBuffer:
            logging.warning('getDataFromBuffer: the serial number could not be found in the buffer. serialNumber: %s' % deviceId)
            return 

        # Create a local copy of the buffer
        dataBuffer = self.dataBuffer[deviceId]
        # Clear the selected buffer
        self.dataBuffer[deviceId] = []

        # First check if there are any data in the buffer
        if not dataBuffer:
            logging.warning('getDataFromBuffer: data buffer empty, nothing to do.')
            return

        try:
            # Calculate the elapsed time between the extremes values
            totalTime = dataBuffer[-1][0] - dataBuffer[0][0]

            # If not time has transcurred or the data is not ordered, return the last value
            if totalTime <= 0:
                logging.warning('getDataFromBuffer: the time transcurred between the data points is not valid. totalTime: %s.' % totalTime)
                return dataBuffer[-1]

            # Initialize the list
            agregatedValues = [0.0] * len(dataBuffer[0][1:])

            # Calculate the average value from all the data considering the time elapsed between each point
            for index, data in enumerate(dataBuffer[:-1]):
                timestamp = data[0]
                deltaTime = dataBuffer[index+1][0] - timestamp

                # Process the different data features
                for i, value in enumerate(data[1:]):
                    agregatedValues[i] += deltaTime * value
                    

            # Calculate the final value for each feature
            for index, agregatedValue in enumerate(agregatedValues):
                agregatedValues[index] = agregatedValue / totalTime

            return [dataBuffer[0][0]] + agregatedValues

        except Exception as e:
            logging.error('getDataFromBuffer: The buffer data cant be processed. Probably because of an invalid value. Exception: %s' % e)

        return  

    def saveBufferedData(self, stopThread):
        """
        This function reads the data from the buffer and save it in the database
        Args:
            ---
           
        Returns:
           it does not return anything

        """

        while not stopThread.isSet():
            for deviceId in self.dataBuffer:

                data = self.getDataFromBuffer(deviceId)

                if data:

                    currentTimestamp, temperature, humidity, rainPulses = data

                    while not self.db.insert('''INSERT INTO data (deviceId, temperature, humidity, rainPulses, currentTimestamp) VALUES (?,?,?,?,?)''',
                                                    [deviceId, temperature, humidity, rainPulses, currentTimestamp]):
                        logging.error("saveBufferedData: Reintentando guardar los datos...")
                        time.sleep(0.5)
            stopThread.wait(60)

    def stop(self):
        """
        This stops and disconnects gracefully all the opened tasks
        Args:
            ---
           
        Returns:
           it does not return anything

        """

        # Disconnect the MQTT client
        self.client.disconnect()

        # Stop the threads
        r.stopThreads.set()
        # Wait for the thread to stop for 60 seconds max
        r.saveBufferedDataThread.join(60)

   
if __name__ == '__main__':

    try:
        r = Carrascas()
        signal.pause()
    except KeyboardInterrupt:
        print("Parando la aplicacion...")
        r.stop()
        print("La aplicacion se ha cerrado")