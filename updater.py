from git import Repo
import subprocess
import time
import logging
import git
import os
import signal


logging.basicConfig(filename='updater.log', level=logging.INFO, filemode="w")

# Definiemos el comando a ejecutar
cmd = 'python carrascas.py'


def runProcess():
    """Arranca el proceso

    Args:
        ---
    Returns:
        El proceso que se acaba de arrancar
    """
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)


def isSourceCodeUpdated(repo):
    """Compruba si el repositorio pasado esta actualizado con el Github

    Args:
        repo: instancia de la clase Repo a comprobar
    Returns:
        Un booleano indicando si se ha actualizado el codigo fuente
    """
    # Esta variable contiene el resultado de la funcion
    sourceCodeUpdated = 0

    try:

        # Iteramos los remotes
        for remote in repo.remotes:
            # Hacemos fetch y si existe un old_commit significa que el codigo se ha actualido
            info = remote.fetch()[0]
            #logging.info('isSourceCodeUpdated: ref: %s' % info.refe)
            sourceCodeUpdated = info.old_commit != None or sourceCodeUpdated
    except Exception as e:
        logging.error('Excepcion al recuperar los ultimos commits. Excepcion: %s' % e)

    # Si se ha actualizado el codigo lo indicamos en el log
    if sourceCodeUpdated:
        logging.info('Detectado cambio en el codigo fuente')
    return sourceCodeUpdated


def isProcessAlive(process):
    """Compruba si el proceso pasado esta vivo

    Args:
        process: proceso a comprobar
    Returns:
        Un booleano indicando si el proceso esta activo
    """
    # Comprobamos si el proceso esta corriendo
    processIsAlive = process.poll() is None

    # Si el script no esta activo lo mostramos por el log
    if not processIsAlive:
        logging.info('El script no esta activo')

    return processIsAlive


# Arrancamos el proceso por primera vez
process = runProcess()

# Creamos una instancia del repositorio, la ruta del directorio esta vacia porque es la actual
repo = Repo('')
# La instancia de Git es necesaria para ejecutar el comando pull
g = git.cmd.Git('')


try:
    # Ejecutamos la comprobacion de forma periodica e indefinida
    while True:
        # Si el codigo fuente se ha actualizado o el proceso no esta corriendo:
        if isSourceCodeUpdated(repo) or not isProcessAlive(process):
            logging.info('Reiniciamos el proceso...')
            # Recuperamos la nueva version del codigo
            g.pull()
            # Matamos el proceso actual
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            except Exc  eption as e:
                logging.info(
                    'No se ha podido matar el proceso. Excepcion: %s' % e)
            # Rearrancamos el proceso
            process = runProcess()
        # Dormimos durante un tiempo
        time.sleep(60)

except KeyboardInterrupt:
    print 'El usuario cierra la aplicacion...'
