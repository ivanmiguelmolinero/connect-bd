# connect-bd
Conector de base de datos con Python

## Instalación

Hay dos opciones de instalación del programa:

 * **Installer.exe:** Una de las formas de instalar el programa (y la más simple) es ejecutar este archivo. Te pedirá que presiones el botón __Instalar__ para instalar el programa. Después de esperar, te habrá instalado el programa. El ejecutable se encontrará en la carpeta __dist__

 * **Connect-bd.exe:** Es el instalador "tradicional" de Windows. Es más completo que el anterior ya que puedes seleccionar dónde instalar el programa. Aparte del __main.exe__ también te copiará los archivos necesarios para su funcionamiento en el directorio seleccionado anteriormente.

## Funcionamiento

El programa en Python se conecta a una BD (nombre proporcionado por el usuario en el archivo de configuración, así como la IP y la contraseña si es necesaria) una vez al día a una hora especificada por el usuario (característica aún por configurar). Recoge los datos necesarios para parsearlos a un fichero JSON que posteriormente subirá a un bucket de S3 mediante un POST a una Lambda de AWS. El bucket al que se subirá este archivo es a elección del usuario.

Otra función que realiza es lanzar un GET a otra Lambda para obtener un fichero JSON y e insertar los datos en un BD (proporcionada por el usuario). Si la tabla donde se van a insertar los datos aún no existe, el programa la crea. En caso de que exista, comprueba si hay datos nuevos para actualizar la tabla.
