# Setup
___
## Importar archivos remotos

### CSV bicicletas
_usando scp a pampero_
```shell
./output/scripts/import_bikes_csv.sh usuario_pampero [maximo_lineas]
```
### CSV estaciones
_usando scp a pampero_
```shell
./output/scripts/import_stations_csv.sh usuario_pampero
```
Ambos scripts dejan los archivos en el `pwd`.

# Compilar
___
```sh
mvn clean install
```
Para compilar el proyecto con maven.
Luego, para extraer los `.tar.gz`:

```bash
mkdir -p ./output/tmp && find . -name '*tar.gz' -exec tar -C ./output/tmp -xzf {} \;
find ./output/tmp -path '*/tpe2-l61481-*/*' -exec chmod u+x {} \;
find ./output/tmp -maxdepth 1 -path '*/tpe2-l61481-*' -exec cp .env {} \;
```
Esto extraerá los `.tar.gz` generados, guardándolos en un directorio temporal `./output/tmp` y le dará permisos de ejecución a los `.sh` tanto del cliente como del servidor.

Además, copiará el archivo `.env` que se encuentra en la raíz del proyecto a cada directorio para que pueda ser utilizado para la configuración. En este archivo
se encuentra el nombre y la contraseña del grupo de hazelcast para que utilicen tanto el cluster como el cliente.

# Ejecutar

Para ejecutar el cliente y el servidor basta con usar los scripts bash dentro de cada directorio correspondiente.

Por ejemplo, para correr un nodo en la interfaz localhost con un timeout de 20 minutos:
```shell
cd output/tmp/tpe2-l61481-server-1.0-SNAPSHOT/
./run-node.sh -Dinterface='127.0.0.*' -Dtimeout=20
```
Si se quieren ejecutar más nodos basta con volver a correr la última línea n veces.

Si no se especifica timeout se toma un timeout por defecto de 30 minutos.

Luego, para correr la query 2 del cliente donde:

- se tienen 2 nodos corriendo en las direcciones 127.0.0.1:5701 y 127.0.0.1:5702 
- los archivos se leen del directorio raíz del proyecto
- se quiere dejar el resultado en el directorio donde se encuentra la aplicación cliente.
- se quieren mostrar los primeros 10 resultados.
```shell
cd output/tmp/tpe2-l61481-client-1.0-SNAPSHOT/
./query2 -Daddresses='127.0.0.1:5701,127.0.0.1:5702' -DinPath='../../..' -DoutPath='.' -Dn=10
```

## Ejecución automática

En el directorio `output/scripts` se encuentran una serie de scripts que sirven para ejecutar nodos del servidor y el cliente de forma automática, además de leer los resultados de la ejecución y tomar.
- `launch-nodes.sh`: puede compilar el proyecto, crear la carpeta tmp y correr n nodos del servidor con un timeout especificado, dejando en un directorio `output/analytics/node_outputs` sus logs y pids en un archivo por si se quiere matar a los procesos prematuramente.
- `run-client.sh`: ejecuta ambas queries del cliente n veces, dejando sus resultados en un directorio `output/analytics/client_results`
ambos scripts pueden recibir parámetros y cuentan con un parámetro -h para imprimir ayuda.
- `compute_times.sh`: lee los logs de las ejecuciones del cliente creadas por `run-clientes.sh`, calcula los deltas de lectura de archivos y ejecución del mapReduce y los imprime en archivos `.results` dentro de los correspondientes `output/analytics/client_results`
- `average_results.py`: lee los `.results` generados por `compute_times.sh` y calcula el promedio (en segundos) de ejecución de lectura y procesamiento del mapReduce, imprimiendo por consola el resultado en forma de diccionario.

Por ejemplo, para replicar el caso anterior con 2 nodos corriendo en localhost:
```shell
./output/scripts/launch-nodes.sh -c -n 2 -i "127.0.0.*" -t 20
```
Donde los logs de salida de cada nodo pueden encontrarse en el directorio `output/analytics/node_outputs`, y sus correspondientes pids en el archivo `output/analytics/nodes_pids.txt`.
Luego de obtener la ip y puerto de cada nodo se puede correr el cliente de la siguiente manera:
```shell
./output/scripts/run-client.sh -a '127.0.0.1:5701,127.0.0.1:5702' -r 1 -l 10
```
En este caso, se ejecutarían ambas queries una vez (pudiéndose cambiar esa cantidad con el parámetro r).
Los resultados de cada corrida se pueden encontrar en `output/analytics/client_results`.

Una vez que se realizaron todas las ejecuciones del cliente, con
```shell
./output/scripts/compute_times.sh
```
se leen los logs de cada ejecución del cliente y se calculan sus tiempos de ejecución, dejando los resultados en `text1.txt.results` y `text2.txt.results`.

Finalmente, con 
```shell
python3 ./output/scripts/average_results.py
```
se leen los archivos `.results` y se imprime en consola los promedios de ejecución obtenidos.