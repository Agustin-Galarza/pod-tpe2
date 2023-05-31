# Setup
___
## Importar archivos remotos
### CSV bicicletas
_usando scp a pampero_
```shell
./output/analytics_scripts/import_bikes_csv.sh usuario_pampero [maximo_lineas]
```
### CSV estaciones
_usando scp a pampero_
```shell
./output/analytics_scripts/import_stations_csv.sh usuario_pampero
```

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

Luego para ejecutar el cliente y el servidor basta con ejecutar los script bash dentro de cada directorio correspondiente.

## Ejecución automática

En el directorio `output/analytics_scripts` se encuentran una serie de scripts que sirven para ejecutar nodos del servidor y el cliente de forma automática, además de leer los resultados de la ejecución y tomar.
- `launch-nodes.sh`: puede compilar el proyecto, crear la carpeta tmp y correr n nodos del servidor con un timeout especificado, dejando en un directorio `output/analytics/node_outputs` sus logs y pids en un archivo por si se quiere matar a los procesos prematuramente.
- `run-client.sh`: ejecuta ambas queries del cliente n veces, dejando sus resultados en un directorio `output/analytics/client_results`
ambos scripts pueden recibir parámetros y cuentan con un parámetro -h para imprimir ayuda.
- `compute_times.sh`: lee los logs de las ejecuciones del cliente creadas por `run-clientes.sh`, calcula los deltas de lectura de archivos y ejecución del mapReduce y los imprime en archivos `.results` dentro de los correspondientes `output/analytics/client_results`
- `average_results.py`: lee los `.results` generados por `compute_times.sh` y calcula el promedio de ejecución de lectura y procesamiento del mapReduce.