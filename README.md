# Setup
___
## Importar archivos remotos
### CSV bicicletas
_usando scp a pampero_
```shell
./import_bikes_csv.sh usuario_pampero [maximo_lineas]
```
### CSV estaciones
_usando scp a pampero_
```shell
./import_stations_csv.sh usuario_pampero
```

# Compilar
___
```sh
mvn clean install
```