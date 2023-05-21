# usage: ./import_stations_csv.sh username

user=$1
path=../../../it.itba.edu.ar/pub/pod/stations.csv

scp "$user"@pampero.itba.edu.ar:"$path" /dev/stdout > stations.csv


