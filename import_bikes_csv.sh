# usage: ./import_bikes_csv.sh username [max_lines]

user=$1
lines=$2
path=../../../it.itba.edu.ar/pub/pod/bikes.csv

if [ ! "$lines" ]
then
  scp "$user"@pampero.itba.edu.ar:"$path" /dev/stdout > bikes.csv
else
  scp "$user"@pampero.itba.edu.ar:"$path" /dev/stdout | head -n"$lines" > bikes.csv
fi


