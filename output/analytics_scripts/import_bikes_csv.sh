# usage: ./import_bikes_csv.sh username [max_lines]

user=$1
lines=$2
curr_dir=$(pwd)
path=../../../it.itba.edu.ar/pub/pod/bikes.csv

if [ ! "$lines" ]
then
  scp "$user"@pampero.itba.edu.ar:"$path" /dev/stdout > "$curr_dir"/bikes.csv
else
  scp "$user"@pampero.itba.edu.ar:"$path" /dev/stdout | head -n"$lines" > "$curr_dir"/bikes.csv
fi


