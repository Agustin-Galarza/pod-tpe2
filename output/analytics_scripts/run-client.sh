Color_Off='\033[0m'
BRed='\033[1;31m'

runs=10
query2_output_lines=40

curr_dir=$(pwd)
tmp_dir="$curr_dir"/tmp

client_dir="$tmp_dir"/tpe2-l61481-client-1.0-SNAPSHOT
analytics_dir="$curr_dir"/z_analytics
results_dir="$analytics_dir"/client_results

rm -rf "$results_dir"
mkdir -p "$results_dir"

cp -f "$curr_dir"/bikes.csv "$client_dir"
cp -f "$curr_dir"/stations.csv "$client_dir"

addresses=""

usage() { echo "Usage: $0 -a <str> (list of node addresses) [-r <int> = $runs (client runs)] [-l <int> = $query2_output_lines (output lines in query2)]" 1>&2; exit 1; }

while getopts "ha:r:l:" o; do
    case "${o}" in
        a)
            addresses=${OPTARG} || usage
            ;;
        *)
            usage
            ;;
    esac
done

# Launch client
cd "$client_dir" || (echo 'error on cd to tmp' && exit 1)
for i in $(eval echo "{1..$runs}"); do
  echo "######################### Cliente $i #########################"
  # Query 1
  "$client_dir"/query1 -Daddresses="$addresses" -DinPath=. -DoutPath=.;
  res="$?"
  if [ "$res" -ne 0 ];then
    echo -e "$BRed""Error with query 1""$Color_Off"
    exit 1
  fi
  # Query 2
  "$client_dir"/query2 -Daddresses="$addresses" -DinPath=. -DoutPath=. -Dn="$query2_output_lines";
  res="$?"
  if [ "$res" -ne 0 ];then
    echo -e "$BRed""Error with query 2""$Color_Off"
    exit 1
  fi

  # Move result files to corresponding results directory
  dir="$results_dir"/results"$i"
  mkdir -p "$dir"
  find "$client_dir" -name "*.txt" -exec mv {} "$dir" \;
  mv "$client_dir"/query1.csv "$dir";
  mv "$client_dir"/query2.csv "$dir";
done
cd "$curr_dir"