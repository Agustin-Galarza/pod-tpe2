Color_Off='\033[0m'
BBlue='\033[1;34m'
BRed='\033[1;31m'

curr_dir=$(pwd)
tmp_dir="$curr_dir"/output/tmp
timeout=600
server_folder=$tmp_dir/tpe2-l61481-server-1.0-SNAPSHOT
analytics_dir="$curr_dir"/output/analytics
node_results="$analytics_dir"/node_outputs
pids_file="$analytics_dir"/nodes_pids.txt


usage() { echo "Usage: $0 -n <int> (amount of nodes) [-c (compile project)]  [-t <number> = $timeout (timeout in sec)]" 1>&2; exit 1; }

while getopts "hn:ct:" o; do
    case "${o}" in
        c)
            mvn clean install
            ;;
        n)
            n=${OPTARG} || usage
            nodes="$n"
            ;;
        t)
            t=${OPTARG} || usage
            timeout="$t"
            ;;
        *)
            usage
            ;;
    esac
done

if [ "$nodes" -lt 1 ]; then
  echo 'amount of nodes should be positive'
  exit
fi

echo -e "$BBlue""Running $nodes nodes""$Color_Off"

rm -rf "$tmp_dir"

# Create temporary directory and copy jars with .env file
mkdir -p "$tmp_dir" && find . -name '*tar.gz' -exec tar -C "$tmp_dir" -xzf {} \;
find "$tmp_dir" -path "*/tpe2-l61481-*/*" -exec chmod u+x {} \;
find "$tmp_dir" -maxdepth 1 -path "*/tpe2-l61481-*" -exec cp .env {} \;


# Create node directories and launch them
rm -rf "$node_results"
mkdir -p "$node_results"

cd "$server_folder" || (echo "Error with cd"; exit)
> "$pids_file"
for i in $(eval echo "{1..$nodes}");
do
  node_name=node"$i";
  timeout "$(("$timeout"))" ./run-node.sh -Dtimeout="$(("$timeout"/60))" > "$node_results"/"$node_name".ouput &
  sleep 1
  words=($(netstat -ntlp 2> /dev/null | egrep :57"$(printf %02d $i)")) 
  p=$(echo "${words[-1]}" | egrep -o [0-9]*)
  echo "$p" >> "$pids_file"
done
# shellcheck disable=SC2164
cd "$curr_dir"