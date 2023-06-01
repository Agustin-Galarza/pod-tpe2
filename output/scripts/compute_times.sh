curr_dir=$(pwd)
results_dir=$curr_dir/output/analytics/client_results
py_timeparser_script=$curr_dir/output/scripts/parse_timestamp.py

compute_times(){
  timestamps=()
  while IFS= read -r line; do
    words=($(echo "$line"))
    raw_timestamp="${words[*]:0:2}"
    timestamps+=("$(python3 "$py_timeparser_script" "$raw_timestamp")")
  done < "$1"
  deltas=()
  deltas+=($(("${timestamps[1]}" - "${timestamps[0]}")))
  deltas+=($(("${timestamps[3]}" - "${timestamps[2]}")))

  filename="$1.results"
  echo "lectura: $(echo "scale=2;${deltas[0]}/1000" | bc) seg" > "$filename"
  echo "procesamiento: $(echo "scale=2;${deltas[1]}/1000" | bc) seg" >> "$filename"
}

for dir in "$results_dir"/*;
do
  echo "$dir";
  cd "$dir"
  file=text1.txt
  compute_times "$file"
  file=text2.txt
  compute_times "$file"
done