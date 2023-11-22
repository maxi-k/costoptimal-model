#!/usr/bin/env sh

usage() {
    echo "Usage: $0 <input-file='raw/instances.json'>"
    echo "  <input-file>  Path to the input file to be converted."
    echo ""
    echo "  Converts the given instances.vantage.sh instances.json file to"
    echo "  a csv file in the schema of data/historical-data-raw.csv"
    echo ""
    echo "  respected environment variables: "
    echo "  - header  = [true|false]"
}

# if $1 is help or --help, print usage
if [ "$1" = "help" ] || [ "$1" = "--help" ]; then
    usage
    exit 1
fi

input_file="${1:-raw/instances.json}"

test -z "$header" && header='false'


if [ "$header" = "true" ]; then
    echo "family,generation,instance_type,ECU,vCPU,physical_processor,clock_speed_ghz,pretty_name,memory,network_performance,FPGA,GPU,storage_devices,storage_ssd,storage_nvme_ssd,storage_size,pricing"
fi

# Convert the instances.json file to a csv file with columns
cat "$input_file" | jq -r '
  .[] | [
  .family,
  .generation,
  .instance_type,
  .ECU,
  .vCPU,
  .physical_processor,
  .clock_speed_ghz,
  .pretty_name,
  .memory,
  .network_performance,
  .FPGA,
  .GPU,
  .storage.devices,
  .storage.ssd,
  .storage.nvme_ssd,
  .storage.size,
  .pricing."us-east-1".linux.ondemand ] | @csv'
