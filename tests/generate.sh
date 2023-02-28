#!/bin/bash

data_dir="tests/data"
chunkers=("size-262144" "size-512" "size-32" "size-1")
modes=("normal" "trickle")

# Remove existing .car files in data directory
rm -rf "$data_dir"/*.car

# Generate test vectors
for size in 1K 10K 100K; do
  head -c $size </dev/urandom > $data_dir/rand_$size.bin
  truncate -s $size $data_dir/zero_$size.bin
done

for count in 1000 2000 5000; do
  seq -s ' ' 1 $count > $data_dir/seq_$count.txt
done

# Create .car files for each source file
for file in "$data_dir"/*; do
  if [[ -f "$file" && "${file##*.}" != "car" ]]; then
    for chunker in "${chunkers[@]}"; do
      for mode in "${modes[@]}"; do
        # Build the ipfs add command
        add_command=(ipfs add "$file" --chunker="$chunker" -q)

        if [[ "$mode" == "trickle" ]]; then
          add_command+=(--trickle)
        fi

        # Run ipfs add command and capture the output
        command_output=$("${add_command[@]}")
        hash=$(echo "$command_output" | cut -d' ' -f2)

        # Download the CAR file and save it to a new file
        car_file="$data_dir/$(basename "$file").$chunker.$mode.car"
        curl -s "http://localhost:8080/ipfs/$hash?format=car" > "$car_file"
      done
    done
  fi
done
