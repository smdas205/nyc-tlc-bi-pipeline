#!/bin/bash

# Define the start dates for 2-month clusters
clusters=(
  "2022-01-01 2022-03-01"
  "2022-03-01 2022-05-01"
  "2022-05-01 2022-07-01"
  "2022-07-01 2022-09-01"
  "2022-09-01 2022-11-01"
  "2022-11-01 2023-01-01"
  "2023-01-01 2023-03-01"
  "2023-03-01 2023-05-01"
  "2023-05-01 2023-07-01"
  "2023-07-01 2023-09-01"
  "2023-09-01 2023-11-01"
  "2023-11-01 2024-01-01"
)

echo "Starting 12 parallel PL/pgSQL procedures"

# Loop through each cluster and run 12 separate parallel procedures
for dates in "${clusters[@]}"; do
  start_date=$(echo $dates | awk '{print $1}')
  end_date=$(echo $dates | awk '{print $2}')

  psql -U airflow -d warehouse -h localhost <<EOF &
    SET TIME ZONE 'America/New_York';
    SET synchronous_commit = off;
    SET work_mem = '256MB';
    CALL hvfhv_summary_fill('$start_date', '$end_date');
EOF
done

# Wait for all procedures to finish
wait

echo "All parallel processes have completed."
