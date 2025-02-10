#!/bin/bash

# Find all SQL files and execute them in Hive
find HiveDWHExploration/LoadDataToHive -type f -name "*.sql" | while read file; do
    echo "Running $file..."
    docker exec -it presto-setup_hive_1 beeline -u jdbc:hive2://localhost:10000 -f "/$file"
done

echo "All load scripts executed!"