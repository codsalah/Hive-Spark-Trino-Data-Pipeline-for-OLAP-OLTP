#!/bin/bash

HDFS_PLAYER_DIM_CSV="/CsvStore/PlayerDim_csv"
HDFS_TIME_DIM_CSV="/CsvStore/TimeDim_csv"
HDFS_CLUB_DIM_CSV="/CsvStore/ClubDim_csv"
HDFS_COMPETITION_DIM_CSV="/CsvStore/CompetitionDim_csv"

HDFS_PLAYER_DIM_PARQUET="/CsvStore/PlayerDim_parquet"
HDFS_TIME_DIM_PARQUET="/CsvStore/TimeDim_parquet"
HDFS_CLUB_DIM_PARQUET="/CsvStore/ClubDim_parquet"
HDFS_COMPETITION_DIM_PARQUET="/CsvStore/CompetitionDim_parquet"

LOCAL_PLAYER_DIM_CSV="./DataSchema/Star_schema/PlayersDim_csv"
LOCAL_TIME_DIM_CSV="./DataSchema/Star_schema/TimeDim_csv"
LOCAL_CLUB_DIM_CSV="./DataSchema/Star_schema/ClubDim_csv"
LOCAL_COMPETITION_DIM_CSV="./DataSchema/Star_schema/CompetitionDim_csv"

LOCAL_PLAYER_DIM_PARQUET="./DataSchema/Star_schema/PlayersDim_parquet"
LOCAL_TIME_DIM_PARQUET="./DataSchema/Star_schema/TimeDim_parquet"
LOCAL_CLUB_DIM_PARQUET="./DataSchema/Star_schema/ClubDim_parquet"
LOCAL_COMPETITION_DIM_PARQUET="./DataSchema/Star_schema/CompetitionDim_parquet"



# Check before Upload
echo "Checking and creating directories on HDFS if necessary..."
hdfs dfs -test -d $HDFS_PLAYER_DIM_CSV || hdfs dfs -mkdir -p $HDFS_PLAYER_DIM_CSV
hdfs dfs -test -d $HDFS_TIME_DIM_CSV || hdfs dfs -mkdir -p $HDFS_TIME_DIM_CSV
hdfs dfs -test -d $HDFS_CLUB_DIM_CSV || hdfs dfs -mkdir -p $HDFS_CLUB_DIM_CSV
hdfs dfs -test -d $HDFS_COMPETITION_DIM_CSV || hdfs dfs -mkdir -p $HDFS_COMPETITION_DIM_CSV

hdfs dfs -test -d $HDFS_PLAYER_DIM_PARQUET || hdfs dfs -mkdir -p $HDFS_PLAYER_DIM_PARQUET
hdfs dfs -test -d $HDFS_TIME_DIM_PARQUET || hdfs dfs -mkdir -p $HDFS_TIME_DIM_PARQUET
hdfs dfs -test -d $HDFS_CLUB_DIM_PARQUET || hdfs dfs -mkdir -p $HDFS_CLUB_DIM_PARQUET
hdfs dfs -test -d $HDFS_COMPETITION_DIM_PARQUET || hdfs dfs -mkdir -p $HDFS_COMPETITION_DIM_PARQUET


# Upload files to HDFS by format
upload_to_hdfs() {
    local local_dir=$1
    local hdfs_dir=$2
    local file_format=$3

    if [ -d "$local_dir" ]; then
        files_to_upload=$(find "$local_dir" -type f -name "*.$file_format")
        if [ -n "$files_to_upload" ]; then
            for file in $files_to_upload; do
                echo "Uploading $file to $hdfs_dir"
                hdfs dfs -put "$file" "$hdfs_dir"
            done
        else
            echo "No $file_format files found in $local_dir"
        fi
    else
        echo "Directory $local_dir does not exist."
    fi  
}


echo "Verifying the upload..."

verify_upload() {
    local hdfs_dir=$1
    echo "Checking HDFS directory: $hdfs_dir"

    files=$(hdfs dfs -ls $hdfs_dir)
  
    # Check if the directory is empty or not
    if [ -z "$files" ]; then
        echo "No files found in $hdfs_dir."
    else
        echo "Files found in $hdfs_dir:"
        echo "$files"
    fi
}


verify_upload $HDFS_PLAYER_DIM_CSV
verify_upload $HDFS_TIME_DIM_CSV
verify_upload $HDFS_CLUB_DIM_CSV
verify_upload $HDFS_COMPETITION_DIM_CSV

verify_upload $HDFS_PLAYER_DIM_PARQUET
verify_upload $HDFS_TIME_DIM_PARQUET
verify_upload $HDFS_CLUB_DIM_PARQUET
verify_upload $HDFS_COMPETITION_DIM_PARQUET

echo "All tasks completed successfully ^_^"






















