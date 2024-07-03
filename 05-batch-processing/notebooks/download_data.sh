
# Interrupt the execution of the script at the occurence of the first non-zero code. e.g when it encounters an invalid url, for months that don't have data
set -e

# command-line arguments
TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`      # format MONTH to a 2-digit number

  URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"        # dynamic download url

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"          # storage location in local environment

  echo "downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}

  # # compress the local csv files. easier to read in the compressed file
  # echo "compressing ${LOCAL_PATH}"
  # gzip ${LOCAL_PATH}

done
