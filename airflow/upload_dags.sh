set -e
set -o xtrace
set -o pipefail

dag_gcs_prefix=${1}

if [ -z "${dag_gcs_prefix}" ]; then
    echo "Usage: $0 <dag_gcs_prefix>"
    exit 1
fi

cp -r ../cli/polygonetl ./dags/
gsutil -m cp -r dags/* ${dag_gcs_prefix}