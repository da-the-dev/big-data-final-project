#!/bin/bash
password=$(head -n 1 secrets/.hive.pass)

DASHBOARD_QUERIES="sql/dashboard"
DASHBOARD_RESULTS="output/dashboard"
HDFS_OUTPUT_DIR="project/output"
HDFS_WAREHOUSE_DIR="project/hive/warehouse"

rm -rf "${DASHBOARD_RESULTS}"
mkdir -p "${DASHBOARD_RESULTS}"

for hql_path in "${DASHBOARD_QUERIES}"/*.hql; do
    base_name=$(basename "${hql_path}" .hql)

    csv_path="${DASHBOARD_RESULTS}/${base_name}.csv"
    log_path="${DASHBOARD_RESULTS}/${base_name}.log"
    err_path="${DASHBOARD_RESULTS}/${base_name}.err"

    hdfs_output="${HDFS_OUTPUT_DIR}/${base_name}"
    warehouse_path="${HDFS_WAREHOUSE_DIR}/${base_name}"

    result_table="${base_name}_results"
    sh_path="${DASHBOARD_QUERIES}/${base_name}.sh"

    [[ -f "${sh_path}" ]] || continue

    hdfs dfs -rm -r -f "${hdfs_output}" >/dev/null 2>&1 || true
    hdfs dfs -rm -r -f "${warehouse_path}" >/dev/null 2>&1 || true

    beeline \
        -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
        -n team26 -p "$password" \
        --hivevar RESULT_TABLE="${result_table}" \
        --hivevar OUTPUT_PATH="${hdfs_output}" \
        --hivevar WAREHOUSE_PATH="${warehouse_path}" \
        -f "${hql_path}" >"${log_path}" 2>"${err_path}"

    RESULT_TABLE="${result_table}" bash "${sh_path}" "${csv_path}" >>"${log_path}" 2>>"${err_path}"

    hdfs dfs -cat "${hdfs_output}"/* >>"${csv_path}" 2>>"${err_path}"
done
