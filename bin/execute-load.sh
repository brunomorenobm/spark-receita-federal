#!/usr/bin/env bash
TIMESTAMP=$(date +%s)
DATA_PATH=/data
LANDING_PATH=${DATA_PATH}/landing/receita-federal
BKP_PATH=${DATA_PATH}/bkp/receita-federal/${TIMESTAMP}
GS_PATH=gs://<your-googlecloud-project-name>/public
INSTALL_PATH=/opt/justto/tools

# Download Unzip and Upload File
downloadFile () {
       local INDEX=$1
       # Download file
       wget -O ${LANDING_PATH}/DADOS_ABERTOS_CNPJ_${INDEX}.zip http://200.152.38.155/CNPJ/DADOS_ABERTOS_CNPJ_${INDEX}.zip

       # Unzip New Version of the Data
       unzip ${LANDING_PATH}/DADOS_ABERTOS_CNPJ_${INDEX}.zip -d ${LANDING_PATH}/

       # Upload file to google storage
       gsutil -o GSUtil:parallel_composite_upload_threshold=150M cp ${LANDING_PATH}/*${INDEX}  ${GS_PATH}/receita-federal/landing/
       rm -rf ${LANDING_PATH}/*${INDEX}
}

echo "Variables: TIMESTAMP=${TIMESTAMP}|DATA_PATH=${DATA_PATH}|LANDING_PATH=${LANDING_PATH}|LANDING_PATH=${LANDING_PATH}|BKP_PATH=${BKP_PATH}|GS_PATH=${GS_PATH}|INSTALL_PATH=${INSTALL_PATH}"

# Init folders
echo "Creating Folder: ${BKP_PATH}"
mkdir -p ${BKP_PATH}

echo "Creating Folder: ${LANDING_PATH}"
mkdir -p ${LANDING_PATH}


#Move bkp files
echo "Moving local files from ${LANDING_PATH}/* to: ${BKP_PATH}/"
mv ${LANDING_PATH}/* ${BKP_PATH}/

# Move Google Storge files
echo "Moving Google storage files from: ${GS_PATH}/receita-federal/landing/ to: ${GS_PATH}/receita-federal/bkp/landing_${TIMESTAMP}"
gsutil -m mv ${GS_PATH}/receita-federal/landing ${GS_PATH}/receita-federal/bkp/landing_${TIMESTAMP}

# Execute All 20 files download in parallel included more 10 files for safety
#TODO create a function to read the total of files
for run in {1..30}
 do
   printf -v run "%02d" $run
   echo "Calling function download with parameter run=${run}"
   downloadFile "$run" &
 done
 wait

# Start Spark load
echo "Starting workflow execution with file: ${INSTALL_PATH}/receita-federal/workflows/justto-receita-federal.yml content: `cat ${INSTALL_PATH}/receita-federal/workflows/dataproc-receita-federal.yml` "
gcloud dataproc workflow-templates instantiate-from-file --file ${INSTALL_PATH}/receita-federal/workflows/dataproc-receita-federal.yml

# Remove Extracted Files
rm -rf ${LANDING_PATH}/F*