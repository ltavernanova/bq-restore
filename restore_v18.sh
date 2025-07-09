#!/usr/bin/env bash
# ------------------------ restore.sh v18 ------------------------
#   Restaura un snapshot de BigQuery.
#
#   Ejemplos:
#     ./restore.sh 20250709                       # todo el dataset + rutinas
#     ./restore.sh 20250709 tabla1                # solo tabla1
#     ./restore.sh 20250709 tabla1,tabla2         # varias tablas
#     ./restore.sh 20250709 routines              # solo rutinas
#     ./restore.sh 20250709 tabla1,routines       # tabla1 + rutinas
#
#   Requiere las vars: PROJECT_ID, DATASET, BUCKET, PUBSUB_TOPIC
#   Opcional: LOCATION (si no, se auto-detecta)

set -euo pipefail
shopt -s nullglob            # evita que *.sql quede literal sin matches

###############################################################################
# 0. Argumentos
###############################################################################
if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Uso: $0 YYYYMMDD [TABLA[,TABLA2…]|routines]" >&2
  exit 1
fi

DATE="$1"
RAW_FILTER="${2:-}"          # "" → dataset completo

# ——— Parseo de lista y flag routines ———
IFS=',' read -ra TOKENS <<< "$RAW_FILTER"
RESTORE_ROUTINES=false
TABLE_LIST=()

for tk in "${TOKENS[@]}"; do
  if [[ $tk == routines ]]; then
    RESTORE_ROUTINES=true
  elif [[ -n $tk ]]; then
    TABLE_LIST+=("$tk")
  fi
done

need_table(){
  [[ ${#TABLE_LIST[@]} -eq 0 ]] && return 0
  for t in "${TABLE_LIST[@]}"; do [[ $1 == "$t" ]] && return 0; done
  return 1
}

###############################################################################
# 1. Variables obligatorias
###############################################################################
: "${PROJECT_ID?}" "${DATASET?}" "${BUCKET?}" "${PUBSUB_TOPIC?}"
LOCATION=${LOCATION:-""}

log(){ echo "{\"ts\":\"$(date --iso-8601=seconds)\",\"msg\":\"$*\"}"; }
publish(){ gcloud pubsub topics publish "$PUBSUB_TOPIC" --project="$PROJECT_ID" \
           --message="$1" >/dev/null; }
trap 'publish "{\"date\":\"'"$DATE"'\",\"dataset\":\"'"$DATASET"'\",\"status\":\"FAILURE\"}"' ERR

###############################################################################
# 2. Detectar región y asegurar dataset
###############################################################################
if [[ -z $LOCATION ]]; then
  LOCATION=$(bq --project_id="$PROJECT_ID" show --format=prettyjson "$DATASET" | jq -r '.location')
  log "📍 LOCATION auto-detectado: $LOCATION"
fi

if ! bq --project_id="$PROJECT_ID" ls "$DATASET" >/dev/null 2>&1; then
  log "🆕 Creando dataset $DATASET"
  bq --project_id="$PROJECT_ID" mk --location="$LOCATION" "$DATASET"
fi

###############################################################################
# 3. Descargar artefactos al tmp
###############################################################################
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

if (( ${#TABLE_LIST[@]} )); then
  for T in "${TABLE_LIST[@]}"; do
    gsutil cp "gs://${BUCKET}/${DATE}/tables/${T}.sql" "$TMP/" || {
      log "🚫 Falta DDL para $T"; exit 1; }
  done
else
  gsutil -mq cp "gs://${BUCKET}/${DATE}/tables/*.sql" "$TMP/" || true
fi

# siempre copiamos rutinas; se aplicarán o no más adelante
gsutil -mq cp "gs://${BUCKET}/${DATE}/routines/*.sql" "$TMP/" || true

###############################################################################
# 4. Helpers
###############################################################################
is_ddl(){ grep -qiE '^\s*CREATE' "$1"; }
have_data(){ gsutil -q ls "$1" >/dev/null 2>&1; }

OK=0

###############################################################################
# 5. Procesar tablas
###############################################################################
for DDL in "$TMP"/*.sql; do
  FILE=$(basename "$DDL")
  [[ $FILE == restore_rutinas.sql ]] && continue   # saltar rutinas

  TABLE=${FILE%.sql}
  need_table "$TABLE" || continue

  SNAP=$(gsutil cat "gs://${BUCKET}/${DATE}/manifests/${TABLE}.txt" 2>/dev/null || echo "$DATE")
  URI="gs://${BUCKET}/${SNAP}/${TABLE}/${TABLE}-*.parquet"

  if is_ddl "$DDL"; then
    log "🛠 DDL para $TABLE"
    perl -pe 'if($.==1){s/CREATE TABLE `[^`]+`/CREATE OR REPLACE TABLE `'"$PROJECT_ID.$DATASET.$TABLE"'`/i}' \
      "$DDL" | bq query -q --project_id="$PROJECT_ID" --location="$LOCATION" \
                       --use_legacy_sql=false >/dev/null
  else
    log "⚠️ $TABLE sin DDL (autodetectar)"
  fi

  if ! have_data "$URI"; then
    log "🚫 Sin Parquet para $TABLE"; continue
  fi

  REPL=""
  if bq --project_id="$PROJECT_ID" show --location="$LOCATION" "$DATASET.$TABLE" >/dev/null 2>&1; then
    REPL="--replace"
  fi

  log "📥 Cargando $TABLE (bq load $REPL)"
  if bq -q load $REPL --source_format=PARQUET --autodetect \
        --project_id="$PROJECT_ID" --location="$LOCATION" \
        "${PROJECT_ID}:${DATASET}.${TABLE}" "$URI"; then
    (( OK+=1 ))
  else
    log "❌ Error cargando $TABLE"
  fi
done

###############################################################################
# 6. Rutinas / UDF
###############################################################################
ROUT_SRC="$TMP/restore_rutinas.sql"
if [[ -s "$ROUT_SRC" && ( $RESTORE_ROUTINES = true || ${#TABLE_LIST[@]} -eq 0 ) ]]; then
  log "🔧 Restaurando rutinas/UDF"
  sed -e 's/^CREATE /CREATE OR REPLACE /I' "$ROUT_SRC" \
  | bq query -q --project_id="$PROJECT_ID" \
             --location="$LOCATION" --use_legacy_sql=false
fi

###############################################################################
# 7. Resultado final
###############################################################################
if (( OK > 0 || RESTORE_ROUTINES == true )); then
  STATUS="SUCCESS"
else
  STATUS="NO_DATA"
fi

publish "{\"date\":\"$DATE\",\"dataset\":\"$DATASET\",\"status\":\"$STATUS\"}"
log "✅ Restore terminado — $OK tablas restauradas"

