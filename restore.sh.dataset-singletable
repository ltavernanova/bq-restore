#!/usr/bin/env bash
# ------------------------ restore.sh v15 ------------------------
#   Restaura un snapshot completo **o** una tabla puntual.
#   Ej.: 
#     ./restore.sh '{"date":"20250707"}'                        # todo el dataset
#     ./restore.sh '{"date":"20250707","table":"sentinel1_r11_bands"}'     # solo esa tabla
set -euo pipefail
shopt -s nullglob                     # evita que *.sql quede literal si no hay matches

# --- DEBUG -------------------------------------------------------------
if [[ ${DEBUG_RESTORE:-0} -eq 1 ]]; then
  echo "DEBUG argc=$#"
  i=0; for a in "$@"; do echo "DEBUG argv[$((i++))]=$a"; done
  set -x       # traza cada comando del script
fi
# ----------------------------------------------------------------------


###############################################################################
# 0. Argumentos: DATE [TABLE]
###############################################################################
if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Uso: $0 YYYYMMDD [TABLE]" >&2
  exit 1
fi

DATE="$1"             # siempre requerido
TABLE_FILTER="${2:-}" # vacío ⇒ restaurar todo el snapshot


###############################################################################
# 0. Argumento JSON
###############################################################################
#if [[ $# -ne 1 ]]; then
#  echo "Uso: $0 '{\"date\":\"YYYYMMDD\"[,\"table\":\"NOMBRE\"]}'" >&2; exit 1; fi
#command -v jq >/dev/null || { echo "❌  jq no instalado" >&2; exit 1; }

#DATE=$(jq -r .date  <<<"$1")
#TABLE_FILTER=$(jq -r '.table // empty' <<<"$1")     # "" si no se pide tabla

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
# 2. Detectar región
###############################################################################
if [[ -z "$LOCATION" ]]; then
  LOCATION=$(bq --project_id="$PROJECT_ID" show --format=prettyjson "$DATASET" | jq -r '.location')
  log "📍  LOCATION auto-detectado: $LOCATION"
fi

###############################################################################
# 3. Asegurar dataset
###############################################################################
if ! bq --project_id="$PROJECT_ID" ls "$DATASET" >/dev/null 2>&1; then
  log "🆕  Creando dataset $DATASET en $LOCATION"
  bq --project_id="$PROJECT_ID" mk --location="$LOCATION" "$DATASET"
fi

###############################################################################
# 4. Descargar artefactos
###############################################################################
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

if [[ -n "$TABLE_FILTER" ]]; then
  gsutil cp "gs://${BUCKET}/${DATE}/tables/${TABLE_FILTER}.sql" "$TMP/" || {
    log "🚫  No existe DDL para ${TABLE_FILTER}"; exit 1; }
else
  gsutil -m cp "gs://${BUCKET}/${DATE}/tables/*.sql" "$TMP/" || true
fi
gsutil -m cp "gs://${BUCKET}/${DATE}/routines/*.sql" "$TMP/" || true

###############################################################################
# 5. Helpers
###############################################################################
is_ddl(){ grep -qiE '^\s*CREATE' "$1"; }
have_files(){ gsutil -q ls "$1" >/dev/null 2>&1; }

OK=0

###############################################################################
# 6. Procesar tablas
###############################################################################
for DDL in "$TMP"/*.sql; do
  FILE=$(basename "$DDL"); [[ "$FILE" == "restore_rutinas.sql" ]] && continue
  TABLE=${FILE%.sql}

  # Si se pidió tabla específica y no coincide, saltar
  [[ -n "$TABLE_FILTER" && "$TABLE" != "$TABLE_FILTER" ]] && continue

  SNAP=$(gsutil cat "gs://${BUCKET}/${DATE}/manifests/${TABLE}.txt" 2>/dev/null || echo "$DATE")
  URI="gs://${BUCKET}/${SNAP}/${TABLE}/${TABLE}-*.parquet"

  if is_ddl "$DDL"; then
    log "🛠  DDL para $TABLE"
    perl -pe 'if($.==1){s/CREATE TABLE `[^`]+`/CREATE OR REPLACE TABLE `'"$PROJECT_ID.$DATASET.$TABLE"'`/i}' \
      "$DDL" | bq query -q --project_id="$PROJECT_ID" \
      --location="$LOCATION" --use_legacy_sql=false >/dev/null
  else
    log "⚠️  $TABLE sin DDL — se autodescubrirá"
  fi

  if ! have_files "$URI"; then
    log "🚫  Sin Parquet para $TABLE"; continue; fi

  if bq --project_id="$PROJECT_ID" show --location="$LOCATION" "$DATASET.$TABLE" >/dev/null 2>&1; then
    REPL="--replace"; else REPL=""; fi

  log "📥  Cargando $TABLE (bq load $REPL)"
  if bq -q load $REPL --source_format=PARQUET --autodetect \
         --project_id="$PROJECT_ID" --location="$LOCATION" \
         "${PROJECT_ID}:${DATASET}.${TABLE}" "$URI"; then
    (( OK+=1 ))
  else
    log "❌  Error cargando $TABLE"
  fi
done


###############################################################################
# 7. Rutinas / UDF (solo si es restore completo)
###############################################################################
ROUT_SRC="$TMP/restore_rutinas.sql"
if [[ -z "$TABLE_FILTER" && -s "$ROUT_SRC" ]]; then
  log "🔧 Restaurando rutinas/UDF"
  sed -e 's/^CREATE /CREATE OR REPLACE /' \
      -e "s/\`bold-api-dev\`\.$DATASET/\`$DATASET\`/g" \
      "$ROUT_SRC" \
  | bq query -q --project_id="$PROJECT_ID" \
             --location="$LOCATION" --use_legacy_sql=false
fi

###############################################################################
# 7. Rutinas / UDF (sólo si es restore completo)
###############################################################################
#ROUT_SRC="$TMP/restore_rutinas.sql"
#if [[ -z "$TABLE_FILTER" && -f "$ROUT_SRC" ]] && grep -qiE '^\s*"?CREATE FUNCTION' "$ROUT_SRC"; then
#  log "🔧  Restaurando rutinas/UDF"
#  sed -e 's/^"CREATE FUNCTION/CREATE OR REPLACE FUNCTION/' \
#      -e 's/^"//' -e 's/;;"$/;/' \
#      -e "s/\`bold-api-dev\`\.$DATASET/\`$DATASET\`/g" \
#      -e "s@\"\"gs://@'gs://@g" -e "s@\.js\"\"@.js'@g" \
#      -e 's/"""/"/g' "$ROUT_SRC" \
#  | bq query -q --project_id="$PROJECT_ID" --location="$LOCATION" \
#             --dataset_id="$DATASET" --use_legacy_sql=false
#fi

###############################################################################
# 8. Resultado final
###############################################################################
# 8. Resultado final
###############################################################################
if (( OK > 0 )); then
  STATUS="SUCCESS"
else
  STATUS="NO_DATA"
fi

publish "{\"date\":\"$DATE\",\"dataset\":\"$DATASET\",\"status\":\"$STATUS\"}"
log "✅  Restore terminado — $OK tablas restauradas"

