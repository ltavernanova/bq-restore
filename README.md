### restore.sh v18 – Restaurar snapshots de BigQuery

Restaura tablas y/o rutinas almacenadas por **backup.sh** en un bucket GCS.

| Parámetro | Descripción |
|-----------|-------------|
| `YYYYMMDD` | **Fecha** del snapshot a restaurar (obligatorio). |
| `TABLAS`   | Lista de tablas separadas por comas **y/o** la palabra `routines` (opcional).<br>Si se omite, se restauran **todas** las tablas & rutinas. |

#### Variables de entorno requeridas

* `PROJECT_ID` – ID del proyecto GCP  
* `DATASET`    – Dataset destino en BigQuery  
* `BUCKET`     – Bucket donde vive el snapshot (`gs://$BUCKET/$DATE/...`)  
* `PUBSUB_TOPIC` – Tema donde se publica el resultado

Opcional: `LOCATION` (región del dataset). Si no se define, el script la autodetecta.

#### Ejemplos de uso

```bash
# Restore completo (tablas + rutinas)
./restore.sh 20250709                      

# Solo una tabla
./restore.sh 20250709 tabla1               

# Varias tablas
./restore.sh 20250709 tabla1,tabla2        

# Solo rutinas/UDF
./restore.sh 20250709 routines             

# Una tabla + rutinas
./restore.sh 20250709 tabla1,routines
 ```
#### Al finalizar publica un mensaje en PubSub
{ "date":"YYYYMMDD", "dataset":"<DATASET>", "status":"SUCCESS|FAILURE|NO_DATA" }

