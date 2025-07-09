 ------------------------ restore.sh v18 ------------------------
   Restaura un snapshot de BigQuery.

   Ejemplos:
     ./restore.sh 20250709                       # todo el dataset + rutinas
     ./restore.sh 20250709 tabla1                # solo tabla1
     ./restore.sh 20250709 tabla1,tabla2         # varias tablas
     ./restore.sh 20250709 routines              # solo rutinas
     ./restore.sh 20250709 tabla1,routines       # tabla1 + rutinas

   Requiere las vars: PROJECT_ID, DATASET, BUCKET, PUBSUB_TOPIC
   Opcional: LOCATION (si no, se auto-detecta)
