# Initialisation du Data Warehouse.
# A lancer une seule fois, apres "docker compose up -d --build".

Write-Host "Application du schema dimensionnel..."
docker cp sql\01_schema.sql taiss_postgres:/tmp/01_schema.sql
docker exec taiss_postgres psql -U taiss -d warehouse -f /tmp/01_schema.sql

Write-Host "Creation de la table de staging..."
docker cp sql\02_staging.sql taiss_postgres:/tmp/02_staging.sql
docker exec taiss_postgres psql -U taiss -d warehouse -f /tmp/02_staging.sql

Write-Host "Creation des vues analytiques..."
docker cp sql\03_views.sql taiss_postgres:/tmp/03_views.sql
docker exec taiss_postgres psql -U taiss -d warehouse -f /tmp/03_views.sql

Write-Host "Activation du DAG..."
docker exec taiss_airflow_scheduler airflow dags unpause retail_sales_pipeline

Write-Host ""
Write-Host "Initialisation terminee."
Write-Host "  Airflow    http://localhost:8080   admin/admin"
Write-Host "  API        http://localhost:8000/docs"
Write-Host "  Prometheus http://localhost:9090"
Write-Host "  Grafana    http://localhost:3000   admin/admin"
Write-Host ""
Write-Host "Lancer le pipeline :"
Write-Host "  docker exec -it taiss_airflow_scheduler airflow dags trigger retail_sales_pipeline"