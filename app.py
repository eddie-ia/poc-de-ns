from pyspark.sql.functions import col, to_date, max as spark_max, expr
from pyspark.sql.utils import AnalysisException
import os


class CovidIngestor:

    # Evitar usar la Access Key del Storage Account directamente; usar Azure Key Vault para mayor seguridad.
    configs = {
        "fs.azure.account.key.stcoviddevlake01.blob.core.windows.net":
        "phxxxx"
    }
    def mount(container_name, mount_point):
        mount_path = f"/mnt/{mount_point}"
        if any(m.mountPoint == mount_path for m in dbutils.fs.mounts()):
            print(f"Previamente montado: {container_name}")
        else:
            dbutils.fs.mount(
                source=f"wasbs://{container_name}@stcoviddevlake01.blob.core.windows.net/",
                mount_point=mount_path,
                extra_configs=CovidIngestor.configs
            )
            print(f"Montado: {container_name}: {mount_path}")

    def load_csv_to_delta_replace_by_date(csv_path, delta_path, date_column="date_"):
        df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        df_csv = df_csv.withColumn(date_column, to_date(col(date_column)))

        try:
            # Leer Delta existente
            df_delta = spark.read.format("delta").load(delta_path)
            fechas_csv = df_csv.select(date_column).distinct()
            df_delta_filtrado = df_delta.join(fechas_csv, on=date_column, how="anti")

            # unir los datos limpios con los nuevos
            df_union = df_delta_filtrado.unionByName(df_csv)

            # Sobrescribir tabla completa (particionado por fecha) // segun la seccion 7
            df_union.write.format("delta") \
                .mode("overwrite") \
                .partitionBy(date_column) \
                .save(delta_path)

            print(f"Reemplazo completo realizado para fechas en {csv_path}")

        except AnalysisException:
            # Si no existe Delta Table, crearla por 1era vez
            df_csv.write.format("delta") \
                .mode("overwrite") \
                .partitionBy(date_column) \
                .save(delta_path)

            print("Tabla Delta creada desde cero.")

    def export_last_30_days(delta_path, export_path, date_column="date_"):
        df = spark.read.format("delta").load(delta_path)
        df = df.withColumn(date_column, to_date(col(date_column)))

        max_date = df.agg(spark_max(date_column)).collect()[0][0]
        if not max_date:
            print("Fecha invalida")
            return

        df_last_30 = df.filter(
            (col(date_column) >= expr(f"date_sub('{max_date}', 30)")) &
            (col(date_column) <= expr(f"'{max_date}'"))
        )

        df_last_30.write.mode("overwrite").option("header", "true").csv(export_path)
        print(f"Exportado: {export_path}")

# montar solo una vez
CovidIngestor.mount("analytics", "analytics")
CovidIngestor.mount("master", "master")

# definir rutas
csv_path = "/mnt/master/*.csv"
delta_path = "/mnt/analytics/delta/covid19"
export_path = "/mnt/analytics/pbi/covid19_csv"

# ejecutar flujo completo
CovidIngestor.load_csv_to_delta_replace_by_date(csv_path, delta_path)
CovidIngestor.export_last_30_days(delta_path, export_path)
