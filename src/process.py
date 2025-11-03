from abc import ABC, abstractmethod
import logging
import random

from delta.tables import DeltaTable
from pyspark.sql.types import StructType


class BaseProcessor(ABC):
    def __init__(self, spark, settings):
        self.spark = spark
        self.settings = settings
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def _read_source(self):
        pass

    @abstractmethod
    def _validate_settings(self):
        pass

    def _create_database(self):
        spark = self.spark
        logger = self.logger
        settings = self.settings
        database_name = settings['target']['database_name']
        database_location = settings['target']['database_path']

        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name} LOCATION '{database_location}'")
            logger.info(f"Database {database_name} created successfully.")
        except Exception as e:
            logger.error("An error occurred while creating the database: %s", str(e))
            raise e

    def _apply_sql_transformations(self, df):
        logger = self.logger
        settings = self.settings
        sql_transformations = settings['target']['sql_transformations']

        try:
            for key_transformations, list_transformations in sorted(sql_transformations.items()):
                logger.info(f"Applying SQL Transformations: {key_transformations}.")
                df = df.selectExpr("*", *list_transformations)
            logger.info("SQL transformations applied successfully.")
            return df
        except Exception as e:
            logger.error("An error occurred while applying SQL Transformations: %s", str(e))
            raise e
    
    def _drop_columns(self, df):
        logger = self.logger
        settings = self.settings
        drop_columns = settings['target']['drop_columns']

        if drop_columns:
            df = df.drop(*drop_columns)

        return df

    def _create_table(self, schema):
        spark = self.spark
        logger = self.logger
        settings = self.settings
        database_name = settings['target']['database_name']
        table_name = settings['target']['table_name']
        table_path = settings['target']['path']
        table_full_name = f"{database_name}.{table_name}"

        try:
            if not DeltaTable.isDeltaTable(spark, table_path):
                delta_table_builder = (
                    DeltaTable.createIfNotExists(spark)
                    .tableName(table_full_name)
                    .location(table_path)
                    .comment(f"Table {table_full_name} created by framework.")
                    .addColumns(schema)
                )
                delta_table = delta_table_builder.execute()
                logger.info(f"Table {table_path} created successfully.")
                return delta_table
            else:
                logger.info(f"Table {table_path} already exists.")
                return DeltaTable.forPath(spark, table_path)
        except Exception as e:
            logger.error("An error occurred while creating the table: %s", str(e))
            raise e

    def _merge_delta_table(self, table, df, source, target):
        logger = self.logger
        settings = self.settings
        primary_key = settings['target']['options']['primary_key']
        merge_condition = "\n AND ".join([f"{source}.{k} = {target}.{k}" for k in primary_key]) if primary_key else "1 = 0"
        sequence_by = settings['target']['options']['sequence_by']
        sequence_by_condition = "\n AND ".join([f"{source}.{k} >= {target}.{k}" for k in sequence_by]) if sequence_by else None
        apply_as_delete = settings['target']['options']['apply_as_delete']

        logger.info("Delta table merge starting.")
        logger.info(f"""
                    [MEGE PARAMETERS]:
                    primary_key: {primary_key}
                    merge_condition: {merge_condition}
                    sequence_by: {sequence_by}
                    apply_as_delete: {apply_as_delete}
                    """)

        try:
            merge = (
                table.alias(target)
                    .merge(
                        df.alias(source),
                        merge_condition
                    )
                    # .whenMatchedDelete(condition = f"{target}.op = 'd")
                    # .whenNotMatchedInsertAll()
            )

            if sequence_by:
                merge = merge.whenMatchedUpdateAll(condition=sequence_by_condition)
            else:
                merge = merge.whenMatchedUpdateAll()

            if apply_as_delete:
                merge = merge.whenMatchedDelete(condition=apply_as_delete)

            merge = merge.whenNotMatchedInsertAll()
            merge.execute()
            logger.info("Delta table merged successfully.")
        except Exception as e:
            logger.error("An error occurred while merging the Delta table: %s", str(e))
            raise e

    def _foreach_batch(self, df, epoch_id):
        logger = self.logger
        source = "source_"
        target = "target_"

        logger.info(f"Micro-batch id: {epoch_id}")

        self._create_database()

        df = self._apply_sql_transformations(df)

        df = self._drop_columns(df)

        table = self._create_table(df.schema)

        self._merge_delta_table(table, df, source, target)

    def _build_writer(self, df):
        settings = self.settings
        type_processing = settings["type_processing"]

        if df.isStreaming:
            writer = (
                df.writeStream
                .format(settings["target"]["format"])
                .option("path", settings["target"]["path"])
                .option("checkpointLocation", settings["target"]["options"]["checkpoint_location"])
                .outputMode(settings["target"]["options"]["mode"])
                .foreachBatch(self._foreach_batch)
                .queryName(f"writing_{type_processing}_{settings['target']['database_name']}_{settings['target']['table_name']}")
            )

            return writer

        self._foreach_batch(df, random.randint(1, 1000000000))

        return None

    def _start_streaming(self, writer):
        settings = self.settings
        type_processing = settings["type_processing"]

        if writer:
            if type_processing == "streaming":
                writer = writer.trigger(
                    processingTime="30 seconds"
                )
            else:
                writer = writer.trigger(
                    availableNow=True
                )

            writer.start().awaitTermination()

    def process(self):
        if self._validate_settings():
            df = self._read_source()
            writer = self._build_writer(df)
            self._start_streaming(writer)

    def execute(self):
        logger = self.logger

        try:
            logger.info("Data processing started.")
            self.process()
            logger.info("Data processing finished.")
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            logger.info("Data processing finished with errors.")
            raise e

class FileProcessor(BaseProcessor):
    def _read_source(self):
        spark = self.spark
        settings = self.settings

        # TODO: It's necessary to inferschema without autoloader delta feature.
        # We must to replace it by a tool autoloader like ASAP!
        if settings["source"]["format"] != "delta":
            if settings["source"]["options"]["inferSchema"] != 'true':
                if "schema" in settings["source"]:
                    try:
                        schema = StructType.fromJson(settings["source"]["schema"])
                    except Exception as e:
                        raise ValueError("Error on schema json.", e)
                else:
                    raise ValueError("No schema found on source.options.")
            else:
                static_df = (spark
                        .read
                        .format(settings["source"]["format"])
                        .options(**settings["source"]["options"])
                        .load(settings["source"]["path"])
                )
                schema = static_df.schema
                            
            # maxFilesPerTrigger=1
            df = (
                spark.readStream
                    .format(settings["source"]["format"])
                    .options(**settings["source"]["options"])
                    .schema(schema)
                    .load(settings["source"]["path"])
            )
        else:
            df = (
                spark.readStream
                    .format(settings["source"]["format"])
                    .load(settings["source"]["path"])
            )
        return df

    def _validate_settings(self):
        settings = self.settings
        if "source" not in settings:
            raise ValueError("The source settings are required.")
        if "format" not in settings["source"]:
            raise ValueError("The format is required in the source settings.")
        if "path" not in settings["source"]:
            raise ValueError("The path is required in the source settings.")
        
        return True
