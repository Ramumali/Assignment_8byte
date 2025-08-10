# workspace/jobs/fetch_stock_job.py
from dagster import job, op, In, Out, String, get_dagster_logger
import os
from libs.stock_loader import fetch_and_store

LOGGER = get_dagster_logger()

@op(config_schema={"symbol": String, "outputsize": String}, out=Out(int))
def fetch_and_save_op(context):
    symbol = context.op_config["symbol"]
    outputsize = context.op_config.get("outputsize", "compact")
    LOGGER.info("Running fetch_and_save_op for %s", symbol)
    try:
        count = fetch_and_store(symbol, outputsize)
        LOGGER.info("Fetched and stored %d rows for %s", count, symbol)
        return count
    except Exception as e:
        LOGGER.error("Error in fetch_and_save_op for %s: %s", symbol, e)
        # re-raise so Dagster marks this run as failed
        raise

@job
def fetch_stock_job():
    fetch_and_save_op()