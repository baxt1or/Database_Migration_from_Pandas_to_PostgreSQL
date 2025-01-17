import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def migrate_data_postgresql(user, password, host, port, database, df, table_name):
    """
    Migrate data from a pandas DataFrame to a PostgreSQL table.

    Returns:
        bool: True if migration is successful, False otherwise.
    """
  
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

   
    connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(connection_string)

    try:
        logger.info(f"Starting data migration to PostgreSQL table '{table_name}'.")

        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        logger.info(f"Data successfully migrated to table '{table_name}'.")
        return True

    except SQLAlchemyError as e:
        logger.error(f"An error occurred while migrating data to PostgreSQL: {e}")
        return False

    finally:
        engine.dispose()
        logger.info("Database connection closed.")