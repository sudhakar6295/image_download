import os
import requests
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection string for MariaDB

DATABASE_URI = 'mariadb+mariadbconnector://scrape:Nnk0okMJqXcbZxt9@170.239.84.29/scrape' # Update with your credentials
# Directory to save images
BASE_DIR = "./downloaded_images"

# Function to download an image
def download_image(url, save_path):
    try:
        if os.path.exists(save_path):
            logger.info(f"Skipped: {save_path} already exists.")
            return
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(save_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logger.info(f"Downloaded: {url} -> {save_path}")
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")

# Function to process a table
def process_table(engine, table_name):
    try:
        with engine.connect() as connection:
            # Fetch all rows from the table
            result = connection.execute(text(f"SELECT * FROM `{table_name}`"))  # Backticks for MariaDB table names
            rows = result.fetchall()
            if not rows:
                logger.info(f"No data found in table {table_name}")
                return

            result = connection.execute(text(f"SELECT * FROM `{table_name}`")) 
            columns = list(result.keys())

            if 'images' not in columns:
                return
            
            # Create a directory for the table
            table_dir = os.path.join(BASE_DIR, table_name)
            os.makedirs(table_dir, exist_ok=True)
            
            # Iterate through rows and download images
            for row in rows:
                images_lst = row[columns.index('images_url')] 
                if not images_lst:
                    continue
                
                # Convert images string to a list if needed
                if isinstance(images_lst, str):
                    try:
                        image_urls = json.loads(images_lst)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid format in table {table_name}, skipping row: {images_lst}")
                        continue
                
                if isinstance(image_urls, list):
                    for index, url in enumerate(image_urls):
                         if table_name == 'xag':
                             image_name = f"{url.split('=')[-1]}.jpg"  # Replace `id` with your primary key column name
                         else:
                            image_name = url.split('/')[-1]  # Replace `id` with your primary key column name
                         save_path = os.path.join(table_dir, image_name)
                         download_image(url, save_path)
    except SQLAlchemyError as e:
        logger.error(f"Error processing table {table_name}: {e}")

# Main function
def main():
    # Create a database engine
    engine = create_engine(
        DATABASE_URI,
        connect_args={
            "ssl": {
                "sslrootcert": "ca-prodigiodata.pem",  # Path to the CA file
                "sslmode": "verify-ca"  # SSL mode for CA verification
            }
        }
    )
    inspector = inspect(engine)
    
    try:
        # Get all table names
        tables = inspector.get_table_names()
        if not tables:
            logger.info("No tables found in the database.")
            return
        
        # Iterate through each table
        for table_name in tables:
            logger.info(f"Processing table: {table_name}")
            process_table(engine, table_name)
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")

if __name__ == "__main__":
    main()
