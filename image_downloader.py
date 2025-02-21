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
# Directories for saving files
IMAGE_DIR = "./downloaded_images"
PDF_DIR = "./pdf_download"

# Function to download a file (for both images and PDFs)
def download_file(url, save_path):
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

# Function to process a table (for images and PDFs)
def process_table(engine, table_name):
    try:
        with engine.connect() as connection:
            # Fetch all rows from the table
            result = connection.execute(text(f"SELECT * FROM `{table_name}`"))
            rows = result.fetchall()
            if not rows:
                logger.info(f"No data found in table {table_name}")
                return

            result = connection.execute(text(f"SELECT * FROM `{table_name}`")) 
            columns = list(result.keys())

            # Create directories for this table
            table_image_dir = os.path.join(IMAGE_DIR, table_name)
            table_pdf_dir = os.path.join(PDF_DIR, table_name)
            os.makedirs(table_image_dir, exist_ok=True)
            os.makedirs(table_pdf_dir, exist_ok=True)

            for row in rows:
                # Process Images
                if 'images_url' in columns:
                    images_lst = row[columns.index('images_url')]
                    if images_lst:
                        process_files(images_lst, table_image_dir, "image")

                # Process PDFs
                if 'pdf_url' in columns:
                    pdf_lst = row[columns.index('pdf_url')]
                    if pdf_lst:
                        process_files(pdf_lst, table_pdf_dir, "pdf")

    except SQLAlchemyError as e:
        logger.error(f"Error processing table {table_name}: {e}")

# Function to process and download images or PDFs
def process_files(file_lst, save_dir, file_type):
    if isinstance(file_lst, str):
        try:
            file_urls = json.loads(file_lst)
        except json.JSONDecodeError:
            logger.warning(f"Invalid format in {save_dir}, skipping row: {file_lst}")
            return

    if isinstance(file_urls, list):
        for url in file_urls:
            file_name = get_file_name(url, file_type)
            save_path = os.path.join(save_dir, file_name)
            download_file(url, save_path)

# Function to generate filenames based on URL
def get_file_name(url, file_type):
    # Extract the filename from the URL
    file_name = url.split('/')[-1]
    
    # Remove query parameters (e.g., ?unique=110d0b1)
    file_name = file_name.split('?')[0]
    
    # Replace invalid characters with underscores
    file_name = re.sub(r'[<>:"/\\|?*]', '_', file_name)
    
    # Ensure correct file extension
    if file_type == "image" and not file_name.endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
        file_name += ".jpg"  # Default to .jpg for images
    elif file_type == "pdf" and not file_name.endswith('.pdf'):
        file_name += ".pdf"  # Ensure .pdf extension

    return file_name

# Main function
def main():
    engine = create_engine(
        DATABASE_URI,
        connect_args={
            "ssl": {
                "sslrootcert": "ca-prodigiodata.pem",
                "sslmode": "verify-ca"
            }
        }
    )
    inspector = inspect(engine)

    try:
        tables = inspector.get_table_names()
        if not tables:
            logger.info("No tables found in the database.")
            return

        for table_name in tables:
            logger.info(f"Processing table: {table_name}")
            process_table(engine, table_name)
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")

if __name__ == "__main__":
    main()
