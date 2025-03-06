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
IMG_BASE_DIR = "./downloaded_images"
PDF_BASE_DIR = "./downloaded_pdf"

# Function to download an image
def download_image(url, save_path):
    try:
        if os.path.exists(save_path):
            logger.info(f"Skipped: {save_path} already exists.")
            return
        headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    }
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        with open(save_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logger.info(f"Downloaded: {url} -> {save_path}")
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")

def download_pdf(url, save_path):
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

def process_images(images_lst, img_table_dir, table_name):
 
        if isinstance(images_lst, str):
            try:
                images_lst = json.loads(images_lst)
            except json.JSONDecodeError:
                logger.warning(f"Invalid format in table {table_name}, skipping row: {images_lst}")
                return
        
        if isinstance(images_lst, list):
            for index, url in enumerate(images_lst):
                if table_name == 'xag':
                        image_name = f"{url.split('=')[-1]}.jpg"  # Replace `id` with your primary key column name
                elif table_name == 'xmg':
                        image_name = url.split('/')[-1].split('?')[0]
                elif table_name.lower() == 'xsf':
                    image_url = url
                    image_name1 = image_url.split('/')[-1].replace('.jpg','')
                    image_part2 = image_url.split('/')[-2].split('-')[0]
                    updated_image_name = f"{image_name1}_{image_part2}.jpg"
                    image_name = updated_image_name
                else:
                    image_name = url.split('/')[-1]  # Replace `id` with your primary key column name
                save_path = os.path.join(img_table_dir, image_name)
                download_image(url, save_path)

def process_pdf(pdf_lst, pdf_table_dir, table_name):

    if table_name == 'xag':
        return
    
    if isinstance(pdf_lst, dict):
        for file_name,url in pdf_lst.items():
            if '/' in file_name:
                file_name = file_name.replace('/','')
            if '.pdf' not in file_name:
                file_name = f"{file_name}.pdf"
            save_path = os.path.join(pdf_table_dir, file_name)
            download_pdf(url, save_path)

def safe_json_loads(data):
    import json
    while isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            break
    return data
    

def process_files(table_name,rows,columns):
    # Create a directory for the table
    img_table_dir = os.path.join(IMG_BASE_DIR, table_name)
    os.makedirs(img_table_dir, exist_ok=True)

    pdf_table_dir = os.path.join(PDF_BASE_DIR, table_name)
    os.makedirs(pdf_table_dir, exist_ok=True)

    for row in rows:
        images_lst = row[columns.index('images_url')] 
        
        if isinstance(images_lst, str):
            try:
                images_lst = json.loads(images_lst)
            except json.JSONDecodeError:
                logger.warning(f"Invalid format in table {table_name}, skipping row: {images_lst}")

        if images_lst:
            process_images(images_lst, img_table_dir, table_name)

        pdf_lst = row[columns.index('pdf')] 
        
        if isinstance(pdf_lst, str):
            try:
                pdf_lst = safe_json_loads(pdf_lst)
            except json.JSONDecodeError:
                logger.warning(f"Invalid format in table {table_name}, skipping row: {pdf_lst}")

        if pdf_lst:
            process_pdf(pdf_lst, pdf_table_dir, table_name)


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

            columns = list(result.keys())
            if 'images_url' not in columns and 'pdf' not in columns:
                return

            process_files(table_name,rows,columns)
                
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
        tables_to_ignore = ['requests_log','sessions']
        if not tables:
            logger.info("No tables found in the database.")
            return
        
        # Iterate through each table
        for table_name in tables:
            if table_name in tables_to_ignore:
                continue    
            logger.info(f"Processing table: {table_name}")
            process_table(engine, table_name)
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")

if __name__ == "__main__":
    main()
