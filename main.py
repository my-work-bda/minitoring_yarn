import sys  
import logging  
from datetime import datetime    
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup    
import requests    
import polars as pl    
import re    
import time
import pytz    
import os  
import json
import pandas as pd    
  
# Configure logging  
logging.basicConfig(  
    level=logging.INFO,  # Set the logging level  
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format  
    handlers=[  
        logging.FileHandler("yarn_scraper.log"),  # Log to a file  
        logging.StreamHandler()  # Also log to console  
    ]  
)  
  
url_yarn = os.environ.get('URL_YARN')  
mysql_access = os.environ.get('MYSQL_ACCESS')  
interval = int(os.environ.get('INTERVAL'))  
  
# Get table names from command-line arguments  
if len(sys.argv) != 4:  
    logging.error("Usage: python main.py <table_name1> <table_name2> <table_name3>")  
    sys.exit(1)  
  
table_name = [sys.argv[1], sys.argv[2], sys.argv[3]]  
 

def scraping_all_yarn_resource():    
    try:    
        response = requests.get(url_yarn, timeout=10)    
        response.raise_for_status()  # Check for HTTP errors    
    except requests.exceptions.RequestException as e:    
        logging.error(f"Error while accessing the URL: {e}")    
        return None  # Return None if there's an issue with the request    
        
    # Parse HTML content    
    soup = BeautifulSoup(response.content, "html.parser")    
    table = soup.find("table", {"id": "metricsoverview"})    
        
    if table:    
        # Extract headers    
        headers = [header.text.strip() for header in table.find_all("th")]    
        
        # Extract rows (skip the header row)    
        rows = table.find_all("tr")[1:]    
        
        # Parse the data into a list of dictionaries    
        data = []    
        for row in rows:    
            cols = [col.text.strip() for col in row.find_all("td")]    
            if cols:    
                data.append(dict(zip(headers, cols)))    
        
        for row in data:    
            # Extract values from complex columns using regex    
            used_match = re.search(r"<memory:(\d+(\.\d+)?)\sGB, vCores:(\d+)>", row.get("Used Resources", ""))    
            total_match = re.search(r"<memory:(\d+(\.\d+)?)\s(GB|TB), vCores:(\d+)>", row.get("Total Resources", ""))    
            reserved_match = re.search(r"<memory:(\d+)\sB, vCores:(\d+)>", row.get("Reserved Resources", ""))    
        
            # Convert units if necessary    
            if used_match:    
                row["Used Resources Vcpu"] = int(used_match.group(3))    
                row["Used Resources Memory"] = float(used_match.group(1))    
        
            if total_match:    
                memory_value = float(total_match.group(1))    
                memory_unit = total_match.group(3)    
                if memory_unit == "TB":    
                    memory_value *= 1024  # Convert TB to GB    
                row["Total Resources Memory GB"] = int(memory_value)    
                row["Total Resources Vcpu"] = int(total_match.group(4))    
        
            if reserved_match:    
                row["Reserved Resources Vcpu"] = int(reserved_match.group(2))    
                row["Reserved Resources Memory"] = 0  # No memory information in this column    
        
        # Format data and ensure the necessary columns are available    
        formatted_data = [    
            {    
                "Apps Submitted": int(row.get("Apps Submitted", 0)),    
                "Apps Pending": int(row.get("Apps Pending", 0)),    
                "Apps Running": int(row.get("Apps Running", 0)),    
                "Apps Completed": int(row.get("Apps Completed", 0)),    
                "Containers Running": int(row.get("Containers Running", 0)),    
                "Used Resources Vcpu": row.get("Used Resources Vcpu", 0),    
                "Used Resources Memory GB": row.get("Used Resources Memory", 0),    
                "Total Resources Memory GB": row.get("Total Resources Memory GB", 0),    
                "Total Resources Vcpu": row.get("Total Resources Vcpu", 0),    
                "Physical Mem Used %": int(row.get("Physical Mem Used %", 0)),    
                "Physical VCores Used %": int(row.get("Physical VCores Used %", 0)),    
            }    
            for row in data    
        ]    
        
        return pl.DataFrame(formatted_data)    
    else:    
        logging.warning("Table not found on the page.")    
        return None    
    
def scraping_yarn_job_running():    
    try:    
        response = requests.get(url_yarn+"/apps/RUNNING", timeout=10)    
        response.raise_for_status()  # Check for HTTP errors    
    except requests.exceptions.RequestException as e:    
        logging.error(f"Error while accessing the URL: {e}")    
        return None  # Return None if there's an issue with the request    
        
    # Parse HTML content    
    soup = BeautifulSoup(response.content, "html.parser")    
        
    # Find data table in JavaScript    
    script = soup.find("script", string=lambda t: "var appsTableData=" in t)    
    if script:    
        # Extract data from JavaScript    
        table_data = script.string.split("var appsTableData=")[-1].strip()    
        table_data = table_data.split(";")[0]    
            
        # Convert data to Python list    
        apps_data = json.loads(table_data)    
            
        # Define table columns    
        columns = [    
            "ID", "User", "Name", "Application Type", "Application Tags",    
            "Queue", "Application Priority", "Start Time", "Launch Time",    
            "Finish Time", "State", "Final Status", "Running Containers",    
            "Allocated CPU VCores", "Allocated Memory MB", "Allocated GPUs",    
            "Reserved CPU VCores", "Reserved Memory MB", "Reserved GPUs",    
            "% of Queue", "% of Cluster", "Progress", "Tracking UI", "Blacklisted Nodes"    
        ]    
            
        # Create DataFrame    
        df = pd.DataFrame(apps_data, columns=columns)    
            
        # Function to extract ID from ID column    
        def extract_id(id_str):    
            match = re.search(r'application_\d+_\d+', id_str)    
            return match.group(0) if match else None    
            
        # Function to extract URL from Tracking UI column    
        def extract_tracking_ui(url_str):    
            match = re.search(r"'(http://[^']+)'", url_str)    
            return match.group(1) if match else None    
            
        # Apply functions to ID and Tracking UI columns    
        df['ID'] = df['ID'].apply(extract_id)    
        df['Tracking UI'] = df['Tracking UI'].apply(extract_tracking_ui)    
            
        df['Start Time'] = pd.to_datetime(df['Start Time'].astype(float), unit='ms')    
        df['Launch Time'] = pd.to_datetime(df['Launch Time'].astype(float), unit='ms')    
        df['Finish Time'] = pd.to_datetime(df['Finish Time'].astype(float), unit='ms')    
            
        # Drop Progress column    
        df.drop(columns=['Progress'], inplace=True)    
            
        # Rename columns    
        df = df.rename(columns={    
            "ID": "id",    
            "Name": "name",    
            "Application Type": "application_type",    
            "Running Containers": "running_containers",    
            "Allocated CPU VCores": "allocated_cpu_vcores",    
            "Allocated Memory MB": "allocated_memory_mb",    
            "Reserved CPU VCores": "reserved_cpu_vcores",    
            "Reserved Memory MB": "reserved_memory_mb",    
            "% of Cluster": "usage_of_cluster",    
            "Tracking UI": "tracking_ui"    
        })    
            
        # Convert columns to numeric types with default value of 0 for null values
        df['running_containers'] = pd.to_numeric(df['running_containers'], errors='coerce').fillna(0).astype(int)
        df['allocated_cpu_vcores'] = pd.to_numeric(df['allocated_cpu_vcores'], errors='coerce').fillna(0).astype(int)
        df['allocated_memory_mb'] = pd.to_numeric(df['allocated_memory_mb'], errors='coerce').fillna(0).astype(int)
        df['reserved_cpu_vcores'] = pd.to_numeric(df['reserved_cpu_vcores'], errors='coerce').fillna(0).astype(int)
        df['reserved_memory_mb'] = pd.to_numeric(df['reserved_memory_mb'], errors='coerce').fillna(0).astype(int)
        df['usage_of_cluster'] = pd.to_numeric(df['usage_of_cluster'], errors='coerce').fillna(0).astype(float)
        # Split the "name" column and take the first part as "prefix"
        df['prefix'] = df['name'].str.split("-").str[0].str.strip().fillna('')
            
        return df  
    else:    
        logging.warning("Data table not found.")    
        return None    
  
def calculate_group_usages():  
    df_list_job = scraping_yarn_job_running()  
    if df_list_job is None:  
        return None  
  
    # Group by prefix and sum the relevant columns    
    df_usage_groups = df_list_job.groupby("prefix")[["allocated_cpu_vcores", "allocated_memory_mb", "reserved_cpu_vcores", "reserved_memory_mb", "usage_of_cluster"]].sum().reset_index()  
          
    all_prefixes = ['BDA', 'GG', 'SM']  
  
    # DataFrame for dummy rows  
    dummy_rows = []  
  
    # Loop to check each prefix  
    for prefix in all_prefixes:  
        if prefix not in df_usage_groups['prefix'].values:  
            dummy_row = {  
                'prefix': prefix,  
                'allocated_cpu_vcores': 0,  
                'allocated_memory_mb': 0,  
                'reserved_cpu_vcores': 0,  
                'reserved_memory_mb': 0,  
                'usage_of_cluster': 0,  
            }  
            dummy_rows.append(dummy_row)  
  
    # If there are new prefixes, add to DataFrame  
    if dummy_rows:  
        df_usage_groups = pd.concat([df_usage_groups, pd.DataFrame(dummy_rows)], ignore_index=True)  
  
    return df_usage_groups  
  
engine = create_engine(f"mysql+pymysql://{mysql_access}")

while True:              
    df_all_resource = scraping_all_yarn_resource()    
    if df_all_resource is not None:    
        # Add the current UTC date to the DataFrame    
        df_all_resource_utc = df_all_resource.with_columns(    
            pl.lit(datetime.now(pytz.UTC)).alias("date")    
        )    
        try:    
            # Polars  
            df_all_resource_utc.write_database(    
                table_name=table_name[0],    
                connection=engine,    
                if_table_exists="append",  
            )    
            logging.info(f"All Resource data: '{df_all_resource.shape[0]} rows' inserted into '{table_name[0]}'.")    
        except Exception as e:    
            logging.error(f"Error while writing resource data to database: {e}")    
            # Scrape Running Jobs Yarn  
    df_list_job = scraping_yarn_job_running()  
    if df_list_job is not None:  
        try:
            # Delete all row in table using proper SQLAlchemy syntax
            with engine.connect() as conn:
                conn.execute(text(f"DELETE FROM {table_name[1]}"))
                conn.commit()  # Add commit to ensure changes are saved
                
            # Insert Data
            df_list_job.to_sql(  
                name=table_name[1],   
                con=engine,   
                if_exists='append',   
                index=False
            )    
            
            logging.info(f"List job yarn : '{len(df_list_job)} rows' inserted into '{table_name[1]}'.")    
        except Exception as e:    
            logging.error(f"Error while writing 'list job yarn' to database: {e}")  
    # Scrape Calculation Jobs Yarn  
    df_usage_groups = calculate_group_usages()  
    if df_usage_groups is not None:    
        try:    
            # Pandas  
            df_usage_groups['date'] = pd.Timestamp.now(pytz.UTC)  
            df_usage_groups.to_sql(  
                name=table_name[2],   
                con=engine,  
                if_exists='append',   
                index=False  # Avoid including index column  
            )    
            logging.info(f"Usage Group Data : '{len(df_usage_groups)} rows' inserted into '{table_name[2]}'.")    
        except Exception as e:    
            logging.error(f"Error while writing application data to database: {e}") 

    time.sleep(interval)
# Usage: python main.py yarn_all_resource_test yarn_list_jobs yarn_usage_groups  