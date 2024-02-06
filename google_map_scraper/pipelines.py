import pyodbc
import logging

logger = logging.getLogger(__name__)

def split_time(t: str):
    # Check if the string includes minutes
    if ':' in t:
        # Splitting the time from the AM/PM part
        time_part, am_pm = t[:-2], t[-2:]
        
        # Splitting the hour and minute
        hour, minute = time_part.split(':')
    else:
        # For formats without minutes (like "9pm")
        hour, am_pm = t[:-2], t[-2:]
        minute = "00"

    return hour, minute, am_pm

class GoogleScraperPipeline:
    SERVER = 'database-2.cbm4oasq2u17.eu-north-1.rds.amazonaws.com'
    DATABASE = 'google_map_db'
    USERNAME = 'admin'
    PASSWORD = ']n78XwP96+q'

    def __init__(self):
        connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.SERVER};DATABASE={self.DATABASE};UID={self.USERNAME};PWD={self.PASSWORD}'
        self.conn = pyodbc.connect(connectionString, autocommit=True)
        self.cursor = self.conn.cursor()

    def open_spider(self, spider):
        self.cursor.execute("USE google_map_db;")
        if self.cursor.tables(table='google_maps_data', tableType='TABLE').fetchone():
            print("Table exists")
        else:
            self.cursor.execute(
                '''CREATE TABLE google_maps_data (Bussiness_Name TEXT, Bussiness_Contact TEXT, 
                Business_URL TEXT, Bussiness_Website TEXT, Bussiness_Service TEXT,Bussiness_Serving_Area TEXT,Address TEXT,
                Industry TEXT,Street TEXT,State TEXT,Zipcode TEXT,City TEXT, Country TEXT,Description TEXT,Review_Type TEXT,
                Google_Map_Link TEXT, Opening_Hours TEXT, About TEXT,Images TEXT,Lat TEXT,Lon TEXT);''')
            
        if self.cursor.tables(table='business_table', tableType='TABLE').fetchone():
            print("Table exists")
        else:
            self.cursor.execute(
                '''CREATE TABLE business_table (Bussiness_Name TEXT, Business_URL TEXT, Weekday TEXT, 
                OpenH TEXT, OpenM TEXT, OpenAMPM TEXT, CloseH TEXT, CloseM TEXT, CloseAMPM TEXT);''')

    def close_spider(self, spider):
        self.conn.close()

    def process_item(self, item, spider):
        try:
            check_query = f"SELECT Business_URL FROM google_maps_data WHERE CAST(Business_URL AS VARCHAR(MAX)) = '{item['Business_URL']}'"
            self.cursor.execute(check_query)
            if not self.cursor.fetchone():
                insert_query = """INSERT INTO google_maps_data (Bussiness_Name, Bussiness_Contact, Business_URL,
                Bussiness_Website, Bussiness_Service, Bussiness_Serving_Area, Address, Industry, Street, State, Zipcode, City, Country,
                Description, Review_Type, Google_Map_Link, Opening_Hours, About, Images, Lat, Lon)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                values = [
                    item.get('Bussiness_Name', ''),
                    item.get('Bussiness_Contact', ''),
                    item.get('Business_URL', ''),
                    item.get('Bussiness_Website', ''),
                    item.get('Bussiness_Service', ''),
                    item.get('Bussiness_Serving_Area', ''),
                    item.get('Address', ''),
                    item.get('Industry', ''),
                    item.get('Street', ''),
                    item.get('State', ''),
                    item.get('Zipcode', ''),
                    item.get('City', ''),
                    item.get('Country', ''),
                    item.get('Description', ''),
                    item.get('Review_Type', ''),
                    item.get('Google_Map_Link', ''),
                    item.get('Opening Hours', ''),
                    item.get('About', ''),
                    item.get('Images', ''),
                    str(item.get('Lat', '')),
                    str(item.get('Lon', ''))]
                self.cursor.execute(insert_query, values)
                logging.info(f"Inserted {item['Bussiness_Name']} into the database")
                
            else:
                print('Already exist!')
            check_business_query = f"SELECT Business_URL FROM business_table WHERE CAST(Business_URL AS VARCHAR(MAX)) = '{item['Business_URL']}'"
            self.cursor.execute(check_business_query)
            if not self.cursor.fetchone():
                business_insert_query = """INSERT INTO business_table (Bussiness_Name, Business_URL, Weekday, OpenH, OpenM,
                OpenAMPM, CloseH, CloseM, CloseAMPM) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                for weekday, hrs in item.get('Business_Hours', {}).items():
                    split_hrs = hrs.split("–")
                    op, cl = split_hrs if len(split_hrs) == 2 else ["", ""]
                    op_h, op_m, op_z = split_time(op)
                    cl_h, cl_m, cl_z = split_time(cl)
                    business_values = [
                        item.get('Bussiness_Name', ''),
                        item.get('Business_URL', ''),
                        weekday,
                        op_h,
                        op_m,
                        op_z,
                        cl_h,
                        cl_m,
                        cl_z
                    ]
                    self.cursor.execute(business_insert_query, business_values)
                    logger.info(f"Inserted {item['Bussiness_Name']} business hours into the database")
            else:
                print("Already exist!")
            
        
            self.conn.commit()
        except:
            logger.error(f"Error inserting {item['Bussiness_Name']} into the database")
        return item
