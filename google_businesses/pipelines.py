# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pyodbc


class GoogleBusinessesPipeline:
    SERVER = "database-2.cbm4oasq2u17.eu-north-1.rds.amazonaws.com"
    DATABASE = "google_map_db"
    USERNAME = "admin"
    PASSWORD = "]n78XwP96+q"

    def __init__(self):
        connectionString = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.SERVER};DATABASE={self.DATABASE};UID={self.USERNAME};PWD={self.PASSWORD}"
        self.conn = pyodbc.connect(connectionString, autocommit=True)
        self.cursor = self.conn.cursor()

    def open_spider(self, spider):
        self.cursor.execute("USE google_map_db;")
        if self.cursor.tables(table=f"google_maps_data", tableType="TABLE").fetchone():
            print("Table exists")
        else:
            self.cursor.execute(
                f"""CREATE TABLE google_maps_data (Keyword TEXT, Bussiness_Name TEXT, Bussiness_Contact TEXT, 
                Business_URL TEXT, Bussiness_Website TEXT, Bussiness_Service TEXT,Bussiness_Serving_Area TEXT,Address TEXT,
                Industry TEXT,Street TEXT,State TEXT,Zipcode TEXT,City TEXT, Country TEXT,Description TEXT,Review_Type TEXT,
                Google_Map_Link TEXT, Opening_Hours TEXT, About TEXT,Images TEXT,Lat TEXT,Lon TEXT);"""
            )

    def process_item(self, item, spider):
        spider.logger.info(
            f"active request: {len(spider.crawler.engine.slot.inprogress)}"
        )
        formatted_item = {
            "Keyword": item.get("Keyword", ""),
            "Business_Name": item.get("Bussiness_Name", ""),
            "Business_Contact": item.get("Bussiness_Contact", ""),
            "Business_URL": item.get("Business_URL", ""),
            "Business_Website": item.get("Bussiness_Website", ""),
            "Business_Service": item.get("Bussiness_Service", ""),
            "Business_Serving_Area": item.get("Bussiness_Serving_Area", ""),
            "Address": item.get("Address", ""),
            "Industry": item.get("Industry", ""),
            "Street": item.get("Street", ""),
            "State": item.get("State", ""),
            "Zipcode": item.get("Zipcode", ""),
            "City": item.get("City", ""),
            "Country": item.get("Country", ""),
            "Description": item.get("Description", ""),
            "Review_Type": item.get("Review_Type", ""),
            "Google_Map_Link": item.get("Google_Map_Link", ""),
            "Opening_Hours": item.get("Opening Hours", ""),
            "About": item.get("About", ""),
            "Images": item.get("Images", ""),
            "Lat": str(item.get("Lat", "")),
            "Lon": str(item.get("Lon", "")),
        }
        try:
            insert_query = f"""INSERT INTO google_maps_data (Keyword, Bussiness_Name, Bussiness_Contact, Business_URL,
            Bussiness_Website, Bussiness_Service, Bussiness_Serving_Area, Address, Industry, Street, State, Zipcode, City, Country,
            Description, Review_Type, Google_Map_Link, Opening_Hours, About, Images, Lat, Lon)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            values = [
                item.get("Keyword", ""),
                item.get("Bussiness_Name", ""),
                item.get("Bussiness_Contact", ""),
                item.get("Business_URL", ""),
                item.get("Bussiness_Website", ""),
                item.get("Bussiness_Service", ""),
                item.get("Bussiness_Serving_Area", ""),
                item.get("Address", ""),
                item.get("Industry", ""),
                item.get("Street", ""),
                item.get("State", ""),
                item.get("Zipcode", ""),
                item.get("City", ""),
                item.get("Country", ""),
                item.get("Description", ""),
                item.get("Review_Type", ""),
                item.get("Google_Map_Link", ""),
                item.get("Opening Hours", ""),
                item.get("About", ""),
                item.get("Images", ""),
                str(item.get("Lat", "")),
                str(item.get("Lon", "")),
            ]
            self.cursor.execute(insert_query, values)
            self.conn.commit()
        except Exception as e:
            spider.logger.error(f"Error inserting {item['Bussiness_Name']} into the database", e)

        return formatted_item
