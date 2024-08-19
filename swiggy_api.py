import http.client
import json
import zlib
import pandas as pd
import csv
import os
import logging
import sys
import time
import pdb

def set_log_file(log_file_name):   
    path = os.getcwd()
    logging.basicConfig(filename=log_file_name,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filemode='a')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    return logger
def scrape_data(start_count, end_count, source_file_name, output_file_name):
    log_file = f'swiggy_restaurants_log_{start_count}_{end_count}.log'
    logger = set_log_file(log_file)

    try:
        input_data = pd.read_csv(source_file_name)
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        return
        # print(e)

    try:
        with open(output_file_name, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if f.tell() == 0:  
                writer.writerow(["city", "area", "scraped_city", "restaurant_id", "restaurant_name","restaurant_locality","restautant_area_name","restaurant_rating","restaurant_rating_count","restaurant_cost", "restaurant_url"])
    except Exception as e:
        logger.error(f"Error preparing output file: {e}")
        return
        # print(e)

    for index, row in input_data[start_count:end_count].iterrows():
        try:
            serial_no = row["serial_no"]
            city = row["city"]
            area = row["area"]
            url = row['area_url']
            latitude = row['latitude']
            longitude = row['longitude']
            logger.info(f"Starting data scraping for {serial_no}. {city}, {area}")
        except Exception as e:
            logger.error(f"Error reading row data: {e}")
            print(f"Exception in file reading ::: {e}")
            continue

        conn = http.client.HTTPSConnection("www.swiggy.com")
        
        
        last_file_size = os.path.getsize(output_file_name)
        no_change_count = 0
        
        for offset in range(0, 150):
            res = []
            try:
                payload = f'{{"widgetOffset":{{"collectionV5RestaurantListWidget_SimRestoRelevance_food_seo":"{11+(15*offset)}"}},"nextOffset":"CJY7ELQ4KICw07+0l+nobTDYEDgC"}}'

                headers = {
                    'cookie': "_device_id=6217eff5-421b-dd28-2559-0b3898caf52e; _gcl_au=1.1.999884072.1717754457; _guest_tid=7786d2d5-c89f-4f5c-a180-7359912528a2; _gid=GA1.2.173292954.1717999048; _sid=ebna1843-5669-428d-9658-e6187fe51325; _ga_34JYJ0BCRN=GS1.1.1717999047.3.1.1718000808.0.0.0; _ga=GA1.2.1964739012.1717754457; _gat_0=1",
                    'authority': "www.swiggy.com",
                    'accept': "*/*",
                    'accept-language': "en-US,en;q=0.9",
                    'content-type': "application/json",
                    'origin': "https://www.swiggy.com",
                    'referer': url,
                    'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
                    'sec-ch-ua-mobile': "?0",
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': "empty",
                    'sec-fetch-mode': "cors",
                    'sec-fetch-site': "same-origin",
                    'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
                }

                conn.request("POST", f"/api/seo/getListing?lat={latitude}&lng={longitude}", payload, headers)
                # pdb.set_trace()
                time.sleep(1)  
                res = conn.getresponse()
                data = res.read()
                # print(data)
                if res.getheader('Content-Encoding') == 'gzip':
                    data = zlib.decompress(data, zlib.MAX_WBITS | 16)
                elif res.getheader('Content-Encoding') == 'deflate':
                    data = zlib.decompress(data)

                data_str = data.decode("utf-8")
                json_data = json.loads(data_str)
                # print(data)
                restaurants = json_data["data"]["success"]["cards"][0]["card"]["card"]["gridElements"]["infoWithStyle"]["restaurants"]
                
                if not restaurants:
                    logger.info(f"No more restaurants found for {city}, {area} after {offset} pages.")
                    break

                for restaurant in restaurants:
                    try:
                        try:
                            restaurant_id = restaurant["info"]["id"]
                        except:
                            restaurant_id = ""

                        try:
                            restaurant_name = restaurant["info"]["name"]
                        except:
                            restaurant_name = ""

                        try:
                            restaurant_locality = restaurant["info"]["locality"]
                        except:
                            restaurant_locality = ""
                        try:
                            restautant_area_name = restaurant["info"]["areaName"]
                        except:
                            restautant_area_name = ""
                        
                        try:
                            restaurant_rating = restaurant["info"]["avgRating"]
                        except:
                            restaurant_rating = ""

                        try:
                            restaurant_rating_count = restaurant["info"]["totalRatingsString"]
                        except:
                            restaurant_rating_count = ""
                        
                        try:
                            restaurant_cost = restaurant["info"]["costForTwo"]
                        except:
                            restaurant_cost = ""
                            
                        try:
                            restaurant_url = restaurant["cta"]["link"]
                        except:
                            restaurant_url = ""

                        scraped_city = str(restaurant["cta"]["link"].split("-")[-2])
                        logger.info(f"Found restaurant: {restaurant_name} in {scraped_city}")

                        details_dict = {
                            "city":city,
                            "area":area,
                            "scraped_city":scraped_city,
                            "restaurant_id": restaurant_id,
                            "restaurant_name": restaurant_name,
                            "restaurant_locality":restaurant_locality,
                            "restautant_area_name":restautant_area_name,
                            "restaurant_rating": restaurant_rating,
                            "restaurant_rating_count": restaurant_rating_count,
                            "restaurant_cost": restaurant_cost,
                            "restaurant_url": restaurant_url
                        }
                        print(city, area, restaurant_name)
                        df = pd.DataFrame(details_dict, index=[0], columns=[
                            "city","area","scraped_city"
                            "restaurant_id", "restaurant_name", "restaurant_locality","restautant_area_name","restaurant_rating","restaurant_rating_count",
                            "restaurant_cost","restaurant_url"
                        ])

                        with open(output_file_name, 'a', encoding='utf-8', newline='') as f:
                            df.to_csv(f, mode='a', header=f.tell() == 0)
                        current_file_size = os.path.getsize(output_file_name)
                        if current_file_size == last_file_size:
                            no_change_count += 1
                            if no_change_count >= 5:
                                logger.warning(f"No new data added for {no_change_count} requests. Pausing for 5 minutes.")
                                time.sleep(120)  
                                no_change_count = 0
                        else:
                            no_change_count = 0
                        last_file_size = current_file_size




                    except Exception as e:
                        logger.error(f"Error processing restaurant data: {e}")
                        # print(e)


            except KeyError as e:
                # print(e)
                if str(e) == "'cards'":
                    logger.warning(f"No more data available for {city}, {area} at offset {offset}")
                    break
                else:
                    logger.error(f"Unexpected KeyError at {city}, {area} in the page no: {offset} - {str(e)}")
            except Exception as e:
                logger.error(f"Error at {city}, {area} in the page no: {offset} - {str(e)}")

            logger.info(f"Finished data scraping for {city}, {area}")
if __name__ == "__main__":
    file_no = sys.argv[1]
    start_count = int(sys.argv[2])
    end_count = int(sys.argv[3])

    source_file_name = "filtered_coordinates.csv"
    output_file_name = f"swiggy_restaurants_{file_no}.csv"

    log_file_name = f"swiggy_restaurants_log_rerun_{file_no}.log"
    logger = set_log_file(log_file_name)

    scrape_data(start_count, end_count, source_file_name, output_file_name)
