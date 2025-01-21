import time
import datetime
import requests
import numpy as np
import concurrent.futures
from bs4 import BeautifulSoup
from selenium import webdriver
from tqdm import tqdm
from confluent_kafka import Producer
import socket
from selenium.webdriver.chrome.options import Options
import json

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Enable headless mode
chrome_options.add_argument("--disable-gpu")  # Disable GPU (optional but recommended for headless mode)
chrome_options.add_argument("--no-sandbox")  # For Linux systems
chrome_options.add_argument("--disable-extensions")  # Disable extensions (optional)
chrome_options.add_argument("--disable-dev-shm-usage")


url = "https://www.flightradar24.com/data/airports/---"
headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'}
res = requests.get(url,headers=headers)
soup = BeautifulSoup(res.content, "html.parser")

def produce_message(data):

    conf = {'bootstrap.servers': '0.0.0.0:9092',
        'client.id': socket.gethostname()}

    print(socket.gethostname())

    producer = Producer(conf)

    def acked(err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))
    try:
        data = json.dumps(data).encode('utf-8') 

        producer.produce("flightdata", key="key", value=data, callback=acked)

        producer.flush()
    except Exception as e:
        print(e)
        


def airport_urls():
    table = soup.find("table",id="tbl-datatable").find("tbody")
    airports = []
    for row in table.findAll('tr'):
        link = row.find("a", href=True)
        rating_span = row.find("span", class_="pull-right")
        if link:
            name = link.get("title")
            url = link.get("href")
            lat = link.get("data-lat")
            lon = link.get("data-lon")
            rating = rating_span.text.replace("Rating: ", "") if rating_span else None
            airports.append({"name": name, "url": url, "lat": lat, "lon": lon, "rating": rating})
    filtered_airports = [airport for airport in airports if airport['url'] != "#"]
    return filtered_airports



def fetch_airport_data(airport_url):
    print("STARTED")
    driver = None
    driver = webdriver.Chrome(options=chrome_options)
    airport_data = {}
    try:
        driver.get(airport_url)
        # 200 response eklenecek
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        # General Airport Information
        # çözülecek sorunlar 1 rating değeri nan geliyor eğer nan geliyorsa olduğu gibi kalsın nandan farklı ise float yapsın onu revize et
        # çözülecek sorunlar 2 bazı airportların uçuş değerleri yok. onları kontrol et
        airport_data = {
            'airport_code': soup.find("div", {"class": "row cnt-airport-details"}).find("div", {"class": "col-md-6 n-p"}).h2.text.strip() if soup.find("div", {"class": "row cnt-airport-details"}) else "Nan",
            'airport_name': soup.find("div", {"class": "row cnt-airport-details"}).find("div", {"class": "col-md-6 n-p"}).h1.text.strip() if soup.find("div", {"class": "row cnt-airport-details"}) else "Nan",
            'city': soup.find("div", {"class": "row cnt-airport-details"}).find("div", {"class": "col-md-6 n-p"}).h3.text.strip() if soup.find("div", {"class": "row cnt-airport-details"}) else "Nan",
            'country': soup.find("div", {"class": "row cnt-airport-details"}).find("div", {"class": "col-md-6 n-p"}).h3.text.strip() if soup.find("div", {"class": "row cnt-airport-details"}) else "Nan",
            'rating': soup.find("div", {"class": "row cnt-airport-details"}).find("div", {"class": "col-xs-4 col-sm-4 hidden-md hidden-lg"}).find("div", {"class": "cnt-chart"}).text.replace('%', '').strip() if soup.find("div", {"class": "row cnt-airport-details"}) else "Nan"
        }
        # Routes Data
        routes = []
        try:
            routes_body = soup.find("div", {"class": "tab-pane p-l active"}).find("aside", {"class": "col-sm-3 col-sm-pull-9 airport-additional-data"}).find("div", {"class": "p-s"}).find("ul", {"class": "top-routes"}).find_all("li")
            if routes_body:
                for route in routes_body:
                    routes.append({
                    'rank': route.text.split()[0] if route.text else "Nan",
                    'airport_code': route.find("a").text.strip() if route.find("a") else "Nan",
                    'airport_name': route.find("a")['title'] if route.find("a") and 'title' in route.find("a").attrs else "Nan",
                    'flights_per_week': route.find("span").text.split()[0] if route.find("span") else "Nan"
                })
        except:
            routes.append({
                'rank': "Nan",
                'airport_code': "Nan",
                'airport_name': "Nan",
                'flights_per_week':"Nan"
            })
        
        # Arrivals Data
        arrivals = []
        arrivals_body = soup.find("aside", {"class": "col-sm-9 col-sm-push-3 airport-schedule-data p-l-l n-p-l-xs"}).find("div", {"class": "row cnt-schedule-table"}).find("table", {"class": "table table-condensed table-hover data-table m-n-t-15"}).find("tbody").find_all("tr", {"class": "hidden-md hidden-lg ng-scope"})
        if arrivals_body:
            for arrival_deep in arrivals_body:
                arrivals.append({
                    'status_arrivals': arrival_deep.find("div", {"class": "col-xs-12 col-sm-12 p-xxs ng-binding"}).text.strip() if arrival_deep.find("div", {"class": "col-xs-12 col-sm-12 p-xxs ng-binding"}) else "Nan",
                    'time_arrivals': arrival_deep.find("div", {"class": "col-xs-3 col-sm-3 p-xxs"}).text.strip() if arrival_deep.find("div", {"class": "col-xs-3 col-sm-3 p-xxs"}) else "Nan",
                    'flight_arrivals': arrival_deep.find("a").text.strip() if arrival_deep.find("a") else "Nan",
                    'from_arrivals': arrival_deep.find("div", {"class": "col-xs-6 col-sm-6 p-xxs"}).find("span", {"class": "ng-binding"}).text.strip() if arrival_deep.find("div", {"class": "col-xs-6 col-sm-6 p-xxs"}) and arrival_deep.find("div", {"class": "col-xs-6 col-sm-6 p-xxs"}).find("span", {"class": "ng-binding"}) else "Nan",
                    'aircraft_arrivals': arrival_deep.find("div", {"class": "col-xs-3 col-sm-3 p-xxs ng-binding"}).text.strip() if arrival_deep.find("div", {"class": "col-xs-3 col-sm-3 p-xxs ng-binding"}) else "Nan",
                    'airline_arrivals': [a.text.strip() for a in arrival_deep.find_all("a", {"class": "notranslate ng-binding"})][-1] if arrival_deep.find_all("a", {"class": "notranslate ng-binding"}) else "Nan"
                })
        else:
            arrivals.append({
                    'status_arrivals': "Nan",
                    'time_arrivals': "Nan",
                    'flight_arrivals': "Nan",
                    'from_arrivals': "Nan",
                    'aircraft_arrivals': "Nan",
                    'airline_arrivals': "Nan"
                })
            
    
        # Departures Data
        departures = []
        departures_body = soup.find("div", {"class": "row m-t-l cnt-schedule-table"}).find("table", {"class": "table table-condensed table-hover data-table m-n-t-15"}).find("tbody").find_all("tr", {"class": "hidden-md hidden-lg ng-scope"})
        if departures_body:
            for departures_deep in departures_body:
                departures.append({
                    'status_departures': departures_deep.find("div", {"class": "col-xs-12 col-sm-12 p-xxs ng-binding"}).text.strip() if departures_deep.find("div", {"class": "col-xs-12 col-sm-12 p-xxs ng-binding"}) else "Nan",
                    'time_departures': departures_deep.find("div", {"class": "col-xs-3 col-sm-3 p-xxs"}).text.strip() if departures_deep.find("div", {"class": "col-xs-3 col-sm-3 p-xxs"}) else "Nan",
                    'flight_departures': departures_deep.find("a").text.strip() if departures_deep.find("a") else "Nan",
                    'to_departures': departures_deep.find("div", {"class": "col-xs-6 col-sm-6 p-xxs"}).find("span", {"class": "ng-binding"}).text.strip() if departures_deep.find("div", {"class": "col-xs-6 col-sm-6 p-xxs"}) and departures_deep.find("div", {"class": "col-xs-6 col-sm-6 p-xxs"}).find("span", {"class": "ng-binding"}) else "Nan",
                    'aircraft_departures': departures_deep.find("div", {"class": "col-xs-3 col-sm-3 p-xxs ng-binding"}).text.strip() if departures_deep.find("div", {"class": "col-xs-3 col-sm-3 p-xxs ng-binding"}) else "Nan",
                    'airline_departures': [a.text.strip() for a in departures_deep.find_all("a", {"class": "notranslate ng-binding"})][-1] if departures_deep.find_all("a", {"class": "notranslate ng-binding"}) else "Nan"
                })
        else:
            departures.append({
                    'status_departures':"Nan" ,
                    'time_departures': "Nan",
                    'flight_departures': "Nan",
                    'to_departures': "Nan",
                    'aircraft_departures': "Nan",
                    'airline_departures': "Nan"
                })
        airport_data = {
            'airport_data': airport_data,
            'routes': routes,
            'arrivals': arrivals,
            'departures': departures,
            'url':airport_url,
            'scrape_time':str(datetime.datetime.now())
        }
    except Exception as e:
        print(f"Bir hata oluştu: {str(e)}",airport_url)
        airport_data = None
    finally:
        if driver:
            driver.quit()
    return airport_data



def process_batches(airport_list):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for i in tqdm(range(0, len(airport_list), 5), desc="Processing in batches"):
            # 5'li gruplar halinde listeyi böl
            batch = airport_list[i:i+5]  
            print(f"\nBatch işleniyor: {batch}")

            # Her URL için işlem başlat
            futures = {executor.submit(fetch_airport_data, url): url for url in batch}

            # Her bir işlemin tamamlanmasını bekle
            for future in concurrent.futures.as_completed(futures):
                url = futures[future]  # Hangi URL'ye ait olduğunu bul
                try:
                    result = future.result()
                    time.sleep(4)
                    produce_message(result)

                except Exception as e:
                    print(f"URL: {url} işleminde hata alındı: {e}")

            # Eğer hala işlenmemiş batch varsa bekle
            if i + 5 < len(airport_list):
                print("5'li batch tamamlandı, 60 saniye bekleniyor...")
                time.sleep(60)



airpotlist = airport_urls()

url_list =  [ i['url'] for i in airpotlist]
print(url_list)
r = process_batches(url_list[:5])
print(r)

print("*"*50) 


