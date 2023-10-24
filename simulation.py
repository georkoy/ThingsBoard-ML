#!/usr/bin/env python3
#from mn_wifi.net import Station, OVSKernelAP
from mininet.node import Controller
from mininet.log import setLogLevel, info
from mn_wifi.cli import CLI
from mn_wifi.net import Mininet_wifi
from mininet.link import TCLink
import requests
import json
import os
from datetime import datetime, timedelta
import time
import math
import psycopg2
import random
import csv
import subprocess


def mydata():	
	login_url = "https://ellonasoft.io/api/v2/login"
	data_url = "https://ellonasoft.io/api/v2/extract-data"
	body_login = {'login': 'scanuser', 'password': 'wuv[eenee9eixeichoV3'}
	# Get the current date and time in UTC
	currentdate = datetime.utcnow()
	# Calculate the from date by subtracting 30 seconds from the current date
	fromdate = currentdate - timedelta(seconds=30)
	# Format the dates in the desired string format
	myfromdate = fromdate.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
	mytodate = currentdate.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
	# Update the body_data dictionary with the new from and to dates
	body_data = {
	    "from": myfromdate,
	    "to": mytodate,
	    "sources": ["WT1-30073"]
	}
	#print(myfromdate)
	print(mytodate)
	# sending post request and saving response as response object
	r_login = requests.post(url=login_url, json=body_login)
	#print('login retrieval status: %s' % r_login.status_code)
	if (r_login.status_code == 200):
	    json = r_login.json()
	token = json['token']
	#print('token: %s' % token)
	mydata = requests.post(url=data_url, json=body_data, headers={'x-auth': token})
	
	print('data retrieval status: %s' % mydata.status_code)
	if (mydata.status_code == 200):
		return mydata.json()
	  
	    # Access the first measurement
	    #first_measurement = measurements[0]
	    #print(mydata.json())
	    #print(json.dumps(mydata.json(), indent=4))

def openweatherdata():
	weatherdataurl="https://api.openweathermap.org/data/2.5/weather?lat=37.968145522425466&lon=23.766511470550878&appid=18180c327a566111812f10f13cec35d7&units=metric"
	response=requests.get(weatherdataurl)
	if response.status_code==200:
		json_data=response.json()
		return json_data["main"]

def openweatherwind():
        weatherdataurl="https://api.openweathermap.org/data/2.5/weather?lat=37.968145522425466&lon=23.766511470550878&appid=18180c327a566111812f10f13cec35d7&units=metric"
        response=requests.get(weatherdataurl)
        if response.status_code==200:
                json_data=response.json()
                return json_data["wind"]["speed"]


def openweatherpollution():
	pollutiondataurl="https://api.openweathermap.org/data/2.5/air_pollution?lat=37.968145522425466&lon=23.766511470550878&appid=18180c327a566111812f10f13cec35d7"
	response=requests.get(pollutiondataurl)
	if response.status_code==200:
		json_data=response.json()
		return json_data["list"][0]["components"];
def openmeteorain():
	weatherdataurlom="https://api.open-meteo.com/v1/forecast?latitude=37.9681&longitude=23.7665&hourly=rain&forecast_days=1&timezone=auto"
	response=requests.get(weatherdataurlom)
	if response.status_code==200:
		json_data=response.json()
		return json_data["hourly"]['rain'][datetime.now().hour]

def EuropeanAirQuality(no2,o3,so2,pm10,pm25):
	no2HourlyThresholds = [0, 40, 90, 120, 230, 340]
	o3HourlyThresholds = [0, 50, 100, 130, 240, 380]
	so2HourlyThresholds = [0, 100, 200, 350, 500, 750]
	pm2_5_24HourlyMeanThresholds = [0, 10, 20, 25, 50, 75]
	pm10_24HourlyMeanThresholds = [0, 20, 40, 50, 100, 150]
	positiono2 = positionExtrapolated(no2HourlyThresholds, no2)*20
	positiono3=positionExtrapolated(o3HourlyThresholds, o3)*20
	positionso2=positionExtrapolated(so2HourlyThresholds, so2)
	positionpm10=positionExtrapolated(pm10_24HourlyMeanThresholds, pm10)*20
	positionpm25=positionExtrapolated(pm2_5_24HourlyMeanThresholds, pm25)*20
	return  max(positiono2, positiono3, positionso2, positionpm10, positionpm25)

def positionExtrapolated(array, search):
	        if search <= array[0]:
	            return 0
	        if search >= array[-1]:
	            return len(array) - 1
	        for i in range(len(array) - 1):
	            if array[i] <= search < array[i+1]:
	                if math.isnan(array[i+1]):
	                    return i
	                else:
	                    return i + (search - array[i]) / (array[i+1] - array[i])
	        return len(array) - 1




def dbdata(temp, rain, pressure, humidity, windspeed, noise, o3, no2, nh3, n2s, so2, co, no, pm1, pm25, pm10):
    try:
        # Database connection parameters
        db_params = {
            'dbname': 'thingsboard',
            'user': 'postgres',
            'password': 'kH7apYLZAjTEaa3iw1dL',
            'host': 'localhost',
            'port': '5432'
        }
        # Establish a connection to the database
        conn = psycopg2.connect(**db_params)
        # Check if the connection is successful
        if conn:
            cursor = conn.cursor()
            currentdat = datetime.now()
            formatdate = currentdat.strftime('%d/%m/%y %H:00')
            insert_query = f'''
            INSERT INTO last24 (date, temperature, rain, pressure, humidity, windspeed, noise, co, no, no2, o3, so2, pm2_5, pm10, nh3, h2s, pm1)
            VALUES ('{formatdate}', {temp}, {rain}, {pressure}, {humidity}, {windspeed}, {noise}, {co}, {no}, {no2}, {o3}, {so2}, {pm25}, {pm10}, {nh3}, {n2s}, {pm1});
            '''
            cursor.execute(insert_query)
            conn.commit()

            
            if datetime.now().hour == 23:
                get_data_query = '''SELECT * FROM last24;'''
                cursor.execute(get_data_query)
                rows = cursor.fetchall()
                conn.commit()
                script_directory = os.path.dirname(os.path.abspath(__file__))
                csv_file_path = os.path.join(script_directory, "data.csv")
                with open(csv_file_path, "a", newline="") as csvfile:
                    csv_writer = csv.writer(csvfile)
                    for row in rows:
                        csv_writer.writerow(row)
                        #print("data write to csv")
                subprocess.run(['python', 'predictions.py'])
                delete_data_query = '''DELETE FROM last24;'''
                cursor.execute(delete_data_query)
                conn.commit()

            # Close the connection
            cursor.close()
            conn.close()

    except psycopg2.OperationalError as e:
        print("Error:", e)
        print("Database connection failed.")


def publish_predictions():
	try:
	# Database connection parameters
		db_params = {
		'dbname': 'thingsboard',
		'user': 'postgres',
		'password': 'kH7apYLZAjTEaa3iw1dL',
		'host': 'localhost',
		'port': '5432'
		}
	# Establish a connection to the database
		conn = psycopg2.connect(**db_params)
        # Check if the connection is successful
		if conn:
			cursor=conn.cursor()
			mytime=datetime.now().strftime('%H')
			get_data_query=f'''SELECT *FROM predictions where date='{mytime}';'''
			cursor.execute(get_data_query)
			row=cursor.fetchone()
			conn.commit()
			# Create the MQTT payload string
			payload ='{"temperature_pred":' + str(row[1]) + ',"rain_pred":' + str(row[2]) + ',"pressure_pred":' + str(row[3]) +',"humidity_pred":' + str(row[4]) + ',"windspeed_pred":' + str(row[5]) + ',"noise_pred":' + str(row[6]) +',"co_pred":' + str(row[7]) + ',"no_pred":' + str(row[8]) + ',"no2_pred":' + str(row[9]) + ',"o3_pred":' + str(row[10]) +',"so2_pred":' + str(row[11]) + ',"pm2_5_pred":' + str(row[12]) + ',"pm10_pred":' + str(row[13]) + ',"nh3_pred":' + str(row[14]) +',"h2s_pred":' + str(row[15]) + ',"pm1_pred":' + str(row[16]) + '}'
			os.system( "mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u bYAHbGXWJzQAVoEvbWnL -i prediction -m '" + payload + "'" )
			#print(payload)

        # Close the connection
		cursor.close()
		conn.close()

	except psycopg2.OperationalError as e:
		print("Error:", e)
		print("Database connection failed.")


def topology(mydatajson):
	net = Mininet_wifi()
	# Create weather station hosts

	temp=net.addStation('temp', ip='172.16.10.3')
	pressure=net.addStation('pressure',ip='172.16.10.4')
	humidity=net.addStation('humidity',ip='172.16.10.5')
	noise=net.addStation('noise',ip='172.16.10.6')
	o3=net.addStation('o3',ip='172.16.10.7')
	no2=net.addStation('no2',ip='172.16.10.8')
	nh3=net.addStation('nh3',ip='172.16.10.9')
	n2s=net.addStation('ns2',ip='172.16.10.10')
	so2=net.addStation('so2',ip='172.16.10.11')
	co=net.addStation('co',ip='172.16.10.12')
	pm1=net.addStation('pm1',ip='172.16.10.13')
	pm25=net.addStation('pm25',ip='172.16.10.14')
	pm10=net.addStation('pm10',ip='172.16.10.15')
	# Create access point
	ap1 = net.addAccessPoint('ap1',ip='172.16.10.2',datapath='user', ssid='weather_station', mode='g', channel='1')
	net.configureWifiNodes()
	net.setPropagationModel(model="friis")
	# Create links between hosts and access point
	net.addLink(ap1,temp)
	net.addLink(ap1,pressure)
	net.addLink(ap1,humidity)
	net.addLink(ap1,noise) 
	net.addLink(ap1,o3)
	net.addLink(ap1,no2)
	net.addLink(ap1,nh3)   
	net.addLink(ap1,n2s) 
	net.addLink(ap1,so2) 
	net.addLink(ap1,co) 
	net.addLink(ap1,pm1) 
	net.addLink(ap1,pm25) 
	net.addLink(ap1,pm10) 
	#add values
	#measurements = mydatajson['data']['measurements']
	#print(len(measurements))
	#print(mydatajson)
	datajsonpol=openweatherpollution()
	try:
		if len (mydatajson['data']['measurements'])!=0:	
			try:
				temp.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-env_temp'],2)	
				pressure.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-env_pres'],2)	
				humidity.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-env_rh'],2) 	
				noise.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-sound_leqa'],2)	
				o3.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-o3_ug_m3'],2)	
				no2.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-no2_ug_m3'],2)	
				nh3.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-nh3_ug_m3'],2)	
				n2s.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-h2s_ug_m3'],2)	
				so2.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-so2_ug_m3'],2)	
				co.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-co_ug_m3'],2)	
				pm1.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-pm_pm10'],2)	
				pm25.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-pm_pm25'],2)	
				pm10.value=round(mydatajson['data']['measurements'][1]['620d10f28c0df11bf77b34db-pm_pm100'],2)
				euaqi=math.ceil(EuropeanAirQuality(no2.value,o3.value,so2.value,pm10.value,pm25.value))
				mysensor=0
			except KeyError as e:
				print("key error exception")
				datajson=openweatherdata()
				temp.value=datajson["temp"]
				pressure.value=datajson["pressure"]
				humidity.value=datajson["humidity"]
				noise.value=round(random.uniform(54,64),2)
				o3.value=datajsonpol["o3"]
				no2.value=datajsonpol["no2"]
				nh3.value=datajsonpol["nh3"]
				n2s.value=round(random.uniform(0,130),2)
				so2.value=datajsonpol["so2"]
				co.value=datajsonpol["co"]
				pm1.value=round(random.uniform(1,12),2)
				pm25.value=datajsonpol["pm2_5"]
				pm10.value=datajsonpol["pm10"]
				euaqi=math.ceil(EuropeanAirQuality(no2.value,o3.value,so2.value,pm10.value,pm25.value))
				mysensor=1
		else:
			print("empty basic api")
			datajson=openweatherdata()
			temp.value=datajson["temp"]
			pressure.value=datajson["pressure"]
			humidity.value=datajson["humidity"]
			noise.value=round(random.uniform(54,64),2)
			o3.value=datajsonpol["o3"]
			no2.value=datajsonpol["no2"]
			nh3.value=datajsonpol["nh3"]
			n2s.value=round(random.uniform(0,130),2)
			so2.value=datajsonpol["so2"]
			co.value=datajsonpol["co"]
			pm1.value=round(random.uniform(1,12),2)
			pm25.value=datajsonpol["pm2_5"]
			pm10.value=datajsonpol["pm10"]
			euaqi=math.ceil(EuropeanAirQuality(no2.value,o3.value,so2.value,pm10.value,pm25.value))
			mysensor=1
	except Exception as e:
		print(f'ellona error {e}')
		datajson=openweatherdata()
		temp.value=datajson["temp"]
		pressure.value=datajson["pressure"]
		humidity.value=datajson["humidity"]
		noise.value=round(random.uniform(54,64),2)
		o3.value=datajsonpol["o3"]
		no2.value=datajsonpol["no2"]
		nh3.value=datajsonpol["nh3"]
		n2s.value=round(random.uniform(0,130),2)
		so2.value=datajsonpol["so2"]
		co.value=datajsonpol["co"]
		pm1.value=round(random.uniform(1,12),2)
		pm25.value=datajsonpol["pm2_5"]
		pm10.value=datajsonpol["pm10"]
		euaqi=math.ceil(EuropeanAirQuality(no2.value,o3.value,so2.value,pm10.value,pm25.value))
		mysensor=1
		#print(o3.value)
	#same velue for all cases
	wind=openweatherwind()
	no=datajsonpol["no"]
	rain=openmeteorain()
	ap1.cmd('ip route add 172.16.10.0/24 dev ap1-wlan1')
	# Start network
	net.build()
	net.start()
	ap1.start([])
	# Create the MQTT payload string
	#payload = '{"temperature":' + str(temp.value) +',"pressure":'+str(pressure.value)+',"humidity":'+str(humidity.value)+',"noise":'+str(noise.value)+',"Ozone (O3)":'+str(o3.value)+',"Nitrogen dioxide (NO2)":'+str(no2.value)+',"Ammonia (NH3)":'+str(nh3.value)+',"Hydrogen sulfide (H2S)":'+str(n2s.value)+',"Sulphur dioxide (SO2) ":'+str(so2.value)+',"Corbon monoxide (CO)":'+str(co.value)+',"Particulate matter (PM1)":'+str(pm1.value)+',"Fine particles matter (PM2.5)":'+str(pm25.value)+',"Coarse particulate matter (PM10)":'+str(pm10.value)+',"AQI-EURO":'+str(euaqi)+',"Mysensor":'+str(mysensor)+'}'
	#ap1.cmd( "mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -m '" + payload + "'" )
	time.sleep(5)
	temp.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i temperature -m '{{\"temperature\":{temp.value}}}'")
	pressure.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i pressure -m '{{\"pressure\":{pressure.value}}}'")
	humidity.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i humidity -m '{{\"humidity\":{humidity.value}}}'")
	noise.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i noise -m '{{\"noise\":{noise.value}}}'")
	o3.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i Ozone-O3 -m '{{\"Ozone (O3)\":{o3.value}}}'")
	no2.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i Nitrogen-dioxide-NO2 -m '{{\"Nitrogen dioxide (NO2)\":{no2.value}}}'")
	nh3.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i Ammonia-NH3 -m '{{\"Ammonia (NH3)\":{nh3.value}}}'")
	n2s.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i Hydrogen-sulfide-H2S -m '{{\"Hydrogen sulfide (H2S)\":{n2s.value}}}'")
	so2.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i Sulphur-dioxide-SO2 -m '{{\"Sulphur dioxide (SO2)\":{so2.value}}}'")
	co.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i Corbon-monoxide-CO -m '{{\"Corbon monoxide (CO)\":{co.value}}}'")
	pm1.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i Particulate-matter-PM1 -m '{{\"Particulate matter (PM1)\":{pm1.value}}}'")
	pm25.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i Fine-particles-mtter-PM2.5 -m '{{\"Fine particles matter (PM2.5)\":{pm25.value}}}'")
	pm10.cmd(f"mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i Coarse-particulate-matter-PM10 -m '{{\"Coarse particulate matter (PM10)\":{pm10.value}}}'")
	#print(a)
	payload1='{"AQI-EURO":'+str(euaqi)+',"Mysensor":'+str(mysensor)+'}'
	ap1.cmd( "mosquitto_pub -d -q 1 -h 172.16.10.250 -p 1883 -t v1/devices/me/telemetry -u qRNT3cpAQdj7QZVuvJhH -i ap1 -m '" + payload1 + "'" )
	publish_predictions()
	dbdata(temp.value,rain,pressure.value,humidity.value,wind,noise.value,o3.value,no2.value,nh3.value,n2s.value,so2.value,co.value,no,pm1.value,pm25.value,pm10.value)

	# Start CLI
	#CLI(net)
	# Stop network
	net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    topology(mydata()) 
    command = "sudo mn -c"
    os.system(command)
