import mariadb
import sys
from datetime import datetime
import serial

mariaDB_user = "USER"
mariaDB_pw = "PW"
mariaDB_host= "IP"
mariaDB_DB= "DB"

ser = serial.Serial('/dev/ttyUSB0')
ser.baudrate = 115200
ser.bytesize = serial.EIGHTBITS
ser.parity = serial.PARITY_NONE
ser.stopbits = serial.STOPBITS_ONE

def splitter(firstsplit, secondsplit):
	split1 = data.split(firstsplit, 1)
	split2 = split1[1].split(secondsplit, 1)
	return float(split2[0])

#try 5 times to obtain a good bytestring, reading up to *m3 and a maximum of 1900 bytes
for attempt in range(3):
	try:
		data = str(ser.read_until(expected=b'*m3', size=1900))
		#get current time and format it nicely
		datum = datetime.now().strftime("%Y-%m-%d %H:%M")
		#extract consumption, energyreturn and gas
		elec_f1 = splitter("1-0:1.8.1(", "*kWh")
		elec_f2 = splitter("1-0:1.8.2(", "*kWh")
		terug_f1 = splitter("1-0:2.8.1(", "*kWh")
		terug_f2 = splitter("1-0:2.8.2(", "*kWh")
		gas1 = data.rsplit(")(")
		gas2 = gas1[-1].rsplit("*m3")
		gas = str("{:.2f}".format(float(gas2[0])))
		el_verbruik = str("{:.3f}".format(elec_f1 + elec_f2))
		el_terug = str("{:.3f}".format(terug_f1 + terug_f2))
	except IndexError:
		print("attempt ", attempt, " failed")	

#connect to the mariaDB database
try:
	conn = mariadb.connect(user= mariaDB_user,password= mariaDB_pw,host= mariaDB_host,port=3306,database= mariaDB_DB)
	conn.autocommit = True
except mariadb.Error as e:
	print(f"error connecting to MariaDB Platform: {e}")
	sys.exit(1)
cur = conn.cursor()
#############################

#write data to database
try:
	cur.execute("INSERT INTO p1_2023 (datetime, el_verbruik, el_terug, gas) VALUES (?, ?, ?, ?)", (datum, el_verbruik, el_terug, gas))
	cur.execute("SELECT * FROM p1_2023")
except mariadb.Error as e:
	print(f"error executing to MariaDB Platform: {e}")
conn.close()
#######################
