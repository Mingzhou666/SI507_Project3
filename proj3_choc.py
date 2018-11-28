import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'


print('\n*********** PART 1 ***********')
# Creates a database called choc.db
def create_choc_db():
	try:
		conn = sqlite3.connect(DBNAME)
		cur = conn.cursor()
		
		statement = '''
			DROP TABLE IF EXISTS 'Bars';
		'''
		cur.execute(statement)

		statement = '''
			CREATE TABLE `Bars` (
				`Id`    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
				`Company`   TEXT,
				`SpecificBeanBarName`   TEXT,
				`REF`   TEXT,
				`ReviewDate`    TEXT,
				`CocoaPercent`  REAL,
				`CompanyLocationId` INTEGER,
				`Rating`    REAL,
				`BeanType`  TEXT,
				`BroadBeanOriginId` INTEGER
			);
		'''
		cur.execute(statement)

		statement = '''
			DROP TABLE IF EXISTS 'Countries';
		'''
		cur.execute(statement)

		statement = '''
			CREATE TABLE `Countries` (
				`Id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
				`Alpha2`	TEXT,
				`Alpha3`	TEXT,
				`EnglishName`	TEXT,
				`Region`	TEXT,
				`Subregion`	TEXT,
				`Population`	INTEGER,
				`Area`	REAL
			);
		'''
		cur.execute(statement)


		conn.commit()
		conn.close()

	except:
		print('Fail to create ' + DBNAME)

create_choc_db()
print("Created choc Database")

# Populates choc database using csv files
def populate_choc_db():

	# Connect to choc database
	conn = sqlite3.connect(DBNAME)
	cur = conn.cursor()

	# load in the json file to Countries
	country_info_lst = []

	with open(COUNTRIESJSON, 'r') as f:
		country_lst = json.load(f)

	for country in country_lst:
		single_country_lst = []
		
		single_country_lst.append(country['alpha2Code'])
		single_country_lst.append(country['alpha3Code'])
		single_country_lst.append(country['name'])
		single_country_lst.append(country['region'])
		single_country_lst.append(country['subregion'])
		single_country_lst.append(country['population'])
		single_country_lst.append(country['area'])

		country_info_lst.append(single_country_lst)

	for inst in country_info_lst:
		insertion = (None, inst[0], inst[1], inst[2], inst[3], inst[4], inst[5], inst[6])
		statement = 'INSERT INTO "Countries" '
		statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
		cur.execute(statement, insertion)

	# load in the csv file to Bars
	choc_lst = []

	with open(BARSCSV) as csvDataFile:
		csvReader = csv.reader(csvDataFile)
		for row in csvReader:
			choc_lst.append(row)

	for inst in choc_lst[1:]:
		if inst[8] != 'Unknown':
			insertion = (None, inst[0], inst[1], inst[2], inst[3], str(float(inst[4][:-1])/100), inst[5], inst[6], inst[7], inst[8])
			statement = 'INSERT INTO "Bars" '
			statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
			cur.execute(statement, insertion)

			# update country names to ids
			statement = 'SELECT Bars.CompanyLocationId, co.Id, Bars.BroadBeanOriginId, coun.Id '
			statement += 'FROM Bars JOIN Countries AS co ON Bars.CompanyLocationId = co.EnglishName '
			statement += 'JOIN Countries AS coun ON Bars.BroadBeanOriginId = coun.EnglishName'
			cur.execute(statement)

			result_lst = cur.fetchall()

			for return_tup in result_lst:
				statement = 'UPDATE Bars '
				statement += 'SET CompanyLocationId = '
				statement += str(return_tup[1]) + ', BroadBeanOriginId = '
				statement += str(return_tup[3]) + ' '
				statement += 'WHERE CompanyLocationId = "'
				statement += str(return_tup[0]) + '" AND BroadBeanOriginId = "'
				statement += str(return_tup[2]) + '"'
				cur.execute(statement)
		else:
			insertion = (None, inst[0], inst[1], inst[2], inst[3], str(float(inst[4][:-1])/100), inst[5], inst[6], inst[7], None)
			statement = 'INSERT INTO "Bars" '
			statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
			cur.execute(statement, insertion)

			# update country names to ids
			statement = 'SELECT Bars.CompanyLocationId, co.Id '
			statement += 'FROM Bars JOIN Countries AS co ON Bars.CompanyLocationId = co.EnglishName '
			cur.execute(statement)

			result_lst = cur.fetchall()

			for return_tup in result_lst:
				statement = 'UPDATE Bars '
				statement += 'SET CompanyLocationId = '
				statement += str(return_tup[1]) + ' '
				statement += 'WHERE CompanyLocationId = "'
				statement += str(return_tup[0]) + '"'
				cur.execute(statement)

	# Close connection
	conn.commit()
	conn.close()

populate_choc_db()
print("Populated choc Database")



# Part 2: Implement logic to process user commands
def bars_search(sort_by = 'ratings', num = 'top=10', place = None):
	conn = sqlite3.connect(DBNAME)
	cur = conn.cursor()

	statement = 'SELECT Bars.SpecificBeanBarName, Bars.Company, co.EnglishName AS CompanyLocation, Bars.Rating, Bars.CocoaPercent, coun.EnglishName AS BroadBeanOrigin '
	statement += 'FROM Bars JOIN Countries AS co ON Bars.CompanyLocationId = co.Id '
	statement += 'LEFT JOIN Countries AS coun ON Bars.BroadBeanOriginId = coun.Id '

	if place != None:
		statement += 'WHERE '
		if place[:12] == 'sellcountry=':			
			statement += 'co.Alpha2 = "' + str(place[12:] + '" ')
		elif place[:11] == 'sellregion=':			
			statement += 'co.Region = "' + str(place[11:] + '" ')
		elif place[:14] == 'sourcecountry=':			
			statement += 'coun.Alpha2 = "' + str(place[14:] + '" ')
		elif place[:13] == 'sourceregion=':			
			statement += 'coun.Region = "' + str(place[13:] + '" ')			

	if sort_by == 'ratings':
		statement += 'ORDER BY Rating '
	elif sort_by == 'cocoa':
		statement += 'ORDER BY CocoaPercent '

	if num[:4] == 'top=':
		statement += 'DESC '
		statement += 'LIMIT ' + str(num[4:])
	elif num[:7] == 'bottom=':
		statement += 'LIMIT ' + str(num[7:])

	cur.execute(statement)
	result = cur.fetchall()
	conn.close()

	return result


def company_search(sort_by = 'ratings', num = 'top=10', place = None):
	conn = sqlite3.connect(DBNAME)
	cur = conn.cursor()

	statement = 'SELECT Bars.Company, co.EnglishName AS CompanyLocation, '
	
	if sort_by == 'ratings':
		statement += 'ROUND(AVG(Bars.Rating), 1) '
		statement += 'FROM Bars JOIN Countries AS co ON Bars.CompanyLocationId = co.Id '
		statement += 'GROUP BY Bars.Company HAVING COUNT(*) > 4 '
	elif sort_by == 'cocoa':
		statement += 'ROUND(AVG(Bars.CocoaPercent), 2) '
		statement += 'FROM Bars JOIN Countries AS co ON Bars.CompanyLocationId = co.Id '
		statement += 'GROUP BY Bars.Company HAVING COUNT(*) > 4 '
	elif sort_by == 'bars_sold':
		statement += 'COUNT(*) AS BarsSold '
		statement += 'FROM Bars JOIN Countries AS co ON Bars.CompanyLocationId = co.Id '
		statement += 'GROUP BY Bars.Company HAVING COUNT(*) > 4 '

	if place != None:
		statement += 'AND '
		if place[:8] == 'country=':
			statement += 'co.Alpha2 = "' + str(place[8:] + '" ')
		elif place[:7] == 'region=':
				statement += 'co.Region = "' + str(place[7:] + '" ')

	if sort_by == 'ratings':
		statement += 'ORDER BY AVG(Bars.Rating) '
	elif sort_by == 'cocoa':
		statement += 'ORDER BY AVG(Bars.CocoaPercent) '
	elif sort_by == 'bars_sold':
		statement += 'ORDER BY BarsSold '

	if num[:4] == 'top=':
		statement += 'DESC '
		statement += 'LIMIT ' + str(num[4:])
	elif num[:7] == 'bottom=':
		statement += 'LIMIT ' + str(num[7:])

	cur.execute(statement)

	result = cur.fetchall()
	conn.close()

	return result


def country_search(seller = 'sellers', sort_by = 'ratings', num = 'top=10', place = None):
	conn = sqlite3.connect(DBNAME)
	cur = conn.cursor()

	statement = 'SELECT co.EnglishName, co.Region, '
	
	if sort_by == 'ratings':
		statement += 'ROUND(AVG(Bars.Rating), 1) '
		statement += 'FROM Countries AS co JOIN Bars ON co.Id = Bars.'
		if seller == 'sellers':
			statement += 'CompanyLocationId GROUP BY Bars.CompanyLocationId HAVING COUNT(*) > 4 '
		elif seller == 'sources':
			statement += 'BroadBeanOriginId GROUP BY Bars.BroadBeanOriginId HAVING COUNT(*) > 4 '
	elif sort_by == 'cocoa':
		statement += 'ROUND(AVG(Bars.CocoaPercent), 2) '
		statement += 'FROM Countries AS co JOIN Bars ON co.Id = Bars.'
		if seller == 'sellers':
			statement += 'CompanyLocationId GROUP BY Bars.CompanyLocationId HAVING COUNT(*) > 4 '
		elif seller == 'sources':
			statement += 'BroadBeanOriginId GROUP BY Bars.BroadBeanOriginId HAVING COUNT(*) > 4 '
	elif sort_by == 'bars_sold':
		statement += 'COUNT(*) AS BarsSold '
		statement += 'FROM Countries AS co JOIN Bars ON co.Id = Bars.'
		if seller == 'sellers':
			statement += 'CompanyLocationId GROUP BY Bars.CompanyLocationId HAVING COUNT(*) > 4 '
		elif seller == 'sources':
			statement += 'BroadBeanOriginId GROUP BY Bars.BroadBeanOriginId HAVING COUNT(*) > 4 '

	if place != None:
		statement += 'AND '
		if place[:7] == 'region=':
			statement += 'co.Region = "' + str(place[7:] + '" ')

	if sort_by == 'ratings':
		statement += 'ORDER BY AVG(Bars.Rating) '
	elif sort_by == 'cocoa':
		statement += 'ORDER BY AVG(Bars.CocoaPercent) '
	elif sort_by == 'bars_sold':
		statement += 'ORDER BY BarsSold '

	if num[:4] == 'top=':
		statement += 'DESC '
		statement += 'LIMIT ' + str(num[4:])
	elif num[:7] == 'bottom=':
		statement += 'LIMIT ' + str(num[7:])

	cur.execute(statement)

	result = cur.fetchall()
	conn.close()

	return result


def region_search(seller = 'sellers', sort_by = 'ratings', num = 'top=10'):
	conn = sqlite3.connect(DBNAME)
	cur = conn.cursor()

	statement = 'SELECT co.Region, '
	
	if sort_by == 'ratings':
		statement += 'ROUND(AVG(Bars.Rating), 1) '
		statement += 'FROM Countries AS co JOIN Bars ON co.Id = Bars.'
		if seller == 'sellers':
			statement += 'CompanyLocationId GROUP BY co.Region HAVING COUNT(*) > 4 '
		elif seller == 'sources':
			statement += 'BroadBeanOriginId GROUP BY co.Region HAVING COUNT(*) > 4 '
		statement += 'ORDER BY AVG(Bars.Rating) '
	elif sort_by == 'cocoa':
		statement += 'ROUND(AVG(Bars.CocoaPercent), 2) '
		statement += 'FROM Countries AS co JOIN Bars ON co.Id = Bars.'
		if seller == 'sellers':
			statement += 'CompanyLocationId GROUP BY co.Region HAVING COUNT(*) > 4 '
		elif seller == 'sources':
			statement += 'BroadBeanOriginId GROUP BY co.Region HAVING COUNT(*) > 4 '
		statement += 'ORDER BY AVG(Bars.CocoaPercent) '
	elif sort_by == 'bars_sold':
		statement += 'COUNT(*) AS BarsSold '
		statement += 'FROM Countries AS co JOIN Bars ON co.Id = Bars.'
		if seller == 'sellers':
			statement += 'CompanyLocationId GROUP BY co.Region HAVING COUNT(*) > 4 '
		elif seller == 'sources':
			statement += 'BroadBeanOriginId GROUP BY co.Region HAVING COUNT(*) > 4 '
		statement += 'ORDER BY BarsSold '

	if num[:4] == 'top=':
		statement += 'DESC '
		statement += 'LIMIT ' + str(num[4:])
	elif num[:7] == 'bottom=':
		statement += 'LIMIT ' + str(num[7:])

	cur.execute(statement)

	result = cur.fetchall()
	conn.close()

	return result




def process_command(command):
	command_lst = command.split(' ')
	return_lst = []
	
	if command_lst[0] == 'bars':

		place_command = ['sellcountry=', 'sellregion=', 'sourcecountry=', 'sourceregion=']
		sort_by_command = ['ratings', 'cocoa']
		num_command = ['top=', 'bottom=']
		
		status = 0
		place = None
		sort_by = ''
		num = ''

		for element in command_lst[1:]:
					
			if element in sort_by_command:
				sort_by = element			
			elif element[:4] == num_command[0] or element[:7] == num_command[1]:
				num = element
			elif element[:12] == place_command[0] or element[:11] == place_command[1] or element[:14] == place_command[2] or element[:13] == place_command[3]:
				place = element				
			else:
				print('Command not recognized: ' + command)
				status = 1
				break
		
		if status == 0:	
			if sort_by == '':
				sort_by = 'ratings'
			if num == '':
				num = 'top=10'
		
			return_lst = bars_search(sort_by, num, place)
	
	elif command_lst[0] == 'companies':

		status = 0
		place = None
		sort_by = ''
		num = ''
		
		place_command = ['country=', 'region=']
		sort_by_command = ['ratings', 'cocoa', 'bars_sold']
		num_command = ['top=', 'bottom=']
	
		for element in command_lst[1:]:
	
			if element[:8] == place_command[0] or element[:7] == place_command[1]:
				place = element							
			elif element in sort_by_command:
				sort_by = element				
			elif element[:4] == num_command[0] or element[:7] == num_command[1]:
				num = element
			else:
				print('Command not recognized: ' + command)
				status = 1
				break

		if status == 0:	
			if sort_by == '':
				sort_by = 'ratings'
			if num == '':
				num = 'top=10'
			
			return_lst = company_search(sort_by, num, place)
		

	elif command_lst[0] == 'countries':

		seller_command = ['sellers', 'sources']
		sort_by_command = ['ratings', 'cocoa', 'bars_sold']
		num_command = ['top=', 'bottom=']

		status = 0
		place = None
		seller = ''
		sort_by = ''
		num = ''

		for element in command_lst[1:]:
			
			if element[:7] == 'region=':
				place = element		
			elif element in seller_command:
				seller = element			
			elif element in sort_by_command:
				sort_by = element				
			elif element[:4] == num_command[0] or element[:7] == num_command[1]:
				num = element
			else:
				print('Command not recognized: ' + command)
				status = 1
				break
		
		if status == 0:	
			if sort_by == '':
				sort_by = 'ratings'
			if seller == '':
				seller = 'sellers'
			if num == '':
				num = 'top=10'

			return_lst = country_search(seller, sort_by, num, place)
		

	elif command_lst[0] == 'regions':
		
		seller_command = ['sellers', 'sources']
		sort_by_command = ['ratings', 'cocoa', 'bars_sold']
		num_command = ['top=', 'bottom=']

		status = 0
		seller = ''
		sort_by = ''
		num = ''
				
		for element in command_lst[1:]:
			
			if element in seller_command:
				seller = element
			elif element in sort_by_command:
				sort_by = element			
			elif element[:4] == num_command[0] or element[:7] == num_command[1]:
				num = element
			else:
				print('Command not recognized: ' + command)
				status = 1
				break
		
		if status == 0:	
			if sort_by == '':
				sort_by = 'ratings'
			if seller == '':
				seller = 'sellers'
			if num == '':
				num = 'top=10'

			return_lst = region_search(seller, sort_by, num)

	else:
		print('Command not recognized: ' + command)

	return return_lst


def load_help_text():
	with open('help.txt') as f:
		return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def get_max_column_length(return_lst_tup, i):
	max_len = 0
	for tup in return_lst_tup:
		if len(str(tup[i])) > max_len:
			max_len = len(str(tup[i]))
	return max_len

def pretty_print(return_lst_tup):
	margin_lst = []
	margin = 2
	for i in range(0, len(return_lst_tup[0])):
		margin_lst.append(get_max_column_length(return_lst_tup, i))
		
	for row in return_lst_tup:
		out = ''
		for i in range(0, len(row)):
			try:
				if float(row[i]) > 0 and float(row[i]) < 1:
					out += str(int(float(row[i])*100)) + '%' + (margin_lst[i] + margin - len(str(int(float(row[i])*100))) - 1) * ' '
				elif float(row[i]) >= 1:
					out += str(row[i]) + (margin_lst[i] + margin - len(str(row[i]))) * ' '
			except:
				out += str(row[i]) + (margin_lst[i] + margin - len(str(row[i]))) * ' '
		print(out)


def interactive_prompt():
	help_text = load_help_text()
	response = input('Enter a command: ')
	while response != 'exit':
		
		if response == 'help':
			print(help_text)
			continue

		else:
			return_lst = process_command(response)
			try:
				pretty_print(return_lst)
			except:
				pass

		response = input('Enter a command: ')

	print('bye')




# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
	interactive_prompt()
