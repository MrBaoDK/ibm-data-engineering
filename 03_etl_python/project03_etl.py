# Code for ETL operations on Country-GDP data

# Importing the required libraries
from requests import get
from bs4 import BeautifulSoup
from pandas import DataFrame, read_sql
from datetime import datetime
import sqlite3

def extract(url, table_attribs):
	''' This function extracts the required
	information from the website and saves it to a dataframe. The
	function returns the dataframe for further processing. '''

	web_content = get(url).text
	parsed_soup = BeautifulSoup(web_content, 'html.parser')
	table_rows = parsed_soup.find_all('tbody')[2].find_all('tr')
	data_dicts = []
	for table_row in table_rows:
		if table_row.is_empty_element:
			continue
		cells = table_row.find_all("td")
		if not cells:
			continue
		if not cells[0].find('a'):
			continue
		if not cells[2].text or '—' in cells[2].text:
			continue
		data_dict = {"Country": cells[0].a.contents[0]
						,"GDP_USD_millions": cells[2].contents[0]}
		data_dicts.append(data_dict)
	df = DataFrame(columns=table_attribs, data=data_dicts)
	return df

def transform(df):
	''' This function converts the GDP information from Currency
	format to float value, transforms the information of GDP from
	USD (Millions) to USD (Billions) rounding to 2 decimal places.
	The function returns the transformed dataframe.'''

	def text_to_float(txt):
		f_m = float(txt.replace(',', ''))
		f_b = round(f_m/1000,2)
		return f_b

	df["GDP_USD_billions"] = df["GDP_USD_millions"].apply(text_to_float)
	df.drop("GDP_USD_millions", axis=1, inplace=True)

	return df

def load_to_csv(df, csv_path):
	''' This function saves the final dataframe as a `CSV` file 
	in the provided path. Function returns nothing.'''
	df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
	''' This function saves the final dataframe as a database table
	with the provided name. Function returns nothing.'''
	df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
	''' This function runs the stated query on the database table and
	prints the output on the terminal. Function returns nothing. '''
	print(query_statement)
	query_output = read_sql(query_statement, sql_connection)
	print(query_output)

def log_progress(message):
	''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
	timestamp_format = r"%Y-%h-%d-%H:%M:%S"
	now = datetime.now()
	timestamp = now.strftime(timestamp_format)
	with open("./etl_project_log.txt", "a") as f:
		print(timestamp, ' : ', message, file=f)

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
def main():
	BASE_URL = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
	TABLE_ATTRIBS = ["Country", "GDP_USD_millions"]
	DB_NAME = 'World_Economies.db'
	TABLE_NAME = 'Countries_by_GDP'
	CSV_PATH = './Countries_by_GDP.csv'
	log_progress('Preliminaries complete. Initiating ETL process')
	df = extract(BASE_URL, TABLE_ATTRIBS)
	log_progress('Data extraction complete. Initiating Transformation process')
	df = transform(df)
	log_progress('Data transformation complete. Initiating loading process')
	load_to_csv(df, CSV_PATH)
	log_progress('Data saved to CSV file')
	sql_connection = sqlite3.connect(DB_NAME)
	log_progress('SQL Connection initiated.')
	load_to_db(df, sql_connection, TABLE_NAME)
	log_progress('Data loaded to Database as table. Running the query')
	query_statement = f"SELECT * from {TABLE_NAME} WHERE GDP_USD_billions >= 100"
	run_query(query_statement, sql_connection)
	log_progress('Process Complete.')
	sql_connection.close()

if __name__=='__main__':
	main()