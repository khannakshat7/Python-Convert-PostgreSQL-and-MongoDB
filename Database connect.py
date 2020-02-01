import mysql.connector,sys

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="classicmodels"
)

mycursor = mydb.cursor()
mycursor.execute("SELECT customerName,phone FROM customers WHERE customerName LIKE '"+sys.argv[1]+"%'")
result = mycursor.fetchall()
print (result)
