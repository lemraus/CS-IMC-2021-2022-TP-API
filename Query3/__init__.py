import logging
import os
import pyodbc as pyodbc
import azure.functions as func
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
            
    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""
    try:
        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT birthYear, COUNT(*) FROM tArtist WHERE birthYear = ( SELECT TOP(1) birthYear FROM tArtist WHERE birthYear != '0' GROUP BY birthYear ORDER BY COUNT(*) DESC ) GROUP BY birthYear")

            rows = cursor.fetchall()
            dataString += f"Année de naissance la plus représentée et nombre d'artistes étant nés cette année-là :\n{rows[0][0]} {rows[0][1]}\n"
    except:
        errorMessage = "Erreur de connexion a la base SQL"

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.\n\n{dataString}{errorMessage}")
    else:
        return func.HttpResponse(
             f"This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.\n\n{dataString}{errorMessage}",
             status_code=200
        )