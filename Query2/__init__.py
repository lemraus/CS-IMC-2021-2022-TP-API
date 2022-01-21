import logging

import azure.functions as func


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

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0 or len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""
    try:
        logging.info("Test de connexion avec py2neo...")
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        artists = graph.run("MATCH (:Film)<-[r1]-(n:Artist)-[r2]->(:Film) WHERE type(r1) <> type(r2) WITH DISTINCT n RETURN n.primaryName LIMIT 10")
        dataString += "Noms des artistes ayant eu plusieurs responsabilités au cours de leur carrière (10 premières entrées) :"
        for artist in artists:
            dataString += f"{artist['n.primaryName']}\n"
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.\n\n{dataString}")
    else:
        return func.HttpResponse(
             f"This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.\n\n{dataString}",
             status_code=200
        )
