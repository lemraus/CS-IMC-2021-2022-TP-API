import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
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

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    if len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""
    try:
        logging.info("Test de connexion avec py2neo...")
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        ratings = graph.run("match (g:Genre)<--(f:Film) return g.genre, avg(f.averageRating) as average order by average desc")
        dataString += "Moyenne des films par genre :\n"
        for r in ratings:
            dataString += f"{r['g.genre']} : {r['average']}\n"
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.\n\n{dataString}{errorMessage}")
    else:
        return func.HttpResponse(
             f"This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.\n\n{dataString}{errorMessage}",
             status_code=200
        )
