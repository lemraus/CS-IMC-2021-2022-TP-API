import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    genre = req.params.get('genre')
    acteur = req.params.get('acteur')
    directeur = req.params.get('directeur')

    if not genre and not acteur and not directeur:
        return func.HttpResponse(
            "Veuillez spécifier au moins un genre de film, un acteur ou un directeur afin d'obtenir la note moyenne des films associés",
            status_code=400
        )

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

        request = "match (g:Genre)<--(f:Film)<-[r]-(a:Artist)"
        if genre:
            request += f" where g.genre = '{genre}'"
        if acteur:
            request += f"{" and" if genre else ""} where a = '{acteur}' and type(r) = 'ACTED_IN'"
        if directeur:
            request += f"{" and" if genre or acteur else ""} where a = '{directeur}' and type(r) = 'DIRECTED'"
        request += " return distinct f.primaryTitle, avg(f.averageRating) as rating"

        ratings = graph.run()
        dataString += f"Moyenne des films pour genre='{genre if genre else "*"}' acteur='{acteur if acteur else "*"}' directeur='{directeur if directeur else "*"}' :\n"
        for r in ratings:
            dataString += f"{r['f.primaryTitle']} : {r['rating']}\n"
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.\n\n{dataString}{errorMessage}")
    else:
        return func.HttpResponse(
             f"This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.\n\n{dataString}{errorMessage}",
             status_code=200
        )
