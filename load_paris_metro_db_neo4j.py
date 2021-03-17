from neo4j import GraphDatabase

#by Aymeric Castellanet

driver = GraphDatabase.driver("bolt://0.0.0.0:7687",
                              auth=("neo4j", "neo4j"))

# Deleting previous data
print("Deleting previous data")

query = """
MATCH (n) 
DETACH DELETE n
"""

with driver.session() as session:
    print(query)
    session.run(query)

print("done")

# Inserting data
print("Inserting metro stations")

query = """
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/aymericastellanet/Neo4j_Paris_metro/main/stations.csv' AS row
CREATE (:Station {nom: row.nom_gare, nom_maj: row.nom_clean,
  coordonnees: point({x: toFloat(row.x), y: toFloat(row.y)}),
  trafic: toInteger(row.Trafic), ville: row.Ville,
  ligne:toString(row.ligne)
  });
  """

with driver.session() as session:
    print(query)
    session.run(query)

print("done")


print("Creating relationships")

queries = [
    """
    // Creating the relationships of station connections between metro lines
    MATCH (s1:Station)
    MATCH (s2:Station)
      WHERE s1.nom_maj = s2.nom_maj AND s1.ligne <> s2.ligne
    CREATE (s1)-[:CORRESPONDANCE_AVEC]->(s2)
    """,

    """
    // Creating the relationships for walking between two stations separated from less than a kilometer between them
    MATCH (s1:Station)
    MATCH (s2:Station)
      WHERE s1.nom_maj <> s2.nom_maj 
      AND distance(s1.coordonnees, s2.coordonnees) < 1000
    CREATE (s1)-[:LIAISON_PIED]->(s2)
    """,

    """
    // Loading the relationships of liaisons between stations
    LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/aymericastellanet/Neo4j_Paris_metro/main/liaisons.csv' AS row
    MATCH (s_dep:Station) WHERE s_dep.nom_maj = row.start
      AND s_dep.ligne = toString(row.ligne)
    MATCH (s_arr:Station) WHERE s_arr.nom_maj = row.stop
      AND s_arr.ligne = toString(row.ligne)
    CREATE (s_dep)-[:LIAISON_TRAIN {ligne: toString(row.ligne)}]->(s_arr);
    """
]

with driver.session() as session:
    for q in queries:
        print(q)
        session.run(q)

print("done")