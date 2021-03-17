from neo4j import GraphDatabase
import pandas as pd

#by Aymeric Castellanet

"""
First you need to launch the docker container: 
docker run -p 7474:7474 -p 7687:7687 --name my_neo4j aymericastellanet/neo4j

Then open a web browser and enter the following adress:
http://localhost:7474/ to access the Neo4j Browser
and if needed, log in with the username and password: 'neo4j'

Then you have to fill the Neo4j database with the Paris subway data 
by running the file: python3 load_paris_metro_db_neo4j.py

Finally you can run this file to calculate your metro routes:
python3 calcul_itineraire_metro_neo4j.py
"""

driver = GraphDatabase.driver('bolt://0.0.0.0:7687',
                              auth=('neo4j', 'neo4j'))


def calculate_my_route(x_depart, y_depart, x_arrivee, y_arrivee):
	"""
	Calculate the shortest route by Paris metropolitan.
	Requires 4 arguments: x_depart and y_depart for the departure point, 
	and x_arrivee and y_arrivee for the arrival point.
	x is the latitude in meters (float), and y is the longitude in meters (float).
	For example, the location of Notre-Dame is (652109.5385, 6861853.2270),
	the location of the Arc de Triomphe is (648331.6965, 6864073.8346),
	and the location of the Louvre is (651322.2057, 6862683.9581).
	"""

	## Inserting the start node ##
	query = """
	CREATE (s_dep:Station {{nom: 'depart', nom_maj: 'DEPART', 
	  coordonnees: point({{x: toFloat({x_depart}), y: toFloat({y_depart})}})
	  }});
	  """.format(x_depart=x_depart, y_depart=y_depart)

	with driver.session() as session:
		session.run(query)


	## Inserting the end node ##
	query = """
	CREATE (s_arr:Station {{nom: 'arrivee', nom_maj: 'ARRIVEE', 
	  coordonnees: point({{x: toFloat({x_arrivee}), y: toFloat({y_arrivee})}})
	  }});
	  """.format(x_arrivee=x_arrivee, y_arrivee=y_arrivee)

	with driver.session() as session:
		session.run(query)


	## Searching for the nearest station from the start node ##
	query = """
	MATCH (s0:Station {nom_maj: "DEPART"})
	MATCH (s:Station)
	  WHERE s.nom_maj <> "DEPART" AND s.nom_maj <> "ARRIVEE"
	RETURN s.nom_maj AS station_depart, distance(s0.coordonnees, s.coordonnees) AS distance
	ORDER BY distance LIMIT 1
	"""

	with driver.session() as session:
		result = session.run(query).data()
		station_depart = result[0]["station_depart"]
		marche_depart = result[0]["distance"]
		print("Station de départ la plus proche :", station_depart,
			"à une distance de", round(marche_depart, 2), "mètres.")


	## Searching for the nearest station from the end node ##
	query = """
	MATCH (s0:Station {nom_maj: "ARRIVEE"})
	MATCH (s:Station)
	  WHERE s.nom_maj <> "DEPART" AND s.nom_maj <> "ARRIVEE"
	RETURN s.nom_maj AS station_arrivee, distance(s0.coordonnees, s.coordonnees) AS distance
	ORDER BY distance LIMIT 1
	"""

	with driver.session() as session:
		result = session.run(query).data()
		station_arrivee = result[0]["station_arrivee"]
		marche_arrivee = result[0]["distance"]
		print("Station d'arrivée la plus proche :", station_arrivee, 
			"à une distance de", round(marche_arrivee, 2), "mètres.", end='\n\n')


	## Searching for the shortest route by metro from the departure station to the arrival station ##
	# We use the Dijkstra Shortest Path algorithm implemented in Neo4j
	query = """
	MATCH (start:Station {{nom_maj: '{station_depart}'}})
	MATCH (end:Station {{nom_maj: '{station_arrivee}'}})
	CALL gds.alpha.shortestPath.stream({{
	  nodeQuery: "MATCH (n) RETURN id(n) AS id",
	  relationshipQuery: "MATCH (n1)-[r]->(n2) 
	    WHERE type(r) = 'LIAISON_TRAIN' OR type(r) = 'CORRESPONDANCE_AVEC' 
	    RETURN id(r) AS id, id(n1) AS source, id(n2) AS target",
	  startNode: start,
	  endNode: end}})
	YIELD nodeId
	RETURN gds.util.asNode(nodeId)
	""".format(station_depart=station_depart, station_arrivee=station_arrivee)

	with driver.session() as session:
		result = session.run(query).data()
		previous_station = "" #use to compare a station and the next one on your route
		nb_connections = 0 #use to count the number of connections
		df = pd.DataFrame(columns = ["Station_maj", "Station", "Ligne"]) #use to save the route into a DataFrame
		for station in result:
			#Saving the name of the station and its line for each node into the DataFrame
			df_append = pd.DataFrame({"Station_maj": [""], "Station": [""], "Ligne": [""]})
			df_append["Station_maj"] = station['gds.util.asNode(nodeId)']["nom_maj"]
			df_append["Station"] = station['gds.util.asNode(nodeId)']["nom"]
			df_append["Ligne"] = station['gds.util.asNode(nodeId)']["ligne"]
			df = df.append(df_append, ignore_index=True)

			#Count the number of connections: when there is a connection, two nodes in a row have the same name
			if previous_station == station['gds.util.asNode(nodeId)']["nom_maj"]:
				nb_connections += 1
			previous_station = station['gds.util.asNode(nodeId)']["nom_maj"]

	#Print the route, the number of connections and the number of stations
	print("Votre itinéraire est le suivant :")
	print(df[["Station", "Ligne"]])
	nb_stations = len(df["Station_maj"].unique()) - 1 #length of unique stations -1 (we don't count the departure station)
	print("\nVotre itinéraire comprend {} correspondance(s) et {} station(s).".format(nb_connections, nb_stations), end='\n\n')


	## Calculation of the distance between each station ##
	list_distances = [] #use to save every distance between two stations in a row on your route

	for i in range(df.shape[0] - 1):
		prev_station = list(df["Station_maj"])[i]
		next_station = list(df["Station_maj"])[i+1]

		query = """
		MATCH (s_prev:Station {{nom_maj: '{prev}'}})
		MATCH (s_next:Station {{nom_maj: '{next}'}})
		RETURN distance(s_prev.coordonnees, s_next.coordonnees) AS distance
		""".format(prev=prev_station, next=next_station)

		with driver.session() as session:
			result = session.run(query).data()
			distance_prev_next = result[0]["distance"]
			list_distances.append(distance_prev_next)

	distance_metro = sum(list_distances)


	## Printing the time calculation for the complete route ##
	# constant values:
	METRO_SPEED = 25000 #25 km/h = 25000 m/h (average metro speed couting the stops at each station)
	WALKING_SPEED = 4500 #4.5 km/h = 4500 m/h
	CONNECTING_TIME = 4 #4 minutes for a connection

	# time calculation (speed=distance/time <=> time=distance/speed):
	t_dep = marche_depart/WALKING_SPEED*60
	t_arr = marche_arrivee/WALKING_SPEED*60
	t_metro = distance_metro/METRO_SPEED*60 + nb_connections*CONNECTING_TIME

	print("Temps de marche à pied du point de départ à la station de départ :", 
		round(t_dep, 2), "minutes.")

	print("Temps dans le métro en comptant les correspondances :",
		round(t_metro, 2), "minutes.")

	print("Temps de marche à pied de la station d'arrivée au point d'arrivée :",
		round(t_arr, 2), "minutes.")

	print("============================")
	print("Temps total :", round(t_dep+t_metro+t_arr, 2), "minutes.")
	print("============================", end='\n\n')


	## Removing the start node ##
	query = """
	MATCH (s_dep:Station {nom_maj: 'DEPART'}) 
	DETACH DELETE s_dep
	"""

	with driver.session() as session:
		session.run(query)


	## Removing the end node ##
	query = """
	MATCH (s_arr:Station {nom_maj: 'ARRIVEE'}) 
	DETACH DELETE s_arr
	"""

	with driver.session() as session:
		session.run(query)



# First example:
x_dep, y_dep = 648600, 6863500
x_arr, y_arr = 651050, 6864290

calculate_my_route(x_dep, y_dep, x_arr, y_arr)

# Second example:
x_dep, y_dep = 644400, 6859400
x_arr, y_arr = 652600, 6860600

calculate_my_route(x_dep, y_dep, x_arr, y_arr)