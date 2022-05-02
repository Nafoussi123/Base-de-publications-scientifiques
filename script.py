from pymongo import MongoClient 
import pprint 

user = "root"
mdp = "pass12345"

client = MongoClient(f"mongodb://{user}:{mdp}@localhost:27017")
db = client['DBLP']
collection = db['publis']


#Compter le nombre de documents de la collection publis
#db.publis.count()
nb_documents = collection.count_documents({})
print(nb_documents)


#Lister tous les livres
# db.publis.find({"type":"Book"})
livres = collection.find({"type":"Book"})
for livre in livres :
    print(livre['title'])


#Lister les livres depuis 2014
#db.publis.find({$and:[{"type":"Book"}, {"year":2014}]}).pretty()
livres_2014 = collection.find({"year" : {"$gte":2014}},{"title":1, "year":1})
for livre in livres_2014:
    pprint.pprint(livre)


#Lister les publications de l’auteur “Toru Ishida”
#db.publis.find({"authors":"Toru Ishida"}).pretty()
publi_auteur = collection.find({"authors":"Toru Ishida"})

print("Publications de Toru Ishida")
for publi in publi_auteur :
    print(publi['title'], publi['authors'])


#Lister tous les auteurs distincts
auteurs = collection.distinct("authors")
pprint.pprint(auteurs)


# Trier les publications de “Toru Ishida” par titre de livre
publi_Toru = collection.find(filter={"authors":"Toru Ishida"},sort = [("title",1)])
for publi in publi_Toru :
    print(publi['title'])

#Compter le nombre de ses publications
trouver = {"authors":"Toru Ishida"}
nb_publi = collection.count_documents(trouver)
print("Nombre de publications de Toru Ishida : ")
pprint.pprint(nb_publi)


# Nombre de publications par type
# Compter le nombre de publications depuis 2011 et par type 
annee = {"$match" : {"year" : {"$gte":2011}}}
type = {"$group": {"_id" :"$type", "count": { "$sum" :1}}}
total = collection.aggregate([annee,type])

for nb in total:
    print(nb)


#Compter le nombre de publications par auteur et trier le résultat par ordre croissant

auteurs = collection.aggregate([{"$unwind": "$authors"},
                                {"$group":{"_id":"$authors", "count":{"$sum":1}}},
                                {"$sort":{"count":1}}])
for i in auteurs:
    print(i)