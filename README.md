# Graphene Lambda Client

```coffeescript
# create the client by providing the Lambda you want to target
client = Graphene.Client.create "graphene-beta-development-api"

# create a new database
# the database will have a unique address
db = await client.db.create name: "My Database"

# create a collection within the database
# assign the collection a byname unique to the database
collection = await db.collections.create "favorite-films"
  name: "Favorite Films"

# wait for it to be ready
while ( await collection.getStatus() ).status != "ready"
  await Time.sleep 1000

# let's add something to our collection!
films = collection.entries

# start with a classic
await films.put "star-wars",
  title: "Star Wars"
  year: "1977"
  
data = await films.get "star-wars"

# update it to include the director
await films.put "star-wars",
  { data..., director: "George Lucas}
```
