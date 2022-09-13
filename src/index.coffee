import { test as _test, success } from "@dashkite/amen"
import { print, debug } from "@dashkite/amen-console"
import assert from "@dashkite/assert"

import * as Time from "@dashkite/joy/time"

Env = 
  array: ( name ) -> ( process.env[ name ]?.split /\s/ ) ? []
  text: ( name ) -> process.env[ name ] ? ""

test = ( description, options, f ) ->
  if "genie" in Env.array "DEBUG"
    console.log description
  if f?
    _test { description, options...}, f
  else
    # 2nd argument is f
    _test description, options

wait = ({ interval, predicate, action }) ->
  interval ?= 5
  loop
    break if predicate await action() 
    await Time.sleep interval * 1000

target = ( name, f ) ->
  f() if ( name in Env.array "targets" )

run = do ({ 
  db
  collection
  index
  key
  sort
  response
  content 
} = {}) ->

  ( client, hooks = {}) ->

    print await test "Graphene", [

      await test "Database", await do ->

        [

          await test "Create", wait: false, ->

            db = await client.db.create name: "My Database"

            # console.log "create db", db
            assert db.address?
            assert db.created?
            assert db.updated?
            assert.equal db.name, "My Database"
            hooks?.Database?.Create? db

          await test "Get", wait: false, ->

            await Time.sleep 2000
            db = await client.db.get db.address

            # console.log "get db", db
            assert db.address?
            assert db.created?
            assert db.updated?
            assert.equal db.name, "My Database"

          await test "Put", wait: false, ->

            db = await db.put name: "My Updated Database" 

            # console.log "put db", db
            assert db.address?
            assert db.created?
            assert db.updated?
            assert db.updated > db.created
            assert.equal db.name, "My Updated Database"

        ]

      await test "Collection", await do ->

        [

          await test "Create", ->

            collection = await db.collections.create "favorite-films",
              name: "Favorite Films"

            # console.log "create collection", collection
            assert collection.byname?
            assert collection.created?
            assert collection.updated?
            assert.equal collection.name, "Favorite Films"

          await test "Status (not ready)", wait: false, ->
          
            response = await collection.getStatus()

            # console.log "get status", response
            assert.equal response.status?
            assert response.status == "not ready" ||
              response.status == "ready"

          await test "Status (ready)", wait: false, ->

            wait
              predicate: ( response ) -> 
                # console.log "status ready", response
                response.status == "ready"
              action: -> collection.getStatus()

          await test "Get", wait: false, ->

            collection = await db.collections.get collection.byname
            # console.log "get collection", collection
            assert collection.byname?
            assert collection.created?
            assert collection.updated?
            assert.equal collection.name, "Favorite Films"

          await test "Put", ->

            collection = await collection.put name: "Favorite Shows And Films"
            # console.log "put collection", collection
            assert collection.byname?
            assert collection.created?
            assert collection.updated?
            assert collection.updated > collection.created
            assert.equal collection.name, "Favorite Shows And Films"

          await test "List", ->
            content = await db.collections.list()
            # console.log "list collections", content
            assert content?.length?
            assert.equal content.length, 1
            assert content[0].created?
            assert content[0].updated?
            assert content[0].updated > content[0].created
            assert.equal content[0].name, "Favorite Shows And Films"
        ]

      await test "Indexing", await target "indexing", ->
        
        key = "title"
        sort = "year"

        [

          await test "Create", wait: false, ->

            index = await collection.indices.create { key, sort }

            assert.equal key, index.key
            assert.equal sort, index.sort
            assert.equal "not ready", index.status

            # console.log "create index", response      

          await test "Get", wait: false, ->
            index = await collection.indices.get { key, sort }

            # console.log "get index", index      

            assert.equal key, index.key
            assert.equal sort, index.sort

          await test "Status (ready)", wait: false, ->

            wait
              interval: 30
              predicate: ( index ) -> 
                # console.log "status ready", index
                index.status == "ready"
              action: ->
                collection.indices.get { key, sort }

          await test "List", wait: false, ->

            indices = await collection.indices.list()

            # console.log "list indices", indices      

            assert.equal 1, indices.length 
            assert.equal "ready", indices[0].status
            assert.equal key, indices[0].key
            assert.equal sort, indices[0].sort

        ]

      await test "Metadata", await do ->

        [

          await test "Empty List", ->
            list = await collection.metadata.list()
            content = list.entries

            # console.log "list entries (with metadata)", entries
            assert content.length?
            assert.equal 0, content.length

        ]

      await test "Entry", await do ->

        entry = "star-wars"

        [

          await test "Create", wait: false, ->

            content = await collection.entries.put "star-wars",
              title: "Star Wars"
              year: "1977"

            # console.log "create entry", entry
            assert content.title?
            assert.equal content.title, "Star Wars"
            assert content.year?
            assert.equal content.year, "1977"

          await test "Get", wait: false, ->

            await Time.sleep 2000

            content = await collection.entries.get "star-wars"

            # console.log "get entry", entry
            assert content.title?
            assert.equal content.title, "Star Wars"
            assert content.year?
            assert.equal content.year, "1977"

          await test "Put", wait: false, ->

            content = await collection.entries.put "star-wars",
              { content..., director: "George Lucas" }

            # console.log "update entry", content
            assert content.title?
            assert.equal content.title, "Star Wars"
            assert content.year?
            assert.equal content.year, "1977"
            assert content.director?
            assert.equal content.director, "George Lucas"

          await test "Increment", ->
            # { views } = await collection.entries.increment "star-wars", "views"
            views = await collection.entries.increment "star-wars", "views"
            # console.log "increment", content
            assert.equal 1, views

          await test "Decrement", ->
            views = await collection.entries.decrement "star-wars", "views"
            assert.equal 0, views
        
          await test "List", wait: false, ->

            content = await collection.entries.list()
          
            # console.log "list entries", content
            assert content.length?
            assert.equal 1, content.length
            assert.equal "Star Wars", content[0].title

          await test "Query", wait: false, ->

            await Time.sleep 2000

            content = await collection.entries.query title: "Star Wars"

            # console.log "query entry", content
            assert.equal content.director, "George Lucas"

          await test "Query All", wait: false, ->
            content = await collection.entries.queryAll title: "Star Wars"
            # console.log "query all entry", content
            assert content.length?
            assert.equal 1, content.length
            assert.equal "Star Wars", content[ 0 ].title

        ]

      await test "Metadata", await do ->

        [

          await test "Get", wait: false, ->

            content = await collection.metadata.get "star-wars"

            # console.log "get metadata", content
            assert.equal "star-wars", content.entry
            assert content.content?
            data = content.content
            assert data.title?
            assert.equal data.title, "Star Wars"
            assert data.year?
            assert.equal data.year, "1977"

          await test "List", wait: false, ->

            list = await collection.metadata.list()

            # console.log "list entries", content

            content = list.entries
          
            assert content.length?
            assert.equal 1, content.length
            assert.equal "Star Wars", content[0].content.title
            # console.log content[0].key
            assert.equal "star-wars", content[0].key

        await test "Query", wait: false, ->

          content = await collection.metadata.query title: "Star Wars"

          # console.log "query metadata", content
          assert content?.content?
          data = content.content
          assert.equal data.director, "George Lucas"

        await test "Query All", wait: false, ->
          list = await collection.metadata.queryAll title: "Star Wars"
          content = list.entries

          # console.log "query all metadata", content
          assert content?.length?
          assert.equal 1, content.length
          assert.equal "Star Wars", content[ 0 ].content.title

        ]

      await test "Entry", await do ->

        [
          await test "Delete", wait: false, ->
            collection.entries.delete "star-wars"

        await test "Get (After Delete)", wait: false, ->
          wait
            predicate: ( response ) ->
              # console.log "get entry after delete", content
              !content?
            action: ->
              content = await collection.entries.get "star-wars"

        ]

      await test "Indexing", await target "indexing", ->
    
        [
          
          await test "Delete", wait: false, ->
            collection.indices.delete { key, sort }

          await test "Status (deleted)", wait: false, ->
            wait
              predicate: ( response ) -> 
                # console.log "index status after delete", index
                !index?
              action: ->
                index = await collection.indices.get { key, sort }

          await test "List", wait: false, ->
            indices = await collection.indices.list()

            # console.log "list indices after delete", indices

            assert.equal 0, indices.length 

        ]

      await test "Collection", [

        await test "Delete", wait: false, ->
          collection.delete()

        await test "Get (After Delete)", wait: false, ->

          wait
            predicate: ( response ) -> 
              # console.log "collection get after delete", collection
              !collection?
            action: ->
              collection = await db.collections.get collection.byname
      ]

      await test "DB", [

        await test "Delete", wait: false, ->
          db.delete()

        await test "Get (After Delete)", wait: false, ->

          wait
            predicate: ( response ) ->
              # console.log "db get after delete", db
              !db?
            action: ->
              db = await client.db.get db.address

      ]

    ]

export default run