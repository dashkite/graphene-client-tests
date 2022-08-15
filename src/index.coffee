import "source-map-support/register"
import { test as _test, success } from "@dashkite/amen"
import { print, debug } from "@dashkite/amen-console"
import assert from "@dashkite/assert"

import * as Time from "@dashkite/joy/time"

test = ( description, options, f ) ->
  if f?
    _test { description, options...}, f
  else
    # 2nd argument is f
    _test description, options

run = do ({ db, collection, response, content } = {}) ->

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

          test "Delete"

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
          
            await Time.sleep 2000

            response = await collection.getStatus()

            # console.log "get status", response
            assert.equal response.status?
            assert response.status == "not ready" ||
              response.status == "ready"

          await test "Status (ready)", wait: false, ->

            while response.status != "ready"
              await Time.sleep 1000
              response = await collection.getStatus()

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

          test "Delete"
          
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

      await test "Entry", await do ->

        entry = "star-wars"

        [

          await test "Empty List (with metadata)", wait: false, ->

            list = await collection.metadata.list()

            content = list.entries
          
            # console.log "list entries", content
            assert content.length?
            assert.equal 0, content.length

          await test "Create", wait: false, ->

            content = await collection.entries.put "star-wars",
              title: "Star Wars"
              year: "1977"

            # console.log "create entry", content
            assert content.title?
            assert.equal content.title, "Star Wars"
            assert content.year?
            assert.equal content.year, "1977"

          await test "Get", wait: false, ->

            await Time.sleep 2000

            content = await collection.entries.get "star-wars"

            # console.log "get entry", content
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

          await test "List", wait: false, ->

            content = await collection.entries.list()
          
            # console.log "list entries", content
            assert content.length?
            assert.equal 1, content.length
            assert.equal "Star Wars", content[0].title

          await test "List (with metadata)", wait: false, ->

            list = await collection.metadata.list()

            content = list.entries
          
            # console.log "list entries", content
            assert content.length?
            assert.equal 1, content.length
            assert.equal "Star Wars", content[0].content.title
            # console.log content[0].key
            assert.equal "star-wars", content[0].key

          test "Increment"

          test "Decrement"
          
          await test "Query", wait: false, ->

            await Time.sleep 2000

            content = await collection.entries.query title: "Star Wars"

            # console.log "query entry", content
            assert.equal content.director, "George Lucas"

          test "Query All"

          await test "Delete", wait: false, ->
            collection.entries.delete "star-wars"


        ]

    ]

export default run