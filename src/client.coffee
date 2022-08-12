import * as Fn from "@dashkite/joy/function"
import * as Meta from "@dashkite/joy/metaclass"
import { convert } from "@dashkite/bake"
import { Resource } from "@dashkite/vega-client"
import { DB } from "@dashkite/graphene-client"

import {
  proxy
} from "@dashkite/graphene-client/helpers"

class Client

  Meta.mixin @::, [
    proxy "db", DB, [ "create", "get" ]
  ]

  @create: ({ base }) ->
    Object.assign ( new @ ), { base }

  invoke: ({ method, resource, content }) ->
    resource = await Resource.create {
      origin: @base
      name: resource.name
      bindings: resource.bindings
      # TODO remove this once we have decoration back in API description
      authorization: rune: {}
    }
    ( resource[ method ] content )
    
export { Client }