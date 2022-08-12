import { getSecret } from "@dashkite/dolores/secrets"
import * as Runes from "@dashkite/runes"
import { expand } from "@dashkite/polaris"

import authorizationDBCreate from "./authorization/db-create"
import authorizationDBUse from "./authorization/db"

secret = undefined

getRune = ( type, context ) ->
  secret ?= await getSecret "guardian"
  authorization = switch type
    when "db create" then authorizationDBCreate
    when "db use" then authorizationDBUse
  
  Runes.issue
    authorization: expand authorization, context
    secret: secret

export {
  getRune
}