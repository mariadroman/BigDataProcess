package com.lunatech.bigdata.model

/**
 * Created by mariadominguez on 21/10/2015.
 */
case class Event(id: String,
                 evType: String,
                 actor: Actor,
                 repo: Repo,
                 payload: Payload,
                 evPublic: Boolean,
                 evCreated: String
                  )

case class Payload()

case class Repo(id: Int, name: String, url: String)

case class Actor(id: Int, login: String, gravatar_id: String, avatar_url: String, url: String)
