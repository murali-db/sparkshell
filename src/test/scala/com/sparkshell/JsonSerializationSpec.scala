package com.sparkshell

import com.google.gson.Gson
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JsonSerializationSpec extends AnyFlatSpec with Matchers {

  val gson = new Gson()

  "SqlRequestJson" should "deserialize from JSON" in {
    val json = """{"sql": "SELECT 1"}"""
    val request = gson.fromJson(json, classOf[SqlRequestJson])

    request.sql shouldBe "SELECT 1"
  }

  "SqlResponseJson" should "serialize to JSON" in {
    val response = SqlResponseJson(
      success = true,
      result = "id\n--\n1",
      error = null
    )

    val json = gson.toJson(response)

    json should include("\"success\":true")
    json should include("\"result\":")
    json should include("id")
  }

  it should "serialize error response" in {
    val response = SqlResponseJson(
      success = false,
      result = null,
      error = "Table not found"
    )

    val json = gson.toJson(response)

    json should include("\"success\":false")
    json should include("\"error\":\"Table not found\"")
  }

  "ServerInfo" should "serialize correctly" in {
    val info = ServerInfo(
      sparkVersion = "4.0.0",
      port = "8080",
      endpoints = EndpointsInfo(
        health = "GET /health",
        execute = "POST /sql",
        info = "GET /info"
      )
    )

    val json = gson.toJson(info)

    json should include("\"sparkVersion\":\"4.0.0\"")
    json should include("\"port\":\"8080\"")
    json should include("\"health\":\"GET /health\"")
    json should include("\"execute\":\"POST /sql\"")
    json should include("\"info\":\"GET /info\"")
  }

  "EndpointsInfo" should "serialize correctly" in {
    val endpoints = EndpointsInfo(
      health = "GET /health",
      execute = "POST /sql",
      info = "GET /info"
    )

    val json = gson.toJson(endpoints)

    json should include("\"health\":")
    json should include("\"execute\":")
    json should include("\"info\":")
  }
}
