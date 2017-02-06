library(httr)
library(data.table)
library(httptest)

context("Zendesk api helper functions")

test_that("Ticket increment path is correct", {
  type <- "tickets"
  subdomain <- "test"
  start.time <- 1332034771
  expect.path <- "/api/v2/incremental/tickets.json?start_time=1332034771"
  url <- zdGetPath(type, start.time)
  expect_identical(url, expect.path)
})

context("Zendesk ticket updates extract")

test_that("Tickets are returned as data.table", {
  with_mock_API({
    tickets <- zdGetTickets("test", 0)
    expect_s3_class(tickets, "data.table")
    expect_identical(nrow(tickets), as.integer(2))

    # check that call beyound history returns empty object
    tickets <- zdGetTickets("test", 1383685953)
    expect_null(tickets)
  })
})

context("Zendesk user updates extract")

test_that("Users are returned as data.table", {
  with_mock_API({
    users <- zdGetUsers("test", 0)
    expect_s3_class(users, "data.table")
    expect_identical(nrow(users), as.integer(2))

    # check that call beyound history returns empty object
    users <- zdGetUsers("test", 1383685953)
    expect_null(users)
  })
})

context("Zendesk bad responses")

test_that("Bad responses are processed correctly", {
  resp <- fakeResponse(status_code = 500,
                       headers = list("Content-Type" = "application/xml"),
                       content = '{"error": "ServerFault", "description": "Server Fault"}')
  expect_error(zdProcessResponse(resp), regexp = "ServerFault")

  resp <- fakeResponse(status_code = 200, headers = list("Content-Type" = "application/xml"))
  expect_error(zdProcessResponse(resp))

})