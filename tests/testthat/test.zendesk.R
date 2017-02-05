context("Zendesk api helper functions")

# Environment variables required
# Sys.setenv(ZENDESK_USER = ...)
# Sys.setenv(ZENDESK_PASSWORD = ...)

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
  library(httptest)
  with_mock_API({
    tickets <- zdGetTickets("madetest", 0)
    expect_s3_class(tickets, "data.table")
    expect_identical(nrow(tickets), as.integer(2))
  })

})

context("Zendesk user updates extract")

test_that("Users are returned as data.table", {

 # users <- zdGetUsers("madetest", 0)
  #expect_s3_class(users, "data.table")
#  expect_identical(nrow(users), as.integer(4))
})