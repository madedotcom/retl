context("DoubleClick Search API")

options(
  segmentation.type = "FLOODLIGHT",
  segmentation.name = "ML"
)

test_that("DoubleClick call List to body works", {
  metrics <- c(
    "Predicted Revenue" = 20L,
    "Predicted CVR" = 0.1,
    "Predicted AOV" = 200L
  )
  expect <- read_json("dc-conversion-insert.json")

  res <- dcPredictionBody(
    clickId = "CJPmyaSYmdMCFVcW0wodHMAE2w",
    conversionId = "23436345D",
    datetime = 1491819654000,
    custom.metrics = list(metrics),
    revenue = 10,
    currency.code = 'EUR'
  )
  expect_equal(res, expect)

  # put multiple transactions in the body
  metric2 <- c(
    "Predicted Revenue" = 10L,
    "Predicted CVR" = 0.8,
    "Predicted AOV" = 100L
  )

  res <- dcPredictionBody(
    clickId = c("CJPmyaSYmdMCFVcW0wodHMAE2w", "AJPmyaSYmdMCFVcW0wodHMAE3w"),
    conversionId = c("23436345D", "23436345A"),
    datetime = c(1491819654000, 1491819654002),
    custom.metrics = list(metrics, metric2),
    revenue = 10,
    currency.code = 'EUR'
  )
  expect <- read_json("dc-conversion-multiple-insert.json")
  expect_equal(res, expect)
})

test_that("Metrics vector to list conversion works", {
  custom.metrics <- c(
    "Predicted Revenue" = 20,
    "Predicted CVR" = 0.1,
    "Predicted AOV" = 200
  )
  res <- metricsToList(custom.metrics)
  expected.list <- list(
    list(
      name = "Predicted Revenue",
      value = 20
    ),
    list(
      name = "Predicted CVR",
      value = 0.1
    ),
    list(
      name = "Predicted AOV",
      value = 200
    )
  )
  expect_identical(res, expected.list)
})
