library(googleAuthR)
library(jsonlite)

test_that("DoubleClick call List to body works", {


  j <- '{
      "kind": "doubleclicksearch#conversionList",
      "conversion" : [{
      "clickId" : "COiYmPDTv7kCFcP0KgodOzQAAA", // Replace with a click ID from your site
      "conversionId" : "test_20130906_04",
      "conversionTimestamp" : "1378710000000",
      "segmentationType" : "FLOODLIGHT",
      "segmentationName" : "Test",
      "type": "TRANSACTION",
      "revenueMicros": "10000000", // 10 million revenueMicros is equivalent to $10 of revenue
      "currencyCode": "USD",
      "customMetric": [
        {
          "name": "sales",
          "value": 2.0
        },
        {
          "name": "shipping",
          "value": 3.0
        }
      ]
      }]
  }'

  res <- dcPredictionBody("xxx", "yyy", 1378710000000, 0.1, 100)
  print(toJSON(res, complex = "list", auto_unbox = T))
})


