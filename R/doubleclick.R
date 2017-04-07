library(jsonlite)

dcPredictionBody <- function(clickId, conversionId, datetime, predicted.conversion, predicted.aov) {
  body = list(
    kind = "doubleclicksearch#conversionList",
    conversion = list(
      clickId = clickId,
      conversionId = conversionId,
      conversionTimestamp = datetime,
      segmentationType = "FLOODLIGHT",
      segmentationName = "Model",
      type = "ACTION",
      state = "ACTIVE",
      customMetric = list(
        list(
          name = "Predicted Revenue",
          value = predicted.conversion * predicted.aov
        ),
       list(
        name = "Predicted Conversion",
        value = predicted.conversion
       ),
       list(
         name = "Predicted AOV",
         value = predicted.aov
       )
      )
    )
  )
}



dcWriteConversion <-gar_api_generator("https://www.googleapis.com/doubleclicksearch/v2/conversion", http_header = "POST")

dcWritePredictions <- function(clickId, conversionId, conversion, aov) {

  body <- dcPredictionBody(clickId, conversionId, conversion, aov)
  dcWriteConversion(body)
}
