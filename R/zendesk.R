
# Environment variables required
# Sys.setenv(ZENDESK_USER = ...)
# Sys.setenv(ZENDESK_PASSWORD = ...)

#' Makes url for incremental object extraction from Zendesk
#'
#' @param type type of the object to be returned (users, tickets)
#' @param start.time starting point for new objects based on the update_date
zdGetPath <- function (type, start.time) {
  zendesk.api.url <- paste0("/api/v2/incremental/", type, ".json")
  url <- paste0(zendesk.api.url, "?start_time=", start.time)
  return(url)
}

zdGetUrl<- function(subdomain) {
  url <- paste0("https://", subdomain,  ".zendesk.com")
}

#' Gets Zendesk objects for a given subdomain as a data.table
#'
#' @param type type of the object to be returned (users, tickets)
#' @param subdomain organisation subdomain on zendesk
#' @param start.time starting point for new objects based on the update_date
#' @return list of zendesk objects according to the type
zdGetObjects <- function(type, subdomain, start.time) {

  obj.list <- list() # default results as a blank list

  repeat { # Recursively call the api while end_time is set
    response <- GET(url  = zdGetUrl(subdomain),
                    path = zdGetPath(type, start.time),
                    add_headers("Content-Type" = "application/json"),
                    authenticate(user = Sys.getenv("ZENDESK_USER"),
                                 password = Sys.getenv("ZENDESK_PASSWORD"))
                )
    response.obj <- zdProcessResponse(response)

    end.time <- response.obj$end_time
    if(is.null(end.time)) {
      break # last page
    }

    obj.list <- append(obj.list, response.obj[[type]])
    start.time <- end.time + 1
  }

  return(obj.list)

}

#' Gets Zendesk tickets via increment API
#'
#' @param subdomain organisation subdomain on zendesk
#' @param start.time starting point for new tickets based on the update_date
#' @return data.table with zendesk tickets
zdGetTickets <- function(subdomain, start.time) {
  ticket.fields <- c("id", "created_at", "updated_at", "type", "status", "subject")
  tickets <- zdGetObjects("tickets", subdomain, start.time)
  if(length(tickets) == 0) {
    return(NULL)
  }
  dt <- zdExtractData(tickets, ticket.fields)
  return(dt)
}

#' Gets Zendesk users via increment API
#'
#' @param subdomain organisation subdomain on zendesk
#' @param start.time starting point for new users based on the update_date
#' @return data.table with zendesk users
zdGetUsers <- function(subdomain, start.time) {
  users <- zdGetObjects("users", subdomain, start.time)
  if(length(users) == 0) {
    return(NULL)
  }
  dt <- zdExtractData(users, c("id", "email", "created_at", "updated_at", "role"))
  return(dt)
}

#' Extracts data from list of tickets as parsed from json by httr::content
#'
#' @param items unnamed list of zendesk objects with attributes
#' @param fields string vector of fields to be extracted from the object
#' @return data.table with relevant object attributes
zdExtractData <- function(items, fields) {
  res <- lapply(items, function(t) {
    # subset attributes of a ticket
    return(t[fields])
  })

  dt <- rbindlist(res)
  return(dt)
}

#' Processes http response and returns a list of results
#'
#' @param response http response from the Zendesk API
#' @return list of resutls parsed from the response
zdProcessResponse <- function(response) {

  if (status_code(response) >= 200 && status_code(response) < 300)  {
    if (http_type(response) != "application/json") {
      stop("Zendesk API did not return json", call. = FALSE)
    }
    return(content(response, "parsed", "application/json"))
  }
  else { # error
    type <- http_type(response)
    if (type == "application/json") {
      out <- content(response, "parsed", "application/json")
      stop("HTTP error [", out$error, "] ", out$description, call. = FALSE)
    } else {
      out <- content(response, "text")
      stop("HTTP error [", response$status, "] ", out, call. = FALSE)
    }
  }
}
