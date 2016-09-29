rm(list = ls())
library(RevoScaleR)

rxOptions(numCoresToUse = 14)
rxSetComputeContext("localpar") # use the local parallel compute context

xdfDir <- "C:/Users/adminuser/Downloads/XDF1M/"
xdfs <- list.files(xdfDir,pattern = "yellow*", full.names = TRUE)

Sys.setenv(TZ = 'America/New_York')

passengers_hour <- function(inXdfFile, hour24, minPassengers, outXdfPath) {
    outXdfFile <- file.path(paste(outXdfPath, "hour_", hour24, "_passenger_", minPassengers, ".xdf"))
    rxDataStep(inData = inXdfFile, outFile = outXdfFile, rowSelection = (passenger_count >= passengers) & (strftime(pickup_datetime, format = "%H") == hour), transformObjects = list(hour = hour24, passengers = minPassengers), varsToKeep = c("pickup_longitude", "pickup_latitude", "trip_distance"), overwrite = TRUE)
}

fileList <- list(list(inXdfFile = xdfs[1], hour24 = "20", minPassengers = 5), list(inXdfFile = xdfs[2], hour24 = "21", minPassengers = 5), list(inXdfFile = xdfs[3], hour24 = "22", minPassengers = 5))
outXdfPath <- "C:/Users/adminuser/Downloads/XDF1M/subset_hour_passenger/"
rxExec(passengers_hour, elemArgs = fileList, outXdfPath)

filenames <- list.files(outXdfPath, pattern = "hour_*", full.names = TRUE)
datalist = lapply(filenames, function(x) { rxImport(inData = x) })
df <- do.call(rbind, datalist[1:length(datalist)])