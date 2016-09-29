rm(list = ls())
library(RevoScaleR)

rxOptions(numCoresToUse = 14)
rxSetComputeContext("localpar") # use the local parallel compute context

# Examine file using Powershell
# Print first 2 lines of the file
#PS C:\Users\adminuser\Downloads> powershell -command "& {Get-Content yellow_tripdata_2015-01.csv -TotalCount 2}"
#VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude, pickup_latitude, RateCodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
#2, 2015 - 01 - 15 19:05:39, 2015 - 01 - 15 19:23:42, 1, 1.59, -73.993896484375, 40.750110626220703, 1, N, -73.974784851074219, 40.750617980957031, 1, 12, 1, 0.5, 3.25, 0, 0.3, 17.05


colX <-
    list("VendorID" = list(type = "string", newName = "vendor_id"),
    "tpep_pickup_datetime" = list(type = "Date", newName = "pickup_datetime"),
    "tpep_dropoff_datetime" = list(type = "Date", newName = "dropoff_datetime"),
    "passenger_count" = list(type = "int16", newName = "passenger_count"),
    "trip_distance" = list(type = "numeric", newName = "trip_distance"),
    "pickup_longitude" = list(type = "numeric", newName = "pickup_longitude"),
    "pickup_latitude" = list(type = "numeric", newName = "pickup_latitude"),
    #"RateCodeID" = list(type = "string", newName = "rate_code_id"),
    "store_and_fwd_flag" = list(type = "string", newName = "store_and_fwd_flag"),
    "dropoff_longitude" = list(type = "numeric", newName = "dropoff_longitude"),
    "dropoff_latitude" = list(type = "numeric", newName = "dropoff_latitude"),
    "payment_type" = list(type = "string", newName = "payment_type"),
    "fare_amount" = list(type = "numeric", newName = "fare_amount"),
    "extra" = list(type = "numeric", newName = "extra"),
    "mta_tax" = list(type = "numeric", newName = "mta_tax"),
    "tip_amount" = list(type = "numeric", newName = "tip_amount"),
    "tolls_amount" = list(type = "numeric", newName = "tolls_amount"),
    "improvement_surcharge" = list(type = "numeric", newName = "improvement_surcharge"),
    "total_amount" = list(type = "numeric", newName = "total_amount"))


save2xdf <- function(file_name, csvPath, xdfPath, colInfo) {
    csvFile <- paste(csvPath, "/", file_name, ".csv", sep = "")
    xdfFile <- paste(xdfPath, "/", file_name, ".xdf", sep = "")
    # Use rowSelection to filter out the header row by validating the value of a column, such as total_amount
    rxImport(inData = csvFile, outFile = xdfFile, 
             transforms = list(pickup_datetime = as.POSIXct(pickup_datetime, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"), dropoff_datetime = as.POSIXct(dropoff_datetime, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York")),
             rowSelection = !is.na(pickup_datetime) & !is.na(dropoff_datetime),
     colInfo = colInfo, numRows = 1000000, overwrite = TRUE)
}

get_filename <- function(file_grid) {
    month0 <- sprintf("%02d", as.numeric(file_grid['month']))
    return(list(file_name = paste(file_grid['color'], "_tripdata_", file_grid['year'], "-", month0, sep = "")))
}

years <- 2015:2015
months <- 1:12
colors <- c("yellow")
xdfPath <- c("C:/Users/adminuser/Downloads/XDF1M")
csvPath <- c("C:/Users/adminuser/Downloads")

file_grid <- expand.grid(year = years, month = months, color = colors)
file_list <- apply(file_grid, 1, get_filename)

system.time(res <- rxExec(save2xdf, elemArgs = file_list, csvPath, xdfPath, colX))