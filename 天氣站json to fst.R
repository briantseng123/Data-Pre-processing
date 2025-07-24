library(dplyr)
library(jsonlite)
mannedstationjson <- fromJSON("E:/brain/解壓縮data/資料處理/天氣資料/氣象站點/有人站/C-B0074-001.json")
mannedstation <- mannedstationjson$cwaopendata$resources$resource$data$stationsStatus$station
mannedstation <- mannedstation %>%
  dplyr::select(StationID,StationName,StationAltitude, StationLongitude, StationLatitude)

autostationjson <- fromJSON("E:/brain/解壓縮data/資料處理/天氣資料/氣象站點/無人站/C-B0074-002.json")
autostation <- autostationjson$cwaopendata$resources$resource$data$stationsStatus$station
autostation <- autostation %>%
  dplyr::select(StationID,StationName,StationAltitude, StationLongitude, StationLatitude)

combinedstation <- rbind(mannedstation,autostation)
write_fst(combinedstation,"E:/brain/解壓縮data/資料處理/天氣資料/氣象站點/all_weather_stations_info.fst")
