{
library(arrow)
library(tibble)
library(smacof)
library(lubridate)
library(readr)
library(data.table)
library(fst)
library(dplyr)
library(doParallel)
library(tidyr)
library(ggplot2)
library(forcats)
library(scales)
library(disk.frame)
library(future)
library(geosphere)
library(psych)
library(transport)
library(proxy) 
library(gridExtra)
library(qs)
library(utils)
library(pryr)  
library(fasttime)
library(pbapply)
library(transport)
library(ggrepel)
library(fastDummies)
library(MASS)
library(AER)
library(stringr)
library(lmtest) 
library(car)        
library(DHARMa)     
library(purrr) 
library(MNB)
}


NTPmrt2024df_output_fst5v2 <- "E:/brain/解壓縮data/資料處理/2024/2024 公車捷運合併未加天氣資料/2024新北市捷運(發展程度移動_voronoi_v3)5.fst"
NTPmrt2024df <- read_fst(NTPmrt2024df_output_fst5v2, as.data.table = TRUE)
names(fst(NTPmrt2024df_output_fst5v2))
nrow(fst(NTPmrt2024df_output_fst5v2))

bus_path54_trancated <- "E:/brain/解壓縮data/資料處理/2024/2024 公車捷運合併未加天氣資料/2024公車(發展程度移動_voronoi_truncated)5v4.fst"
bus2024df <- read_fst(bus_path54_trancated, as.data.table = TRUE)
names(fst(bus_path54_trancated))
nrow(fst(bus_path54_trancated))

names(bus2024df)
names(NTPmrt2024df)

#資料對齊
{
NTPmrt2024df <- NTPmrt2024df %>%
  rename(
    BoardingStopUID = EntryStationID,
    BoardingStopName = EntryStationName,
    BoardingTime = EntryTime,
    DeboardingStopUID = ExitStationID,
    DeboardingStopName = ExitStationName,
    DeboardingTime = ExitTime
  )
keep_cols <- c("Authority", "HolderType", "TicketType", "SubTicketType",
               "BoardingTime","DeboardingTime","BoardingStopName","DeboardingStopName",
               "BoardingStopUID","DeboardingStopUID","Distance","Bdevelopment_level","Ddevelopment_level","BStationID",
               "dev_movement","movement_level")
bus2024df       <- bus2024df %>% dplyr::select(all_of(keep_cols))
NTPmrt2024df    <- NTPmrt2024df %>% dplyr::select(all_of(keep_cols))
combined_dt <- rbindlist(
  list(bus2024df, NTPmrt2024df),
  use.names = TRUE,   
  fill      = TRUE    
)
combined_dt_path <- "E:/brain/解壓縮data/資料處理/2024/2024北北基桃公車和新北捷運(合併)1.fst"
combined_dt_path_v2 <- "E:/brain/解壓縮data/資料處理/2024/2024北北基桃公車和新北捷運(合併_voronoi_v2)1.fst"
write.fst(combined_dt,combined_dt_path)
write.fst(combined_dt,combined_dt_path_v2)
}
names(weather)

weather2024_output_path <- "E:/brain/解壓縮data/資料處理/天氣資料/2024每小時氣象資料(加上警報_將特殊值轉為NA_v3)3.fst"
combined_dt_path <- "E:/brain/解壓縮data/資料處理/2024/2024北北基桃公車和新北捷運(合併)1.fst"
combined_dt_path_v2 <- "E:/brain/解壓縮data/資料處理/2024/2024北北基桃公車和新北捷運(合併_voronoi_v2)1.fst"
TPCmrt_1_6_path <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運1-6月(發展程度移動_voronoi_v3)5chunk"
TPCmrt_7_12_path <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運7-12月(發展程度移動_voronoi_v3)5chunk"
rail2024_path <- "E:/brain/解壓縮data/資料處理/2024/2024臺鐵(發展程度移動_truncated)5.fst"
"E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運1-6月(發展程度移動_voronoi_v3)5chunk"
head(fst("E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運1-6月(發展程度移動_voronoi_v3)5chunk/chunk_001.fst"))
head(fst("E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運7-12月(發展程度移動_voronoi_v3)5chunk/chunk_001.fst"))

names(fst(weather2024_output_path))
head(fst(combined_dt_path))
combined_dt <- setDT(read.fst(combined_dt_path_v2))
TPCmrt<- read_fst_folder(TPCmrt_1_6_path)
TPCmrt <- setDT(read.fst(TPCmrt_7_12_path))

weather_dt <- setDT(read.fst(weather2024_output_path,c("datetime","weather_station_ID","temperature_c",
                                                       "high_temp_alert","low_temp_alert","precipitation_mm",
                                                       "rain_alert")))
weather_dt[, datetime := datetime - hours(8)]
  TPCmrt <- TPCmrt %>%
  rename(
    BoardingStopUID = EntryStationID,
    BoardingStopName = EntryStationName,
    BoardingTime = EntryTime,
    DeboardingStopUID = ExitStationID,
    DeboardingStopName = ExitStationName,
    DeboardingTime = ExitTime
  )
keep_cols <- c("Authority", "HolderType", "TicketType", "SubTicketType",
               "BoardingTime","DeboardingTime","BoardingStopName","DeboardingStopName",
               "BoardingStopUID","DeboardingStopUID","Distance","Bdevelopment_level","Ddevelopment_level","BStationID",
               "dev_movement","movement_level")
TPCmrt    <- TPCmrt %>% select(all_of(keep_cols))

rail2024df <- read_fst(rail2024_path,as.data.table=TRUE)
names(rail2024df)
rail2024df[, EntryTime := EntryTime - hours(8)]
rail2024df <- rail2024df %>%
  rename(
    BoardingStopUID = EntryStationID,
    BoardingStopName = EntryStationName,
    BoardingTime = EntryTime,
    DeboardingStopUID = ExitStationID,
    DeboardingStopName = ExitStationName,
    DeboardingTime = ExitTime
  )
keep_cols <- c("Authority", "HolderType", "TicketType", "SubTicketType",
               "BoardingTime","DeboardingTime","BoardingStopName","DeboardingStopName",
               "BoardingStopUID","DeboardingStopUID","Distance","Bdevelopment_level","Ddevelopment_level","BStationID",
               "dev_movement","movement_level")
rail2024df    <- rail2024df %>% dplyr::select(all_of(keep_cols))

names(combined_dt)
names(weather_dt)
merge_weather <- function(combined_dt,
                          weather_dt,
                          combined_station = "BStationID",
                          weather_station  = "weather_station_ID",
                          combined_time    = "BoardingTime",
                          weather_time     = "datetime") {
  combined_dt[, (combined_time) := as.POSIXct(get(combined_time))]
  weather_dt[,  (weather_time)  := as.POSIXct(get(weather_time))]
  
  result_dt <- merge(
    x      = combined_dt,
    y      = weather_dt,
    by.x   = c(combined_station, combined_time),
    by.y   = c(weather_station,  weather_time),
    all.x  = TRUE,
    sort   = FALSE
  )
  
  return(result_dt)
}
NTPmrt2024df <- merge_weather(NTPmrt2024df,weather_dt)
NTPmrt2024df_path2 <- "E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/2024新北捷運(加入天氣資料v2)2.fst"
write.fst(NTPmrt2024df,NTPmrt2024df_path2)

rail2024df <- merge_weather(rail2024df,weather_dt)
rail2024df_path <- "E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/2024臺鐵(加入天氣資料)2.fst"
write.fst(rail2024df,rail2024df_path)

NTPmrt <- read.fst(NTPmrt2024df_path2, as.data.table = TRUE)
uniNTP<- sort(NTPmrt[,.(value=c(BoardingStopUID,DeboardingStopUID))][,unique(value)])
uniNTP
TPCmrt <- read_fst_folder("E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併",c("BoardingStopUID","DeboardingStopUID"),"^台北.*\\.fst$")
uniTPC <- sort(TPCmrt[,.(value=c(BoardingStopUID,DeboardingStopUID))][,unique(value)])
uniTPC

bus2024df <- merge_weather(bus2024df,weather_dt)
bus2024df_path2 <- "E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/2024北北基桃公車(加入天氣資料v2)2.fst"
write.fst(bus2024df,bus2024df_path2)
nrow(fst(bus2024df_path2))

combined_dt <- merge_weather(combined_dt,weather_dt)
names(combined_dt)
combined_dt_path2 <- "E:/brain/解壓縮data/資料處理/2024/2024北北基桃公車和新北捷運(加入天氣資料)2.fst"
write.fst(combined_dt,combined_dt_path2)
combined_dt_path2v2 <- "E:/brain/解壓縮data/資料處理/2024/2024北北基桃公車和新北捷運(加入天氣資料_voronoi_v2)2.fst"
write.fst(combined_dt,combined_dt_path2v2)

TPCmrt <- merge_weather(TPCmrt,weather_dt)
TPCmrt_1_6_output_path <- "E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/2024臺北市捷運1-6月(加入天氣資料_voronoi_v3)2.fst"
TPCmrt_7_12_output_path <- "E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/2024臺北市捷運7-12月(加入天氣資料_voronoi_v3)2.fst"
write_fst(TPCmrt,TPCmrt_1_6_output_path)
write_fst(TPCmrt,TPCmrt_7_12_output_path)

merge_weather_TPCmrt_fst_chunks <- function(input_folder,
                               output_folder,
                               weather_fst_path,
                               keep_cols,
                               chunk_size = 10000000,
                               name) {
  
  weather_dt <- setDT(read.fst(weather_fst_path,
                               columns = c("datetime",
                                           "weather_station_ID",
                                           "temperature_c",
                                           "high_temp_alert",
                                           "low_temp_alert",
                                           "precipitation_mm",
                                           "rain_alert")))
  weather_dt[, datetime_utc := as.POSIXct(datetime, tz = "Asia/Taipei")]
  weather_dt[, datetime := datetime_utc - hours(8)]
  weather_dt[, datetime := as.POSIXct(datetime)]
  files <- list.files(input_folder,
                      pattern    = "\\.fst$",
                      full.names = TRUE)
  dir.create(output_folder,
             recursive    = TRUE,
             showWarnings = FALSE)
  
  for (file in files) {
    meta   <- metadata_fst(file)
    n_rows <- meta$nrOfRows
    base   <- tools::file_path_sans_ext(basename(file))
    
    message("開始處理：", base, "（共 ", n_rows, " 列）")
    
    for (start in seq(1, n_rows, by = chunk_size)) {
      end <- min(start + chunk_size - 1, n_rows)
      message("  區間 ", start, "–", end)
      
      dt <- setDT(read.fst(file,
                           columns = keep_cols,
                           from    = start,
                           to      = end))
      
      #dt[, BoardingTime   := as.POSIXct(EntryTime, tz = "Asia/Taipei")]
      #dt[, DeboardingTime := as.POSIXct(ExitTime,  tz = "Asia/Taipei")]
      dt[, BStationID     := as.character(EntryStationID)]
      setnames(dt,
               old = c("EntryStationID","EntryStationName","EntryTime",
                       "ExitStationID","ExitStationName","ExitTime"),
               new = c("BoardingStopUID","BoardingStopName","BoardingTime",
                       "DeboardingStopUID","DeboardingStopName","DeboardingTime"),
               skip_absent = TRUE)
      
    dt[, BoardingTime   := as.POSIXct(BoardingTime, tz = "Asia/Taipei")]
    dt[, DeboardingTime := as.POSIXct(DeboardingTime,  tz = "Asia/Taipei")]
      
      
      dt <- dt[, .(Authority,
                   HolderType,
                   TicketType,
                   SubTicketType,
                   BoardingTime,
                   DeboardingTime,
                   BoardingStopName,
                   DeboardingStopName,
                   BoardingStopUID,
                   DeboardingStopUID,
                   Distance,
                   Bdevelopment_level,
                   Ddevelopment_level,
                   BStationID,
                   dev_movement,
                   movement_level)]
      
      merged <- merge(dt,
                      weather_dt,
                      by.x  = c("BStationID", "BoardingTime"),
                      by.y  = c("weather_station_ID", "datetime"),
                      all.x = TRUE,
                      sort  = FALSE)
      
      out_file <- file.path(output_folder,
                            paste0(name,base,".fst"))
      write_fst(merged, out_file)
      
      rm(dt, merged)
      gc()
    }
  }
  
  message("全部完成！")
}
keep_cols <- c("Authority", "HolderType", "TicketType", "SubTicketType",
               "EntryTime", "ExitTime",
               "EntryStationID", "EntryStationName",
               "ExitStationID",  "ExitStationName",
               "Distance", "Bdevelopment_level",
               "Ddevelopment_level", "dev_movement","BStationID",
               "movement_level")
merge_weather_TPCmrt_fst_chunks(
  input_folder      = TPCmrt_1_6_path,
  output_folder     = "E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併",
  weather_fst_path  = weather2024_output_path,
  keep_cols         = keep_cols,
  chunk_size        = 10000000,
  name              = "台北捷運2024(1-6月)"
)
merge_weather_TPCmrt_fst_chunks(
  input_folder      = TPCmrt_7_12_path,
  output_folder     = "E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併",
  weather_fst_path  = weather2024_output_path,
  keep_cols         = keep_cols,
  chunk_size        = 10000000,
  name              = "台北捷運2024(7-12月)"
)

table(combined_dt$Authority)
head(fst("E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/2024北北基桃公車(加入天氣資料v2)2.fst"))
head(fst("E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/2024新北捷運(加入天氣資料v2)2.fst"))
head(fst("E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/台北捷運2024(1-6月)chunk_001.fst"))
head(fst("E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/台北捷運2024(7-12月)chunk_001.fst"))
nrow(fst("E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/2024臺鐵(加入天氣資料)2.fst"))
combined_dt <- read.fst(combined_dt_path2v2, as.data.table=TRUE)

mrtbus2024df_path <- "E:/brain/解壓縮data/資料處理/2024/2024公車捷運火車合併"
temperature_var <- c("Authority","BoardingTime","HolderType","TicketType",
                     "precipitation_mm","Ddevelopment_level","temperature_c")
sub_temperature_var <- c("BoardingTime","HolderType","temperature_c")
sub_temperature_var2 <- c("BoardingTime","TicketType","temperature_c")
sub_temperature_var3 <- c("BoardingTime","movement_level","temperature_c")
sub_temperature_var4 <- c("BoardingTime","Authority","temperature_c")
sub_auth <- c("HolderType","TicketType","SubTicketType")
precip_var <- c("Authority","BoardingTime","HolderType","TicketType",
            "movement_level","precipitation_mm")
sub_precip_var <- c("BoardingTime","HolderType","precipitation_mm")
sub_precip_var2 <- c("BoardingTime","TicketType","precipitation_mm")
sub_precip_var3 <- c("BoardingTime","movement_level","precipitation_mm")
sub_precip_var4 <- c("BoardingTime","Authority","precipitation_mm")

auth_time <- c("Authority","BoardingTime","TicketType","HolderType")
auth_time <- c("Authority","DeboardingTime","BoardingTime")
gephi_edge <- c("BoardingTime","BoardingStopUID","DeboardingStopUID")
monthly <- c("BoardingStopName","DeboardingStopName","TicketType")
find_top_n_routes_robust <- function(data, category_col_str, n = 50) {
  dt <- as.data.table(data)
  unique_categories <- unique(dt[[category_col_str]])
  result_list <- lapply(unique_categories, function(current_cat) {
    subset_dt <- dt[get(category_col_str) == current_cat]
    route_counts <- subset_dt[, .(Count = .N), by = .(BoardingStopName, DeboardingStopName)]
    total_count_for_cat <- nrow(subset_dt)
    route_counts[, Percentage := Count / total_count_for_cat]
    setorder(route_counts, -Count)
    top_n <- head(route_counts, n)
    top_n[, (category_col_str) := current_cat]
    top_n[, TopRoute := paste(BoardingStopName, "->", DeboardingStopName)]
    setcolorder(top_n, c(category_col_str, "TopRoute", "Count", "Percentage"))
    return(top_n[, .(get(category_col_str), TopRoute, Count, Percentage)])
  })
  
  final_result <- rbindlist(result_list)
  setnames(final_result, "V1", category_col_str) 
  
  return(final_result)
}
combined_dt[, (monthly) := lapply(.SD, as.factor), .SDcols = monthly]
TicketTypetop50 <- find_top_n_routes_robust(combined_dt,"TicketType")

files <- list.files(path = mrtbus2024df_path, pattern = "\\.fst$", full.names = TRUE)

for (f in files) {
  df <- read_fst(f, columns = "BoardingTime")
  cat("檔案:", f, "\n")
  print(class(df$BoardingTime))  
  print(head(df$BoardingTime))
  cat("\n")
}
#校正時間
read_fst_folder <- function(folder, cols, pattern = "\\.fst$") {
  files <- list.files(path = folder, pattern = pattern, full.names = TRUE)
  
  dt_list <- lapply(files, function(f) {
    cat("讀取", f, "…\n")
    df <- read_fst(f, columns = cols)
    df <- as.data.table(df)
    
    if ("BoardingTime" %in% names(df)) {
      cur_tz <- attr(df$BoardingTime, "tzone") %||% ""
      
      if (cur_tz != "Asia/Taipei") {
        df[, BoardingTime := with_tz(BoardingTime, tzone = "Asia/Taipei")]
        df[, BoardingTime := BoardingTime - hours(8)]
      }
      attr(df$BoardingTime, "tzone") <- "Asia/Taipei"
    }
    
    return(df)
  })
  
  cat("合併中...\n")
  combined <- rbindlist(dt_list, use.names = TRUE)
  return(combined)
}

read_fst_folder <- function(folder, cols, pattern = "\\.fst$") {
  files <- list.files(path = folder,
                      pattern = pattern,
                      full.names = TRUE)
  dt_list <- lapply(files, function(f) {
    cat("讀取",f,"\n")
    df <- read_fst(f, columns = cols)
    as.data.table(df)
  })
  cat("合併","\n")
  combined <- rbindlist(dt_list, use.names = TRUE)
  return(combined)
}
combined_dt <- read_fst_folder(mrtbus2024df_path,temperature_var)
combined_dt <- read_fst_folder(mrtbus2024df_path,sub_temperature_var)
combined_dt <- read_fst_folder(mrtbus2024df_path,sub_temperature_var2)
combined_dt <- read_fst_folder(mrtbus2024df_path,sub_temperature_var3)
combined_dt <- read_fst_folder(mrtbus2024df_path,sub_temperature_var4)
combined_dt <- read_fst_folder(mrtbus2024df_path,auth_time)

Ddevelopment <- unique(read_fst_folder(mrtbus2024df_path,"Ddevelopment_level"))
comtemp <- read_fst_folder(mrtbus2024df_path,c("temperature_c","BoardingTime"))
combined_dt <- read_fst_folder(mrtbus2024df_path,sub_auth_time)
combined_dt <- read_fst_folder(mrtbus2024df_path,precip_var)
combined_dt <- read_fst_folder(mrtbus2024df_path,sub_precip_var)
combined_dt <- read_fst_folder(mrtbus2024df_path,sub_precip_var2)
combined_dt <- read_fst_folder(mrtbus2024df_path,sub_precip_var3)
combined_dt <- read_fst_folder(mrtbus2024df_path,sub_precip_var4)
combined_dt <- read_fst_folder(mrtbus2024df_path,sub_auth)
combined_dt <- read_fst_folder(mrtbus2024df_path,gephi_edge)
combined_dt <- read_fst_folder(mrtbus2024df_path, monthly)
BStation <- unique(read_fst_folder(mrtbus2024df_path,"BStationID"))
write_fst(BStation,"E:/brain/解壓縮data/資料處理/天氣資料/北北基桃天氣站點.fst")

bounds <- as.POSIXct(c("2024-01-01", "2024-12-31 23:59:59"), tz = "Asia/Taipei")
combined_dt <- combined_dt[
  BoardingTime %between% bounds
]

table(combined_dt$Authority)

options(datatable.optimize = 1L)
{
hourly_counts <- combined_dt[
  , .(count = .N),                    
  by = .(hour = hour(BoardingTime))    
]
ggplot(hourly_counts, aes(x = hour, y = count)) +
  geom_line(size = 1) +
  scale_x_continuous(breaks = 0:23) +    
  labs(
    x = "Hour (小時)",
    y = "Count (次數)",
    title = "每小時搭車次數"
  ) +
  theme_minimal()

hourly_by_auth <- combined_dt[
  , .(count = .N),
  by = .(hour = hour(BoardingTime), Authority)
]
ggplot(hourly_by_auth, aes(x = hour, y = count, color = Authority)) +
  geom_line(size = 1) +
  scale_x_continuous(breaks = 0:23) +
  labs(
    x     = "Hour (小時)",
    y     = "Count (次數)",
    title = "每小時搭車次數（分 Authority）",
    color = "Authority"
  ) +
  theme_minimal()

combined_dt[, `:=`(
  abs_movement_level = abs(movement_level),
  movement_level     = NULL
)]

head(combined_dt)
combined_dt[, BoardingTime := as.POSIXct(
  BoardingTime,
  format = "%Y-%m-%d %H:%M:%S",
  tz     = "Asia/Taipei"
)]

filtered_dt <- combined_dt[
  BoardingTime >= as.POSIXct("2024-10-30 00:00:00", tz="Asia/Taipei") &
    BoardingTime <= as.POSIXct("2024-11-02 00:00:00", tz="Asia/Taipei")
]

result_dt <- combined_dt[
  , .(count = .N)                             
  , by = .(BoardingTime, Authority)
][
  , total := sum(count), by = BoardingTime  
]

names(fst("E:/brain/解壓縮data/fst/2024/2024臺北市捷運1-6月.fst"))
TPC1_6_fst <- read_fst_folder("E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/台北捷運1-6月","BoardingTime")
TPC1_6 <- fread("E:/brain/解壓縮data/csv/2024/臺北捷運電子票證資料(TO2A)_2024年1~6月/臺北捷運電子票證資料(TO2A).csv",
  skip=1,
  select="EntryTime"
)
TPC1_6 <- TPC1_6[year(EntryTime) == 2024]
TPC1_6_fst <- TPC1_6_fst[year(EntryTime) == 2024]
TPC1_6_result_dt <- TPC1_6[
  , .(count = .N)                             
  , by = .(EntryTime)
]
hourly_counts <- TPC1_6_fst[
  , .(count = .N),                    
  by = .(hour = hour(BoardingTime))    
]
ggplot(hourly_counts, aes(x = hour, y = count)) +
  geom_line(size = 1) +
  scale_x_continuous(breaks = 0:23) +    
  labs(
    x = "Hour (小時)",
    y = "Count (次數)",
    title = "每小時搭車次數"
  ) +
  theme_minimal()

TPC7_12 <- fread("E:/brain/解壓縮data/csv/2024/臺北捷運電子票證資料(TO2A)_2024年7~12月/臺北捷運電子票證資料(TO2A).csv",
                skip=1,
                select="EntryTime"
)
TPC7_12_fst <- read_fst("E:/brain/解壓縮data/fst/2024/2024臺北市捷運7-12月.fst",columns = "EntryTime",as.data.table = TRUE)
TPC7_12_fst <- read_fst_folder("E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/台北捷運7-12月","BoardingTime")
TPC7_12_parquet <- read_parquet(
  "E:/brain/解壓縮data/fst/2024/2024臺北市捷運7-12月.parquet",
  col_select = "EntryTime"
)
TPC7_12_parquet <- as.data.table(TPC7_12_parquet)
TPC7_12 <- TPC7_12[year(EntryTime) == 2024]
TPC7_12_fst <- TPC7_12_fst[year(EntryTime) == 2024]
TPC7_12_parquet <- TPC7_12_parquet[year(EntryTime) == 2024]
hourly_counts <- TPC7_12_fst[
  , .(count = .N),                    
  by = .(hour = hour(BoardingTime))    
]
ggplot(hourly_counts, aes(x = hour, y = count)) +
  geom_line(size = 1) +
  scale_x_continuous(breaks = 0:23) +    
  labs(
    x = "Hour (小時)",
    y = "Count (次數)",
    title = "每小時搭車次數"
  ) +
  theme_minimal()

bus <- read_fst("E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/2024北北基桃公車(加入天氣資料v2)2.fst", columns = "EntryTime",as.data.table = TRUE)
bus_result_dt <- bus[
  , .(count = .N)                             
  , by = .(EntryTime)
]
}
#分成每小時有多少筆數據，做NB Regression

df <- disk.frame(mrtbus2024df_path)

#groupby
{
  #data.table
  {
  combined_dt  <- combined_dt[
    TicketType %in% c("1","4") & grepl("^[ABC]", HolderType),
    .(HolderType = substr(HolderType, 1, 1),
      Authority  = ifelse(
        Authority %in% c("NewTaipei","Taipei","Keelung","Taoyuan"),
        "Bus", ifelse(
          Authority %in% c("TRA"), "Train", "MRT"
        )
      ),
      Ddevelopment_level
    )
  ]
  
  res <- combined_dt [, .(N = .N),
            by = .(HolderType, TicketType, Ddevelopment_level, Authority)]
  }
  {
    files <- list.files(mrtbus2024df_path, pattern="\\.fst$", full.names = TRUE)
    
    agg <- data.table(
      Date               = as.IDate(character()),
      Hour               = integer(),
      HolderType         = character(),
      TicketType         = character(),
      Authority          = character(),
      N                  = integer(),
      Bdevelopment_level= character(),
      Ddevelopment_level= character()
    )
    
    all_data_list <- list() 
    
    for (f in files) {
      dt <- read_fst(
        f,
        columns = c("HolderType", "TicketType", "Authority", "BoardingTime",
                    "BoardingStopName", "DeboardingStopName",
                    "Bdevelopment_level", "Ddevelopment_level"),
        as.data.table = TRUE
      )
      
      
      dt <- dt[!is.na(BoardingStopName) & !is.na(DeboardingStopName) & BoardingStopName != DeboardingStopName]
      dt[, c("BoardingStopName", "DeboardingStopName") := NULL]
      
      if ((attr(dt$BoardingTime, "tzone") %||% "") != "Asia/Taipei") {
        dt[, BoardingTime := with_tz(BoardingTime, "Asia/Taipei") - hours(8)]
        dt[, BoardingTime := force_tz(BoardingTime, "Asia/Taipei")]
      }
      
      dt[, `:=`(Date = as.IDate(BoardingTime), Hour = hour(BoardingTime))]
      dt <- dt[year(BoardingTime) == 2024]
      
      all_data_list[[f]] <- dt
      
      rm(dt); gc(verbose = FALSE)
      
    } 
    
    all_data <- rbindlist(all_data_list, use.names = TRUE)
    rm(all_data_list); gc()
    
    all_data_filtered <- all_data[TicketType %in% c("1", "4") & grepl("^[ABC]", HolderType)]
    all_data_filtered[, `:=`(
      HolderType = substr(HolderType, 1, 1),
      Authority = fcase(
        Authority %in% c("NewTaipei", "Taipei", "Keelung", "Taoyuan"), "Bus",
        Authority == "TRA", "Train",
        default = "MRT"
      )
    )]
    
    options(datatable.optimize = 1L)
    actual_counts <- all_data_filtered[, .N, by = .(Date, Hour, HolderType, TicketType, Authority, Bdevelopment_level, Ddevelopment_level)]
    
    complete_grid <- all_data_filtered %>%
      expand(
        Date, 
        Hour, 
        HolderType, 
        TicketType,
        nesting(Authority, Bdevelopment_level, Ddevelopment_level)
      )
    
    setDT(complete_grid)
    complete_data <- actual_counts[complete_grid, on = .(Date, Hour, HolderType, TicketType, Authority, Bdevelopment_level, Ddevelopment_level)]
    complete_data[is.na(N), N := 0]
  
    
    setorder(complete_data, Date, Hour, HolderType, TicketType, Authority, Bdevelopment_level,Ddevelopment_level)
    
    complete_data2 <- complete_data
    b_unknown_indices <- Encoding(complete_data$Bdevelopment_level) == "unknown"
    d_unknown_indices <- Encoding(complete_data$Ddevelopment_level) == "unknown"
    
    complete_data2$Bdevelopment_level[b_unknown_indices] <- iconv(
      complete_data2$Bdevelopment_level[b_unknown_indices],
      from = "Big5", to = "UTF-8"
    )
    
    complete_data2$Ddevelopment_level[d_unknown_indices] <- iconv(
      complete_data2$Ddevelopment_level[d_unknown_indices],
      from = "Big5", to = "UTF-8"
    )
    table(complete_data2$Bdevelopment_level)
    Encoding(complete_data2$Bdevelopment_level)
    table(dt_count$Bdevelopment_level)
    Encoding(dt_count$Bdevelopment_level)
    
    write_fst(complete_data2,"E:/brain/解壓縮data/資料處理/2024/Negative Binomial Regression/NBModel_V8.fst")
    
    dt_count <- read_fst("E:/brain/解壓縮data/資料處理/2024/Negative Binomial Regression/NBModel_V6.fst", as.data.table= TRUE)
    table(dt_count$Authority)
    dt_count <- as.data.table(dt_count)
    dt_count[, HolderType_B := as.integer(HolderType == "B")]
    dt_count[, HolderType_C := as.integer(HolderType == "C")]
    nrow(dt_count)
    sum(dt_count$HolderType_B | dt_count$HolderType_C)
    dt_count[, Transport_BUS := as.integer(Authority == "Bus")]
    dt_count[, MonthlyPass   := as.integer(TicketType == "4")]
    
    dt_count[, Season := fifelse(month(Date) %in% 3:5, "春季",
                                 fifelse(month(Date) %in% 6:8, "夏季",
                                         fifelse(month(Date) %in% 9:11, "秋季", "冬季")))]
    holiday_dates <- as.Date(c(
      # 元旦連假
      "2023-12-30", "2023-12-31", "2024-01-01",
      # 農曆春節
      seq(as.Date("2024-02-08"), as.Date("2024-02-14"), by = "day"),
      # 228
      "2024-02-28",
      # 兒童清明
      seq(as.Date("2024-04-04"), as.Date("2024-04-07"), by = "day"),
      # 勞動節
      "2024-05-01",
      # 端午
      seq(as.Date("2024-06-08"), as.Date("2024-06-10"), by = "day"),
      # 中秋
      "2024-09-17",
      # 國慶
      "2024-10-10"
    ))
    disaster_dates <- as.Date(c(
      # 凱米颱風（7/24、7/25 停班停課）
      "2024-07-24", "2024-07-25",
      # 山陀兒颱風（10/02、10/03 停班停課）
      "2024-10-02", "2024-10-03",
      # 1023 豪雨（10/25 停班停課）
      "2024-10-25",
      # 康芮颱風（10/31 停班停課）
      "2024-10-31"
    ))
    dt_count[, c("isHoliday", "DisasterAlert") := .(
      as.integer(Date %in% holiday_dates | weekdays(Date) %in% c("星期六","星期日")),
      as.integer(Date %in% disaster_dates)
    )]
    
    BStationID <- read_fst("E:/brain/解壓縮data/資料處理/天氣資料/北北基桃天氣站點.fst")
    weather2024_output_path <- "E:/brain/解壓縮data/資料處理/天氣資料/2024每小時氣象資料(加上警報_將特殊值轉為NA_v3)3.fst"
    dt <- read.fst(weather2024_output_path,as.data.table = TRUE)
    weatherstation_id <- BStationID$BStationID
    dt_filtered <- dt[weather_station_ID %in% weatherstation_id]
    dt_filtered$datetime <- dt_filtered$datetime - hours(8)
    
    dt_filtered_aggregate <- dt_filtered[
      , .(
        AvgTemp     = mean(temperature_c,    na.rm = TRUE),
        AvgPrecip   = mean(precipitation_mm, na.rm = TRUE)
      ),
      by = .(datetime)
    ]
    dt_count[, DateTime := as.POSIXct(
      paste(Date, sprintf("%02d:00:00", Hour)),
      format = "%Y-%m-%d %H:%M:%S",
      tz     = "Asia/Taipei"       
    )]
    setkey(dt_count,        DateTime)
    setkey(dt_filtered_aggregate, datetime)
    dt_merged <- dt_filtered_aggregate[dt_count]
    dt_merged[, MorningPeak     := as.integer(Hour >= 6  & Hour <=  9)]
    dt_merged[, DaytimeOffPeak  := as.integer(Hour >=10  & Hour <= 16)]
    dt_merged[, EveningPeak     := as.integer(Hour >=17  & Hour <= 19)]
    dt_merged[, Night     := as.integer(Hour >=20  & Hour <= 23)]
    table(dt_merged$Authority)
    write_fst(dt_merged,"E:/brain/解壓縮data/資料處理/2024/Negative Binomial Regression/NBModel_V7.fst")
    
    }
  
}

describe(comtemp)
#temp_precip
{
  cols_sum <- c(
    "HolderType_A_Ind", "HolderType_B_Ind", "HolderType_C_Ind",
    "TicketType_1_Ind", "TicketType_4_Ind",
    "DevLvl1",      "DevLvl2",      "DevLvl3", "DevLvl4"
  )
  
  
  dt_count <- df_chunked %>%
    group_by(Date, Hour) %>%
    summarise(
      across(everything(), sum),
      .groups = "drop"
    ) %>%
    rename_with(~ str_remove(.x, "_Ind$"), ends_with("_Ind"))
  
  print(dt_count)
  
  if (nrow(dt_merged) > 0 && all(c("DateTime", "Count", "temperature_c") %in% names(dt_merged))) {
    
   
    min_c <- min(dt_merged$Count, na.rm = TRUE)
    max_c <- max(dt_merged$Count, na.rm = TRUE)
    min_t <- min(dt_merged$AvgTemp.y, na.rm = TRUE) 
    max_t <- max(dt_merged$AvgTemp.y, na.rm = TRUE) 
    
    if (is.infinite(min_c) || is.infinite(max_c) || is.infinite(min_t) || is.infinite(max_t)) {
      print("Count 或 AvgTemp.y 欄位可能全為 NA，或資料不足，無法計算有效範圍。")
    } else if ((max_t - min_t) == 0 || (max_c - min_c) == 0) {
      print("Count 或 AvgTemp.y 的數值範圍為零，可能導致無法繪製有效的雙軸圖或轉換出錯。")
    } else {
      plot_combined_over_time <- ggplot(dt_merged, aes(x = DateTime)) +
        geom_line(aes(y = Count, color = "Count"), linewidth = 1) +
        geom_line(aes(y = ((AvgTemp.y - min_t) / (max_t - min_t)) * (max_c - min_c) + min_c, color = "Temperature"), linewidth = 1) +
        scale_y_continuous(
          name = "次數 (Count)",
          sec.axis = sec_axis(
            transform = ~ ((. - min_c) / (max_c - min_c)) * (max_t - min_t) + min_t,
            name = "溫度 (°C)" 
          )
        ) +
        scale_color_manual(
          name = "圖例 (Legend)",
          values = c("Count" = "deepskyblue3", "Temperature" = "firebrick3"), 
          labels = c("Count", "溫度 (°C)") # 更新圖例標籤
        ) +
        labs(title = "次數與溫度隨時間變化",
             x = "日期與時間",
             subtitle = "完整期間") +
        theme_minimal() +
        theme(
          plot.title = element_text(hjust = 0.5),
          plot.subtitle = element_text(hjust = 0.5),
          legend.position = "top"
        )
      print(plot_combined_over_time)
      
      zoom_start_datetime_str <- "2024-11-01 00:00:00"
      zoom_end_datetime_str <- "2024-12-31 00:00:00" 
      
      
      current_tz <- attr(dt_merged$DateTime, "tzone")
      if (is.null(current_tz)) {
        current_tz <- "UTC" 
        warning("DateTime 欄位時區未知，假設為 UTC。請確認 DateTime 欄位建立時的時區設定。")
      }
      
      zoom_start_datetime <- as_datetime(zoom_start_datetime_str, tz = current_tz)
      zoom_end_datetime <- as_datetime(zoom_end_datetime_str, tz = current_tz)
      
      if (is.na(zoom_start_datetime) || is.na(zoom_end_datetime)) {
        print("錯誤：您輸入的放大期間字串無法正確轉換為日期時間格式，請檢查。")
      } else if (zoom_start_datetime >= zoom_end_datetime) {
        print("錯誤：放大期間的開始時間必須早於結束時間。")
      } else {
        min_data_datetime <- min(dt_merged$DateTime, na.rm = TRUE)
        max_data_datetime <- max(dt_merged$DateTime, na.rm = TRUE)
        
        if (is.infinite(min_data_datetime) || is.infinite(max_data_datetime)) {
          print("錯誤: dt_merged 中的 DateTime 欄位沒有有效的時間範圍。")
        } else if (zoom_end_datetime <= min_data_datetime || zoom_start_datetime >= max_data_datetime) {
          print(paste("警告：您設定的放大期間 (", format(zoom_start_datetime, tz=current_tz), " 至 ", format(zoom_end_datetime, tz=current_tz),
                      ") 完全落在資料的時間範圍 (", format(min_data_datetime, tz=current_tz), " 至 ", format(max_data_datetime, tz=current_tz), ") 之外。"))
        }
        
        # 確保 plot_combined_over_time 物件存在才進行下一步
        if(exists("plot_combined_over_time") && !is.null(plot_combined_over_time)){
          plot_zoomed <- plot_combined_over_time +
            coord_cartesian(xlim = c(zoom_start_datetime, zoom_end_datetime)) + # Y軸範圍會自動調整
            labs(subtitle = paste("放大期間:",
                                  format(zoom_start_datetime, "%Y-%m-%d %H:%M", tz=current_tz), "至",
                                  format(zoom_end_datetime, "%Y-%m-%d %H:%M", tz=current_tz)))
          print(plot_zoomed)
        } else {
          print("基礎圖表 'plot_combined_over_time' 未成功建立，無法進行放大。請檢查之前的錯誤訊息。")
        }
      }
    }
  } else {
    print("合併後的資料框 'dt_merged' 為空或缺少必要的欄位 (DateTime, Count, temperature_c)。請檢查合併步驟。")
  }
}
write_fst(dt_count,"E:/brain/解壓縮data/資料處理/2024/Negative Binomial Regression/groupby_holder_ticket_time.fst")
dt_count <- read_fst("E:/brain/解壓縮data/資料處理/2024/Negative Binomial Regression/groupby_holder_ticket_time.fst")

cor(dt_count$Count,dt_count$AvgTemp)
drop_na(dt_count)
head(dt_count)
names(dt_count)
table(dt_count$abs_movement_level)

write_fst(dt_count,"E:/brain/解壓縮data/資料處理/2024/Negative Binomial Regression/temp_percip_groupbyall_withDis.fst")
dt_count <- read.fst("E:/brain/解壓縮data/資料處理/2024/Negative Binomial Regression/groupby_holder_ticket_time.fst")


names(dt_merged)
head(dt_merged[["MonthlyPass"]])
seasons   <- c("春季","夏季","秋季","冬季")
MonthlyPass <- c(1,0)
mod_types <- c("lm","nb")   

combos <- expand.grid(
  season    = seasons,
  MonthlyPass = MonthlyPass,
  mod_type  = mod_types,
  stringsAsFactors = FALSE
)

models_all <- apply(combos, 1, function(row) {
  sea   <- row["season"]
  mtype <- row["mod_type"]
  mpass <- row["MonthlyPass"]
  
  fml <- as.formula(
    paste0("N", " ~ AvgTemp + AvgPrecip + isHoliday + DisasterAlert + MorningPeak + DaytimeOffPeak + EveningPeak +",
           "HolderType_A + HolderType_B + Transport_BUS ")
  )
  
  subset_cond <- dt_merged$Season == sea & dt_merged$MonthlyPass == mpass
  if (mtype == "lm") {
    lm(fml, data = dt_merged, subset = subset_cond)
  } else {
    glm.nb(fml, data = dt_merged, subset =subset_cond)
  }
})

names(models_all) <- with(combos,
                          paste(season,MonthlyPass,mod_type, sep = "_")
)
summary(models_all[["春季_1_lm"]])
summary(models_all[["春季_0_lm"]])
summary(models_all[["夏季_1_lm"]])
summary(models_all[["夏季_0_lm"]])
summary(models_all[["秋季_1_lm"]])
summary(models_all[["秋季_0_lm"]])
summary(models_all[["冬季_1_lm"]])
summary(models_all[["冬季_0_lm"]])

dt_count_noabs_ <- dt_count[ ,
                             .(Count    = sum(Count),
                               AvgTemp  = mean(AvgTemp),
                               AvgPrecip= mean(AvgPrecip)),
                             by = .(Date, Hour, HolderType)
]

{
  dt_count_noabs_ <- as.data.table(dt_count_noabs_)
  dt_count_noabs_[, HolderType_A := as.integer(HolderType == "A")]
  dt_count_noabs_[, HolderType_B := as.integer(HolderType == "B")]
  
  dt_count_noabs_[, Season := fifelse(month(Date) %in% 3:5, "春季",
                               fifelse(month(Date) %in% 6:8, "夏季",
                                       fifelse(month(Date) %in% 9:11, "秋季", "冬季")))]
  holiday_dates <- as.Date(c(
    # 元旦連假
    "2023-12-30", "2023-12-31", "2024-01-01",
    # 農曆春節
    seq(as.Date("2024-02-08"), as.Date("2024-02-14"), by = "day"),
    # 228
    "2024-02-28",
    # 兒童清明
    seq(as.Date("2024-04-04"), as.Date("2024-04-07"), by = "day"),
    # 勞動節
    "2024-05-01",
    # 端午
    seq(as.Date("2024-06-08"), as.Date("2024-06-10"), by = "day"),
    # 中秋
    "2024-09-17",
    # 國慶
    "2024-10-10"
  ))
  disaster_dates <- as.Date(c(
    # 凱米颱風（7/24、7/25 停班停課）
    "2024-07-24", "2024-07-25",
    # 山陀兒颱風（10/02、10/03 停班停課）
    "2024-10-02", "2024-10-03",
    # 1023 豪雨（10/25 停班停課）
    "2024-10-25",
    # 康芮颱風（10/31 停班停課）
    "2024-10-31"
  ))
  dt_count_noabs_[, c("isHoliday", "DisasterAlert") := .(
    as.integer(Date %in% holiday_dates | weekdays(Date) %in% c("星期六","星期日")),
    as.integer(Date %in% disaster_dates)
  )]
  dt_count_noabs_[ , c("AvgTemp","AvgPrecip") := NULL]
  
  dt_filtered_aggregate <- dt_filtered[
    , .(
      AvgTemp     = mean(temperature_c,    na.rm = TRUE),
      AvgPrecip   = mean(precipitation_mm, na.rm = TRUE)
    ),
    by = .(datetime)
  ]
  dt_count_noabs_[, DateTime := as.POSIXct(
    paste(Date, sprintf("%02d:00:00", Hour)),
    format = "%Y-%m-%d %H:%M:%S",
    tz     = "Asia/Taipei"       
  )]
  setkey(dt_count_noabs_,        DateTime)
  setkey(dt_filtered_aggregate, datetime)
  dt_merged_noabs <- dt_filtered_aggregate[dt_count_noabs_]
  dt_merged_noabs[ , TimeSegment := fcase(
    hour(datetime) %in% 0:6   , "LateNight",
    hour(datetime) %in% 7:9   , "MorningPeak",
    hour(datetime) %in% 10:16 , "DaytimeOffPeak",
    hour(datetime) %in% 17:19 , "EveningPeak",
    hour(datetime) %in% 20:23 , "Night"
  )]
  dt_merged_noabs[ , MorningPeak    := as.integer(TimeSegment == "MorningPeak")]
  dt_merged_noabs[ , DaytimeOffPeak := as.integer(TimeSegment == "DaytimeOffPeak")]
  dt_merged_noabs[ , EveningPeak    := as.integer(TimeSegment == "EveningPeak")]
  dt_merged_noabs[ , Night          := as.integer(TimeSegment == "Night")]
  
}

names(dt_merged_noabs)
seasons   <- c("春季","夏季","秋季","冬季")
mod_types <- c("lm","nb")   

combos <- expand.grid(
  season    = seasons,
  mod_type  = mod_types,
  stringsAsFactors = FALSE
)

models_all <- mapply(
  FUN = function(sea, mtype) {
    current_data_subset <- subset(dt_merged_noabs, Season == sea)
if (mtype == "lm") {

  min_count_subset <- min(current_data_subset$Count)
  temp_shift_constant <- 0 
  if (min_count_subset <= 0) {
    temp_shift_constant <- abs(min_count_subset) + 0.001
    current_data_subset$Count_shifted <- current_data_subset$Count + temp_shift_constant

  } else {
    current_data_subset$Count_shifted <- current_data_subset$Count 
  }
  
  temp_fml_for_boxcox <- as.formula(
    "Count_shifted ~ AvgTemp + AvgPrecip + HolderType_A + HolderType_B +
        isHoliday + MorningPeak + DaytimeOffPeak + EveningPeak + Night"
  )
  temp_model_for_boxcox <- lm(temp_fml_for_boxcox, data = current_data_subset)
  
  boxcox_result <- boxcox(temp_model_for_boxcox, plotit = FALSE)
  lambda_optimal <- boxcox_result$x[which.max(boxcox_result$y)]
  
  if (lambda_optimal == 0) {
    current_data_subset$Count_transformed <- log(current_data_subset$Count_shifted)
  } else {
    current_data_subset$Count_transformed <- (current_data_subset$Count_shifted^lambda_optimal - 1) / lambda_optimal
  }
  
  fml2 <- as.formula(
    "Count_transformed ~ AvgTemp + AvgPrecip + HolderType_A + HolderType_B +
        isHoliday + MorningPeak + DaytimeOffPeak + EveningPeak + Night"
  )
  lm(fml2, data = current_data_subset)
  
} else {
  fml_glm_nb <- as.formula(
    "Count ~ AvgTemp + AvgPrecip + HolderType_A + HolderType_B + Monthly_Pass+
        isHoliday + MorningPeak + DaytimeOffPeak + EveningPeak + Night"
  )
  MASS::glm.nb(fml_glm_nb, data = current_data_subset)
}
},
  sea = combos$season,
  mtype = combos$mod_type,
  SIMPLIFY = FALSE
  )
  names(models_all) <- paste(combos$season, combos$mod_type, sep = "_")
  
  names(dt_merged)
  parameter <- paste0(" ~ AvgTemp + AvgPrecip + HolderType_B + HolderType_C+ isHoliday + MonthlyPass + ",
           " MorningPeak + DaytimeOffPeak + EveningPeak + Night+ Transport_BUS")
  subset_data <- dt_merged[dt_merged$Season == "春季" & dt_merged$HolderType == 'C', ]
  mean(subset_data$N, na.rm = TRUE)
  
names(dt_merged)
head(dt_merged)
invmodel <- function(mydata, sea) {
  subset_cond <- mydata$Season == sea 
  
    df_sub <- subset(mydata, subset_cond)
    df_sub <- as.data.frame(df_sub)   
    
    fml <- as.formula(
      paste0("(1/N)", parameter))
    
    final_model <- lm(fml,data = df_sub)
    
    return(final_model)
}
rotmodel <- function(mydata, sea) {
  subset_cond <- mydata$Season == sea 
  
  df_sub <- subset(mydata, subset_cond)
  df_sub <- as.data.frame(df_sub)  
  
  formula_string <- paste0("sqrt(N+1)", parameter)
  fml <- as.formula(formula_string)
  vars_in_formula <- all.vars(fml)
  df_sub_clean <- df_sub[complete.cases(df_sub[, vars_in_formula]), ]
  
  
  final_model <- lm(fml,data = df_sub_clean)
  
  return(final_model)
}
logmodel <- function(mydata, sea) {
  subset_cond <- dt_merged$Season == sea 
  
    df_sub <- subset(mydata, subset_cond)
    df_sub <- as.data.frame(df_sub)   
    
    fml <- as.formula(
      paste0("log(N)", parameter))
    
    final_model <- lm(fml,data = df_sub)
    
    return(final_model)
}
orimodel <- function(mydata, sea) {
  subset_cond <- dt_merged$Season == sea
  df_sub <- subset(mydata, subset_cond)
  df_sub <- as.data.frame(df_sub)   
  
  fml <- as.formula(
    paste0("N", parameter))
  
  final_model <- lm(fml,data = df_sub)
  
  return(final_model)
}
wlscalcu <- function(final_model_ols){
  wls_weights <- NULL 
  
  ols_residuals <- residuals(final_model_ols)
  ols_fitted_values <- fitted(final_model_ols)
  
  log_squared_residuals <- log(ols_residuals^2)
  
  valid_indices_for_var_model <- is.finite(log_squared_residuals) & is.finite(ols_fitted_values)
  
  if (sum(valid_indices_for_var_model) >= 2 && length(unique(ols_fitted_values[valid_indices_for_var_model])) >= 2) {
    df_for_var_model <- data.frame(
      lsq_res = log_squared_residuals[valid_indices_for_var_model],
      fitted_ols = ols_fitted_values[valid_indices_for_var_model]
    )
    
    var_model <- lm(lsq_res ~ fitted_ols, data = df_for_var_model)
    
    predicted_log_variance <- predict(var_model, newdata = data.frame(fitted_ols = ols_fitted_values))
    
    wls_weights <- 1 / exp(predicted_log_variance)
    
    if (any(!is.finite(wls_weights)) || any(wls_weights <= 0)) {
      
      good_weights <- wls_weights[is.finite(wls_weights) & wls_weights > 0]
      if (length(good_weights) > 0) {
        default_weight <- mean(good_weights, na.rm = TRUE)
        wls_weights[!is.finite(wls_weights) | wls_weights <= 0] <- default_weight
      } else {
        wls_weights[!is.finite(wls_weights) | wls_weights <= 0] <- 1 # 如果沒有好的權重，默認為1
      }
      if(any(wls_weights <= 0)) wls_weights[wls_weights <=0] <- .Machine$double.eps # 替換為一個很小的正數
    }
    
  } else {
    print("警告：沒有足夠的有效數據點或擬合值變異性來建立變異數模型以計算WLS權重。WLS權重將設為 NULL。")
    wls_weights <- NULL 
  }
  return(list(ols_model = final_model_ols, wls_calculated_weights = wls_weights))
}
nbmodel <- function(mydata, sea){
  subset_cond <- mydata$Season%in%sea
  
  fml <- as.formula(
    paste0("N", parameter))
  
  final_model <- glm.nb(fml, data = mydata, subset =subset_cond,
           control = glm.control(maxit = 100))
  
  #nbvif <- vif(final_model)
  #print(nbvif)
  return(final_model)
}
poimodel <- function(mydata, sea){
  subset_cond <- mydata$Season%in%sea
  
  fml <- as.formula(
    paste0("N", parameter))
  
  final_model <- glm(fml, data = mydata, subset =subset_cond, family = "poisson",
                        control = glm.control(maxit = 100))
  
  #nbvif <- vif(final_model)
  #print(nbvif)
  return(final_model)
}
names(dt_merged)
modelcheck <- function(model_obj){
  model_name_str <- deparse(substitute(model_obj))
  tag <- ifelse(model_name_str=="model","標準模型 ",
         ifelse(model_name_str=="modellog","Log轉換模型 ",
         ifelse(model_name_str=="modelrot","Sqrt轉換模型 ",
         ifelse(model_name_str=="modelwls","WLS模型 ","Inv轉換模型 "))))
  summ <- summary(model_obj)
  coef <- ifelse(model_name_str=="model",coef(model_obj),
                ifelse(model_name_str=="modellog",exp(coef(model_obj)),
                       ifelse(model_name_str=="modelrot",(coef(model_obj))^2,1/(coef(model_obj)))))
  vif <- vif(model_obj)
  qqnorm(residuals(model_obj), main = paste0(tag,"殘差 Q-Q 圖"))
  qqline(residuals(model_obj), col = "red")
  plot(fitted(model_obj), residuals(model_obj),
       xlab = "Fitted Values",
       ylab = "Residuals",
       main = paste0(tag,"殘差圖：檢視變異數同質性"))
  abline(h = 0, col = "red", lty = 2)
  print(summ)
  print(coef)
  print(vif)
}
modelcheck_nb <- function(model_obj,poimodel_obj,datareg,sea){
  model_name_str <- deparse(substitute(model_obj))
  
  
  print(paste0("========== summary ==========","\n"))
  summ <- summary(model_obj)
  print(summ)
  
  print(paste0("========== coef ==========","\n"))
  coef <- format(round(exp(coef(model_obj)),4),scientific=FALSE)
  print(coef)
  
  print(paste0("========== vif ==========","\n"))
  vif <- round(vif(model_obj),4)
  print(vif)
  
  print(paste0("========== dispersion ==========","\n"))
  pearsonchi2 <- sum(residuals(model_obj, type="pearson")^2)
  disp_ratio <- pearsonchi2/df.residual(model_obj)
  print(paste("Dispersion ratio:",round(disp_ratio,3)))
  pv <- pchisq(2*logLik(model_obj)-logLik(poimodel_obj),df=1,lower.tail=FALSE)
  print(paste("p-value:",pv))
  
  par(mfrow=c(1,2))
  plot(fitted(model_obj),
  residuals(model_obj, type="deviance"),
  xlab="Fitted",ylab="Deviance residuals")
  abline(h=0, lty=2)
  plot(fitted(model_obj),
       residuals(model_obj, type="pearson"),
       xlab="Fitted",ylab="Pearson residuals")
  abline(h=0, lty=2)
  par(mfrow=c(1,1))
  qqnorm(residuals(model_obj,type="pearson"))
  qqline(residuals(model_obj,type="pearson"))
  
  print(paste0("========== influence point ==========","\n"))
  subset_cond <- datareg$Season%in%sea
  n <- nrow(subset(datareg,subset_cond))
  print(paste("N:",n))
  pearson_res <- residuals(model_obj,type="pearson")
  leverage_val <- hatvalues(model_obj)
  std_res <- pearson_res/sqrt(1-leverage_val)
  dfbeta_mat <- dfbetas(model_obj)
  
  idx_res <- which(abs(std_res)>3)
  #dfb_thres <- 2/sqrt(n)
  #idx_dfb <- which(apply(abs(dfbeta_mat),1,function(x) any(x>dfb_thres)))
  #outlier_idx <- sort(unique(c(idx_res,idx_dfb)))
  outlier_idx <- sort(unique(c(idx_res)))
  #print(outlier_idx)
  print(paste("outlier",length(outlier_idx)))
  
  #print(paste0("========== DHARMa ==========","\n"))
  #simulationOutput <- DHARMa::simulateResiduals(fittedModel = model_obj, n = 250) 
  
  #print("--- DHARMa Object Summary ---")
  #print(simulationOutput) 
  
 # print("--- DHARMa Main Plot ---")
  #tryCatch({
    #plot(simulationOutput)
  #}, error = function(e) {
   # print(paste("Error plotting DHARMa residuals:", e$message))
  #})
  
  #print("--- DHARMa Uniformity Test ---")
  #print(DHARMa::testUniformity(simulationOutput))
  
  #print("--- DHARMa Dispersion Test ---")
  #print(DHARMa::testDispersion(simulationOutput))
  
  #print("--- DHARMa Outlier Test ---")
  #print(DHARMa::testOutliers(simulationOutput))
  
  #print("--- DHARMa Zero-Inflation Test ---")
  #if (family(model_obj)$family %in% c("poisson", "Negative Binomial", "negbin")) {
    #print(DHARMa::testZeroInflation(simulationOutput))
  #} else {
    #print("Zero-inflation test not applicable for this model family.")
  #}
}
fit_wls_model <- function(ols_model_object, wls_weights) {
  
  if (!inherits(ols_model_object, "lm")) {
    stop("輸入錯誤：'ols_model_object' 必須是一個 'lm' (線性模型) 物件。")
  }
  if (is.null(wls_weights)) {
    stop("輸入錯誤：'wls_weights' (WLS 權重) 不能是 NULL。")
  }
  if (!is.numeric(wls_weights)) {
    stop("輸入錯誤：'wls_weights' 必須是一個數值向量。")
  }
  
  model_formula <- formula(ols_model_object)
  ols_data <- model.frame(ols_model_object) 
  
  if (length(wls_weights) != nrow(ols_data)) {
    stop(paste0("輸入錯誤：WLS 權重向量的長度 (", length(wls_weights),
                ") 與 OLS 模型數據的觀測點數量 (", nrow(ols_data), ") 不匹配。"))
  }
  
  if (any(!is.finite(wls_weights))) {
    stop("輸入錯誤：'wls_weights' 中包含非有限值 (NA, NaN, Inf, -Inf)。請確保所有權重都是有限的。")
  }
  
  if (any(wls_weights <= 0)) {
    
    warning("警告：'wls_weights' 中包含非正數。'lm' 函數對於權重有特定要求 (通常是正數)。非正權重可能導致錯誤或意外行為。建議權重是嚴格正數。")
  }
  
  message("開始配適 WLS 模型...") 
  
  response_term_expression <- deparse(model_formula[[2]]) 
  
  if (!(response_term_expression %in% colnames(ols_data))) {
    stop(paste0("無法在 ols_data 中找到預期的反應變數欄位：", response_term_expression))
  }
  
  predictors_terms <- attr(terms(model_formula), "term.labels")
  
  new_wls_formula_string <- paste0("`", response_term_expression, "` ~ ", paste(predictors_terms, collapse = " + "))
  if (length(predictors_terms) == 0) {
    new_wls_formula_string <- paste0("`", response_term_expression, "` ~ 1")
  }
  
  wls_formula_for_lm <- as.formula(new_wls_formula_string)
  
  wls_model <- tryCatch({
    lm(formula = wls_formula_for_lm, data = ols_data, weights = wls_weights) 
  }, error = function(e) {
    stop(paste0("配適 WLS 模型時發生錯誤：", e$message))
  })
  message("WLS 完成。")
  
  return(wls_model)
}

dt_aggregated <- dt_merged %>%
  group_by(Date,Hour, HolderType) %>%
  summarise(
    Total_N = sum(N, na.rm = TRUE),
    AvgTemp = mean(AvgTemp, na.rm = TRUE),    
    AvgPrecip = mean(AvgPrecip, na.rm = TRUE), 
    isHoliday = mean(isHoliday, na.rm = TRUE),
    DisasterAlert = mean(DisasterAlert, na.rm = TRUE),
    MorningPeak = mean(MorningPeak, na.rm = TRUE),
    DaytimeOffPeak = mean(DaytimeOffPeak, na.rm = TRUE),
    EveningPeak = mean(EveningPeak, na.rm = TRUE),
    Night = mean(Night, na.rm = TRUE),
    Season = first(Season),
    .groups = "drop" 
  )%>%
  pivot_wider(
    names_from = HolderType,   
    values_from = Total_N,     
    values_fill = 0            
  )%>%
  filter(complete.cases(.))

response_vars <- c("A", "B", "C") 
Y <- as.matrix(dt_aggregated[, response_vars])


mnbmodel <- function(mydata,mres,mexplain,sea){
  data_subset <- mydata %>% 
    filter(Season == sea)
  
  Y <- as.matrix(data_subset[, mres])
  final_formula <- as.formula(paste("Y ~", mexplain))
  predictor_formula <- as.formula(paste("~", mexplain))
  X <- model.matrix(predictor_formula, data = data_subset)
  
  mglm_final_model <- MGLMreg(
    Y,X,
    dist = "MNB"
  )
  
  #nbvif <- vif(final_model)
  #print(nbvif)
  return(mglm_final_model)
}

mnbmodelinter <- function(mydata, mres_names, mexplain_str, sea) {
  
  data_subset <- mydata %>%
    filter(Season == sea) %>%
    droplevels()
  
  data_long <- data_subset %>%
    mutate(ind = row_number()) %>%            
    pivot_longer(
      cols      = all_of(mres_names),
      names_to  = "category",
      values_to = "Y"
    ) %>%
    mutate(
      AvgTemp = scale(AvgTemp),
      AvgPrecip = scale(AvgPrecip)
    )
  
  full_formula <- as.formula(
    paste0("Y ~ category * (", mexplain_str, ")")
  )
  
  mm <- model.matrix(full_formula, data_long)
  coef_names <- colnames(mm)
  beta_mapping <- data.frame(
    Beta_Name = paste0("beta", seq_along(coef_names) - 1),
    Real_Coefficient = coef_names
  )
  
  print(beta_mapping)
  
  simple_formula <- as.formula(paste0("Y ~ ", mexplain_str))
  tmp_model <- MASS::glm.nb(simple_formula, data = data_long)
  simple_coefs <- coef(tmp_model)
  
  beta_star_list <- as.list(rep(0, length(coef_names)))
  names(beta_star_list) <- paste0("beta", seq_along(coef_names) - 1)
  
  for (i in seq_along(coef_names)) {
    coef_name <- coef_names[i]  
    if (coef_name %in% names(simple_coefs)) {
      beta_star_list[[i]] <- simple_coefs[coef_name]
    }
  }
  star <- c(list(phi = 1.0), beta_star_list)
  
  print("自動產生的起始值 (star):")
  print(star)

  model <- MNB::fit.MNB(
    formula = full_formula,
    star    = star,
    dataSet = data_long
  )
  
  return(model)
}
mnbmodel <- function(mydata, mres_names, mexplain_str, sea) {
  
  data_subset <- mydata %>%
    filter(Season == sea) %>%
    droplevels()
  
  data_long <- data_subset %>%
    mutate(ind = row_number()) %>%            
    pivot_longer(
      cols      = all_of(mres_names),
      names_to  = "category",
      values_to = "Y"
    ) %>%
    mutate(
      AvgTemp = scale(AvgTemp),
      AvgPrecip = scale(AvgPrecip)
    )
  
  print(head(data_long))
  full_formula <- as.formula(
    paste0("Y ~ category + ", mexplain_str)
  )
  
  mm <- model.matrix(full_formula, data_long)
  coef_names <- colnames(mm)
  
  beta_mapping <- data.frame(
    Beta_Name = paste0("beta", seq_along(coef_names) - 1),
    Real_Coefficient = coef_names
  )
  
  print(beta_mapping)
  
  simple_formula <- as.formula(paste0("Y ~ ", mexplain_str))
  tmp_model <- MASS::glm.nb(simple_formula, data = data_long)
  simple_coefs <- coef(tmp_model)
  
  beta_star_list <- as.list(rep(0, length(coef_names)))
  names(beta_star_list) <- paste0("beta", seq_along(coef_names) - 1)
  star <- c(list(phi = 1.0), beta_star_list)
  
  print("自動產生的起始值 (star):")
  print(star)
  lm_for_vif <- lm(full_formula, data=data_long)
  print(vif(lm_for_vif))
  
  num_betas <- length(star) - 1 
  lower_bounds <- c(1e-8, rep(-Inf, num_betas))
  opt_res <- optim(
    par = star,
    fn = MNB:::l.MNB,
    method = "L-BFGS-B",  
    lower = lower_bounds,
    control = list( 
      maxit = 2000,
      factr  = 1e7,
      trace = 1
    ),
    formula = full_formula,
    dataSet = data_long,
    hessian = TRUE
  )
  model_opt <- list(
    par = opt_res$par,
    converged = (opt_res$convergence==0),
    logLik = opt_res$value,
    vcov = solve(-opt_res$hessian),
    formula=full_formula,
    dataSet=data_long
  )
  class(model_opt) <- "MNB"
  #model <- MNB::fit.MNB(
    #formula = full_formula,
    #star    = star,
    #dataSet = data_long
  #)
  summary(model_opt)
  #return(model)
  return(model_opt)
}

write_fst(dt_merged,"E:/brain/解壓縮data/資料處理/2024/Negative Binomial Regression/data_for_regression_v1.fst")
dt_merged <- read_fst("E:/brain/解壓縮data/資料處理/2024/Negative Binomial Regression/data_for_regression_v1.fst", as.data.table=TRUE)
names(dt_merged)
modelnb <- nbmodel(dt_merged,c("春季","夏季","秋季","冬季"))
modelpoi <- poimodel(dt_merged,c("春季","夏季","秋季","冬季"))
modelcheck_nb(modelnb,modelpoi,dt_merged,c("春季","夏季","秋季","冬季"))

model <- orimodel(dt_merged,"春季")
modellog <- logmodel(dt_merged,"春季")
modelinv <- invmodel(dt_merged,"春季")
modelrot <- rotmodel(dt_merged,"春季")
modelnb <- nbmodel(dt_merged,"春季")
modelpoi <- poimodel(dt_merged,"春季")
modelcheck(model)
modelcheck(modellog)
modelcheck(modelinv)
modelcheck(modelrot)
modelcheck_nb(modelnb,modelpoi,dt_merged,"春季")
pchisq(23814, df = 19267, lower.tail = FALSE)

wlsmo <- wlscalcu(modelrot)
calculated_wls_weights <- wlsmo$wls_calculated_weights
ols_model_object <- wlsmo$ols_model
modelwls <- fit_wls_model(ols_model_object,calculated_wls_weights)
modelcheck(modelwls)

names(dt_aggregated)
modelmnb <- mnbmodel(dt_aggregated,c("A","B","C"),
                     "AvgTemp + AvgPrecip + isHoliday + MorningPeak + DaytimeOffPeak + EveningPeak + Night",
                     "春季")
modelmnb <- mnbmodel(dt_aggregated,c("A","B","C"),
                     "AvgTemp + AvgPrecip ",
                     "春季")
summary(modelmnb)
modelmnb$Coefficients
modelmnb$Converged

modelmnbinter <- mnbmodelinter(dt_aggregated,c("A","B","C"),
                     "AvgTemp+AvgPrecip+isHoliday+MorningPeak+DaytimeOffPeak+EveningPeak+Night",
                     "春季")


model <- Convergedmodel <- orimodel(dt_merged,"夏季")
modellog <- logmodel(dt_merged,"夏季")
modelinv <- invmodel(dt_merged,"夏季")
modelrot <- rotmodel(dt_merged,"夏季")
modelnb <- nbmodel(dt_merged,"夏季")
modelpoi <- poimodel(dt_merged,"夏季")
modelcheck(model)
modelcheck(modellog)
modelcheck(modelinv)
modelcheck(modelrot)
modelcheck_nb(modelnb,modelpoi,dt_merged,"夏季")
wlsmo <- wlscalcu(modelrot)
calculated_wls_weights <- wlsmo$wls_calculated_weights
ols_model_object <- wlsmo$ols_model
modelwls <- fit_wls_model(ols_model_object,calculated_wls_weights)
modelcheck(modelwls)

model <- orimodel(dt_merged,"秋季")
modellog <- logmodel(dt_merged,"秋季")
modelinv <- invmodel(dt_merged,"秋季")
modelrot <- rotmodel(dt_merged,"秋季")
modelnb <- nbmodel(dt_merged,"秋季")
modelpoi <- poimodel(dt_merged,"秋季")
modelcheck(model)
modelcheck(modellog)
modelcheck(modelinv)
modelcheck(modelrot)
modelcheck_nb(modelnb,modelpoi,dt_merged,"秋季")


model <- orimodel(dt_merged,"冬季")
modellog <- logmodel(dt_merged,"冬季")
modelinv <- invmodel(dt_merged,"冬季")
modelrot <- rotmodel(dt_merged,"冬季")
modelnb <- nbmodel(dt_merged,"冬季")
modelpoi <- poimodel(dt_merged,"冬季")
modelcheck(model)
modelcheck(modellog)
modelcheck(modelinv)
modelcheck(modelrot)
modelcheck_nb(modelnb,modelpoi,dt_merged,"冬季")

names(dt_merged)
dt_merged$time_period <- cut(
  dt_merged$Hour,
  breaks = c(-1,5,9,16,19,23),
  labels = c("凌晨","上午尖峰","中午離峰","下午尖峰","晚上"),
  right=TRUE
)
names(dt_merged)
twotable <- function(var1,var2,mydata){
mydata <- as.data.table(mydata)
time_holder <- xtabs(N ~ var1+var2,data=mydata)
print(time_holder)
time_holderchi <- chisq.test(time_holder)
print(time_holderchi$stdres)
print(time_holderchi)
}
twotable("HolderType","time_period",dt_merged)
twotable("Authority","time_period","dt_merged")
threetable <- function(var1,var2,var3,mydata){
  time_holder <- xtabs(N ~ var1+var2+var3,data=mydata)
  print(time_holder)
  time_holderchi <- chisq.test(time_holder)
  print(time_holderchi$stdres)
  print(time_holderchi)
}
threetable("Authority","time_period","HolderType","dt_merged")

dt_merged_noabs %>%
  group_by(TimeSegment) %>%
  summarise(mean_count = mean(Count), .groups="drop") %>%
  arrange(desc(mean_count))

dt <- read_fst("E:/brain/解壓縮data/資料處理/2024/Negative Binomial Regression/groupby_time.fst", as.data.table = TRUE)
dt %>%
  group_by(TimeSegment) %>%
  summarise(mean_count = mean(Count), .groups="drop") %>%
  arrange(desc(mean_count))
dt[ , TimeSegment := fcase(
  Hour %in% 0:6   , "LateNight",
  Hour %in% 7:9   , "MorningPeak",
  Hour %in% 10:16 , "DaytimeOffPeak",
  Hour %in% 17:19 , "EveningPeak",
  Hour %in% 20:23 , "Night"
)]
dt[ , MorningPeak    := as.integer(TimeSegment == "MorningPeak")]
dt[ , DaytimeOffPeak := as.integer(TimeSegment == "DaytimeOffPeak")]
dt[ , EveningPeak    := as.integer(TimeSegment == "EveningPeak")]
dt[ , Night          := as.integer(TimeSegment == "Night")]
dt %>%
  group_by(TimeSegment) %>%
  summarise(mean_count = mean(Count), .groups="drop") %>%
  arrange(desc(mean_count))

# 分析BoardingTime的時間次數直方圖
{
# 季節
combined_dt[, month_num := month(BoardingTime)]
combined_dt[, Season := fifelse(month_num %in% 3:5, "春季",
                                fifelse(month_num %in% 6:8, "夏季",
                                        fifelse(month_num %in% 9:11, "秋季", "冬季")))]
season_order <- c("春季", "夏季", "秋季", "冬季")
combined_dt[, Season := factor(Season, levels = season_order)]
options(datatable.optimize = 1L)
season_counts <- combined_dt[, .N, by = Season]
setnames(season_counts, "N", "Count")
ggplot(season_counts, aes(x = Season, y = Count, fill = Season)) +
  geom_bar(stat = "identity") + 
  labs(title = "每個季節的搭乘次數",
       x = "季節",
       y = "搭乘次數") +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))+
  scale_fill_manual(values = c("春季" = "lightgreen",
                               "夏季" = "salmon",
                               "秋季" = "orange",
                               "冬季" = "lightblue")) 

# 月份
month_counts_num <- combined_dt[, .N, by = list(month_num = lubridate::month(BoardingTime))]
setnames(month_counts_num, "N", "Count")
month_counts_num[, Month_Name := lubridate::month(month_num, label = TRUE, abbr = FALSE)]
month_counts_num <- month_counts_num[order(month_num)]

ggplot(month_counts_num, aes(x = Month_Name, y = Count, fill = Month_Name)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  labs(title = "每個月份的搭乘次數",
       x = "月份",
       y = "搭乘次數") +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold")) + 
  guides(fill = "none")

# 星期幾
combined_dt[, wday_numeric := lubridate::wday(BoardingTime, week_start = 1)]
day_of_week_counts <- combined_dt[, .N, by = wday_numeric]
setnames(day_of_week_counts, "N", "Count")
day_names <- c("星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日")
day_of_week_counts[, Day_Name := factor(wday_numeric, 
                                        levels = 1:7, 
                                        labels = day_names, 
                                        ordered = TRUE)]
ggplot(day_of_week_counts, aes(x = Day_Name, y = Count)) +
  geom_bar(stat = "identity", fill = "steelblue") + 
  labs(title = "每星期各天的搭乘次數",      
       x = "星期幾",                       
       y = "搭乘次數") +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))


#分Authority累積直方圖
class(combined_dt$BoardingTime)
combined_dt[, Month := lubridate::month(BoardingTime)]
month_levels <- levels(lubridate::month(ymd("2000-01-01") + months(0:11), label = TRUE, abbr = FALSE))
combined_dt[, Month_Name := factor(Month_Name, levels = month_levels, ordered = TRUE)]
print(unique(combined_dt$Month_Name))
print(table(combined_dt$Month_Name, useNA = "ifany"))
summary(combined_dt$Authority)

if (!is.factor(combined_dt$Authority)) {
  combined_dt[, Authority := as.factor(Authority)]
}
options(datatable.optimize = 1L) 
plot_data_summarized <- combined_dt[, .N, by = .(Month_Name, Authority)]
setnames(plot_data_summarized, "N", "Ride_Count")
print(head(plot_data_summarized))

table(combined_dt$Authority)
combined_dt[, Authority := trimws(Authority)]
desired_authority_order <- c("NewTaipei", "Taipei", "Keelung","Taoyuan","TRTC","NTMC","TRA")
plot_data_summarized[, Authority := factor(Authority, levels = desired_authority_order)]
combined_dt[, Authority := factor(Authority, levels = desired_authority_order)]
custom_colors <- c(
  "NewTaipei" = "#E69F00",    
  "Taipei" = "#56B4E9",    
  "Keelung" = "#009E73",
  "Taoyuan" = "#F0E442",
  "TRTC" = "#0072B2",
  "NTMC" = "#D55E00",
  "TRA" = "#8C564B"
)
legend_labels <- c(
  "NewTaipei" = "新北市公車",  
  "Taipei"    = "臺北市公車",
  "Keelung"   = "基隆市公車",
  "Taoyuan"   = "桃園市公車",
  "TRTC"      = "臺北捷運",   
  "NTMC"      = "新北捷運",
  "TRA"       = "臺鐵"
)

month_labels_chinese <- paste0(1:12, "月")
combined_dt[, Month_Name := month_labels_chinese[Month]]

month_order <- paste0(1:12, "月")
plot_data_summarized[, Month_Name := factor(Month_Name, levels = month_order, ordered = TRUE)]
head(combined_dt)
ggplot(plot_data_summarized, aes(x = Month_Name, y = Ride_Count, fill = Authority)) +
  geom_col(position = position_stack(reverse = TRUE)) +  
  scale_fill_manual(values = custom_colors, labels = legend_labels) +      
  labs(title = "每月搭乘次數 (按交通工具疊加)",
       x = "月份",
       y = "搭乘次數") +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))

ggplot(plot_data_summarized, aes(x = Month_Name, y = Ride_Count, fill = Authority)) +
  geom_col(position = position_fill(reverse = TRUE)) +  
  scale_fill_manual(values = custom_colors, labels = legend_labels) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) + 
  labs(title = "每月各交通工具搭乘百分比", 
       x = "月份",
       y = "百分比") +                   
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))

#星期幾
if (!"wday_numeric" %in% names(combined_dt)) {
  combined_dt[, wday_numeric := lubridate::wday(BoardingTime, week_start = 1)]
}
plot_data_summarized_dow <- combined_dt[, .N, by = .(wday_numeric, Authority)]
setnames(plot_data_summarized_dow, "N", "Ride_Count")
names(plot_data_summarized_dow)

day_names <- c("星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日")
plot_data_summarized_dow[, Day_Name := factor(wday_numeric,
                                              levels = 1:7,        
                                              labels = day_names,    
                                              ordered = TRUE)] 

plot_data_summarized_dow[, Authority := factor(Authority, levels = desired_authority_order, ordered = TRUE)]
print(head(plot_data_summarized_dow))

ggplot(plot_data_summarized_dow, aes(x = Day_Name, y = Ride_Count, fill = Authority)) +
  geom_col(position = position_stack(reverse = TRUE)) +
  scale_fill_manual(values = custom_colors, labels = legend_labels) +      
  labs(title = "每星期各天搭乘次數 (按交通工具疊加)",
       x = "星期幾",
       y = "搭乘次數") +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))

combined_dt[, .(Latest_BoardingTime = max(BoardingTime, na.rm = TRUE)), by = Authority]
combined_dt[, .(Latest_BoardingTime = min(BoardingTime, na.rm = TRUE)), by = Authority]
cutoff_datetime <- as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Taipei")
combined_dt[Authority == "TRTC" & BoardingTime < cutoff_datetime,.N ]
combined_dt <- combined_dt[BoardingTime >= cutoff_datetime]

ggplot(plot_data_summarized_dow, aes(x = Day_Name, y = Ride_Count, fill = Authority)) +
  geom_col(position = position_fill(reverse = TRUE)) +  
  scale_fill_manual(values = custom_colors, labels = legend_labels) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) + 
  labs(title = "每星期各天各交通工具搭乘百分比", 
       x = "星期幾",
       y = "百分比") +                   
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))

#小時
combined_dt[, Hour := hour(BoardingTime)]

if (!is.factor(combined_dt$Authority)) {
  combined_dt[, Authority := factor(Authority)]
}

plot_data_summarized <- combined_dt[, .(
  Ride_Count = .N
), by = .(Hour, Authority)]
ggplot(plot_data_summarized, aes(x = factor(Hour), y = Ride_Count, fill = Authority)) +
  geom_col(position = position_stack(reverse = TRUE)) +
  scale_fill_manual(values = custom_colors, labels = legend_labels) +
  scale_x_discrete(breaks = as.character(0:23)) +
  labs(
    title = "每日 24 小時搭乘次數 (按交通工具堆疊)",
    x = "小時 (0–23)",
    y = "搭乘次數"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(hjust = 0.5, face = "bold")
  )

ggplot(plot_data_summarized, aes(x = factor(Hour), y = Ride_Count, fill = Authority)) +
  geom_col(position = position_fill(reverse = TRUE)) +  
  scale_fill_manual(values = custom_colors, labels = legend_labels) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) + 
  labs(title = "每日 24 小時各交通工具搭乘百分比", 
       x = "小時 (0–23)",
       y = "百分比") +                   
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))
}

combined_dt[ , .N, by = .(Authority)][order(-N)]
combined_dt[ , .N, by = .(abs_movement_level)][order(-N)]
nrow(combined_dt)
describegroup <- function(dt, variable, group){
  dt[, .(
    n = .N,
    mean = mean(get(variable), na.rm = TRUE),
    sd = sd(get(variable), na.rm = TRUE),
    min = min(get(variable), na.rm = TRUE),
    max = max(get(variable), na.rm = TRUE),
    q25 = quantile(get(variable), 0.25, na.rm = TRUE),
    q75 = quantile(get(variable), 0.75, na.rm = TRUE)
  ), by = group]
}
describegroup(combined_dt, "Distance", "high_temp_alert")
describegroup(combined_dt, "Distance", "low_temp_alert")
describegroup(combined_dt, "Distance", "wind_alert")
describegroup(combined_dt, "Distance", "rain_alert")
describegroup(combined_dt, "Distance", "uv_level")
describegroup(combined_dt, "Distance", "fog_alert")

varbyalert <- function(dt, var, byvar) {
  res <- dt[
    , .(N = .N),
    by = c(byvar, var)
  ][
    , proportion := N / sum(N),
    by = byvar
  ][
    order(get(byvar), get(var))
  ]
  
  return(res)
}
varbyalert(combined_dt,"HolderType","high_temp_alert")
varbyalert(combined_dt,"HolderType","low_temp_alert")
varbyalert(combined_dt,"HolderType","wind_alert")
varbyalert(combined_dt,"HolderType","rain_alert")
varbyalert(combined_dt,"HolderType","uv_level")
varbyalert(combined_dt,"HolderType","fog_alert")

varbyalert(combined_dt,"TicketType","high_temp_alert")
varbyalert(combined_dt,"TicketType","low_temp_alert")
varbyalert(combined_dt,"TicketType","wind_alert")
varbyalert(combined_dt,"TicketType","rain_alert")
varbyalert(combined_dt,"TicketType","uv_level")
varbyalert(combined_dt,"TicketType","fog_alert")

cor(combined_dt$Distance, combined_dt$temperature_c)
cor(combined_dt$Distance, combined_dt$temperature_c, method = "spearman")

sum(substr(weatherstation_id, 1, 1) == "4")
sum(substr(weatherstation_id, 1, 1) == "C")
names(dt)
# 繪圖
# 平均搭乘人次/溫度
weather2024_output_path <- "E:/brain/解壓縮data/資料處理/天氣資料/2024每小時氣象資料(加上警報_將特殊值轉為NA_v3)3.fst"
dt <- read.fst(weather2024_output_path,as.data.table = TRUE)
weatherstation_id <- BStation$BStationID
dt_filtered <- dt[weather_station_ID %in% weatherstation_id]
dt_filtered$datetime <- dt_filtered$datetime - hours(8)

names(dt_filtered)
head(dt_filtered$datetime)

str(dt_filtered$datetime)
attr(dt_filtered$datetime,"tzone")

hourly_weather <- dt_filtered%>%
  mutate(hour=hour(datetime))%>%
  group_by(hour)%>%
  summarise(avg_temp_c = mean(temperature_c, na.rm=TRUE))%>%
  ungroup

ggplot(hourly_weather, aes(x = hour, y = avg_temp_c))+
  geom_line(size=1)+
  scale_x_continuous(breaks=0:23)+
  theme_minimal()

{
weekday_lookup <- c("星期日","星期一","星期二",
                    "星期三","星期四","星期五","星期六")
season_lookup <- c(
  "冬季","冬季","春季","春季","春季",
  "夏季","夏季","夏季","秋季","秋季","秋季","冬季"
)
time_labels <- c(
  rep("凌晨(0-5)", 6),
  rep("早上(6-11)", 6),
  rep("下午(12-17)", 6),
  rep("晚上(20-23)", 6)
)
add_time_vars <- function(dt, time_col = "BoardingTime") {
  lt <- as.POSIXlt(dt[[time_col]])
  dt[, Weekday   := weekday_lookup[lt$wday + 1]]
  dt[, Season    := season_lookup[ month(dt[[time_col]]) ]]
  dt[, TimePeriod:= time_labels[ hour(dt[[time_col]]) + 1 ]]
}
add_time_vars(combined_dt,  "BoardingTime")
add_time_vars(dt_filtered,  "datetime")

names(combined_dt)
freq_dt <- dt_filtered[, .N, by = .(temperature_c)][order(temperature_c)]
ggplot(dt_filtered, aes(x = temperature_c)) +
  geom_histogram(bins = 30, fill = "#D55E00", color = "white") +
  labs(title = "每小時北北基桃天氣站溫度直方圖",
       x     = "溫度 (°C)",
       y     = "數量") +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))

{
  combined_dt <- as.data.table(combined_dt)
  binwidth <- 1
  combined_dt[, temp_bin := round(temperature_c / binwidth) * binwidth]
  options(datatable.optimize = 1L) 
  hist_dt <- combined_dt[, .(count = .N), by = temp_bin][order(temp_bin)]
  ggplot(hist_dt, aes(x = temp_bin, y = count)) +
    geom_col(fill = "skyblue", color = "white") +
    labs(title = "每小時搭乘人次與溫度直方圖",
         x     = "溫度 (°C)",
         y     = "數量") +
    theme_minimal(base_size = 14) +
    theme(plot.title = element_text(hjust = 0.5, face = "bold"))

freq_weather   <- dt_filtered [,        .N, by = .(temp_bin = round(temperature_c))][]
setnames(freq_weather,   "N", "count_weather")
freq_transport <- combined_dt[, .N, by = .(temp_bin = round(temperature_c))][]
setnames(freq_transport, "N", "count_transport")
freq_merged <- merge(freq_transport, freq_weather, by = "temp_bin", all = TRUE)
freq_merged[, ratio := count_transport / count_weather]
p <- ggplot(freq_merged, aes(temp_bin, ratio)) +
  geom_col(fill = "#56B4E9") +
  labs(title = "溫度與平均搭乘人次長條圖",
       x = "溫度 (°C)", y = "搭乘人次／測量次數 比值") +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))

out_dir <- "E:/brain/解壓縮data/資料視覺化/2024/溫度"
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
out_file <- file.path(out_dir, "溫度與平均搭乘人次長條圖.png")
png(filename = out_file,
    width    = 675,    
    height   = 675,    
    units    = "px",   
    res      = 100)   
print(p)
dev.off()
}

table(combined_dt$TicketType)
temp_vartype <- function(weather_data,transport_data,
                         variable=NULL,typecode=NULL,titlename="",
                         weathervar,weathervartitle,
                         xlabname,out_dir,seasons,
                         weekdays,timeperiods){
  if (!is.null(seasons)) {
    weather_data <- weather_data[ Season %in% seasons ]
    transport_data <- transport_data[ Season %in% seasons ]
  }
  if (!is.null(weekdays)) {
    weather_data <- weather_data[ Weekday %in% weekdays ]
    transport_data <- transport_data[ Weekday %in% weekdays ]
  }
  if (!is.null(timeperiods)) {
    weather_data <- weather_data[ TimePeriod %in% timeperiods ]
    transport_data <- transport_data[ TimePeriod %in% timeperiods ]
  }
  
  if (is.null(variable) || is.null(typecode)) {
    transport_data_filtered <- transport_data
  } else {
    transport_data_filtered <- transport_data[get(variable) == typecode]
  }
  freq_weather   <- weather_data [,        .N, by = .(temp_bin = round(get(weathervar)))][]
  setnames(freq_weather,   "N", "count_weather")
  print(head(freq_weather)); flush.console()
  freq_transport <- transport_data_filtered[, .N, by = .(temp_bin = round(get(weathervar)))][]
  setnames(freq_transport, "N", "count_transport")
  print(head(freq_transport)); flush.console()
  freq_merged <- merge(freq_transport, freq_weather, by = "temp_bin", all = TRUE)
  freq_merged[, ratio := count_transport / count_weather]
  freq_merged[, percent := ratio / sum(ratio, na.rm = TRUE) * 100]
  print(head(freq_merged)); flush.console()
  cat("繪圖中...")
  p <- ggplot(freq_merged, aes(temp_bin, percent)) +
    geom_col(fill = "#56B4E9") +
    labs(title = paste0(titlename,weathervartitle,"與平均搭乘人次長條圖","\n"),
         x = xlabname, y = "搭乘人數/測量次數 百分比(%)") +
    theme_minimal(base_size = 14) +
    theme(plot.title = element_text(hjust = 0.5, face = "bold"))
  print(p)
  cat(paste0(titlename,weathervartitle,"與平均搭乘人次長條圖","\n"))
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  out_file <- file.path(out_dir, paste0(titlename,weathervartitle,"與平均搭乘人次長條圖.png"))
  png(filename = out_file,
      width    = 675,    
      height   = 675,    
      units    = "px",   
      res      = 100)   
  print(p)
  dev.off()
}
names(combined_dt)
table(combined_dt$Weekday)
table(combined_dt$Season)
table(combined_dt$TimePeriod)
}

#所有排列組合
seasons_vec    <- c("春季", "夏季", "秋季", "冬季")
weekdays_list  <- list(
  平日 = c("星期一","星期二","星期三","星期四","星期五"),
  假日 = c("星期六","星期日")
)
timeperiods_vec <- c("凌晨(0-5)", "早上(6-11)", "下午(12-17)", "晚上(20-23)")
combos <- expand.grid(
  season     = seasons_vec,
  day_type   = names(weekdays_list),
  timeperiod = timeperiods_vec,
  stringsAsFactors = FALSE
)
pblapply(seq_len(nrow(combos)), function(i) {
  season_i  <- combos$season[i]
  daytype_i <- combos$day_type[i]
  time_i    <- combos$timeperiod[i]
  wd_vec    <- weekdays_list[[daytype_i]]
  time_simple <- gsub("\\(.*\\)", "", time_i)
  title_i     <- paste0("(", season_i, "、", daytype_i, "、", time_simple, ")")
  out_dir_i   <- file.path("E:/brain/解壓縮data/資料視覺化/2024/溫度", season_i, daytype_i, time_i)
  
  temp_vartype(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    variable        = NULL,
    typecode        = NULL,
    titlename       = title_i,
    weathervar      = "temperature_c",
    weathervartitle = "溫度",
    xlabname        = "溫度 (°C)",
    out_dir         = out_dir_i,
    seasons         = season_i,
    weekdays        = wd_vec,
    timeperiods     = time_i
  )
  gc()
})

# 加入身分別所有排列組合
holder_types <- list(
  A     = "^A$",    
  B     = "^B$",    
  C_star = "^C"    
)
holder_labels <- c(
  A      = "普通",
  B      = "學生",
  C_star = "優待"
)
seasons_vec     <- c("春季", "夏季", "秋季", "冬季")
weekdays_list   <- list(
  平日 = c("星期一","星期二","星期三","星期四","星期五"),
  假日 = c("星期六","星期日")
)
timeperiods_vec <- c("凌晨(0-5)", "早上(6-11)", "下午(12-17)", "晚上(20-23)")
combos <- expand.grid(
  season     = seasons_vec,
  day_type   = names(weekdays_list),
  timeperiod = timeperiods_vec,
  holder_key = names(holder_types),
  stringsAsFactors = FALSE
)
pblapply(seq_len(nrow(combos)), function(i) {
  row        <- combos[i, ]
  season_i   <- row$season
  daytype_i  <- row$day_type
  time_i     <- row$timeperiod
  holder_i   <- row$holder_key
  pattern_i      <- holder_types[[holder_i]]
  dt_filt_t      <- combined_dt[    grepl(pattern_i, HolderType) ]
  wd_vec    <- weekdays_list[[daytype_i]]
  display_i <- holder_labels[holder_i]
  time_simple <- gsub("\\(.*\\)", "", time_i)
  title_i     <- paste0("(", season_i, "、", daytype_i, "、", time_simple, "、", display_i ,")")
  out_dir_i   <- file.path("E:/brain/解壓縮data/資料視覺化/2024/溫度", season_i, daytype_i, time_i)
  
  temp_vartype(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    variable        = NULL,
    typecode        = NULL,
    titlename       = title_i,
    weathervar      = "temperature_c",
    weathervartitle = "溫度",
    xlabname        = "溫度 (°C)",
    out_dir         = out_dir_i,
    seasons         = season_i,
    weekdays        = wd_vec,
    timeperiods     = time_i
  )
  gc()
})

#MDS投影比較不同搭乘行為
# ---輔助函數：計算特定條件下的溫度百分比分佈 ---
{
#溫度(四捨五入)
get_temperature_distribution <- function(weather_dt_filtered,
                                         transport_dt_filtered,
                                         weathervar) {
  if (nrow(weather_dt_filtered) == 0) {
    return(data.table(temp_bin = integer(0), percent = numeric(0)))
  }
  
  freq_weather <- weather_dt_filtered[
    , .N, by = .(temp_bin = round(get(weathervar)))
  ][order(temp_bin)]
  setnames(freq_weather, "N", "count_weather")
  
  if (nrow(transport_dt_filtered) == 0) {
    return(freq_weather[, .(temp_bin, percent = 0)])
  }
  freq_transport <- transport_dt_filtered[
    , .N, by = .(temp_bin = round(get(weathervar)))
  ][order(temp_bin)]
  setnames(freq_transport, "N", "count_transport")
  
  freq_merged <- freq_weather[freq_transport, on = "temp_bin"]
  freq_merged[is.na(count_transport), count_transport := 0]
  freq_merged[, ratio := fifelse(count_weather > 0,
                                 count_transport / count_weather, 0)]
  
  sum_ratio <- sum(freq_merged$ratio, na.rm = TRUE)
  if (sum_ratio > 0) {
    freq_merged[, percent := ratio / sum_ratio * 100]
  } else {
    freq_merged[, percent := 0]
  }
  
  return(freq_merged[, .(temp_bin, percent)])
}
#降雨量(5為單位)
get_temperature_distribution <- function(weather_dt_filtered,
                                         transport_dt_filtered,
                                         weathervar) {
  if (nrow(weather_dt_filtered) == 0) {
    return(data.table(temp_bin = integer(0), percent = numeric(0)))
  }
  temp_bin_width <- 5
  freq_weather <- weather_dt_filtered[
    , .N, by = .(temp_bin = floor(get(weathervar) / temp_bin_width) * temp_bin_width)
  ][order(temp_bin)]
  setnames(freq_weather, "N", "count_weather")
  
  if (nrow(transport_dt_filtered) == 0) {
    return(freq_weather[, .(temp_bin, percent = 0)])
  }
  freq_transport <- transport_dt_filtered[
    , .N, by = .(temp_bin = floor(get(weathervar) / temp_bin_width) * temp_bin_width)
  ][order(temp_bin)]
  setnames(freq_transport, "N", "count_transport")
  
  freq_merged <- freq_weather[freq_transport, on = "temp_bin"]
  freq_merged[is.na(count_transport), count_transport := 0]
  freq_merged[, ratio := fifelse(count_weather > 0,
                                 count_transport / count_weather, 0)]
  
  sum_ratio <- sum(freq_merged$ratio, na.rm = TRUE)
  if (sum_ratio > 0) {
    freq_merged[, percent := ratio / sum_ratio * 100]
  } else {
    freq_merged[, percent := 0]
  }
  
  return(freq_merged[, .(temp_bin, percent)])
}
get_all_temp_bins <- function(weather_data, transport_data,
                              seasons_vec, weekdays_list,
                              timeperiods_vec, holder_col_name,
                              holder_types, holder_labels,
                              weathervar) { 
  setDT(weather_data)
  setDT(transport_data)
  
  cat("Generating condition combinations...\n")
  conditions_grid <- CJ(
    season = seasons_vec,
    weekday_group_name = names(weekdays_list),
    timeperiod = timeperiods_vec,
    holder_name = names(holder_labels)
  )
  conditions_grid[, label := paste(season, weekday_group_name, timeperiod, holder_name, sep = "_")]
  gc() # Clean up after CJ
  
  # --- 階段 1: 確定 all_temp_bins ---
  cat("Phase 1: Determining all_temp_bins...\n")
  all_temp_bins_collector <- vector("list", nrow(conditions_grid)) # Pre-allocate list
  
  for (k in 1:nrow(conditions_grid)) {
    cond <- conditions_grid[k, ]
    if (k %% 20 == 0 || k == 1 || k == nrow(conditions_grid)) {
      cat(sprintf("Phase 1 progress: %d/%d processing %s\n", k, nrow(conditions_grid), cond$label))
    }
    
    current_weekdays <- weekdays_list[[cond$weekday_group_name]]
    current_holder_type_pattern <- holder_types[[cond$holder_name]]
    
    # Subsetting data.tables is generally memory efficient (doesn't deep copy immediately)
    weather_filtered <- weather_data[Season == cond$season & Weekday %in% current_weekdays & TimePeriod == cond$timeperiod]
    transport_filtered_base <- transport_data[Season == cond$season & Weekday %in% current_weekdays & TimePeriod == cond$timeperiod]
    
    if (!is.null(holder_col_name) && !is.null(current_holder_type_pattern)) {
      if (!holder_col_name %in% names(transport_filtered_base)) stop(paste("Holder variable", holder_col_name, "not found in transport_data"))
      transport_filtered_specific <- transport_filtered_base[grepl(current_holder_type_pattern, get(holder_col_name))]
    } else {
      transport_filtered_specific <- transport_filtered_base # No further subsetting, still a reference or shallow copy
    }
    
    # get_temperature_distribution creates new data.tables
    dist_dt_temp <- get_temperature_distribution(
      weather_dt_filtered = weather_filtered,
      transport_dt_filtered = transport_filtered_specific,
      weathervar = weathervar
    )
    all_temp_bins_collector[[k]] <- dist_dt_temp$temp_bin
    
    # Aggressively remove intermediate objects for this iteration if memory is extremely tight
    rm(weather_filtered, transport_filtered_base, transport_filtered_specific, dist_dt_temp)
    if (k %% 50 == 0) { gc() } # Optional: more frequent garbage collection
  }
  #rm(all_temp_bins_collector, weather_filtered, transport_filtered_base, transport_filtered_specific); gc() # Clean up Phase 1 objects
  gc()
  
  all_temp_bins <- sort(unique(unlist(all_temp_bins_collector)))
  if (length(all_temp_bins) == 0) stop("No bins found")
  return(all_temp_bins)
}
build_aligned_matrix <- function(weather_data, transport_data,
                                 conditions_grid, all_temp_bins,
                                 holder_col_name, weekdays_list,
                                 holder_types, weathervar) {
  # --- 階段 2: 逐行構建 aligned_matrix  ---
  cat("Phase 2: Building aligned_matrix row by row...\n")
  master_bins_dt <- data.table(temp_bin = all_temp_bins)
  aligned_matrix  <- matrix(0, # Initialize with zeros
                                         nrow = nrow(conditions_grid),
                                         ncol = length(all_temp_bins),
                                         dimnames = list(conditions_grid$label,
                                                         paste0("temp_bin_", all_temp_bins)))
  for (k in 1:nrow(conditions_grid)) {
    cond <- conditions_grid[k, ]
    if (k %% 20 == 0 || k == 1 || k == nrow(conditions_grid)) {
      cat(sprintf("Phase 2 progress: %d/%d processing %s\n", k, nrow(conditions_grid), cond$label))
    }
    
    # Re-filter data (CPU cost for memory saving)
    current_weekdays <- weekdays_list[[cond$weekday_group_name]]
    current_holder_type_pattern <- holder_types[[cond$holder_name]]
    weather_filtered <- weather_data[Season == cond$season & Weekday %in% current_weekdays & TimePeriod == cond$timeperiod]
    transport_filtered_base <- transport_data[Season == cond$season & Weekday %in% current_weekdays & TimePeriod == cond$timeperiod]
    if (!is.null(holder_col_name) && !is.null(current_holder_type_pattern)) {
      if (!holder_col_name %in% names(transport_filtered_base)) stop(paste("Holder variable", holder_col_name, "not found in transport_data"))
      transport_filtered_specific <- transport_filtered_base[grepl(current_holder_type_pattern, get(holder_col_name))]
    } else {
      transport_filtered_specific <- transport_filtered_base
    }
    
    # Re-calculate distribution
    dist_dt <- get_temperature_distribution(
      weather_dt_filtered = weather_filtered,
      transport_dt_filtered = transport_filtered_specific,
      weathervar = weathervar
    )
    
    # Align current distribution
    if (nrow(dist_dt) > 0 && sum(dist_dt$percent, na.rm = TRUE) > 0) {
      # `merge` creates a new data.table
      merged_dist <- merge(master_bins_dt, dist_dt, by = "temp_bin", all.x = TRUE)
      merged_dist[is.na(percent), percent := 0] # In-place modification
      current_percents <- merged_dist$percent   # Vector copy
      sum_p <- sum(current_percents, na.rm = TRUE)
      if (sum_p > 0) {
        # Vector arithmetic and assignment to matrix row
        aligned_matrix[k, ] <- current_percents / sum_p * 100
      } # else row remains 0 (from initialization)
    } # else row remains 0
    
    # Aggressively remove
    rm(weather_filtered, transport_filtered_base, transport_filtered_specific, dist_dt, merged_dist, current_percents)
    if (k %% 50 == 0) { gc() }
  }
  gc() # Clean up Phase 2 loop objects
  return(aligned_matrix)
}

compute_wasserstein_matrix <- function(aligned_matrix, p = 2) {
  # 你原本用的迴圈參考了 aligned_matrix  和 wasserstein_p
  # 需改成：
  num_conditions <- nrow(aligned_matrix)
  dist_matrix <- matrix(0, nrow = num_conditions, ncol = num_conditions,
                        dimnames = list(rownames(aligned_matrix),
                                        rownames(aligned_matrix)))
  for (i in seq_len(num_conditions)) {
    for (j in i: num_conditions) {
      if (i != j) {
        dist_val <- transport::wasserstein1d(
          a = aligned_matrix[i, ], b = aligned_matrix[j, ], p = p
        )
        dist_matrix[i,j] <- dist_val
        dist_matrix[j,i] <- dist_val
      }
    }
  }
  return(dist_matrix)
}

run_mds <- function(dist_matrix, dims = 2) {
  cat("Performing MDS-SMACOF...\n")
  if (any(is.na(dist_matrix)) || any(is.infinite(dist_matrix))) {
    stop("NA or Inf in distance matrix")
  }
  diag(dist_matrix) <- 0
  mds_result <- smacof::smacofSym(dist_matrix, ndim = dims, type = "ratio")
  coords <- as.data.table(mds_result$conf)
  setnames(coords, paste0("Dim", seq_len(dims)))
  coords[, Stress := mds_result$stress]
  return(list(coords = coords, model = mds_result))
}

holder_types <- list(
  A      = "^A$",
  B      = "^B$",
  C_star = "^C"
)
holder_labels <- c( 
  A      = "普通",
  B      = "學生",
  C_star = "優待"
)
seasons_vec     <- c("春季", "夏季", "秋季", "冬季")
weekdays_list   <- list(
  平日 = c("星期一","星期二","星期三","星期四","星期五"),
  假日 = c("星期六","星期日")
)
timeperiods_vec <- c("凌晨(0-5)", "早上(6-11)", "下午(12-17)", "晚上(20-23)")
all_possible_weekdays <- unlist(weekdays_list)
all_possible_seasons <- seasons_vec
all_possible_timeperiods <- timeperiods_vec
names(combined_dt)
combined_dt[, .N, by = Season]
combined_dt[, .N, by = Weekday]
combined_dt[, .N, by = TimePeriod]
names(dt_filtered)
dt_filtered[, .N, by = Season]         
dt_filtered[, .N, by = Weekday]       
dt_filtered[, .N, by = TimePeriod]

#Temp_HolderType
{
conditions_grid <- CJ(
  season = seasons_vec,
  weekday_group_name = names(weekdays_list),
  timeperiod = timeperiods_vec,
  holder_name = names(holder_labels)
)
conditions_grid[, label := paste(season, weekday_group_name, timeperiod, holder_name, sep = "_")]

all_bins <- get_all_temp_bins(
  weather_data    = dt_filtered,
  transport_data  = combined_dt,
  seasons_vec     = seasons_vec,
  weekdays_list   = weekdays_list,
  timeperiods_vec = timeperiods_vec,
  holder_col_name = "HolderType",
  holder_types    = holder_types,
  holder_labels   = holder_labels,
  weathervar      = "temperature_c"
)

aligned_mat <- build_aligned_matrix(
  weather_data    = dt_filtered,
  transport_data  = combined_dt,
  conditions_grid = conditions_grid,
  all_temp_bins   = all_bins,
  holder_col_name = "HolderType",
  weekdays_list   = weekdays_list,
  holder_types    = holder_types,
  weathervar      = "temperature_c"
)

dist_mat <- compute_wasserstein_matrix(
  aligned_matrix = aligned_mat,
  p               = 2      # 對應 wasserstein_p = 2
)

mds_out <- run_mds(
  dist_matrix = dist_mat,
  dims        = 2      # 對應 mds_dims = 2
)

mds_analysis_output <- list(
  mds_data            = cbind(
    conditions_grid[, .(
      Label          = label,
      Season         = season,
      Weekday_Group  = weekday_group_name,
      Time_Period    = timeperiod,
      Holder_Type    = holder_name
    )],
    mds_out$coords
  ),
  wasserstein_matrix  = dist_mat,
  smacof_object       = mds_out$model
)
saveRDS(mds_analysis_output,
        file="E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_溫度_身分).rds")
mds_analysis_output <- readRDS("E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_溫度_身分).rds")
mds_model <- mds_analysis_output$smacof_object
print(mds_model)

mds_df <- mds_analysis_output$mds_data
mds_df$Time_Period <- factor(
  mds_df$Time_Period,
  levels = c("凌晨(0-5)", "早上(6-11)", "下午(12-17)", "晚上(20-23)")
)

mds_df$Holder_Type <- factor(
  mds_df$Holder_Type,
  levels = c("A", "B", "C_star")
)

names(mds_df)
#完整圖
{
ggplot(mds_df, aes(x = Dim1, y = Dim2,
                   color = Time_Period,
                   shape = Holder_Type)) +
  geom_point(size = 3) +
  scale_color_manual(
    name   = "時間段",                 
    breaks = levels(mds_df$Time_Period),  
    values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
  ) +
  scale_shape_manual(
    name   = "持卡身分",
    breaks = levels(mds_df$Holder_Type),
    labels = c("普通", "學生", "優待"),
    values = c(16, 17, 15)
  ) +
  labs(
    title = "MDS投影: 溫度–人次 (時間段＆身分)",
    x     = "Dimension 1",
    y     = "Dimension 2"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    legend.position = "right",
    plot.title      = element_text(hjust = 0.5)
  )
}
#分平日周末圖
{
  ggplot(mds_df, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Holder_Type)) +
    facet_wrap(~ Weekday_Group, scales = "free") +
    geom_point(size = 3) +
    scale_color_manual(
      name   = "時間段",                 
      breaks = levels(mds_df$Time_Period),  
      values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
    ) +
    scale_shape_manual(
      name   = "持卡身分",
      breaks = levels(mds_df$Holder_Type),
      labels = c("普通", "學生", "優待"),
      values = c(16, 17, 15)
    ) +
    labs(
      title = " MDS投影: 溫度–人次(平日假日&時間段&身分)",
      x     = "Dimension 1",
      y     = "Dimension 2"
    ) +
    theme_minimal(base_size = 14) +
    theme(
      legend.position = "right",
      plot.title      = element_text(hjust = 0.5)
    )
}

#分平假日MDS
{
s="假日"
df_s <- subset(mds_df, Weekday_Group == s)
df_s$Season <- factor(df_s$Season,
                      levels = c("春季","夏季","秋季","冬季"))
df_s$Time_Period <- factor(df_s$Time_Period,
                           levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
df_s$Holder_Type <- factor(
  df_s$Holder_Type,
  levels = c("A", "B", "C_star"),
  labels = c("普通", "學生", "優待")
)

names(df_s)
ggplot(df_s, aes(x = Dim1, y = Dim2,
                      color = Time_Period,
                      shape = Season)) +
  scale_color_manual(
    name   = "時間段",                 
    breaks = levels(df_s$Time_Period),  
    values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
  ) +
  scale_shape_manual(
    name   = "季節",
    breaks = levels(df_s$Season),
    values = c(16, 17, 15, 18)
  ) +
  facet_wrap(~ Holder_Type, scales = "free") +
  geom_point(size = 3) +
  labs(
    title = paste0("MDS投影: ","(",s,"、身分)","溫度–人次(時間段&季節)"),
    x     = "Dim1",
    y     = "Dim2"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    legend.position = "right",
    plot.title      = element_text(hjust = 0.5)
  )
print(paste0("MDS投影: ","(",s,"、身分)","溫度–人次(時間段&季節)"))
}

#分季節MDS
{
  s="假日"
  df_s <- subset(mds_df, Weekday_Group == s)
  df_s$Season <- factor(df_s$Season,
                        levels = c("春季","夏季","秋季","冬季"))
  df_s$Time_Period <- factor(df_s$Time_Period,
                             levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
  df_s$Holder_Type <- factor(
    df_s$Holder_Type,
    levels = c("A", "B", "C_star"),
    labels = c("普通", "學生", "優待")
  )
  
  names(df_s)
  ggplot(df_s, aes(x = Dim1, y = Dim2,
                   color = Time_Period,
                   shape = Holder_Type)) +
    scale_color_manual(
      name   = "時間段",                 
      breaks = levels(df_s$Time_Period),  
      values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
    ) +
    scale_shape_manual(
      name   = "身分",
      breaks = levels(df_s$Holder_Type),
      values = c(16, 17, 15)
    ) +
    facet_wrap(~ Season, scales = "free") +
    geom_point(size = 3) +
    labs(
      title = paste0("MDS投影: ","(",s,"、季節)","溫度–人次(時間段&身分)"),
      x     = "Dim1",
      y     = "Dim2"
    ) +
    theme_minimal(base_size = 14) +
    theme(
      legend.position = "right",
      plot.title      = element_text(hjust = 0.5)
    )
  print(paste0("MDS投影 ","(",s,"、季節)","溫度–人次(時間段&身分)"))
}
}

#Temp_TicketType
{
  ticket_types <- list(
    Single_Ticket      = "1",
    Monthly_Pass      = "4"
  )
  ticket_labels <- c( 
    Single_Ticket      = "單程票",
    Monthly_Pass      = "月票"
  )
  conditions_grid <- CJ(
    season = seasons_vec,
    weekday_group_name = names(weekdays_list),
    timeperiod = timeperiods_vec,
    holder_name = names(ticket_labels)
  )
  conditions_grid[, label := paste(season, weekday_group_name, timeperiod, holder_name, sep = "_")]
  
  all_bins <- get_all_temp_bins(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    seasons_vec     = seasons_vec,
    weekdays_list   = weekdays_list,
    timeperiods_vec = timeperiods_vec,
    holder_col_name = "TicketType",
    holder_types    = ticket_types,
    holder_labels   = ticket_labels,
    weathervar      = "temperature_c"
  )
  
  aligned_mat <- build_aligned_matrix(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    conditions_grid = conditions_grid,
    all_temp_bins   = all_bins,
    holder_col_name = "TicketType",
    weekdays_list   = weekdays_list,
    holder_types    = ticket_types,
    weathervar      = "temperature_c"
  )
  
  dist_mat <- compute_wasserstein_matrix(
    aligned_matrix = aligned_mat,
    p               = 2      # 對應 wasserstein_p = 2
  )
  
  mds_out <- run_mds(
    dist_matrix = dist_mat,
    dims        = 2      # 對應 mds_dims = 2
  )
  
  mds_analysis_output <- list(
    mds_data            = cbind(
      conditions_grid[, .(
        Label          = label,
        Season         = season,
        Weekday_Group  = weekday_group_name,
        Time_Period    = timeperiod,
        Holder_Type    = holder_name
      )],
      mds_out$coords
    ),
    wasserstein_matrix  = dist_mat,
    smacof_object       = mds_out$model
  )
  saveRDS(mds_analysis_output,
          file="E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_溫度_單程票月票).rds")
  
  mds_analysis_output <- readRDS("E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_溫度_單程票月票).rds")
  mds_model <- mds_analysis_output$smacof_object
  print(mds_model)
  
  mds_df <- mds_analysis_output$mds_data
  mds_df$Time_Period <- factor(
    mds_df$Time_Period,
    levels = c("凌晨(0-5)", "早上(6-11)", "下午(12-17)", "晚上(20-23)")
  )
  
  table(mds_df$Holder_Type)
  mds_df$Holder_Type <- factor(
    mds_df$Holder_Type,
    levels = c("Monthly_Pass", "Single_Ticket")
  )
  
  names(mds_df)
  #完整圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "月票單程票",
        breaks = levels(mds_df$Holder_Type),
        labels = c("月票", "單程票"),
        values = c(16, 17)
      ) +
      labs(
        title = "MDS投影: 溫度–人次 (時間段＆月票單程票)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    
      ggsave("E:/brain/解壓縮data/資料視覺化/2024/MDS/溫度/月票單程票/MDS投影 溫度–人次 (時間段＆月票單程票).png", p,
             width  = 1125, 
             height = 675, 
             units  = "px")
    
    print("MDS投影 溫度–人次 (時間段＆月票單程票)")
   
    
  }
  #分平日周末圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      facet_wrap(~ Weekday_Group, scales = "free") +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "月票單程票",
        breaks = levels(mds_df$Holder_Type),
        labels = c("月票", "單程票"),
        values = c(16, 17)
      ) +
      labs(
        title = " MDS投影: 溫度–人次(平日假日&時間段＆月票單程票)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(" MDS投影 溫度–人次(平日假日&時間段＆月票單程票)")
  }
  
  #分平假日MDS
  {
    s="平日"
    df_s <- subset(mds_df, Weekday_Group == s)
    df_s$Season <- factor(df_s$Season,
                          levels = c("春季","夏季","秋季","冬季"))
    df_s$Time_Period <- factor(df_s$Time_Period,
                               levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
    df_s$Holder_Type <- factor(
      df_s$Holder_Type,
      levels = c("Monthly_Pass", "Single_Ticket"),
      labels = c("月票", "單程票")
    )
    
    names(df_s)
    ggplot(df_s, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Season)) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(df_s$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "季節",
        breaks = levels(df_s$Season),
        values = c(16, 17, 15, 18)
      ) +
      facet_wrap(~ Holder_Type, scales = "free") +
      geom_point(size = 3) +
      labs(
        title = paste0("MDS投影: ","(",s,"、月票單程票)","溫度–人次(時間段&季節)"),
        x     = "Dim1",
        y     = "Dim2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(paste0("MDS投影 ","(",s,"、月票單程票)","溫度–人次(時間段&季節)"))
  }
  
  #分季節MDS
  {
    s="假日"
    df_s <- subset(mds_df, Weekday_Group == s)
    df_s$Season <- factor(df_s$Season,
                          levels = c("春季","夏季","秋季","冬季"))
    df_s$Time_Period <- factor(df_s$Time_Period,
                               levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
    df_s$Holder_Type <- factor(
      df_s$Holder_Type,
      levels = c("Monthly_Pass", "Single_Ticket"),
      labels = c("月票", "單程票")
    )
    
    names(df_s)
    ggplot(df_s, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Holder_Type)) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(df_s$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "月票單程票",
        breaks = levels(df_s$Holder_Type),
        values = c(16, 17)
      ) +
      facet_wrap(~ Season, scales = "free") +
      geom_point(size = 3) +
      labs(
        title = paste0("MDS投影: ","(",s,"、季節)","溫度–人次(時間段&身分)"),
        x     = "Dim1",
        y     = "Dim2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(paste0("MDS投影 ","(",s,"、季節)","溫度–人次(時間段&月票單程票)"))
  }
}

#Temp_abs_movement
{
  movement_level_types <- list(
    zero      = "0",
    one       = "1",
    two       = "2",
    three     = "3"
  )
  movement_level_labels <- c( 
    zero      = "移動0個層級",
    one       = "移動1個層級",
    two       = "移動2個層級",
    three     = "移動3個層級"
  )
  conditions_grid <- CJ(
    season = seasons_vec,
    weekday_group_name = names(weekdays_list),
    timeperiod = timeperiods_vec,
    holder_name = names(movement_level_labels)
  )
  conditions_grid[, label := paste(season, weekday_group_name, timeperiod, holder_name, sep = "_")]
  
  all_bins <- get_all_temp_bins(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    seasons_vec     = seasons_vec,
    weekdays_list   = weekdays_list,
    timeperiods_vec = timeperiods_vec,
    holder_col_name = "abs_movement_level",
    holder_types    = movement_level_types,
    holder_labels   = movement_level_labels,
    weathervar      = "temperature_c"
  )
  
  aligned_mat <- build_aligned_matrix(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    conditions_grid = conditions_grid,
    all_temp_bins   = all_bins,
    holder_col_name = "abs_movement_level",
    weekdays_list   = weekdays_list,
    holder_types    = movement_level_types,
    weathervar      = "temperature_c"
  )
  
  dist_mat <- compute_wasserstein_matrix(
    aligned_matrix = aligned_mat,
    p               = 2      # 對應 wasserstein_p = 2
  )
  
  mds_out <- run_mds(
    dist_matrix = dist_mat,
    dims        = 2      # 對應 mds_dims = 2
  )
  
  mds_analysis_output <- list(
    mds_data            = cbind(
      conditions_grid[, .(
        Label          = label,
        Season         = season,
        Weekday_Group  = weekday_group_name,
        Time_Period    = timeperiod,
        Holder_Type    = holder_name
      )],
      mds_out$coords
    ),
    wasserstein_matrix  = dist_mat,
    smacof_object       = mds_out$model
  )
  saveRDS(mds_analysis_output,
          file="E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_溫度_移動層級).rds")
  
  mds_model <- mds_analysis_output$smacof_object
  print(mds_model)
  mds_df <- mds_analysis_output$mds_data
  mds_df$Time_Period <- factor(
    mds_df$Time_Period,
    levels = c("凌晨(0-5)", "早上(6-11)", "下午(12-17)", "晚上(20-23)")
  )
  
  mds_df$Holder_Type <- factor(
    mds_df$Holder_Type,
    levels = c("A", "B", "C_star")
  )
  
  names(mds_df)
  #完整圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "持卡身分",
        breaks = levels(mds_df$Holder_Type),
        labels = c("普通", "學生", "優待"),
        values = c(16, 17, 15)
      ) +
      labs(
        title = "MDS投影: 溫度–人次 (時間段＆身分)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
  }
  #分平日周末圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      facet_wrap(~ Weekday_Group, scales = "free") +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "持卡身分",
        breaks = levels(mds_df$Holder_Type),
        labels = c("普通", "學生", "優待"),
        values = c(16, 17, 15)
      ) +
      labs(
        title = " MDS投影: 溫度–人次(平日假日&時間段＆身分)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
  }
  
  #分平假日MDS
  {
    s="假日"
    df_s <- subset(mds_df, Weekday_Group == s)
    df_s$Season <- factor(df_s$Season,
                          levels = c("春季","夏季","秋季","冬季"))
    df_s$Time_Period <- factor(df_s$Time_Period,
                               levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
    df_s$Holder_Type <- factor(
      df_s$Holder_Type,
      levels = c("A", "B", "C_star"),
      labels = c("普通", "學生", "優待")
    )
    
    names(df_s)
    ggplot(df_s, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Season)) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(df_s$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "季節",
        breaks = levels(df_s$Season),
        values = c(16, 17, 15, 18)
      ) +
      facet_wrap(~ Holder_Type, scales = "free") +
      geom_point(size = 3) +
      labs(
        title = paste0("MDS投影: ","(",s,"、身分)","溫度–人次(時間段&季節)"),
        x     = "Dim1",
        y     = "Dim2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(paste0("MDS投影: ","(",s,"、身分)","溫度–人次(時間段&季節)"))
  }
}

#Temp_Authority
{
  {
  authority_types <- list(
    Bus = c("NewTaipei", "Taipei", "Keelung", "Taoyuan"),
    MRT = c("TRTC", "NTMC")
  )
  
  authority_labels <- c(
    Bus = "公車",
    MRT = "捷運"
  )
  conditions_grid <- CJ(
    season = seasons_vec,
    weekday_group_name = names(weekdays_list),
    timeperiod = timeperiods_vec,
    holder_name = names(authority_labels)
  )
  conditions_grid[, label := paste(season, weekday_group_name, timeperiod, holder_name, sep = "_")]
  
  all_bins <- get_all_temp_bins(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    seasons_vec     = seasons_vec,
    weekdays_list   = weekdays_list,
    timeperiods_vec = timeperiods_vec,
    holder_col_name = "Authority",
    holder_types    = authority_types,
    holder_labels   = authority_labels,
    weathervar      = "temperature_c"
  )
  
  aligned_mat <- build_aligned_matrix(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    conditions_grid = conditions_grid,
    all_temp_bins   = all_bins,
    holder_col_name = "Authority",
    weekdays_list   = weekdays_list,
    holder_types    = authority_types,
    weathervar      = "temperature_c"
  )
  
  dist_mat <- compute_wasserstein_matrix(
    aligned_matrix = aligned_mat,
    p               = 2      # 對應 wasserstein_p = 2
  )
  
  mds_out <- run_mds(
    dist_matrix = dist_mat,
    dims        = 2      # 對應 mds_dims = 2
  )
  
  mds_analysis_output <- list(
    mds_data            = cbind(
      conditions_grid[, .(
        Label          = label,
        Season         = season,
        Weekday_Group  = weekday_group_name,
        Time_Period    = timeperiod,
        Holder_Type    = holder_name
      )],
      mds_out$coords
    ),
    wasserstein_matrix  = dist_mat,
    smacof_object       = mds_out$model
  )
  saveRDS(mds_analysis_output,
          file="E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_溫度_交通工具).rds")
  }
mds_analysis_output <- readRDS("E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_溫度_交通工具).rds")
  mds_model <- mds_analysis_output$smacof_object
  print(mds_model)
  
  mds_df <- mds_analysis_output$mds_data
  mds_df$Time_Period <- factor(
    mds_df$Time_Period,
    levels = c("凌晨(0-5)", "早上(6-11)", "下午(12-17)", "晚上(20-23)")
  )
  
  mds_df$Holder_Type <- factor(
    mds_df$Holder_Type,
    levels = c("Bus", "MRT")
  )
  
  names(mds_df)
  #完整圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "持卡身分",
        breaks = levels(mds_df$Holder_Type),
        labels = c("公車", "捷運"),
        values = c(16, 17)
      ) +
      labs(
        title = "MDS投影: 溫度–人次 (時間段＆交通工具)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
  }
  #分平日周末圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      facet_wrap(~ Weekday_Group, scales = "free") +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "交通工具",
        breaks = levels(mds_df$Holder_Type),
        labels = c("公車", "捷運"),
        values = c(16, 17)
      ) +
      labs(
        title = " MDS投影: 溫度–人次(平日假日&時間段&身分)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
  }
  
  #分平假日MDS
  {
    s="假日"
    df_s <- subset(mds_df, Weekday_Group == s)
    df_s$Season <- factor(df_s$Season,
                          levels = c("春季","夏季","秋季","冬季"))
    df_s$Time_Period <- factor(df_s$Time_Period,
                               levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
    df_s$Holder_Type <- factor(
      df_s$Holder_Type,
      levels = c("A", "B", "C_star"),
      labels = c("普通", "學生", "優待")
    )
    
    names(df_s)
    ggplot(df_s, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Season)) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(df_s$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "季節",
        breaks = levels(df_s$Season),
        values = c(16, 17, 15, 18)
      ) +
      facet_wrap(~ Holder_Type, scales = "free") +
      geom_point(size = 3) +
      labs(
        title = paste0("MDS投影: ","(",s,"、身分)","溫度–人次(時間段&季節)"),
        x     = "Dim1",
        y     = "Dim2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(paste0("MDS投影: ","(",s,"、身分)","溫度–人次(時間段&季節)"))
  }
  
  #分季節MDS
  {
    s="假日"
    df_s <- subset(mds_df, Weekday_Group == s)
    df_s$Season <- factor(df_s$Season,
                          levels = c("春季","夏季","秋季","冬季"))
    df_s$Time_Period <- factor(df_s$Time_Period,
                               levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
    df_s$Holder_Type <- factor(
      df_s$Holder_Type,
      levels = c("A", "B", "C_star"),
      labels = c("普通", "學生", "優待")
    )
    
    names(df_s)
    ggplot(df_s, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Holder_Type)) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(df_s$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "身分",
        breaks = levels(df_s$Holder_Type),
        values = c(16, 17, 15)
      ) +
      facet_wrap(~ Season, scales = "free") +
      geom_point(size = 3) +
      labs(
        title = paste0("MDS投影: ","(",s,"、季節)","溫度–人次(時間段&身分)"),
        x     = "Dim1",
        y     = "Dim2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(paste0("MDS投影 ","(",s,"、季節)","溫度–人次(時間段&身分)"))
  }
}

#Percip_HolderType
{
  conditions_grid <- CJ(
    season = seasons_vec,
    weekday_group_name = names(weekdays_list),
    timeperiod = timeperiods_vec,
    holder_name = names(holder_labels)
  )
  conditions_grid[, label := paste(season, weekday_group_name, timeperiod, holder_name, sep = "_")]
  
  all_bins <- get_all_temp_bins(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    seasons_vec     = seasons_vec,
    weekdays_list   = weekdays_list,
    timeperiods_vec = timeperiods_vec,
    holder_col_name = "HolderType",
    holder_types    = holder_types,
    holder_labels   = holder_labels,
    weathervar      = "precipitation_mm"
  )
  
  aligned_mat <- build_aligned_matrix(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    conditions_grid = conditions_grid,
    all_temp_bins   = all_bins,
    holder_col_name = "HolderType",
    weekdays_list   = weekdays_list,
    holder_types    = holder_types,
    weathervar      = "precipitation_mm"
  )
  
  dist_mat <- compute_wasserstein_matrix(
    aligned_matrix = aligned_mat,
    p               = 2      # 對應 wasserstein_p = 2
  )
  
  mds_out <- run_mds(
    dist_matrix = dist_mat,
    dims        = 2      # 對應 mds_dims = 2
  )
  
  mds_analysis_output <- list(
    mds_data            = cbind(
      conditions_grid[, .(
        Label          = label,
        Season         = season,
        Weekday_Group  = weekday_group_name,
        Time_Period    = timeperiod,
        Holder_Type    = holder_name
      )],
      mds_out$coords
    ),
    wasserstein_matrix  = dist_mat,
    smacof_object       = mds_out$model
  )
  
  bin_mid <- all_bins + 2.5  
  avg_precip <- as.numeric(aligned_mat %*% bin_mid) / 100
  mds_analysis_output$mds_data[, AvgPrecip := avg_precip]
  cor(mds_analysis_output$mds_data$Dim1, mds_analysis_output$mds_data$AvgPrecip)
  cor(mds_analysis_output$mds_data$Dim2, mds_analysis_output$mds_data$AvgPrecip)
  
  
  saveRDS(mds_analysis_output,
          file="E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_降雨量_身分).rds")
  mds_analysis_output <- readRDS("E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_降雨量_身分).rds")
  
  mds_model <- mds_analysis_output$smacof_object
  print(mds_model)
  
  mds_df <- mds_analysis_output$mds_data
  mds_df$Time_Period <- factor(
    mds_df$Time_Period,
    levels = c("凌晨(0-5)", "早上(6-11)", "下午(12-17)", "晚上(20-23)")
  )
  
  mds_df$Holder_Type <- factor(
    mds_df$Holder_Type,
    levels = c("A", "B", "C_star")
  )
  
  names(mds_df)
  #完整圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "持卡身分",
        breaks = levels(mds_df$Holder_Type),
        labels = c("普通", "學生", "優待"),
        values = c(16, 17, 15)
      ) +
      labs(
        title = "MDS投影: 降雨量–人次 (時間段＆身分)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print("MDS投影 降雨量–人次 (時間段＆身分)")
  }
  #分平日周末圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      facet_wrap(~ Weekday_Group, scales = "free") +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "持卡身分",
        breaks = levels(mds_df$Holder_Type),
        labels = c("普通", "學生", "優待"),
        values = c(16, 17, 15)
      ) +
      labs(
        title = " MDS投影: 降雨量–人次(平日假日&時間段＆身分)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(" MDS投影 降雨量–人次(平日假日&時間段＆身分)")
  }
  
  #分平假日MDS
  {
    s="假日"
    df_s <- subset(mds_df, Weekday_Group == s)
    df_s$Season <- factor(df_s$Season,
                          levels = c("春季","夏季","秋季","冬季"))
    df_s$Time_Period <- factor(df_s$Time_Period,
                               levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
    df_s$Holder_Type <- factor(
      df_s$Holder_Type,
      levels = c("A", "B", "C_star"),
      labels = c("普通", "學生", "優待")
    )
    
    names(df_s)
    ggplot(df_s, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Season)) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(df_s$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "季節",
        breaks = levels(df_s$Season),
        values = c(16, 17, 15, 18)
      ) +
      facet_wrap(~ Holder_Type, scales = "free") +
      geom_point(size = 3) +
      labs(
        title = paste0("MDS投影: ","(",s,"、身分)","降雨量–人次(時間段&季節)"),
        x     = "Dim1",
        y     = "Dim2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(paste0("MDS投影 ","(",s,"、身分)","降雨量–人次(時間段&季節)"))
  }
  
  #分季節MDS
  {
    s="假日"
    df_s <- subset(mds_df, Weekday_Group == s)
    df_s$Season <- factor(df_s$Season,
                          levels = c("春季","夏季","秋季","冬季"))
    df_s$Time_Period <- factor(df_s$Time_Period,
                               levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
    df_s$Holder_Type <- factor(
      df_s$Holder_Type,
      levels = c("A", "B", "C_star"),
      labels = c("普通", "學生", "優待")
    )
    
    names(df_s)
    ggplot(df_s, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Holder_Type)) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(df_s$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "身分",
        breaks = levels(df_s$Holder_Type),
        values = c(16, 17, 15)
      ) +
      facet_wrap(~ Season, scales = "free") +
      geom_point(size = 3) +
      labs(
        title = paste0("MDS投影: ","(",s,"、季節)","降雨量–人次(時間段&身分)"),
        x     = "Dim1",
        y     = "Dim2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(paste0("MDS投影 ","(",s,"、季節)","降雨量–人次(時間段&身分)"))
  }
}

#Percip_TicketType
{
  {
  ticket_types <- list(
    Single_Ticket      = "1",
    Monthly_Pass      = "4"
  )
  ticket_labels <- c( 
    Single_Ticket      = "單程票",
    Monthly_Pass      = "月票"
  )
  conditions_grid <- CJ(
    season = seasons_vec,
    weekday_group_name = names(weekdays_list),
    timeperiod = timeperiods_vec,
    holder_name = names(ticket_labels)
  )
  conditions_grid[, label := paste(season, weekday_group_name, timeperiod, holder_name, sep = "_")]
  
  all_bins <- get_all_temp_bins(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    seasons_vec     = seasons_vec,
    weekdays_list   = weekdays_list,
    timeperiods_vec = timeperiods_vec,
    holder_col_name = "TicketType",
    holder_types    = ticket_types,
    holder_labels   = ticket_labels,
    weathervar      = "precipitation_mm"
  )
  
  aligned_mat <- build_aligned_matrix(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    conditions_grid = conditions_grid,
    all_temp_bins   = all_bins,
    holder_col_name = "TicketType",
    weekdays_list   = weekdays_list,
    holder_types    = ticket_types,
    weathervar      = "precipitation_mm"
  )
  
  dist_mat <- compute_wasserstein_matrix(
    aligned_matrix = aligned_mat,
    p               = 2      # 對應 wasserstein_p = 2
  )
  
  mds_out <- run_mds(
    dist_matrix = dist_mat,
    dims        = 2      # 對應 mds_dims = 2
  )
  
  mds_analysis_output <- list(
    mds_data            = cbind(
      conditions_grid[, .(
        Label          = label,
        Season         = season,
        Weekday_Group  = weekday_group_name,
        Time_Period    = timeperiod,
        Holder_Type    = holder_name
      )],
      mds_out$coords
    ),
    wasserstein_matrix  = dist_mat,
    smacof_object       = mds_out$model
  )
  saveRDS(mds_analysis_output,
          file="E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_降雨量_單程票月票).rds")
  }
  mds_analysis_output <- readRDS("E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_降雨量_單程票月票).rds")
  
  mds_model <- mds_analysis_output$smacof_object
  print(mds_model)
  mds_df <- mds_analysis_output$mds_data
  mds_df$Time_Period <- factor(
    mds_df$Time_Period,
    levels = c("凌晨(0-5)", "早上(6-11)", "下午(12-17)", "晚上(20-23)")
  )
  
  mds_df$Holder_Type <- factor(
    mds_df$Holder_Type,
    levels = c("Monthly_Pass", "Single_Ticket")
  )
  
  names(mds_df)
  #完整圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "月票單程票",
        breaks = levels(mds_df$Holder_Type),
        labels = c("月票", "單程票"),
        values = c(16, 17)
      ) +
      labs(
        title = "MDS投影: 降雨量–人次 (時間段＆月票單程票)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print("MDS投影 降雨量–人次 (時間段＆月票單程票)")
  }
  #分平日周末圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      facet_wrap(~ Weekday_Group, scales = "free") +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "月票單程票",
        breaks = levels(mds_df$Holder_Type),
        labels = c("月票", "單程票"),
        values = c(16, 17)
      ) +
      labs(
        title = " MDS投影: 降雨量–人次(平日假日&時間段&月票單程票)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print("MDS投影 降雨量–人次(平日假日&時間段&月票單程票)")
  }
  
  #分平假日MDS
  {
    s="假日"
    df_s <- subset(mds_df, Weekday_Group == s)
    df_s$Season <- factor(df_s$Season,
                          levels = c("春季","夏季","秋季","冬季"))
    df_s$Time_Period <- factor(df_s$Time_Period,
                               levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
    df_s$Holder_Type <- factor(
      df_s$Holder_Type,
      levels = c("Monthly_Pass", "Single_Ticket"),
      labels = c("月票", "單程票")
    )
    
    names(df_s)
    ggplot(df_s, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Season)) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(df_s$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "季節",
        breaks = levels(df_s$Season),
        values = c(16, 17, 15, 18)
      ) +
      facet_wrap(~ Holder_Type, scales = "free") +
      geom_point(size = 3) +
      labs(
        title = paste0("MDS投影: ","(",s,"、月票單程票)","降雨量–人次(時間段&季節)"),
        x     = "Dim1",
        y     = "Dim2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(paste0("MDS投影 ","(",s,"、月票單程票)","降雨量–人次(時間段&季節)"))
  }
  
  #分季節MDS
  {
    s="假日"
    df_s <- subset(mds_df, Weekday_Group == s)
    df_s$Season <- factor(df_s$Season,
                          levels = c("春季","夏季","秋季","冬季"))
    df_s$Time_Period <- factor(df_s$Time_Period,
                               levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
    df_s$Holder_Type <- factor(
      df_s$Holder_Type,
      levels = c("Monthly_Pass", "Single_Ticket"),
      labels = c("月票", "單程票")
    )
    
    names(df_s)
    ggplot(df_s, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Holder_Type)) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(df_s$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "月票單程票",
        breaks = levels(df_s$Holder_Type),
        values = c(16, 17)
      ) +
      facet_wrap(~ Season, scales = "free") +
      geom_point(size = 3) +
      labs(
        title = paste0("MDS投影: ","(",s,"、季節)","降雨量–人次(時間段&月票單程票)"),
        x     = "Dim1",
        y     = "Dim2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(paste0("MDS投影 ","(",s,"、季節)","降雨量–人次(時間段&月票單程票)"))
  }
}

#Percip_abs_movement
{
  movement_level_types <- list(
    zero      = "0",
    one       = "1",
    two       = "2",
    three     = "3"
  )
  movement_level_labels <- c( 
    zero      = "移動0個層級",
    one       = "移動1個層級",
    two       = "移動2個層級",
    three     = "移動3個層級"
  )
  conditions_grid <- CJ(
    season = seasons_vec,
    weekday_group_name = names(weekdays_list),
    timeperiod = timeperiods_vec,
    holder_name = names(movement_level_labels)
  )
  conditions_grid[, label := paste(season, weekday_group_name, timeperiod, holder_name, sep = "_")]
  
  all_bins <- get_all_temp_bins(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    seasons_vec     = seasons_vec,
    weekdays_list   = weekdays_list,
    timeperiods_vec = timeperiods_vec,
    holder_col_name = "abs_movement_level",
    holder_types    = movement_level_types,
    holder_labels   = movement_level_labels,
    weathervar      = "precipitation_mm"
  )
  
  aligned_mat <- build_aligned_matrix(
    weather_data    = dt_filtered,
    transport_data  = combined_dt,
    conditions_grid = conditions_grid,
    all_temp_bins   = all_bins,
    holder_col_name = "abs_movement_level",
    weekdays_list   = weekdays_list,
    holder_types    = movement_level_types,
    weathervar      = "precipitation_mm"
  )
  
  dist_mat <- compute_wasserstein_matrix(
    aligned_matrix = aligned_mat,
    p               = 2      # 對應 wasserstein_p = 2
  )
  
  mds_out <- run_mds(
    dist_matrix = dist_mat,
    dims        = 2      # 對應 mds_dims = 2
  )
  
  mds_analysis_output <- list(
    mds_data            = cbind(
      conditions_grid[, .(
        Label          = label,
        Season         = season,
        Weekday_Group  = weekday_group_name,
        Time_Period    = timeperiod,
        Holder_Type    = holder_name
      )],
      mds_out$coords
    ),
    wasserstein_matrix  = dist_mat,
    smacof_object       = mds_out$model
  )
  saveRDS(mds_analysis_output,
          file="E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_降雨量_移動層級).rds")
  
  mds_model <- mds_analysis_output$smacof_object
  print(mds_model)
  mds_df <- mds_analysis_output$mds_data
  mds_df$Time_Period <- factor(
    mds_df$Time_Period,
    levels = c("凌晨(0-5)", "早上(6-11)", "下午(12-17)", "晚上(20-23)")
  )
  
  mds_df$Holder_Type <- factor(
    mds_df$Holder_Type,
    levels = c("A", "B", "C_star")
  )
  
  names(mds_df)
  #完整圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "持卡身分",
        breaks = levels(mds_df$Holder_Type),
        labels = c("普通", "學生", "優待"),
        values = c(16, 17, 15)
      ) +
      labs(
        title = "MDS投影: 溫度–人次 (時間段＆身分)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
  }
  #分平日周末圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      facet_wrap(~ Weekday_Group, scales = "free") +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "持卡身分",
        breaks = levels(mds_df$Holder_Type),
        labels = c("普通", "學生", "優待"),
        values = c(16, 17, 15)
      ) +
      labs(
        title = " MDS投影: 溫度–人次(平日假日&時間段＆身分)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
  }
  
  #分平假日MDS
  {
    s="假日"
    df_s <- subset(mds_df, Weekday_Group == s)
    df_s$Season <- factor(df_s$Season,
                          levels = c("春季","夏季","秋季","冬季"))
    df_s$Time_Period <- factor(df_s$Time_Period,
                               levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
    df_s$Holder_Type <- factor(
      df_s$Holder_Type,
      levels = c("A", "B", "C_star"),
      labels = c("普通", "學生", "優待")
    )
    
    names(df_s)
    ggplot(df_s, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Season)) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(df_s$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "季節",
        breaks = levels(df_s$Season),
        values = c(16, 17, 15, 18)
      ) +
      facet_wrap(~ Holder_Type, scales = "free") +
      geom_point(size = 3) +
      labs(
        title = paste0("MDS投影: ","(",s,"、身分)","溫度–人次(時間段&季節)"),
        x     = "Dim1",
        y     = "Dim2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(paste0("MDS投影: ","(",s,"、身分)","溫度–人次(時間段&季節)"))
  }
}

#Percip_Authority
{
  {
    authority_types <- list(
      Bus = c("NewTaipei", "Taipei", "Keelung", "Taoyuan"),
      MRT = c("TRTC", "NTMC")
    )
    
    authority_labels <- c(
      Bus = "公車",
      MRT = "捷運"
    )
    conditions_grid <- CJ(
      season = seasons_vec,
      weekday_group_name = names(weekdays_list),
      timeperiod = timeperiods_vec,
      holder_name = names(authority_labels)
    )
    conditions_grid[, label := paste(season, weekday_group_name, timeperiod, holder_name, sep = "_")]
    
    all_bins <- get_all_temp_bins(
      weather_data    = dt_filtered,
      transport_data  = combined_dt,
      seasons_vec     = seasons_vec,
      weekdays_list   = weekdays_list,
      timeperiods_vec = timeperiods_vec,
      holder_col_name = "Authority",
      holder_types    = authority_types,
      holder_labels   = authority_labels,
      weathervar      = "precipitation_mm"
    )
    
    aligned_mat <- build_aligned_matrix(
      weather_data    = dt_filtered,
      transport_data  = combined_dt,
      conditions_grid = conditions_grid,
      all_temp_bins   = all_bins,
      holder_col_name = "Authority",
      weekdays_list   = weekdays_list,
      holder_types    = authority_types,
      weathervar      = "precipitation_mm"
    )
    
    dist_mat <- compute_wasserstein_matrix(
      aligned_matrix = aligned_mat,
      p               = 2      # 對應 wasserstein_p = 2
    )
    
    mds_out <- run_mds(
      dist_matrix = dist_mat,
      dims        = 2      # 對應 mds_dims = 2
    )
    
    mds_analysis_output <- list(
      mds_data            = cbind(
        conditions_grid[, .(
          Label          = label,
          Season         = season,
          Weekday_Group  = weekday_group_name,
          Time_Period    = timeperiod,
          Holder_Type    = holder_name
        )],
        mds_out$coords
      ),
      wasserstein_matrix  = dist_mat,
      smacof_object       = mds_out$model
    )
    saveRDS(mds_analysis_output,
            file="E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_降雨量_交通工具).rds")
  }
  mds_analysis_output <- readRDS("E:/brain/解壓縮data/資料處理/2024/MDS投影/2024公車&捷運(時間_降雨量_交通工具).rds")
  mds_model <- mds_analysis_output$smacof_object
  print(mds_model)
  
  mds_df <- mds_analysis_output$mds_data
  mds_df$Time_Period <- factor(
    mds_df$Time_Period,
    levels = c("凌晨(0-5)", "早上(6-11)", "下午(12-17)", "晚上(20-23)")
  )
  
  mds_df$Holder_Type <- factor(
    mds_df$Holder_Type,
    levels = c("Bus", "MRT")
  )
  
  names(mds_df)
  #完整圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "持卡身分",
        breaks = levels(mds_df$Holder_Type),
        labels = c("公車", "捷運"),
        values = c(16, 17)
      ) +
      labs(
        title = "MDS投影: 溫度–人次 (時間段＆交通工具)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
  }
  #分平日周末圖
  {
    ggplot(mds_df, aes(x = Dim1, y = Dim2,
                       color = Time_Period,
                       shape = Holder_Type)) +
      facet_wrap(~ Weekday_Group, scales = "free") +
      geom_point(size = 3) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(mds_df$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "交通工具",
        breaks = levels(mds_df$Holder_Type),
        labels = c("公車", "捷運"),
        values = c(16, 17)
      ) +
      labs(
        title = " MDS投影: 溫度–人次(平日假日&時間段&身分)",
        x     = "Dimension 1",
        y     = "Dimension 2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
  }
  
  #分平假日MDS
  {
    s="假日"
    df_s <- subset(mds_df, Weekday_Group == s)
    df_s$Season <- factor(df_s$Season,
                          levels = c("春季","夏季","秋季","冬季"))
    df_s$Time_Period <- factor(df_s$Time_Period,
                               levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
    df_s$Holder_Type <- factor(
      df_s$Holder_Type,
      levels = c("A", "B", "C_star"),
      labels = c("普通", "學生", "優待")
    )
    
    names(df_s)
    ggplot(df_s, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Season)) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(df_s$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "季節",
        breaks = levels(df_s$Season),
        values = c(16, 17, 15, 18)
      ) +
      facet_wrap(~ Holder_Type, scales = "free") +
      geom_point(size = 3) +
      labs(
        title = paste0("MDS投影: ","(",s,"、身分)","溫度–人次(時間段&季節)"),
        x     = "Dim1",
        y     = "Dim2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(paste0("MDS投影: ","(",s,"、身分)","溫度–人次(時間段&季節)"))
  }
  
  #分季節MDS
  {
    s="假日"
    df_s <- subset(mds_df, Weekday_Group == s)
    df_s$Season <- factor(df_s$Season,
                          levels = c("春季","夏季","秋季","冬季"))
    df_s$Time_Period <- factor(df_s$Time_Period,
                               levels = c("凌晨(0-5)","早上(6-11)","下午(12-17)","晚上(20-23)"))
    df_s$Holder_Type <- factor(
      df_s$Holder_Type,
      levels = c("A", "B", "C_star"),
      labels = c("普通", "學生", "優待")
    )
    
    names(df_s)
    ggplot(df_s, aes(x = Dim1, y = Dim2,
                     color = Time_Period,
                     shape = Holder_Type)) +
      scale_color_manual(
        name   = "時間段",                 
        breaks = levels(df_s$Time_Period),  
        values = c("#66C2A5", "#FC8D62", "#8DA0CB", "#E78AC3")  
      ) +
      scale_shape_manual(
        name   = "身分",
        breaks = levels(df_s$Holder_Type),
        values = c(16, 17, 15)
      ) +
      facet_wrap(~ Season, scales = "free") +
      geom_point(size = 3) +
      labs(
        title = paste0("MDS投影: ","(",s,"、季節)","溫度–人次(時間段&身分)"),
        x     = "Dim1",
        y     = "Dim2"
      ) +
      theme_minimal(base_size = 14) +
      theme(
        legend.position = "right",
        plot.title      = element_text(hjust = 0.5)
      )
    print(paste0("MDS投影 ","(",s,"、季節)","溫度–人次(時間段&身分)"))
  }
}
temp_vartype(dt_filtered,combined_dt,"HolderType","A","(普通身分)","temperature_c",
             "溫度","溫度 (°C)","E:/brain/解壓縮data/資料視覺化/2024/溫度",
             "春季",c("星期一","星期二","星期三","星期四","星期五"),"凌晨(0-5)")
temp_vartype(dt_filtered,combined_dt,"HolderType","B","(學生身分)","temperature_c",
             "溫度","溫度 (°C)","E:/brain/解壓縮data/資料視覺化/2024/溫度")
temp_vartype(dt_filtered,combined_dt,"HolderType","C01","(敬老優待)","temperature_c",
             "溫度","溫度 (°C)","E:/brain/解壓縮data/資料視覺化/2024/溫度")
temp_vartype(dt_filtered,combined_dt,"HolderType","C02","(愛心優待)","temperature_c",
             "溫度","溫度 (°C)","E:/brain/解壓縮data/資料視覺化/2024/溫度")
temp_vartype(dt_filtered,combined_dt,"HolderType","C09","(其他優待)","temperature_c",
             "溫度","溫度 (°C)","E:/brain/解壓縮data/資料視覺化/2024/溫度")
temp_vartype(dt_filtered,combined_dt,"TicketType","1","(單程票)","temperature_c",
             "溫度","溫度 (°C)","E:/brain/解壓縮data/資料視覺化/2024/溫度")
temp_vartype(dt_filtered,combined_dt,"TicketType","4","(月票)","temperature_c",
             "溫度","溫度 (°C)","E:/brain/解壓縮data/資料視覺化/2024/溫度")
temp_vartype(dt_filtered,combined_dt,"abs_movement_level","0","(移動0層級)","temperature_c",
             "溫度","溫度 (°C)","E:/brain/解壓縮data/資料視覺化/2024/溫度")
temp_vartype(dt_filtered,combined_dt,"abs_movement_level","1","(移動1層級)","temperature_c",
             "溫度","溫度 (°C)","E:/brain/解壓縮data/資料視覺化/2024/溫度")
temp_vartype(dt_filtered,combined_dt,"abs_movement_level","2","(移動2層級)","temperature_c",
             "溫度","溫度 (°C)","E:/brain/解壓縮data/資料視覺化/2024/溫度")
temp_vartype(dt_filtered,combined_dt,"abs_movement_level","3","(移動3層級)","temperature_c",
             "溫度","溫度 (°C)","E:/brain/解壓縮data/資料視覺化/2024/溫度")
table(combined_dt$movement_level)
names(combined_dt)

#雨量
names(dt_filtered)
freq_dt <- dt_filtered[, .N, by = .(precipitation_mm)][order(precipitation_mm)]
ggplot(dt_filtered, aes(x = precipitation_mm)) +
  geom_histogram(bins = 30, fill = "#D55E00", color = "white") +
  labs(title = "每小時北北基桃天氣站降雨直方圖",
       x     = "降雨量 (mm)",
       y     = "數量") +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))

combined_dt <- as.data.table(combined_dt)
binwidth <- 5
combined_dt[, temp_bin := floor(precipitation_mm / binwidth) * binwidth]
options(datatable.optimize = 1L) 
hist_dt <- combined_dt[, .(count = .N), by = temp_bin][order(temp_bin)]
ggplot(hist_dt, aes(x = temp_bin, y = count)) +
  geom_col(fill = "skyblue", color = "white") +
  labs(title = "每小時搭乘人次與降雨量直方圖",
       x     = "降雨量 (mm)",
       y     = "數量") +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))

dt_filtered[ , precip_bin := floor(precipitation_mm/5)*5 ]
combined_dt[   , precip_bin := floor(precipitation_mm/5)*5 ]
freq_weather   <- dt_filtered [,        .N, by = .(temp_bin = precip_bin)][]
setnames(freq_weather,   "N", "count_weather")
freq_transport <- combined_dt[, .N, by = .(temp_bin = precip_bin)][]
setnames(freq_transport, "N", "count_transport")
freq_merged <- merge(freq_transport, freq_weather, by = "temp_bin", all = TRUE)
freq_merged[, ratio := count_transport / count_weather]
ggplot(freq_merged, aes(temp_bin, ratio)) +
  geom_col(fill = "#56B4E9") +
  labs(title = "降雨量與平均搭乘人次直方圖",
       x = "降雨量 (mm)", y = "搭乘人次／測量次數 比值") +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))
percip_vartype <- function(weather_data,transport_data,
                         variable,typecode,titlename,
                         weathervar,weathervartitle,
                         xlabname,out_dir){
  transport_data_filtered <- transport_data[get(variable) == typecode]
  freq_weather   <- weather_data [,        .N, by = .(temp_bin = floor((get(weathervar))/5)*5)][]
  setnames(freq_weather,   "N", "count_weather")
  freq_transport <- transport_data_filtered[, .N, by = .(temp_bin = floor((get(weathervar))/5)*5)][]
  setnames(freq_transport, "N", "count_transport")
  freq_merged <- merge(freq_transport, freq_weather, by = "temp_bin", all = TRUE)
  freq_merged[, ratio := count_transport / count_weather]
  cat("繪圖中...")
  p <- ggplot(freq_merged, aes(temp_bin, ratio)) +
    geom_col(fill = "#56B4E9") +
    labs(title = paste0(titlename,weathervartitle,"與平均搭乘人次長條圖","\n"),
         x = xlabname, y = "搭乘人次／測量次數 比值") +
    theme_minimal(base_size = 14) +
    theme(plot.title = element_text(hjust = 0.5, face = "bold"))
  print(p)
  cat(paste0(titlename,weathervartitle,"與平均搭乘人次長條圖","\n"))
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  out_file <- file.path(out_dir, paste0(titlename,weathervartitle,"與平均搭乘人次長條圖.png"))
  png(filename = out_file,
      width    = 675,    
      height   = 675,    
      units    = "px",   
      res      = 100)   
  print(p)
  dev.off()
}
percip_vartype(dt_filtered,combined_dt,"HolderType","A","(普通身分)","precipitation_mm",
               "降雨量","降雨量 (mm)","E:/brain/解壓縮data/資料視覺化/2024/降雨量")
percip_vartype(dt_filtered,combined_dt,"HolderType","B","(學生身分)","precipitation_mm",
               "降雨量","降雨量 (mm)","E:/brain/解壓縮data/資料視覺化/2024/降雨量")
percip_vartype(dt_filtered,combined_dt,"HolderType","C01","(敬老優待)","precipitation_mm",
               "降雨量","降雨量 (mm)","E:/brain/解壓縮data/資料視覺化/2024/降雨量")
percip_vartype(dt_filtered,combined_dt,"HolderType","C02","(愛心優待)","precipitation_mm",
               "降雨量","降雨量 (mm)","E:/brain/解壓縮data/資料視覺化/2024/降雨量")
percip_vartype(dt_filtered,combined_dt,"HolderType","C09","(其他優待)","precipitation_mm",
               "降雨量","降雨量 (mm)","E:/brain/解壓縮data/資料視覺化/2024/降雨量")
percip_vartype(dt_filtered,combined_dt,"TicketType","1","(單程票)","precipitation_mm",
               "降雨量","降雨量 (mm)","E:/brain/解壓縮data/資料視覺化/2024/降雨量")
percip_vartype(dt_filtered,combined_dt,"TicketType","4","(月票)","precipitation_mm",
               "降雨量","降雨量 (mm)","E:/brain/解壓縮data/資料視覺化/2024/降雨量")
percip_vartype(dt_filtered,combined_dt,"movement_level","0","(移動0層級)","precipitation_mm",
               "降雨量","降雨量 (mm)","E:/brain/解壓縮data/資料視覺化/2024/降雨量")
percip_vartype(dt_filtered,combined_dt,"movement_level","1","(移動1層級)","precipitation_mm",
               "降雨量","降雨量 (mm)","E:/brain/解壓縮data/資料視覺化/2024/降雨量")
percip_vartype(dt_filtered,combined_dt,"movement_level","2","(移動2層級)","precipitation_mm",
               "降雨量","降雨量 (mm)","E:/brain/解壓縮data/資料視覺化/2024/降雨量")
percip_vartype(dt_filtered,combined_dt,"movement_level","3","(移動3層級)","precipitation_mm",
               "降雨量","降雨量 (mm)","E:/brain/解壓縮data/資料視覺化/2024/降雨量")

#紫外線
names(dt_filtered)
ggplot(dt_filtered, aes(x = uv_index)) +
  geom_histogram(bins = 30, fill = "#D55E00", color = "white") +
  labs(title = "每小時北北基桃天氣站紫外線指數直方圖",
       x     = "紫外線指數 (UVI)",
       y     = "數量") +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))

ggplot(combined_dt, aes(x = uv_index)) +
  geom_histogram(bins = 30, fill = "skyblue", color = "white") +
  labs(title = "每小時搭乘人次與紫外線指數直方圖",
       x     = "紫外線指數 (UVI)",
       y     = "數量") +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))

freq_weather   <- dt_filtered [,        .N, by = .(temp_bin = round(uv_index))][]
setnames(freq_weather,   "N", "count_weather")
freq_transport <- combined_dt[, .N, by = .(temp_bin = round(uv_index))][]
setnames(freq_transport, "N", "count_transport")
freq_merged <- merge(freq_transport, freq_weather, by = "temp_bin", all = TRUE)
freq_merged[, ratio := count_transport / count_weather]
ggplot(freq_merged, aes(temp_bin, ratio)) +
  geom_col(fill = "#56B4E9") +
  labs(title = "紫外線指數與平均搭乘人次長條圖",
       x = "紫外線指數 (UVI)", y = "搭乘人次／測量次數 比值") +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))
temp_vartype(dt_filtered,combined_dt,"HolderType","A","(普通身分)","uv_index","紫外線指數","紫外線指數 (UVI)")
temp_vartype(dt_filtered,combined_dt,"HolderType","B","(學生身分)","uv_index","紫外線指數","紫外線指數 (UVI)")
temp_vartype(dt_filtered,combined_dt,"HolderType","C01","(敬老優待)","uv_index","紫外線指數","紫外線指數 (UVI)")
temp_vartype(dt_filtered,combined_dt,"HolderType","C02","(愛心優待)","uv_index","紫外線指數","紫外線指數 (UVI)")
temp_vartype(dt_filtered,combined_dt,"HolderType","C09","(其他優待)","uv_index","紫外線指數","紫外線指數 (UVI)")
temp_vartype(dt_filtered,combined_dt,"TicketType","1","(單程票)","uv_index","紫外線指數","紫外線指數 (UVI)")
temp_vartype(dt_filtered,combined_dt,"TicketType","4","(月票)","uv_index","紫外線指數","紫外線指數 (UVI)")
temp_vartype(dt_filtered,combined_dt,"movement_level","0","(移動0層級)","uv_index","紫外線指數","紫外線指數 (UVI)")
temp_vartype(dt_filtered,combined_dt,"movement_level","1","(移動1層級)","uv_index","紫外線指數","紫外線指數 (UVI)")
temp_vartype(dt_filtered,combined_dt,"movement_level","2","(移動2層級)","uv_index","紫外線指數","紫外線指數 (UVI)")
temp_vartype(dt_filtered,combined_dt,"movement_level","3","(移動3層級)","uv_index","紫外線指數","紫外線指數 (UVI)")
names(combined_dt)
}