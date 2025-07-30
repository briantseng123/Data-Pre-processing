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

#資料對齊
{
  keep_cols <- c("Authority", "HolderType", "TicketType", "SubTicketType",
                 "BoardingTime","DeboardingTime","BoardingStopName","DeboardingStopName",
                 "BoardingStopUID","DeboardingStopUID","Distance","Bdevelopment_level","Ddevelopment_level",
                 "dev_movement","movement_level","Bweather_grid_id","Dweather_grid_id","TransferCode")
  hourtrans <- function(df, target_timezone = "Asia/Taipei"){
    tz_diff_hours <- as.numeric(difftime(
      force_tz(as.POSIXct("2000-01-01 00:00:00", tz = "UTC"), tzone = target_timezone),
      as.POSIXct("2000-01-01 00:00:00", tz = "UTC"),
      units = "hours"
    ))
    df[, BoardingTime := BoardingTime + hours(tz_diff_hours)]
    df[, DeboardingTime := DeboardingTime + hours(tz_diff_hours)]
    return(df)
  }
  hourcheck <- function(df){
    df <- as.data.table(df)
    hourly_counts <- df[, .(Frequency = .N), by = .(Hour = hour(BoardingTime))]
    
    all_hours <- data.table(Hour = 0:23)
    hourly_counts <- merge(all_hours, hourly_counts, by = "Hour", all.x = TRUE)
    hourly_counts[is.na(Frequency), Frequency := 0]
    
    ggplot(hourly_counts, aes(x = Hour, y = Frequency)) +
      geom_col(fill = "skyblue", color = "black") + 
      labs(title = paste0("Boarding Time Distribution "),
           x = paste0("Hour of Day"),
           y = "Frequency") +
      theme_minimal() +
      scale_x_continuous(breaks = seq(0, 23, by = 2)) 
  }
  read_fst_folder <- function(folder, cols, pattern = "\\.fst$") {
    files <- list.files(path = folder, pattern = pattern, full.names = TRUE)
    
    dt_list <- lapply(files, function(f) {
      cat("讀取", f, "…\n")
      df <- read_fst(f, columns = cols)
      df <- as.data.table(df)
      
      return(df)
    })
    
    cat("合併中...\n")
    combined <- rbindlist(dt_list, use.names = TRUE)
    return(combined)
  }
  mrt <- fread("E:/brain/解壓縮data/資料處理/交通站點資料/Kriging格點/北台灣捷運站點(加入鄉政市區數位發展分類與Kriging天氣格點).csv",encoding = "UTF-8")
  
  # 新北捷運
  {
    NTPmrtdf <- read_fst("E:/brain/解壓縮data/資料處理/2022/2022新北市捷運(發展程度移動_kriging_v3)5.fst",as.data.table = TRUE)
    NTPmrtdf <- read_fst("E:/brain/解壓縮data/資料處理/2023/2023新北市捷運(發展程度移動_kriging_v3)5.fst",as.data.table = TRUE)
    NTPmrtdf <- read_fst("E:/brain/解壓縮data/資料處理/2024/2024新北市捷運(發展程度移動_kriging_v3)5.fst",as.data.table = TRUE)
    setnames(NTPmrtdf,
             old = c("EntryStationID", "EntryStationName", "EntryTime",
                     "ExitStationID", "ExitStationName", "ExitTime"),
             new = c("BoardingStopUID", "BoardingStopName", "BoardingTime",
                     "DeboardingStopUID", "DeboardingStopName", "DeboardingTime"))
    NTPmrtdf <- NTPmrtdf[, .SD, .SDcols = keep_cols]
    hourcheck(NTPmrtdf)
    NTPmrtdf <- hourtrans(NTPmrtdf)
    
    write_fst(NTPmrtdf,"E:/brain/解壓縮data/資料處理/對齊後資料/2022新北捷運最終資料.fst")
    write_fst(NTPmrtdf,"E:/brain/解壓縮data/資料處理/對齊後資料/2023新北捷運最終資料.fst")
    write_fst(NTPmrtdf,"E:/brain/解壓縮data/資料處理/對齊後資料/2024新北捷運最終資料.fst")
  }
  
  # 臺北捷運
  {
    TPEmrtdf <- read_fst_folder("E:/brain/解壓縮data/資料處理/2022/2022臺北市捷運1-6月(發展程度移動_kriging_v3)5chunk","BoardingTime")
    TPEmrtdf <- read_fst_folder("E:/brain/解壓縮data/資料處理/2022/2022臺北市捷運7-12月(發展程度移動_kriging_v3)5chunk","BoardingTime")
    
    TPEmrtdf <- read_fst("E:/brain/解壓縮data/資料處理/2023/2023新北市捷運(發展程度移動_kriging_v3)5.fst",as.data.table = TRUE)
    TPEmrtdf <- read_fst("E:/brain/解壓縮data/資料處理/2024/2024新北市捷運(發展程度移動_kriging_v3)5.fst",as.data.table = TRUE)
    setnames(TPEmrtdf,
             old = c("EntryStationID", "EntryStationName", "EntryTime",
                     "ExitStationID", "ExitStationName", "ExitTime"),
             new = c("BoardingStopUID", "BoardingStopName", "BoardingTime",
                     "DeboardingStopUID", "DeboardingStopName", "DeboardingTime"))
    TPEmrtdf <- TPEmrtdf[, .SD, .SDcols = keep_cols]
    hourcheck(TPEmrtdf)
    TPEmrtdf <- hourtrans(TPEmrtdf)
    
    write_fst(TPEmrtdf,"E:/brain/解壓縮data/資料處理/對齊後資料/2022新北捷運最終資料.fst")
    write_fst(TPEmrtdf,"E:/brain/解壓縮data/資料處理/對齊後資料/2023新北捷運最終資料.fst")
    write_fst(TPEmrtdf,"E:/brain/解壓縮data/資料處理/對齊後資料/2024新北捷運最終資料.fst")
  }
  
  # 臺鐵
  {
    raildf <- read_fst("E:/brain/解壓縮data/資料處理/2022/2022臺鐵(發展程度移動_truncated)5.fst",as.data.table = TRUE)
    raildf <- read_fst("E:/brain/解壓縮data/資料處理/2023/2023臺鐵(發展程度移動_truncated)5.fst",as.data.table = TRUE)
    raildf <- read_fst("E:/brain/解壓縮data/資料處理/2024/2024臺鐵(發展程度移動_truncated)5.fst",as.data.table = TRUE)
    setnames(raildf,
             old = c("EntryStationID", "EntryStationName", "EntryTime",
                     "ExitStationID", "ExitStationName", "ExitTime"),
             new = c("BoardingStopUID", "BoardingStopName", "BoardingTime",
                     "DeboardingStopUID", "DeboardingStopName", "DeboardingTime"))
    raildf <- raildf[, .SD, .SDcols = keep_cols]
    hourcheck(raildf)
    raildf <- hourtrans(raildf)
    
    write_fst(raildf,"E:/brain/解壓縮data/資料處理/對齊後資料/2022臺鐵最終資料(北北基桃).fst")
    write_fst(raildf,"E:/brain/解壓縮data/資料處理/對齊後資料/2023臺鐵捷運最終資料(北北基桃).fst")
    write_fst(raildf,"E:/brain/解壓縮data/資料處理/對齊後資料/2024臺鐵捷運最終資料(北北基桃).fst")
  }
  
  
  
  bus2024df       <- bus2024df %>% dplyr::select(all_of(keep_cols))
  NTPmrt2024df    <- NTPmrt2024df %>% dplyr::select(all_of(keep_cols))
  
  combined_dt_path <- "E:/brain/解壓縮data/資料處理/2024/2024北北基桃公車和新北捷運(合併)1.fst"
  combined_dt_path_v2 <- "E:/brain/解壓縮data/資料處理/2024/2024北北基桃公車和新北捷運(合併_voronoi_v2)1.fst"
  write.fst(combined_dt,combined_dt_path)
  write.fst(combined_dt,combined_dt_path_v2)
  
}