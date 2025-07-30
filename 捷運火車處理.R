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
  library(progress)
}

fastcheck <- function(df,target_year){
    if (!inherits(df, "data.table")) {
      warning("Input 'df' is not a data.table. Converting it to data.table for processing.")
      df <- as.data.table(df)
    } 
    
    
    na_counts <- colSums(is.na(df))
    cat("每列的 NA 數量:\n")
    print(na_counts) 
    cat("\n")
    
    Entry_exit_same <- NA 
    
    required_cols_for_check <- c("EntryStationID", "ExitStationID", "EntryStationName", "ExitStationName")
    if (!all(required_cols_for_check %in% names(df))) {
      warning(paste0("Not all required columns (", paste(required_cols_for_check, collapse = ", "), ") are present for 'Entry==Exit' check. Skipping this check."))
    } else {
      Entry_exit_same <- df[EntryStationID == ExitStationID | EntryStationName == ExitStationName, .N]
      
      cat("EntryStationID/Name 與 ExitStationID/Name 相同的數量:\n")
      print(Entry_exit_same)
      cat("\n")
    }
    
    cat("所有欄位名稱:\n")
    print(names(df)) 
    cat("\n")
    
    cat("資料框的前幾行 (head):\n")
    print(head(df)) 
    cat("\n")
    
    cat("資料非當年度的資料:\n")
    start_of_target_year <- ymd(paste0(target_year, "-01-01"), tz = "UTC") - days(1)
    end_of_target_year <- ymd(paste0(target_year, "-12-31"), tz = "UTC") + days(1)
    
    lower_bound <- start_of_target_year 
    upper_bound <- end_of_target_year 
    original_rows <- nrow(df)
    df <- df[
      EntryTime < lower_bound | EntryTime > upper_bound |
      ExitTime < lower_bound | ExitTime > upper_bound
    ]
    filtered_rows <- nrow(df)
    print(head(df)) 
    cat("\n")
    
    cat("資料非當年度的資料(筆數):\n")
    print(filtered_rows)
    cat("\n")
}
fstfastcheck <- function(file,target_year,keycheck=TRUE,is_mrt_rail=TRUE){
  cat("Names:")
  print(names(fst(file)))
  cat("\n")
  
  cat("Head:")
  print(head(fst(file)))
  cat("\n")
  
  if(keycheck==TRUE){
    if(is_mrt_rail==TRUE){
      key <- c("EntryStationID","EntryStationName","EntryTime",
               "ExitStationID","ExitStationName","ExitTime")
    }else{
      key <- c("BoardingStopUID","BoardingStopName","BoardingTime",
               "DeboardingStopUID","DeboardingStopName","DeboardingTime")
    }
    
    df <- read_fst(file,columns=key,as.data.table=TRUE)
  }else{
    df <- read_fst(file,as.data.table=TRUE)
  }
  if (!inherits(df, "data.table")) {
    warning("Input 'df' is not a data.table. Converting it to data.table for processing.")
    df <- as.data.table(df)
  } 
  
  
  na_counts <- colSums(is.na(df))
  cat("每列的 NA 數量:\n")
  print(na_counts) 
  cat("\n")
  
  Entry_exit_same <- NA 
  
  if(is_mrt_rail==TRUE){
    required_cols_for_check <- c("EntryStationID", "ExitStationID", "EntryStationName", "ExitStationName")
    if (!all(required_cols_for_check %in% names(df))) {
      warning(paste0("Not all required columns (", paste(required_cols_for_check, collapse = ", "), ") are present for 'Entry==Exit' check. Skipping this check."))
    } else {
      Entry_exit_same <- df[EntryStationID == ExitStationID | EntryStationName == ExitStationName, .N]
    }
  }else{
    required_cols_for_check <- c("BoardingStopUID", "DeboardingStopUID", "BoardingStopName", "DeboardingStopName")
    if (!all(required_cols_for_check %in% names(df))) {
      warning(paste0("Not all required columns (", paste(required_cols_for_check, collapse = ", "), ") are present for 'Entry==Exit' check. Skipping this check."))
    } else {
      Entry_exit_same <- df[BoardingStopUID == DeboardingStopUID | BoardingStopName == DeboardingStopName, .N]
    }
      cat("EntryStationID/Name 與 ExitStationID/Name 相同的數量:\n")
      print(Entry_exit_same)
      cat("\n")
  }
  
  
  cat("所有欄位名稱:\n")
  print(names(df)) 
  cat("\n")
  
  cat("資料框的前幾行 (head):\n")
  print(head(df)) 
  cat("\n")
  
  cat("資料非當年度的資料:\n")
  start_of_target_year <- ymd(paste0(target_year, "-01-01"), tz = "UTC") - days(1)
  end_of_target_year <- ymd(paste0(target_year, "-12-31"), tz = "UTC") + days(1)
  
  lower_bound <- start_of_target_year 
  upper_bound <- end_of_target_year 
  original_rows <- nrow(df)
  if(is_mrt_rail==TRUE){
    df <- df[
      EntryTime < lower_bound | EntryTime > upper_bound |
        ExitTime < lower_bound | ExitTime > upper_bound
    ]
  }else{
    df <- df[
      BoardingTime < lower_bound | BoardingTime > upper_bound |
        DeboardingTime < lower_bound | DeboardingTime > upper_bound
    ]
  }
  
  filtered_rows <- nrow(df)
  print(head(df)) 
  cat("\n")
  
  cat("資料非當年度的資料(筆數):\n")
  print(filtered_rows)
  cat("\n")
  gc()
}
folderfastcheck <- function(folder,target_year,keycheck_folder=TRUE,is_mrt_rail=TRUE){
  if (!dir.exists(folder)) {
    stop(paste0("錯誤：指定的資料夾路徑不存在: ", folder))
  }
  
  fst_files <- list.files(
    path = folder,
    pattern = "\\.fst$",
    full.names = TRUE,
    recursive = FALSE
  )
  
  if (length(fst_files) == 0) {
    message(paste0("在資料夾 '", folder, "' 中沒有找到任何 .fst 檔案。"))
    return(invisible(NULL)) 
  }
  
  cat(paste0("在 '", folder, "' 中找到以下 FST 檔案準備進行檢查:\n"))
  print(basename(fst_files))
  cat("\n")
  
  for (i in seq_along(fst_files)) {
    current_file <- fst_files[i]
    
    fstfastcheck(file = current_file,
                  target_year = target_year,
                  keycheck = keycheck_folder,
                 is_mrt_rail = is_mrt_rail)
    
    if (i < length(fst_files)) {
      response <- ""
      while (!tolower(response) %in% c("y", "n")) {
        response <- readline(prompt = "是否繼續檢查下一個檔案？(Y/N): ")
        if (!tolower(response) %in% c("y", "n")) {
          cat("無效輸入，請輸入 'Y' 或 'N'。\n")
        }
      }
      
      if (tolower(response) == "n") {
        cat("使用者選擇停止，終止檔案檢查。\n")
        break 
      }
    }
  }
  
  cat("\n所有 FST 檔案檢查完成或已終止。\n")
}
fstfastcheck(raildf_output_fst5_truncated,"2022")
folderfastcheck(TPCmrt_1_6_final_fst,"2022",keycheck_folder=FALSE)
folderfastcheck(TPCmrt_7_12_final_fst,"2022",keycheck_folder=FALSE)

#2024
{
  base_path <- "E:/brain/解壓縮data"
  TPCmrtdf_output_fst_1_6 <- file.path(base_path, "fst", "2024", "2024臺北市捷運1-6月.fst")
  TPCmrtdf_output_fst_1_6_2<- file.path(base_path, "資料處理", "2024", "2024臺北市捷運1-6月(去除異常值)2.fst")
  TPCmrtdf_output_fst_1_6_3_chunkv3 <- file.path(base_path, "資料處理", "2024", "2024臺北市捷運1-6月(加入鄉政市區數位發展分類與氣象站_kriging_v3)chunk")
  
  TPCmrtdf_output_fst_7_12 <- file.path(base_path, "fst", "2024", "2024臺北市捷運7-12月.fst")
  TPCmrtdf_output_fst_7_12_2<- file.path(base_path, "資料處理", "2024", "2024臺北市捷運7-12月(去除異常值)2.fst")
  TPCmrtdf_output_fst_7_12_3_chunkv3<- file.path(base_path, "資料處理", "2024", "2024臺北市捷運7-12月(加入鄉政市區數位發展分類與氣象站_kriging_v3)3chunk")
  
  NTPmrtdf_output_fst <- file.path(base_path, "fst", "2024", "2024新北市捷運.fst")
  NTPmrtdf_output_fst2 <- file.path(base_path, "資料處理", "2024", "2024新北市捷運(去除異常值)2.fst")
  NTPmrtdf_output_fst3v3 <- file.path(base_path, "資料處理", "2024", "2024新北市捷運(加入鄉政市區數位發展分類與氣象站_kriging_v3)3.fst")
  NTPmrtdf_output_fst4v3 <- file.path(base_path, "資料處理", "2024", "2024新北市捷運(加入直線距離_kriging_v3)4.fst")
  NTPmrtdf_output_fst5v3 <- file.path(base_path, "資料處理", "2024", "2024新北市捷運(發展程度移動_kriging_v3)5.fst")
  
  raildf_output_fst <- "E:/brain/解壓縮data/fst/2024/2024臺鐵.fst"
  raildf_output_fst2 <- file.path(base_path, "資料處理", "2024", "2024臺鐵(去除異常值)2.fst")
  raildf_output_fst3 <- file.path(base_path, "資料處理", "2024", "2024臺鐵(加入鄉政市區數位發展分類與氣象站)3.fst")
  raildf_output_fst4 <- file.path(base_path, "資料處理", "2024", "2024臺鐵(加入直線距離_kriging)4.fst")
  raildf_output_fst5 <- file.path(base_path, "資料處理", "2024", "2024臺鐵(發展程度移動)5.fst")
  raildf_output_fst5_truncated <- file.path(base_path, "資料處理", "2024", "2024臺鐵(發展程度移動_truncated)5.fst")
  
  TPCmrt_1_6_chunk_dir    <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運1-6月(加入鄉政市區數位發展分類與氣象站_kriging_v3)chunk"              
  TPCmrt_1_6_final_fst    <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運1-6月(發展程度移動_kriging_v3)5chunk" 
  TPCmrt_7_12_chunk_dir    <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運7-12月(加入鄉政市區數位發展分類與氣象站_kriging_v3)3chunk"              
  TPCmrt_7_12_final_fst    <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運7-12月(發展程度移動_kriging_v3)5chunk" 
  
}
#2023
{
  base_path <- "E:/brain/解壓縮data"
  TPCmrtdf_output_fst_1_6 <- file.path(base_path, "fst", "2023", "2023臺北市捷運1-6月.fst")
  TPCmrtdf_output_fst_1_6_2<- file.path(base_path, "資料處理", "2023", "2023臺北市捷運1-6月(去除異常值)2")
  TPCmrtdf_output_fst_1_6_3_chunkv3 <- file.path(base_path, "資料處理", "2023", "2023臺北市捷運1-6月(加入鄉政市區數位發展分類與氣象站_kriging_v3)chunk")
  
  TPCmrtdf_output_fst_7_12 <- file.path(base_path, "fst", "2023", "2023臺北市捷運7-12月.fst")
  TPCmrtdf_output_fst_7_12_2<- file.path(base_path, "資料處理", "2023", "2023臺北市捷運7-12月(去除異常值)2")
  TPCmrtdf_output_fst_7_12_3_chunkv3<- file.path(base_path, "資料處理", "2023", "2023臺北市捷運7-12月(加入鄉政市區數位發展分類與氣象站_kriging_v3)3chunk")
  
  NTPmrtdf_output_fst <- file.path(base_path, "fst", "2023", "2023新北市捷運.fst")
  NTPmrtdf_output_fst2 <- file.path(base_path, "資料處理", "2023", "2023新北市捷運(去除異常值)2.fst")
  NTPmrtdf_output_fst3v3 <- file.path(base_path, "資料處理", "2023", "2023新北市捷運(加入鄉政市區數位發展分類與氣象站_kriging_v3)3.fst")
  NTPmrtdf_output_fst4v3 <- file.path(base_path, "資料處理", "2023", "2023新北市捷運(加入直線距離_kriging_v3)4.fst")
  NTPmrtdf_output_fst5v3 <- file.path(base_path, "資料處理", "2023", "2023新北市捷運(發展程度移動_kriging_v3)5.fst")
  
  raildf_output_fst <- "E:/brain/解壓縮data/fst/2023/2023臺鐵.fst"
  raildf_output_fst2 <- file.path(base_path, "資料處理", "2023", "2023臺鐵(去除異常值)2.fst")
  raildf_output_fst3 <- file.path(base_path, "資料處理", "2023", "2023臺鐵(加入鄉政市區數位發展分類與氣象站)3.fst")
  raildf_output_fst4 <- file.path(base_path, "資料處理", "2023", "2023臺鐵(加入直線距離_kriging)4.fst")
  raildf_output_fst5 <- file.path(base_path, "資料處理", "2023", "2023臺鐵(發展程度移動)5.fst")
  raildf_output_fst5_truncated <- file.path(base_path, "資料處理", "2023", "2023臺鐵(發展程度移動_truncated)5.fst")
  
  TPCmrt_1_6_chunk_dir    <- "E:/brain/解壓縮data/資料處理/2023/2023臺北市捷運1-6月(加入鄉政市區數位發展分類與氣象站_kriging_v3)chunk"              
  TPCmrt_1_6_final_fst    <- "E:/brain/解壓縮data/資料處理/2023/2023臺北市捷運1-6月(發展程度移動_kriging_v3)5chunk" 
  TPCmrt_7_12_chunk_dir    <- "E:/brain/解壓縮data/資料處理/2023/2023臺北市捷運7-12月(加入鄉政市區數位發展分類與氣象站_kriging_v3)3chunk"              
  TPCmrt_7_12_final_fst    <- "E:/brain/解壓縮data/資料處理/2023/2023臺北市捷運7-12月(發展程度移動_kriging_v3)5chunk" 
}
#2022
{
  base_path <- "E:/brain/解壓縮data"
  TPCmrtdf_output_fst_1_6 <- file.path(base_path, "fst", "2022", "2022臺北市捷運1-6月.fst")
  TPCmrtdf_output_fst_1_6_2<- file.path(base_path, "資料處理", "2022", "2022臺北市捷運1-6月(去除異常值)2")
  TPCmrtdf_output_fst_1_6_3_chunkv3 <- file.path(base_path, "資料處理", "2022", "2022臺北市捷運1-6月(加入鄉政市區數位發展分類與氣象站_kriging_v3)chunk")
  
  TPCmrtdf_output_fst_7_12 <- file.path(base_path, "fst", "2022", "2022臺北市捷運7-12月.fst")
  TPCmrtdf_output_fst_7_12_2<- file.path(base_path, "資料處理", "2022", "2022臺北市捷運7-12月(去除異常值)2")
  TPCmrtdf_output_fst_7_12_3_chunkv3<- file.path(base_path, "資料處理", "2022", "2022臺北市捷運7-12月(加入鄉政市區數位發展分類與氣象站_kriging_v3)3chunk")
  
  NTPmrtdf_output_fst <- file.path(base_path, "fst", "2022", "2022新北市捷運.fst")
  NTPmrtdf_output_fst2 <- file.path(base_path, "資料處理", "2022", "2022新北市捷運(去除異常值)2.fst")
  NTPmrtdf_output_fst3v3 <- file.path(base_path, "資料處理", "2022", "2022新北市捷運(加入鄉政市區數位發展分類與氣象站_kriging_v3)3.fst")
  NTPmrtdf_output_fst4v3 <- file.path(base_path, "資料處理", "2022", "2022新北市捷運(加入直線距離_kriging_v3)4.fst")
  NTPmrtdf_output_fst5v3 <- file.path(base_path, "資料處理", "2022", "2022新北市捷運(發展程度移動_kriging_v3)5.fst")
  
  raildf_output_fst <- "E:/brain/解壓縮data/fst/2022/2022臺鐵.fst"
  raildf_output_fst2 <- file.path(base_path, "資料處理", "2022", "2022臺鐵(去除異常值)2.fst")
  raildf_output_fst3 <- file.path(base_path, "資料處理", "2022", "2022臺鐵(加入鄉政市區數位發展分類與氣象站)3.fst")
  raildf_output_fst4 <- file.path(base_path, "資料處理", "2022", "2022臺鐵(加入直線距離_kriging)4.fst")
  raildf_output_fst5 <- file.path(base_path, "資料處理", "2022", "2022臺鐵(發展程度移動)5.fst")
  raildf_output_fst5_truncated <- file.path(base_path, "資料處理", "2022", "2022臺鐵(發展程度移動_truncated)5.fst")
  
  TPCmrt_1_6_chunk_dir    <- "E:/brain/解壓縮data/資料處理/2022/2022臺北市捷運1-6月(加入鄉政市區數位發展分類與氣象站_kriging_v3)chunk"              
  TPCmrt_1_6_final_fst    <- "E:/brain/解壓縮data/資料處理/2022/2022臺北市捷運1-6月(發展程度移動_kriging_v3)5chunk" 
  TPCmrt_7_12_chunk_dir    <- "E:/brain/解壓縮data/資料處理/2022/2022臺北市捷運7-12月(加入鄉政市區數位發展分類與氣象站_kriging_v3)3chunk"              
  TPCmrt_7_12_final_fst    <- "E:/brain/解壓縮data/資料處理/2022/2022臺北市捷運7-12月(發展程度移動_kriging_v3)5chunk" 
}

mrtstop_path <- "E:/brain/解壓縮data/資料處理/交通站點資料/Kriging格點/北台灣捷運站點(加入鄉政市區數位發展分類與Kriging天氣格點).csv"
rail_path <- "E:/brain/解壓縮data/資料處理/交通站點資料/Kriging格點/全臺臺鐵站點(加入鄉鎮市區數位發展分類與Kriging天氣格點).csv"

mrt <- fread(mrtstop_path, encoding = "UTF-8")
rail <- fread(rail_path, encoding = "UTF-8")

#新增mrtstop可以適用在TPCmrt
{
  transfer_map <- data.frame(
    Original_ID = c("BL11", "G09", "G10", "O06"),
    New_ID      = c("BL11 / G12",  "G09 / O05", "G10 / R08", "O06 / R07")
  )
  rows_to_duplicate <- mrt %>%
    semi_join(transfer_map, by = c("StationID" = "Original_ID"))
  newly_created_rows <- rows_to_duplicate %>%
    left_join(transfer_map, by = c("StationID" = "Original_ID")) %>%
    mutate(StationID = New_ID) %>%
    select(-New_ID)
  mrt_final <- bind_rows(mrt, newly_created_rows)
  write_csv(mrt,"E:/brain/解壓縮data/資料處理/交通站點資料/Kriging格點/北台灣捷運站點(加入鄉政市區數位發展分類與Kriging天氣格點).csv")
  mrt$StationID <- mrt$StationID 
}

cleanproblemmrt <- function(df) {
  if (!inherits(df, "data.table")) {
    warning("Input 'df' is not a data.table. Converting it to data.table for processing.")
    df <- as.data.table(df)
  } 
  
  parse_flexible <- function(x) {
    parse_date_time(
      x,
      orders = c(
        "Ymd HMS OS", 
        "Ymd HMS",    
        "Ymd HM",     
        "Ymd"         
      ),
      tz = "Asia/Taipei" 
    )
  }
  
  print("cols_to_na_fill")
  cols_to_na_fill <- c("SubTicketType", "TransferCode")
  for (col in cols_to_na_fill) {
    if (col %in% names(df)) {
      df[, (col) := ifelse(is.na(get(col)), "-99", as.character(get(col)))]
    } else {
      warning(paste0("Column '", col, "' not found for NA replacement. Skipping."))
    }
  }
  
  print("time_cols")
  time_cols <- c("EntryTime", "ExitTime")
  for (col in time_cols) {
    if (col %in% names(df)) {
      df[, (col) := parse_flexible(get(col))]
      
      if (any(is.na(df[[col]]))) {
        warning(paste0("Parsing of '", col, "' resulted in NAs for some values. Check original data formats."))
      }
    } else {
      stop(paste0("Required time column '", col, "' not found for parsing."))
    }
  }
  
  print("df_time")
  df[, `:=`(
    Duration = ExitTime - EntryTime 
  )]
  
  filter_check_cols <- c("Duration", "EntryStationID", "ExitStationID", "EntryStationName", "ExitStationName")
  if (!all(filter_check_cols %in% names(df))) {
    stop(paste0("Not all required columns (", paste(filter_check_cols, collapse = ", "), ") for filtering are present."))
  }
  
  df <- df[
    Duration <= hours(6) &
      EntryStationID != -99 & 
      EntryStationID != ExitStationID &
      EntryStationName != ExitStationName
  ]
  
  return(df)
}
cleanproblemmrt_dt_opti <- function(df) {
  if (!is.data.table(df)) setDT(df)
  
  df[, SubTicketType := fifelse(is.na(SubTicketType), "-99", as.character(SubTicketType))]
  df[, TransferCode := fifelse(is.na(TransferCode), "-99", as.character(TransferCode))]
  
  df[, EntryTime := as.POSIXct(EntryTime, format = "%Y-%m-%d %H:%M:%S", tz = "Asia/Taipei")]
  df[, ExitTime := as.POSIXct(ExitTime, format = "%Y-%m-%d %H:%M:%S", tz = "Asia/Taipei")]
  
  df[, `:=`(
    Duration          = as.numeric(difftime(ExitTime, EntryTime, units = "hours")) 
  )]
  
  df_filtered <- df[!is.na(Duration) & Duration <= 12 &
                      EntryStationID != "-99" &
                      EntryStationID != ExitStationID &
                      EntryStationName != ExitStationName]
  
  return(df_filtered) 
}
process_fst_chunks_direct <- function(fst_file_path, 
                                      output_folder,
                                      processing_function, 
                                      chunk_size = 50000L) {
  
  if (!file.exists(fst_file_path)) {
    stop(paste("FST 檔案不存在:", fst_file_path))
  }
  if (!grepl("\\.fst$", fst_file_path, ignore.case = TRUE)) {
    warning(paste("輸入檔案可能不是 FST 檔案 (副檔名非 .fst):", fst_file_path))
  }
  if (!requireNamespace("fst", quietly = TRUE)) {
    stop("請先安裝並載入 'fst' 套件: install.packages('fst'); library(fst)")
  }
  
  if (!dir.exists(output_folder)) {
    message(paste("輸出資料夾不存在，正在建立:", output_folder))
    dir.create(output_folder, recursive = TRUE)
  }
  message(paste("準備從 FST 檔案直接分塊讀取:", fst_file_path))
  fst_meta <- metadata_fst(fst_file_path)
  n_total_rows <- fst_meta$nrOfRows
  
  if (n_total_rows == 0) {
    message("輸入 FST 檔案是空的或沒有資料列。")
    empty_structured_dt <- read_fst(fst_file_path, from = 1, to = 0, as.data.table = TRUE)
    return(processing_function(empty_structured_dt)) 
  }
  
  n_chunks <- ceiling(n_total_rows / chunk_size)
  message(paste("總行數:", n_total_rows, "| 分塊大小:", chunk_size, "| 總塊數:", n_chunks))
  
  processed_list <- vector("list", n_chunks) 
  
  for (i in 1:n_chunks) {
    message(paste0("正在處理分塊 ", i, " / ", n_chunks, "..."))
    start_row <- (i - 1) * chunk_size + 1
    end_row <- min(i * chunk_size, n_total_rows)
    
    current_chunk_data <- read_fst(fst_file_path, 
                                   from = start_row, 
                                   to = end_row, 
                                   as.data.table = TRUE) 
    
    processed_chunk <- processing_function(current_chunk_data) 
    
    output_file_path <- file.path(output_folder, paste0("_chunk_", i, ".fst"))
    
    message(paste(" -> 正在儲存至:", output_file_path))
    fst::write_fst(processed_chunk, path = output_file_path)
    rm(current_chunk_data)
    rm(processed_chunk)
    gc()
  }
}
TPCmrtdf_1_6 <- process_fst_chunks_direct(TPCmrtdf_output_fst_1_6,TPCmrtdf_output_fst_1_6_2,
                                          cleanproblemmrt_dt_opti)

TPCmrtdf_7_12 <- process_fst_chunks_direct(TPCmrtdf_output_fst_7_12,TPCmrtdf_output_fst_7_12_2,
                                           cleanproblemmrt_dt_opti)


raildf <- read.fst(raildf_output_fst ,as.data.table = TRUE)
raildf <- process_fst_chunks_direct(raildf_output_fst ,cleanproblemmrt_dt_opti)
write.fst(raildf ,raildf_output_fst2, compress=0)

NTPmrtdf <- read.fst(NTPmrtdf_output_fst ,as.data.table = TRUE)
NTPmrtdf <- cleanproblemmrt(NTPmrtdf)
write.fst(NTPmrtdf,NTPmrtdf_output_fst2)

rm(list = ls())
gc()

merge_stopuid_fast_chunk_rail <- function(inputfile, stopuid, outputpath, chunk_size = 50000) {
  library(data.table)
  library(fst)
  
  start_time <- Sys.time()
  cat("[1/9] 載入主資料...\n")
  dt <- as.data.table(inputfile)
  
  cat("[2/9] 處理 stopuid...\n")
  stopuid <- as.data.table(stopuid)
  stopuid[, StopName := as.character(StopName)]
  stopuid <- unique(stopuid, by = "StopName")
  
  cat("[3/9] 建立 stopuid_B / stopuid_D...\n")
  safe_prefix_rename <- function(dt, prefix) {
    cols <- setdiff(names(dt), "StopName")
    cols_to_rename <- cols[!startsWith(cols, prefix)]
    newnames <- paste0(prefix, cols_to_rename)
    setnames(dt, cols_to_rename, newnames)
    return(newnames)
  }
  
  stopuid_B <- copy(stopuid)
  B_cols <- safe_prefix_rename(stopuid_B, "B")
  
  stopuid_D <- copy(stopuid)
  D_cols <- safe_prefix_rename(stopuid_D, "D")
  
  total_rows <- nrow(dt)
  num_chunks <- ceiling(total_rows / chunk_size)
  cat(sprintf("[分塊處理] 總筆數: %d, 每區塊: %d 筆, 共分 %d 區塊\n", total_rows, chunk_size, num_chunks))
  
  result_list <- vector("list", num_chunks)
  total_removed <- 0
  
  for (i in 1:num_chunks) {
    start_idx <- ((i - 1) * chunk_size) + 1
    end_idx <- min(i * chunk_size, total_rows)
    dt_chunk <- dt[start_idx:end_idx]
    cat(sprintf("處理 Chunk %d ...\n", i))
    cat(nrow(dt_chunk),"\n")
    # 合併 Boarding 標籤資訊
    setkey(dt_chunk, EntryStationName)
    setkey(stopuid_B, StopName)
    dt_chunk[stopuid_B, (B_cols) := mget(paste0("i.", B_cols)), on = .(EntryStationName = StopName)]
    
    # 合併 Deboarding 標籤資訊
    setkey(dt_chunk, ExitStationName)
    setkey(stopuid_D, StopName)
    dt_chunk[stopuid_D, (D_cols) := mget(paste0("i.", D_cols)), on = .(ExitStationName = StopName)]
    cat(nrow(dt_chunk),"\n")
    
    cat("[6/9] 刪除缺失或同站資料列...\n")
    # 只刪除缺少名稱或上下站相同的列
    
    cat(nrow(dt_chunk),"\n")
    # 確認必要座標欄位存在
    required_fields <- c("BLongitude", "BLatitude", "DLongitude", "DLatitude")
    missing_fields <- setdiff(required_fields, names(dt_chunk))
    if (length(missing_fields) > 0) {
      stop(paste("缺少必要欄位：", paste(missing_fields, collapse = ", ")))
    }
    cat(nrow(dt_chunk),"\n")
    # 刪除缺少座標的列
    dt_chunk <- dt_chunk[! (is.na(BLongitude) | is.na(BLatitude) | is.na(DLongitude) | is.na(DLatitude))]
    cat(nrow(dt_chunk),"\n")
    cat("[7/9] 清除不必要欄位並移除 NA (保留 address)...\n")
    cat(nrow(dt_chunk),"\n")
    # 定義「必須 non???NA」的欄位
    keep_cols <- c("EntryStationName","ExitStationName",
                   "BLongitude","BLatitude","DLongitude","DLatitude")
    
    # 只對這些欄位檢查
    n_before <- nrow(dt_chunk)
    dt_chunk <- dt_chunk[ complete.cases(dt_chunk[, ..keep_cols]) ]
    n_after  <- nrow(dt_chunk)
    removed <- n_before - n_after
    total_removed <- total_removed + removed
    
    cat(sprintf("Chunk %d/%d：原始列數 %d，移除 %d 列 NA，剩下 %d 列\n",
                i, num_chunks, n_before, removed, n_after))
    
    result_list[[i]] <- dt_chunk
    cat(nrow(result_list))
    rm(dt_chunk)
    gc()
  }
  
  cat("[合併區塊] 正在合併所有區塊...\n")
  final_dt <- rbindlist(result_list)
  cat(nrow(final_dt),"\n")
  cat(sprintf("總共移除缺失資料列數：%d\n", total_removed))
  cat(sprintf("[9/9] 寫出結果至 %s ...\n", outputpath))
  write_fst(as.data.frame(final_dt), outputpath)
  
  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  cat(sprintf("完成！總耗時：%s 秒。\n", elapsed))
  return(final_dt)
}
raildf <- read_fst(raildf_output_fst2,
                   columns=c("Authority","HolderType","TicketType",
                             "SubTicketType","EntryStationName","EntryStationID",
                             "EntryTime","ExitStationName","ExitStationID","ExitTime","TransferCode"), 
                   as.data.table = TRUE)
merge_stopuid_fast_chunk_rail(raildf,railstop,raildf_output_fst3)

merge_stopuid_fast_chunk_dropsamestopname3 <- function(inputfile, stopuid, outputpath, chunk_size = 10000000) {
  library(data.table)
  library(fst)
  
  start_time <- Sys.time()
  cat("[1/9] 載入主資料...\n")
  dt <- as.data.table(inputfile)
  
  cat("[2/9] 處理 stopuid...\n")
  stopuid <- as.data.table(stopuid)
  stopuid[, StationID  := as.character(StationID)]
  stopuid <- unique(stopuid, by = "StationID")
  
  cat("[3/9] 建立 stopuid_B / stopuid_D...\n")
  safe_prefix_rename <- function(dt, prefix) {
    cols <- setdiff(names(dt), "StationID")
    cols_to_rename <- cols[!startsWith(cols, prefix)]
    newnames <- paste0(prefix, cols_to_rename)
    setnames(dt, cols_to_rename, newnames)
    return(newnames)
  }
  
  stopuid_B <- copy(stopuid)
  B_cols <- safe_prefix_rename(stopuid_B, "B")
  
  stopuid_D <- copy(stopuid)
  D_cols <- safe_prefix_rename(stopuid_D, "D")
  
  total_rows <- nrow(dt)
  num_chunks <- ceiling(total_rows / chunk_size)
  cat(sprintf("[分塊處理] 總筆數: %d, 每區塊: %d 筆, 共分 %d 區塊\n", total_rows, chunk_size, num_chunks))
  
  result_list <- vector("list", num_chunks)
  total_removed <- 0
  
  for (i in 1:num_chunks) {
    start_idx <- ((i - 1) * chunk_size) + 1
    end_idx <- min(i * chunk_size, total_rows)
    dt_chunk <- dt[start_idx:end_idx]
    cat(sprintf("處理 Chunk %d ...\n", i))
    cat(nrow(dt_chunk),"\n")
    # 合併 Boarding 標籤資訊
    setkey(dt_chunk, EntryStationID)
    setkey(stopuid_B, StationID)
    dt_chunk[stopuid_B, (B_cols) := mget(paste0("i.", B_cols)), on = .(EntryStationID = StationID)]
    
    # 合併 Deboarding 標籤資訊
    setkey(dt_chunk, ExitStationID)
    setkey(stopuid_D, StationID)
    dt_chunk[stopuid_D, (D_cols) := mget(paste0("i.", D_cols)), on = .(ExitStationID = StationID)]
    cat(nrow(dt_chunk),"\n")
    # 如果標籤與現有名稱不符，使用標籤取代名稱
    dt_chunk[(BStationNameCh != EntryStationName | EntryStationName=="")  & !is.na(BStationNameCh), EntryStationName := BStationNameCh]
    dt_chunk[(DStationNameCh != ExitStationName | ExitStationName=="") & !is.na(DStationNameCh), ExitStationName := DStationNameCh]
    cat(nrow(dt_chunk),"\n")
    # 將 NA 或空字串轉回空字串，保留 address 欄位
    dt_chunk[, EntryStationName   := ifelse(is.na(EntryStationName)   | EntryStationName == "", "", EntryStationName)]
    dt_chunk[, ExitStationName := ifelse(is.na(ExitStationName) | ExitStationName == "", "", ExitStationName)]
    cat(nrow(dt_chunk),"\n")
    cat("[6/9] 刪除缺失或同站資料列...\n")
    # 只刪除缺少名稱或上下站相同的列
    dt_chunk <- dt_chunk[ !(EntryStationName == ExitStationName)]
    cat(nrow(dt_chunk),"\n")
    # 確認必要座標欄位存在
    required_fields <- c("BLongitude", "BLatitude", "DLongitude", "DLatitude")
    missing_fields <- setdiff(required_fields, names(dt_chunk))
    if (length(missing_fields) > 0) {
      stop(paste("缺少必要欄位：", paste(missing_fields, collapse = ", ")))
    }
    cat(nrow(dt_chunk),"\n")
    # 刪除缺少座標的列
    dt_chunk <- dt_chunk[! (is.na(BLongitude) | is.na(BLatitude) | is.na(DLongitude) | is.na(DLatitude))]
    cat(nrow(dt_chunk),"\n")
    cat("[7/9] 清除不必要欄位並移除 NA (保留 address)...\n")
    dt_chunk[, c("BStationNameCh","DStationNameCh") := NULL]
    cat(nrow(dt_chunk),"\n")
    # 定義「必須 non???NA」的欄位
    keep_cols <- c("EntryStationName","ExitStationName",
                   "BLongitude","BLatitude","DLongitude","DLatitude")
    
    # 只對這些欄位檢查
    n_before <- nrow(dt_chunk)
    dt_chunk <- dt_chunk[ complete.cases(dt_chunk[, ..keep_cols]) ]
    n_after  <- nrow(dt_chunk)
    removed <- n_before - n_after
    total_removed <- total_removed + removed
    
    cat(sprintf("Chunk %d/%d：原始列數 %d，移除 %d 列 NA，剩下 %d 列\n",
                i, num_chunks, n_before, removed, n_after))
    
    result_list[[i]] <- dt_chunk
    cat(nrow(result_list))
    rm(dt_chunk)
    gc()
  }
  
  cat("[合併區塊] 正在合併所有區塊...\n")
  final_dt <- rbindlist(result_list)
  cat(nrow(final_dt),"\n")
  cat(sprintf("總共移除缺失資料列數：%d\n", total_removed))
  cat(sprintf("[9/9] 寫出結果至 %s ...\n", outputpath))
  write_fst(as.data.frame(final_dt), outputpath)
  
  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  cat(sprintf("完成！總耗時：%s 秒。\n", elapsed))
  return(final_dt)
}
NTPmrtdf <- setDT(read.fst(NTPmrtdf_output_fst2))
merge_stopuid_fast_chunk_dropsamestopname3(NTPmrtdf,mrt,NTPmrtdf_output_fst3v3)

mega_preprocess_fst <- function(fst_dir_path,
                                stopuid_path,
                                out_dir,
                                chunk_size = 10000000,
                                compress   = 60) {
  library(data.table)
  library(fst)
  library(fs)
  
  message("[1] 讀取 stopuid …")
  stopuid <- fread(stopuid_path, colClasses = list(character = "StationID"),encoding="UTF-8")
  setkey(stopuid, StationID)
  
  stopuid_B <- copy(stopuid)
  setnames(stopuid_B,
           old = setdiff(names(stopuid_B), "StationID"),
           new = paste0("B", setdiff(names(stopuid_B), "StationID")))
  
  stopuid_D <- copy(stopuid)
  setnames(stopuid_D,
           old = setdiff(names(stopuid_D), "StationID"),
           new = paste0("D", setdiff(names(stopuid_D), "StationID")))
  
  files_to_process <- list.files(path = fst_dir_path,
                                 pattern = "\\.fst$", 
                                 full.names = TRUE,  
                                 recursive = TRUE)
  files_to_process <- mixedsort(files_to_process)
  message(sprintf("[2] 找到 %d 個 .fst 檔案準備處理。", length(files_to_process)))
  dir_create(out_dir, recurse = TRUE)
  
  for (i in seq_along(files_to_process)) {
    
    current_file_path <- files_to_process[i]
    original_filename <- basename(current_file_path)
    
    message(sprintf("\n--- (%d/%d) 開始處理檔案: %s ---", i, length(files_to_process), original_filename))
    
    dt <- read_fst(current_file_path, as.data.table = TRUE)
    
    dt[stopuid_B, on = .(EntryStationID = StationID),
       names(stopuid_B)[-1] := mget(paste0("i.", names(stopuid_B)[-1]))]
    dt[stopuid_D, on = .(ExitStationID  = StationID),
       names(stopuid_D)[-1] := mget(paste0("i.", names(stopuid_D)[-1]))]
    
    dt[(BStationNameCh != EntryStationName | EntryStationName == "") & 
         !is.na(BStationNameCh), EntryStationName := BStationNameCh]
    dt[(DStationNameCh != ExitStationName  | ExitStationName  == "") & 
         !is.na(DStationNameCh), ExitStationName  := DStationNameCh]
    
    dt <- dt[EntryStationName != ExitStationName]
    dt <- dt[!is.na(BLongitude) & !is.na(BLatitude) &
               !is.na(DLongitude) & !is.na(DLatitude)]
    
    if (nrow(dt) > 0) {
      out_file_path <- file.path(out_dir, original_filename)
      fst::write_fst(dt, out_file_path, compress = compress)
      message(sprintf(" -> 處理完成，已儲存至 %s", out_file_path))
    } else {
      message(" -> 處理完成，沒有符合條件的資料可儲存。")
    }
    
    rm(dt); gc() 
  }
  
}
mega_preprocess_fst(TPCmrtdf_output_fst_1_6_2,
                    mrtstop_path,
                    TPCmrtdf_output_fst_1_6_3_chunkv3)
mega_preprocess_fst(TPCmrtdf_output_fst_7_12_2,
                    mrtstop_path,
                    TPCmrtdf_output_fst_7_12_3_chunkv3)


NTPmrtdf <- read.fst(NTPmrtdf_output_fst3v3)
NTPmrtdf <- NTPmrtdf %>%
  mutate(Distance = distHaversine(
    cbind(BLongitude, BLatitude),  
    cbind(DLongitude, DLatitude)  
  ) / 1000)
write.fst(NTPmrtdf,NTPmrtdf_output_fst4v3)

raildf<- read.fst(raildf_output_fst3, as.data.table = TRUE)
raildf <- raildf %>%
  mutate(Distance = distHaversine(
    cbind(BLongitude, BLatitude),  
    cbind(DLongitude, DLatitude)  
  ) / 1000)
write.fst(raildf,raildf_output_fst4)

level_change <- function(df){
  df[, Distance := as.numeric(Distance)]
  df <- df[!is.na(Distance) & Distance > 0.35]
  
  level_map <- c(
    "數位發展成熟區(分群1)" = 1,
    "數位發展潛力區(分群2)" = 2,
    "數位發展起步區(分群3)" = 3,
    "數位發展萌動區(分群4)" = 4
  )
  
  df[, Bgroup := level_map[Bdevelopment_level]]
  df[, Dgroup := level_map[Ddevelopment_level]]
  
  df[, dev_movement := paste0("B", Bgroup, "_D", Dgroup)]
  df[, movement_level := Dgroup - Bgroup]
}
NTPmrtdf <- data.table(read.fst(NTPmrtdf_output_fst4v3,
                                columns=c("Authority","HolderType","TicketType","SubTicketType",
                                          "EntryStationID","EntryStationName","EntryTime",
                                          "ExitStationID","ExitStationName","ExitTime",
                                          "TransferCode","Bdevelopment_level","Ddevelopment_level",
                                          "Distance","Bweather_grid_id","Dweather_grid_id")))
NTPmrtdf <- level_change(NTPmrtdf)
write.fst(NTPmrtdf,NTPmrtdf_output_fst5v3)

raildf <- read_fst(raildf_output_fst4, as.data.table=TRUE)
raildf <- level_change(raildf)
write.fst(raildf,raildf_output_fst5)

raildf <- read_fst(raildf_output_fst5,as.data.table = TRUE)
raildf<- raildf[raildf$Bcounty_name%in%c("臺北市","新北市","基隆市","桃園市")&
                          raildf$Dcounty_name%in%c("臺北市","新北市","基隆市","桃園市")]
write_fst(raildf,raildf_output_fst5_truncated)

#一次做step4 step5
lonlatdevelop_fst <- function(chunk_dir,
                              output_dir,
                              min_dist_km = 0.35) {
  
  if (dir.exists(output_dir)) {
    unlink(output_dir, recursive = TRUE)
  }
  dir.create(output_dir, recursive = TRUE)
  
  level_map <- c(
    "數位發展成熟區(分群1)" = 1,
    "數位發展潛力區(分群2)" = 2,
    "數位發展起步區(分群3)" = 3,
    "數位發展萌動區(分群4)" = 4
  )
  
  chunk_files <- list.files(chunk_dir,
                            pattern = "\\.fst$",
                            full.names = TRUE)
  if (length(chunk_files) == 0) {
    stop("在目錄中找不到任何 .fst 檔案：", chunk_dir)
  }
  
  for (i in seq_along(chunk_files)) {
    fpath <- chunk_files[i]
    message(sprintf("[Chunk %d/%d] 處理檔案：%s",
                    i, length(chunk_files), basename(fpath)))
    
    dt <- fst::read_fst(fpath, as.data.table = TRUE)
    
    dt[, Distance := geosphere::distHaversine(
      .SD[, .(BLongitude, BLatitude)],
      .SD[, .(DLongitude, DLatitude)]
    ) / 1000]
    
    dt <- dt[!is.na(Distance) & Distance > min_dist_km]
    
    dt[, `:=`(
      Bgroup = level_map[Bdevelopment_level],
      Dgroup = level_map[Ddevelopment_level]
    )]
    
    if (anyNA(dt$Bgroup) || anyNA(dt$Dgroup)) {
      stop("映射失敗，發現未知等級（檔案：", fpath, "）\n",
           "未知類別：",
           paste(unique(c(
             dt$Bdevelopment_level[is.na(dt$Bgroup)],
             dt$Ddevelopment_level[is.na(dt$Dgroup)]
           )), collapse = ", "))
    }
    
    dt[, `:=`(
      dev_movement   = paste0("B", Bgroup, "_D", Dgroup),
      movement_level = Dgroup - Bgroup
    )]
    
    out_file <- file.path(output_dir, sprintf("chunk_%03d.fst", i))
    fst::write_fst(dt, out_file, compress = 0)
    
    rm(dt)
    gc()
  }
  
  message("所有分塊處理完成，Dataset 存放於：", output_dir)
}
lonlatdevelop_fst(TPCmrt_1_6_chunk_dir,TPCmrt_1_6_final_fst)
lonlatdevelop_fst(TPCmrt_7_12_chunk_dir,TPCmrt_7_12_final_fst)

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
maincheck <- c("EntryStationID","ExitStationID","EntryStationName","ExitStationName","EntryTime","ExitTime")
checkTPE <- read_fst_folder(TPCmrt_1_6_final_fst,maincheck)
fastcheck(checkTPE,"2024")
