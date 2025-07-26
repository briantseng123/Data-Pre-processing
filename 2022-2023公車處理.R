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
library(stringi)
library(readr)

NWTbus2020_7_12csv <- "E:/TPASS/csv/2020/新北市公車電子票證資料(TO2A)2020-07-01 ~ 2020-12-31/新北市公車電子票證資料(TO2A).csv"
NWTbus2020_7_12fst <- "E:/brain/解壓縮data/fst/2020/2020_7_12新北市公車.fst"
NWTbus2020_7_12 <- fread(NWTbus2020_7_12csv, skip = 1, header = TRUE, encoding = "UTF-8")
write_fst(NWTbus2020_7_12,NWTbus2020_7_12fst)

NWTbus2021csv <- "E:/TPASS/csv/2021/新北市公車電子票證資料(TO2A)2021-01-01 ~ 2021-12-31/新北市公車電子票證資料(TO2A).csv"
NWTbus2021fst <- "E:/brain/解壓縮data/fst/2021/2021新北市公車.fst"
NWTbus2021 <- fread(NWTbus2021csv, skip = 1, header = TRUE, encoding = "UTF-8")
write_fst(NWTbus2021,NWTbus2021fst)

NTWbus2022csv <- "E:/TPASS/csv/2022/新北市公車電子票證資料(TO2A)2022-01-01 ~ 2022-12-31/新北市公車電子票證資料(TO2A).csv"
NTWbus2022fst <- "E:/brain/解壓縮data/fst/2022/2022新北市公車.fst"
NTWbus2022 <- fread(NWTbus2022csv, skip = 1, header = TRUE, encoding = "UTF-8")
write_fst(NWTbus2022,NWTbus2022fst)

TAObus2022csv <- "E:/TPASS/csv/2022/桃園市公車電子票證資料(TO2A)2022-01-01 ~ 2022-12-31/桃園市公車電子票證資料(TO2A).csv"
TAObus2022fst <- "E:/brain/解壓縮data/fst/2022/2022桃園市公車.fst"
TAObus2022 <- fread(TAObus2022csv, skip = 1, header = TRUE, encoding = "UTF-8")
write_fst(TAObus2022,TAObus2022fst)

KEEbus2022csv <- "E:/TPASS/csv/2022/基隆市公車電子票證資料(TO1A)2022-01-01 ~ 2022-12-31/基隆市公車電子票證資料(TO1A).csv"
KEEbus2022fst <- "E:/brain/解壓縮data/fst/2022/2022基隆市公車.fst"
KEEbus2022 <- fread(KEEbus2022csv, skip = 1, header = TRUE, encoding = "UTF-8")
write_fst(KEEbus2022,KEEbus2022fst)

rm(list = ls())
gc()
TPEbus2022_1_6csv <- "E:/TPASS/csv/2022/臺北市公車電子票證資料(TO1A)20220101~20220630/臺北市公車電子票證資料(TO1A).csv"
TPEbus2022_1_6fst <- "E:/brain/解壓縮data/fst/2022/2022臺北市公車1-6月.fst"
TPEbus2022 <- fread(TPEbus2022_1_6csv, skip = 1, header = TRUE, encoding = "UTF-8")
write_fst(TPEbus2022,TPEbus2022_1_6fst)

rm(list = ls())
gc()
TPEbus2022_7_12csv <- "E:/TPASS/csv/2022/臺北市公車電子票證資料(TO1A)20220701~20221231/臺北市公車電子票證資料(TO1A).csv"
TPEbus2022_7_12fst <- "E:/brain/解壓縮data/fst/2022/2022臺北市公車7-12月.fst"
TPEbus2022 <- fread(TPEbus2022_7_12csv, skip = 1, header = TRUE, encoding = "UTF-8")
write_fst(TPEbus2022,TPEbus2022_7_12fst)

rm(list = ls())
gc()
TPEbus2023csv <- "E:/TPASS/csv/2023/臺北市公車電子票證資料(TO1A)2023-01-01 ~ 2023-12-31/臺北市公車電子票證資料(TO1A).csv"
TPEbus2023fst <- "E:/brain/解壓縮data/fst/2023/2023臺北市公車.fst"
TPEbus2023 <- fread(TPEbus2023csv, skip = 1, header = TRUE, encoding = "UTF-8")
write_fst(TPEbus2023,TPEbus2023fst)

rm(list = ls())
gc()
NTWbus2023csv <- "E:/TPASS/csv/2023/新北市公車電子票證資料(TO2A)2023-01-01 ~ 2023-12-31/新北市公車電子票證資料(TO2A).csv"
NTWbus2023fst <- "E:/brain/解壓縮data/fst/2023/2023新北市公車.fst"
NTWbus2023 <- fread(NTWbus2023csv, skip = 1, header = TRUE, encoding = "UTF-8")
write_fst(NTWbus2023,NTWbus2023fst)

rm(list = ls())
gc()
TAObus2023csv <- "E:/TPASS/csv/2023/桃園市公車電子票證資料(TO2A)2023-01-01 ~ 2023-12-31/桃園市公車電子票證資料(TO2A).csv"
TAObus2023fst <- "E:/brain/解壓縮data/fst/2023/2023桃園市公車.fst"
TAObus2023 <- fread(TAObus2023csv, skip = 1, header = TRUE, encoding = "UTF-8")
write_fst(TAObus2023,TAObus2023fst)

rm(list = ls())
gc()
KEEbus2023csv <- "E:/TPASS/csv/2023/基隆市公車電子票證資料(TO1A)2023-01-01 ~ 2023-12-31/基隆市公車電子票證資料(TO1A).csv"
KEEbus2023fst <- "E:/brain/解壓縮data/fst/2023/2023基隆市公車.fst"

KEEbus2023 <- read_csv(KEEbus2023csv, skip = 1, locale = locale(encoding = "UTF-8"))
KEEbus2023 <- fread(KEEbus2023csv, skip = 1, header = TRUE, encoding = "unknown")
write_fst(KEEbus2023,KEEbus2023fst)

col_for_read <- c("Authority","HolderType","TicketType","SubTicketType",
                  "BoardingStopUID","BoardingStopName","BoardingStopSequence","BoardingTime",
                  "DeboardingStopUID","DeboardingStopName","DeboardingStopSequence","DeboardingTime",
                  "TransferCode","IsAbnormal","ErrorCode","Result")


ALL <- fread("E:/brain/解壓縮data/資料處理/公車站點資料/站牌、站位、組站位/北北基桃站群(添加鄉政市區&發展程度)3.csv",encoding="UTF-8")
ALL <- ALL%>%
  dplyr::select(StopUID,StopName,StationID,City,StationUID,StationName,Latitude,Longitude,
                StationGroupID,development_level)
#去除上下車相同
process_bus_data <- function(fst_path, columns,year_str) {
  
  df <- read_fst(fst_path, columns = columns)
  #df <- df %>% mutate(
    #DeboardingStopName = iconv(DeboardingStopName, from = "", to = "UTF-8"),
    #DeboardingStopUID  = iconv(DeboardingStopUID,  from = "", to = "UTF-8"),
    #BoardingStopName   = iconv(BoardingStopName,   from = "", to = "UTF-8"),
    #BoardingStopUID    = iconv(BoardingStopUID,    from = "", to = "UTF-8")
  #)
  df <- df %>% 
    mutate(
      BoardingStopName   = if_else(BoardingStopName %in% c("", "0"), NA_character_, BoardingStopName),
      BoardingStopUID   = if_else(BoardingStopUID %in% c("", "0"), NA_character_, BoardingStopUID),
      DeboardingStopName   = if_else(DeboardingStopName %in% c("", "0"), NA_character_, DeboardingStopName),
      DeboardingStopUID = if_else(DeboardingStopUID %in% c("", "0"), NA_character_, DeboardingStopUID)
    )
  print(str(df))
  print(nrow(df))
  print(names(df))
  
  abnormal_rows <- df %>% filter(IsAbnormal == 1)
  print(head(abnormal_rows))
  cat("IsAbnormal == 1 的筆數：", nrow(abnormal_rows), "\n")
  
  print("-99的數量")
  summary_counts <- df %>% summarise(across(everything(), ~ sum(. == -99, na.rm = TRUE)))
  print(summary_counts)
  
  print("NULL的數量")
  summary_counts <- df %>% 
    summarise(across(everything(), ~ sum(as.character(.) == "NULL", na.rm = TRUE)))
  print(summary_counts)
  
  
  ISAB0 <- df %>% filter(IsAbnormal == 0)%>%nrow()
  cat(ISAB0, "筆資料 (ISABNORMAL==0)\n")
  
  df <- df %>% filter(IsAbnormal == 0)
  rows_to_remove <- df %>% filter(BoardingStopName == DeboardingStopName & BoardingStopUID == DeboardingStopUID)%>%nrow()
  cat("將移除", rows_to_remove, "筆資料 (BoardingStopName==DeboardingStopName 和 BoardingStopUID==DeboardingStopName)\n")
  
  year_num <- as.numeric(year_str)
  df <- df %>% 
    filter(!(BoardingStopName == DeboardingStopName & BoardingStopUID == DeboardingStopUID))
  
  rows_to_remove <- df %>%
    filter(!(year(BoardingTime)==year_num & year(DeboardingTime)==year_num))%>%nrow()
  cat("將移除", rows_to_remove, "筆資料 (BoardingTime==2023 和 DeboardingTime==2023)\n")
  
  df <- df %>%filter(year(BoardingTime)==year_num & year(DeboardingTime)==year_num)
  return(df)
  
}
process_bus_data <- function(path, columns,year_str) {
  
  df <- fread(path, skip = 1, header = TRUE, select = columns, encoding = "UTF-8") %>% as_tibble()
  #df <- df %>% mutate(
  #DeboardingStopName = iconv(DeboardingStopName, from = "", to = "UTF-8"),
  #DeboardingStopUID  = iconv(DeboardingStopUID,  from = "", to = "UTF-8"),
  #BoardingStopName   = iconv(BoardingStopName,   from = "", to = "UTF-8"),
  #BoardingStopUID    = iconv(BoardingStopUID,    from = "", to = "UTF-8")
  #)
  df <- df %>% 
    mutate(
      BoardingStopName   = if_else(BoardingStopName %in% c("", "0"), NA_character_, BoardingStopName),
      BoardingStopUID   = if_else(BoardingStopUID %in% c("", "0"), NA_character_, BoardingStopUID),
      DeboardingStopName   = if_else(DeboardingStopName %in% c("", "0"), NA_character_, DeboardingStopName),
      DeboardingStopUID = if_else(DeboardingStopUID %in% c("", "0"), NA_character_, DeboardingStopUID)
    )
  print(str(df))
  print(nrow(df))
  print(names(df))
  
  abnormal_rows <- df %>% filter(IsAbnormal == 1)
  print(head(abnormal_rows))
  cat("IsAbnormal == 1 的筆數：", nrow(abnormal_rows), "\n")
  
  print("-99的數量")
  summary_counts <- df %>% summarise(across(everything(), ~ sum(. == -99, na.rm = TRUE)))
  print(summary_counts)
  
  print("NULL的數量")
  summary_counts <- df %>% 
    summarise(across(everything(), ~ sum(as.character(.) == "NULL", na.rm = TRUE)))
  print(summary_counts)
  
  
  ISAB0 <- df %>% filter(IsAbnormal == 0)%>%nrow()
  cat(ISAB0, "筆資料 (ISABNORMAL==0)\n")
  
  df <- df %>% filter(IsAbnormal == 0)
  rows_to_remove <- df %>% filter(BoardingStopName == DeboardingStopName & BoardingStopUID == DeboardingStopUID)%>%nrow()
  cat("將移除", rows_to_remove, "筆資料 (BoardingStopName==DeboardingStopName 和 BoardingStopUID==DeboardingStopName)\n")
  
  year_num <- as.numeric(year_str)
  df <- df %>% 
    filter(!(BoardingStopName == DeboardingStopName & BoardingStopUID == DeboardingStopUID))
  
  rows_to_remove <- df %>%
    filter(!(year(BoardingTime)==year_num & year(DeboardingTime)==year_num))%>%nrow()
  cat("將移除", rows_to_remove, "筆資料 (BoardingTime==2023 和 DeboardingTime==2023)\n")
  
  df <- df %>%filter(year(BoardingTime)==year_num & year(DeboardingTime)==year_num)
  return(df)
  
}

#填補遺失值
fill_missing_stops <- function(df,df_output, verbose = TRUE) {
  #df <- df %>% mutate(
    #DeboardingStopName = iconv(DeboardingStopName, from = "", to = "UTF-8"),
    #DeboardingStopUID  = iconv(DeboardingStopUID,  from = "", to = "UTF-8"),
    #BoardingStopName   = iconv(BoardingStopName,   from = "", to = "UTF-8"),
    #BoardingStopUID    = iconv(BoardingStopUID,    from = "", to = "UTF-8")
  #)
  
  
  get_mode <- function(v) {
    v <- v[!is.na(v)]
    if(length(v) == 0) return(NA_character_)
    tab <- table(v)
    mode_val <- names(tab)[which.max(tab)]
    return(mode_val)
  }
  
  most_common_names <- df %>% 
    group_by(DeboardingStopUID) %>% 
    summarise(modeName = get_mode(DeboardingStopName), .groups = "drop")
  
  if (verbose) {
    cat("DeboardingStopUID 群組中最常出現的 DeboardingStopName:\n")
    print(most_common_names)
  }
  gc()
  df <- df %>% 
    left_join(most_common_names, by = "DeboardingStopUID") %>% 
    mutate(DeboardingStopName = coalesce(DeboardingStopName, modeName)) %>% 
    select(-modeName)
  
  most_common_uids <- df %>% 
    group_by(DeboardingStopName) %>% 
    summarise(modeUID = get_mode(DeboardingStopUID), .groups = "drop")
  
  if (verbose) {
    cat("DeboardingStopName 群組中最常出現的 DeboardingStopUID:\n")
    print(most_common_uids)
  }
  gc()
  df <- df %>% 
    left_join(most_common_uids, by = "DeboardingStopName") %>% 
    mutate(DeboardingStopUID = coalesce(DeboardingStopUID, modeUID)) %>% 
    select(-modeUID)
  
  most_common_b_uids <- df %>% 
    group_by(BoardingStopName) %>% 
    summarise(modeBUID = get_mode(BoardingStopUID), .groups = "drop")
  
  if (verbose) {
    cat("BoardingStopName 群組中最常出現的 BoardingStopUID:\n")
    print(most_common_b_uids)
  }
  gc()
  df <- df %>% 
    left_join(most_common_b_uids, by = "BoardingStopName") %>% 
    mutate(BoardingStopUID = coalesce(BoardingStopUID, modeBUID)) %>% 
    select(-modeBUID)
  
  most_common_b_names <- df %>% 
    group_by(BoardingStopUID) %>% 
    summarise(modeBName = get_mode(BoardingStopName), .groups = "drop")
  
  if (verbose) {
    cat("BoardingStopUID 群組中最常出現的 BoardingStopName:\n")
    print(most_common_b_names)
  }
  gc()
  df <- df %>% 
    left_join(most_common_b_names, by = "BoardingStopUID") %>% 
    mutate(BoardingStopName = coalesce(BoardingStopName, modeBName)) %>% 
    select(-modeBName)
  write_fst(df, df_output)
  return(df)
}

#清除遺失值
process_stop_codes <- function(df_input, df_output, drop_na_rows = TRUE, verbose = TRUE) {
  df <- read_fst(df_input)
  
  df <- df %>% mutate(
    DeboardingStopName = if_else(DeboardingStopName %in% c("-99", "NULL"), NA_character_, DeboardingStopName),
    DeboardingStopUID  = if_else(DeboardingStopUID %in% c("-99", "NULL"), NA_character_, DeboardingStopUID),
    BoardingStopName   = if_else(BoardingStopName %in% c("-99", "NULL"), NA_character_, BoardingStopName),
    BoardingStopUID    = if_else(BoardingStopUID %in% c("-99", "NULL"), NA_character_, BoardingStopUID)
  )
  
  na_counts <- sapply(df, function(x) sum(is.na(x)))
  if (verbose) {
    cat("轉換後各變數 NA 的數量：\n")
    print(na_counts)
  }
  
  if (drop_na_rows) {
    original_nrow <- nrow(df)
    cols_to_drop <- c("BoardingStopUID", "BoardingStopName", "BoardingTime",
                      "DeboardingStopName", "DeboardingStopUID", "DeboardingTime")
    
    df <- df %>% drop_na(all_of(cols_to_drop))
    
    df <- df %>% mutate(
      across(where(is.numeric), ~ replace_na(., -99)),
      across(where(is.character), ~ replace_na(., "-99"))
    )
    
    removed_nrow <- original_nrow - nrow(df)
    if (verbose) {
      cat("drop_na 後，資料框維度：", paste(dim(df), collapse = " x "), "\n")
      cat("總共移除了", removed_nrow, "筆含 NA 的觀察值。\n")
    }
  }
  
  boarding_seq <- df$BoardingStopSequence
  deboarding_seq <- df$DeboardingStopSequence
  
  b_eq_0 <- boarding_seq == 0
  b_ne_0 <- boarding_seq != 0
  d_eq_0 <- deboarding_seq == 0
  d_ne_0 <- deboarding_seq != 0
  b_eq_d <- boarding_seq == deboarding_seq
  
  df <- df %>% mutate(Label = case_when(
    b_eq_0 & d_eq_0 ~ "BSZ",
    b_ne_0 & d_eq_0 ~ "DZ",
    b_eq_0 & d_ne_0 ~ "BZ",
    b_ne_0 & d_ne_0 & b_eq_d ~ "BNZE",
    b_ne_0 & d_ne_0 & (!b_eq_d) ~ "BNZ",
    TRUE ~ "Unknown"
  ))
  
  if (verbose) {
    cat("Label 數量分佈：\n")
    print(table(df$Label))
  }
  
  name_eq <- df$BoardingStopName == df$DeboardingStopName
  uid_eq  <- df$BoardingStopUID == df$DeboardingStopUID
  
  df <- df %>% mutate(Code = case_when(
    (!name_eq) & uid_eq ~ "UIDSAME",    
    name_eq & (!uid_eq) ~ "NAMESAME",    
    name_eq & uid_eq ~ "UIDNAMESAME",     
    (!name_eq) & (!uid_eq) ~ "CHECK",     
    TRUE ~ "Unknown"
  ))
  
  if (verbose) {
    cat("Code 數量分佈：\n")
    print(table(df$Code))
  }
  
  ref <- df %>% filter(Code == "CHECK", Label == "BNZ")
  
  ref_boarding <- ref %>% select(BoardingStopUID, BoardingStopName) %>% distinct()
  ref_deboarding <- ref %>% select(DeboardingStopUID, DeboardingStopName) %>% distinct()
  
  uid_to_boarding_name <- setNames(ref_boarding$BoardingStopName, ref_boarding$BoardingStopUID)
  uid_to_deboarding_name <- setNames(ref_deboarding$DeboardingStopName, ref_deboarding$DeboardingStopUID)
  
  ref_boarding_rev <- ref %>% select(BoardingStopName, BoardingStopUID) %>% distinct()
  ref_deboarding_rev <- ref %>% select(DeboardingStopName, DeboardingStopUID) %>% distinct()
  
  name_to_boarding_uid <- setNames(ref_boarding_rev$BoardingStopUID, ref_boarding_rev$BoardingStopName)
  name_to_deboarding_uid <- setNames(ref_deboarding_rev$DeboardingStopUID, ref_deboarding_rev$DeboardingStopName)
  
  mask_namesame <- df$Code == "NAMESAME"
  df$BoardingStopName[mask_namesame] <- uid_to_boarding_name[ as.character(df$BoardingStopUID[mask_namesame]) ]
  df$DeboardingStopName[mask_namesame] <- uid_to_deboarding_name[ as.character(df$DeboardingStopUID[mask_namesame]) ]
  
  mask_uidsame <- df$Code == "UIDSAME"
  df$BoardingStopUID[mask_uidsame] <- name_to_boarding_uid[ as.character(df$BoardingStopName[mask_uidsame]) ]
  df$DeboardingStopUID[mask_uidsame] <- name_to_deboarding_uid[ as.character(df$DeboardingStopName[mask_uidsame]) ]
  
  write_fst(df, df_output)
  
  return(df)
}

#更新相同名稱站點
update_stop_codes <- function(df_input, df_output, verbose = TRUE) {
  library(dplyr)
  df <- read_fst(df_input)
  ref <- df %>% filter(Code == "CHECK", Label == "BNZ")
  
  ref_boarding <- ref %>% select(BoardingStopUID, BoardingStopName) %>% distinct()
  ref_deboarding <- ref %>% select(DeboardingStopUID, DeboardingStopName) %>% distinct()
  
  uid_to_boarding_name <- setNames(ref_boarding$BoardingStopName, ref_boarding$BoardingStopUID)
  uid_to_deboarding_name <- setNames(ref_deboarding$DeboardingStopName, ref_deboarding$DeboardingStopUID)
  
  ref_boarding_rev <- ref %>% select(BoardingStopName, BoardingStopUID) %>% distinct()
  ref_deboarding_rev <- ref %>% select(DeboardingStopName, DeboardingStopUID) %>% distinct()
  
  name_to_boarding_uid <- setNames(ref_boarding_rev$BoardingStopUID, ref_boarding_rev$BoardingStopName)
  name_to_deboarding_uid <- setNames(ref_deboarding_rev$DeboardingStopUID, ref_deboarding_rev$DeboardingStopName)
  
  df <- df %>% mutate(
    BoardingStopName = if_else(
      Code == "NAMESAME",
      uid_to_boarding_name[as.character(BoardingStopUID)],
      BoardingStopName,
      missing = BoardingStopName
    ),
    DeboardingStopName = if_else(
      Code == "NAMESAME",
      uid_to_deboarding_name[as.character(DeboardingStopUID)],
      DeboardingStopName,
      missing = DeboardingStopName
    )
  )
  
  df <- df %>% mutate(
    BoardingStopUID = if_else(
      Code == "UIDSAME",
      name_to_boarding_uid[as.character(BoardingStopName)],
      BoardingStopUID,
      missing = BoardingStopUID
    ),
    DeboardingStopUID = if_else(
      Code == "UIDSAME",
      name_to_deboarding_uid[as.character(DeboardingStopName)],
      DeboardingStopUID,
      missing = DeboardingStopUID
    )
  )
  
  if (verbose) {
    uidsame_count <- df %>% filter(Code == "UIDSAME", BoardingStopUID == DeboardingStopUID) %>% nrow()
    namesame_count <- df %>% filter(Code == "NAMESAME", BoardingStopName == DeboardingStopName) %>% nrow()
    cat("更新後：\n")
    cat("  Code 為 UIDSAME，且 BoardingStopUID == DeboardingStopUID 的筆數：", uidsame_count, "\n")
    cat("  Code 為 NAMESAME，且 BoardingStopName == DeboardingStopName 的筆數：", namesame_count, "\n")
  }
  
  n_before <- nrow(df)
  df <- df %>% filter(
    !((Code == "UIDSAME" & BoardingStopUID == DeboardingStopUID) |
        (Code == "NAMESAME" & BoardingStopName == DeboardingStopName))
  )
  n_after <- nrow(df)
  
  if (verbose) {
    cat("更新並刪除後的資料筆數：", n_after, "\n")
    cat("共刪除了", n_before - n_after, "筆符合刪除條件的資料。\n")
  }
  
  write_fst(df, df_output)
  return(df)
}

# for處理臺北公車
update_stop_codes_in_chunks <- function(df_input, df_output, verbose = TRUE, chunk_dates = NULL) {
  
  if (verbose) cat("開始讀取資料：", df_input, "\n")
  df_full <- read_fst(df_input)
  if (verbose) cat("資料讀取完成。總筆數：", nrow(df_full), "\n")
  gc() 
  
  ref <- df_full %>% filter(Code == "CHECK", Label == "BNZ")
  
  ref_boarding <- ref %>% select(BoardingStopUID, BoardingStopName) %>% distinct()
  ref_deboarding <- ref %>% select(DeboardingStopUID, DeboardingStopName) %>% distinct()
  
  uid_to_boarding_name <- setNames(ref_boarding$BoardingStopName, ref_boarding$BoardingStopUID)
  uid_to_deboarding_name <- setNames(ref_deboarding$DeboardingStopName, ref_deboarding$DeboardingStopUID)
  
  ref_boarding_rev <- ref %>% select(BoardingStopName, BoardingStopUID) %>% distinct()
  ref_deboarding_rev <- ref %>% select(DeboardingStopName, DeboardingStopUID) %>% distinct()
  
  name_to_boarding_uid <- setNames(ref_boarding_rev$BoardingStopUID, ref_boarding_rev$BoardingStopName)
  name_to_deboarding_uid <- setNames(ref_deboarding_rev$DeboardingStopUID, ref_deboarding_rev$DeboardingStopName)
  
  rm(ref, ref_boarding, ref_deboarding, ref_boarding_rev, ref_deboarding_rev)
  gc()
  
  processed_chunks_list <- list()
  
  if (is.null(chunk_dates) || length(chunk_dates) == 0) {
    chunks_to_process <- list(df_full)
    if (verbose) cat("不分段處理，將一次性處理所有資料。\n")
  } else {
    if (!"DeboardingTime" %in% names(df_full)) {
      stop("若要依 DeboardingTime 分段，資料框中必須包含 'DeboardingTime' 欄位。")
    }
    
    df_full$DMonth <- as.integer(format(df_full$DeboardingTime, "%m"))
    
    chunks_to_process <- list()
    for (i in seq_along(chunk_dates)) {
      month_range <- chunk_dates[[i]]
      if (verbose) cat("正在處理月份範圍：", paste(month_range, collapse = ", "), "\n")
      chunk_df <- df_full %>% filter(DMonth %in% month_range)
      chunks_to_process[[i]] <- chunk_df
    }
    rm(df_full)
    gc()
  }
  
  for (i in seq_along(chunks_to_process)) {
    chunk_df <- chunks_to_process[[i]]
    if (nrow(chunk_df) == 0) {
      if (verbose) cat(sprintf("第 %d 個資料塊為空，跳過。\n", i))
      next
    }
    if (verbose) cat(sprintf("開始處理第 %d 個資料塊 (筆數: %d)。\n", i, nrow(chunk_df)))
    
    chunk_df <- chunk_df %>% mutate(
      BoardingStopName = if_else(
        Code == "NAMESAME",
        uid_to_boarding_name[as.character(BoardingStopUID)],
        BoardingStopName,
        missing = BoardingStopName
      ),
      DeboardingStopName = if_else(
        Code == "NAMESAME",
        uid_to_deboarding_name[as.character(DeboardingStopUID)],
        DeboardingStopName,
        missing = DeboardingStopName
      )
    )
    
    chunk_df <- chunk_df %>% mutate(
      BoardingStopUID = if_else(
        Code == "UIDSAME",
        name_to_boarding_uid[as.character(BoardingStopName)],
        BoardingStopUID,
        missing = BoardingStopUID
      ),
      DeboardingStopUID = if_else(
        Code == "UIDSAME",
        name_to_deboarding_uid[as.character(DeboardingStopName)],
        DeboardingStopUID,
        missing = DeboardingStopUID
      )
    )
    
    n_before_chunk <- nrow(chunk_df)
    chunk_df <- chunk_df %>% filter(
      !((Code == "UIDSAME" & BoardingStopUID == DeboardingStopUID) |
          (Code == "NAMESAME" & BoardingStopName == DeboardingStopName))
    )
    n_after_chunk <- nrow(chunk_df)
    
    if (verbose) {
      cat(sprintf("第 %d 個資料塊刪除了 %d 筆資料。\n", i, n_before_chunk - n_after_chunk))
    }
    
    processed_chunks_list[[i]] <- chunk_df
    rm(chunk_df) 
    gc()
    if (verbose) cat(sprintf("第 %d 個資料塊處理完成。\n", i))
  }
  
  if (verbose) cat("合併所有處理後的資料塊。\n")
  final_df <- bind_rows(processed_chunks_list)
  if (verbose) cat("最終資料筆數：", nrow(final_df), "\n")
  gc()
  
  if (verbose) {
    uidsame_count <- final_df %>% filter(Code == "UIDSAME", BoardingStopUID == DeboardingStopUID) %>% nrow()
    namesame_count <- final_df %>% filter(Code == "NAMESAME", BoardingStopName == DeboardingStopName) %>% nrow()
    cat("最終檢查：\n")
    cat(" Code 為 UIDSAME，且 BoardingStopUID == DeboardingStopUID 的筆數：", uidsame_count, "\n")
    cat(" Code 為 NAMESAME，且 BoardingStopName == DeboardingStopName 的筆數：", namesame_count, "\n")
  }
  
  if (verbose) cat("寫入最終結果到：", df_output, "\n")
  write_fst(final_df, df_output)
  
  gc() 
  return(final_df)
}

merge_stopuid <- function(df, stopuid, startwith, outputpath) {
  library(dplyr)
  
  stopuid <- stopuid %>%
    mutate(Id = as.character(Id),
           Id = ifelse(!startsWith(Id, startwith), paste0(startwith, Id), Id)) %>%
    distinct(Id, .keep_all = TRUE)
  
  stopuid_B <- stopuid %>% rename_with(~ paste0("B", .), .cols = -Id)
  stopuid_D <- stopuid %>% rename_with(~ paste0("D", .), .cols = -Id)
  
  df <- df %>%
    left_join(stopuid_B, by = c("BoardingStopUID" = "Id")) %>%
    left_join(stopuid_D, by = c("DeboardingStopUID" = "Id"))
  
  mask <- with(df, Code == "NAMESAME" &
                 BoardingStopName == DeboardingStopName &
                 !is.na(BLabel) &
                 !is.na(DLabel) &
                 (BLabel != DLabel))
  
  boarding_df <- df %>%
    select(BoardingStopName, Blongitude, Blatitude) %>%
    rename(StopName = BoardingStopName, longitude = Blongitude, latitude = Blatitude)
  
  deboarding_df <- df %>%
    select(DeboardingStopName, Dlongitude, Dlatitude) %>%
    rename(StopName = DeboardingStopName, longitude = Dlongitude, latitude = Dlatitude)
  
  all_stops_df <- bind_rows(boarding_df, deboarding_df)
  
  mode_func <- function(x) {
    x <- x[!is.na(x)]
    if (length(x) == 0) return(NA)
    tab <- table(x)
    mode_val <- names(tab)[tab == max(tab)]
    return(as.numeric(mode_val[1]))
  }
  
  mode_df <- all_stops_df %>%
    group_by(StopName) %>%
    summarise(longitude = mode_func(longitude),
              latitude = mode_func(latitude),
              .groups = "drop")
  
  mode_dict_long <- setNames(mode_df$longitude, mode_df$StopName)
  mode_dict_lat  <- setNames(mode_df$latitude, mode_df$StopName)
  
  missing_before <- sum(is.na(df$Blongitude)) + sum(is.na(df$Blatitude)) +
    sum(is.na(df$Dlongitude)) + sum(is.na(df$Dlatitude))
  
  df <- df %>%
    mutate(Blongitude = if_else(is.na(Blongitude),
                                as.numeric(mode_dict_long[BoardingStopName]),
                                Blongitude),
           Blatitude = if_else(is.na(Blatitude),
                               as.numeric(mode_dict_lat[BoardingStopName]),
                               Blatitude),
           Dlongitude = if_else(is.na(Dlongitude),
                                as.numeric(mode_dict_long[DeboardingStopName]),
                                Dlongitude),
           Dlatitude = if_else(is.na(Dlatitude),
                               as.numeric(mode_dict_lat[DeboardingStopName]),
                               Dlatitude))
  
  missing_after <- sum(is.na(df$Blongitude)) + sum(is.na(df$Blatitude)) +
    sum(is.na(df$Dlongitude)) + sum(is.na(df$Dlatitude))
  filled_count <- missing_before - missing_after
  
  df$BoardingStopName[mask] <- df$BLabel[mask]
  df$DeboardingStopName[mask] <- df$DLabel[mask]
  
  
  cat("填補前缺失值總數：", missing_before, "\n")
  cat("成功填補缺失值數量：", filled_count, "\n")
  cat("填補後剩餘缺失值數量：", missing_after, "\n")
  df <- df %>% select(-Baddress, -Daddress, -BLabel, -DLabel)
  missing_summary <- sapply(df, function(x) sum(is.na(x)))
  missing_cols <- names(missing_summary[missing_summary > 0])
  if (length(missing_cols) > 0) {
    cat("【仍有缺失值的欄位】\n")
    print(missing_summary[missing_summary > 0])
  } else {
    cat("目前所有欄位均無缺失值。\n")
  }
  
  n_before <- nrow(df)
  df <- df %>% drop_na()
  n_after <- nrow(df)
  cat("\ndrop_na() 已移除", n_before - n_after, "列缺失資料。\n")
  
  cat("\n【最終資料集】\n")
  print(head(df))
  write_fst(df,outputpath)
  return(df)
}

merge_stopuid_fast <- function(inputfile, stopuid, startwith, outputpath) {
  start_time <- Sys.time()
  cat("[1/8] 載入主資料...\n")
  dt <-  as.data.table(inputfile)
  
  cat("[2/8] 處理 stopuid...\n")
  stopuid <- as.data.table(stopuid)
  stopuid[, Id := as.character(Id)]
  stopuid[!startsWith(Id, startwith), Id := paste0(startwith, Id)]
  stopuid <- unique(stopuid, by = "Id")
  
  cat("[3/8] 建立 stopuid_B / stopuid_D...\n")
  stopuid_B <- copy(stopuid)
  stopuid_D <- copy(stopuid)
  cols_B <- setdiff(names(stopuid_B), "Id")
  setnames(stopuid_B, cols_B, paste0("B", cols_B))
  cols_D <- setdiff(names(stopuid_D), "Id")
  setnames(stopuid_D, cols_D, paste0("D", cols_D))
  
  cat("[4/8] 合併 stopuid_B...\n")
  setkey(dt, BoardingStopUID)
  setkey(stopuid_B, Id)
  cols <- setdiff(names(stopuid_B), "Id")
  newcols <- paste0("B", cols)
  dt[stopuid_B, (newcols) := mget(paste0("i.", cols)), on = .(BoardingStopUID = Id)]
  
  cat("[5/8] 合併 stopuid_D...\n")
  setkey(dt, DeboardingStopUID)
  setkey(stopuid_D, Id)
  cols_D <- setdiff(names(stopuid_D), "Id")
  newcols_D <- paste0("D", cols_D)
  dt[stopuid_D, (newcols_D) := mget(paste0("i.", cols_D)), on = .(DeboardingStopUID = Id)]
  
  cat("[6/8] 處理條件站名更新...\n")
  dt[, mask := (Code == "NAMESAME" & 
                  BoardingStopName == DeboardingStopName & 
                  !is.na(BLabel) & !is.na(DLabel) & 
                  (BLabel != DLabel))]
  dt[mask == TRUE, BoardingStopName := BLabel]
  dt[mask == TRUE, DeboardingStopName := DLabel]
  dt[, mask := NULL]
  
  cat("[7/8] 清除欄位、移除 NA...\n")
  remove_cols <- c("Baddress", "Daddress", "BLabel", "DLabel")
  remove_cols <- intersect(names(dt), remove_cols)
  dt[, (remove_cols) := NULL]
  
  n_before <- nrow(dt)
  dt <- dt[complete.cases(dt)]
  n_after <- nrow(dt)
  cat("drop_na() 已移除", n_before - n_after, "列缺失資料。\n")
  
  cat("[8/8] 寫出結果至 ", outputpath, "...\n")
  write_fst(as.data.frame(dt), outputpath)
  
  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  cat(paste0("完成！總耗時：", elapsed, " 秒。\n"))
  return(dt)
}

merge_stopuid_fast_dropsamestopname <- function(inputfile, stopuid, startwith, outputpath) {
  start_time <- Sys.time()
  cat("[1/8] 載入主資料...\n")
  dt <- as.data.table(inputfile)
  
  cat("[2/8] 處理 stopuid...\n")
  stopuid <- as.data.table(stopuid)
  stopuid[, Id := as.character(Id)]
  stopuid[!startsWith(Id, startwith), Id := paste0(startwith, Id)]
  stopuid <- unique(stopuid, by = "Id")
  
  cat("[3/8] 建立 stopuid_B / stopuid_D...\n")
  stopuid_B <- copy(stopuid)
  stopuid_D <- copy(stopuid)
  cols_B <- setdiff(names(stopuid_B), "Id")
  setnames(stopuid_B, cols_B, paste0("B", cols_B))
  cols_D <- setdiff(names(stopuid_D), "Id")
  setnames(stopuid_D, cols_D, paste0("D", cols_D))
  
  cat("[4/8] 合併 stopuid_B...\n")
  setkey(dt, BoardingStopUID)
  setkey(stopuid_B, Id)
  cols <- setdiff(names(stopuid_B), "Id")
  newcols <- paste0("B", cols)
  dt[stopuid_B, (newcols) := mget(paste0("i.", cols)), on = .(BoardingStopUID = Id)]
  
  cat("[5/8] 合併 stopuid_D...\n")
  setkey(dt, DeboardingStopUID)
  setkey(stopuid_D, Id)
  cols_D <- setdiff(names(stopuid_D), "Id")
  newcols_D <- paste0("D", cols_D)
  dt[stopuid_D, (newcols_D) := mget(paste0("i.", cols_D)), on = .(DeboardingStopUID = Id)]
  
  cat("[6/8] 刪除 BoardingStopName == DeboardingStopName 的資料...\n")
  
  dt <- dt[!( !is.na(BoardingStopName) & !is.na(DeboardingStopName) &
                BoardingStopName == DeboardingStopName & BoardingStopName != BLabel &
                DeboardingStopName != DLabel)]
  
  cat("[7/8] 清除不必要欄位、移除 NA...\n")
  remove_cols <- c("Baddress", "Daddress", "BLabel", "DLabel")
  remove_cols <- intersect(names(dt), remove_cols)
  dt[, (remove_cols) := NULL]
  
  n_before <- nrow(dt)
  dt <- dt[complete.cases(dt)]
  n_after <- nrow(dt)
  cat("drop_na() 已移除", n_before - n_after, "列缺失資料。\n")
  
  cat("[8/8] 寫出結果至", outputpath, "...\n")
  write_fst(as.data.frame(dt), outputpath)
  
  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  cat(paste0("完成！總耗時：", elapsed, " 秒。\n"))
  return(dt)
}

merge_stopuid_fast_chunk <- function(inputfile, stopuid, startwith, outputpath, chunk_size = 10000000) {
  library(data.table)
  library(fst)
  
  start_time <- Sys.time()
  
  cat("[1/9] 載入主資料...\n")
  dt <- as.data.table(inputfile)
  
  cat("[2/9] 處理 stopuid...\n")
  stopuid <- as.data.table(stopuid)
  stopuid[, Id := as.character(Id)]
  stopuid[!startsWith(Id, startwith), Id := paste0(startwith, Id)]
  stopuid <- unique(stopuid, by = "Id")
  
  cat("[3/9] 建立 stopuid_B / stopuid_D...\n")
  stopuid_B <- copy(stopuid)
  stopuid_D <- copy(stopuid)
  cols_B <- setdiff(names(stopuid_B), "Id")
  setnames(stopuid_B, cols_B, paste0("B", cols_B))
  cols_D <- setdiff(names(stopuid_D), "Id")
  setnames(stopuid_D, cols_D, paste0("D", cols_D))
  
  total_rows <- nrow(dt)
  num_chunks <- ceiling(total_rows / chunk_size)
  cat(sprintf("[分塊處理] 總筆數: %d, 每區塊: %d 筆, 共分 %d 區塊\n", total_rows, chunk_size, num_chunks))
  
  result_list <- vector("list", num_chunks)
  total_removed <- 0
  
  for (i in 1:num_chunks) {
    start_idx <- ((i - 1) * chunk_size) + 1
    end_idx <- min(i * chunk_size, total_rows)
    dt_chunk <- dt[start_idx:end_idx]
    
    setkey(dt_chunk, BoardingStopUID)
    setkey(stopuid_B, Id)
    cols <- setdiff(names(stopuid_B), "Id")
    newcols <- paste0("B", cols)
    dt_chunk[stopuid_B, (newcols) := mget(paste0("i.", cols)), on = .(BoardingStopUID = Id)]
    
    setkey(dt_chunk, DeboardingStopUID)
    setkey(stopuid_D, Id)
    cols_D <- setdiff(names(stopuid_D), "Id")
    newcols_D <- paste0("D", cols_D)
    dt_chunk[stopuid_D, (newcols_D) := mget(paste0("i.", cols_D)), on = .(DeboardingStopUID = Id)]
    
    if (!("BLabel" %in% names(dt_chunk))) dt_chunk[, BLabel := NA]
    if (!("DLabel" %in% names(dt_chunk))) dt_chunk[, DLabel := NA]
    
    dt_chunk[, mask := (Code == "NAMESAME" & 
                          BoardingStopName == DeboardingStopName & 
                          !is.na(BLabel) & !is.na(DLabel) & 
                          (BLabel != DLabel))]
    dt_chunk[mask == TRUE, BoardingStopName := BLabel]
    dt_chunk[mask == TRUE, DeboardingStopName := DLabel]
    dt_chunk[, mask := NULL]
    
    remove_cols <- intersect(names(dt_chunk), c("Baddress", "Daddress", "BLabel", "DLabel"))
    if (length(remove_cols) > 0) dt_chunk[, (remove_cols) := NULL]
    
    n_before <- nrow(dt_chunk)
    dt_chunk <- dt_chunk[complete.cases(dt_chunk)]
    n_after <- nrow(dt_chunk)
    removed <- n_before - n_after
    total_removed <- total_removed + removed
    
    cat(sprintf("Chunk %d/%d：原始列數 %d，移除 %d 列 NA，剩下 %d 列\n",
                i, num_chunks, n_before, removed, n_after))
    
    result_list[[i]] <- dt_chunk
  }
  
  cat("[合併區塊] 正在合併所有區塊...\n")
  final_dt <- rbindlist(result_list)
  
  cat(sprintf("總共移除缺失資料列數：%d\n", total_removed))
  cat(sprintf("[9/9] 寫出結果至 %s ...\n", outputpath))
  write_fst(as.data.frame(final_dt), outputpath)
  
  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  cat(sprintf("完成！總耗時：%s 秒。\n", elapsed))
  
  return(final_dt)
}

merge_stopuid_fast_chunk_dropsamestopname <- function(inputfile, stopuid, startwith, outputpath, chunk_size = 10000000) {
  library(data.table)
  library(fst)
  
  start_time <- Sys.time()
  
  cat("[1/9] 載入主資料...\n")
  dt <- as.data.table(inputfile)
  
  cat("[2/9] 處理 stopuid...\n")
  stopuid <- as.data.table(stopuid)
  stopuid[, Id := as.character(Id)]
  stopuid[!startsWith(Id, startwith), Id := paste0(startwith, Id)]
  stopuid <- unique(stopuid, by = "Id")
  
  cat("[3/9] 建立 stopuid_B / stopuid_D...\n")
  safe_prefix_rename <- function(dt, prefix) {
    cols <- setdiff(names(dt), "Id")
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
    
    setkey(dt_chunk, BoardingStopUID)
    setkey(stopuid_B, Id)
    cols <- setdiff(names(stopuid_B), "Id")
    newcols <- paste0("B", cols)
    dt_chunk[stopuid_B, (newcols) := mget(paste0("i.", cols)), on = .(BoardingStopUID = Id)]
    
    setkey(dt_chunk, DeboardingStopUID)
    setkey(stopuid_D, Id)
    cols_D <- setdiff(names(stopuid_D), "Id")
    newcols_D <- paste0("D", cols_D)
    dt_chunk[stopuid_D, (newcols_D) := mget(paste0("i.", cols_D)), on = .(DeboardingStopUID = Id)]
    
    if (!("BLabel" %in% names(dt_chunk))) dt_chunk[, BLabel := NA]
    if (!("DLabel" %in% names(dt_chunk))) dt_chunk[, DLabel := NA]
    
    cat("[6/9] 刪除滿足條件的資料列...\n")
    dt_chunk <- dt_chunk[!( !is.na(BoardingStopName) & 
                              !is.na(DeboardingStopName) & 
                              BoardingStopName == DeboardingStopName & 
                              BoardingStopName != BLabel & 
                              DeboardingStopName != DLabel&
                              BoardingStopName == ""&
                              DeboardingStopName == "")]
    dt_chunk <- dt_chunk[! (is.na(Blongitude) | is.na(Blatitude) | is.na(Dlongitude) | is.na(Dlatitude))]
    
    cat("[7/9] 清除不必要欄位、移除 NA...\n")
    remove_cols <- intersect(names(dt_chunk), c("Baddress", "Daddress", "BLabel", "DLabel"))
    if (length(remove_cols) > 0) dt_chunk[, (remove_cols) := NULL]
    
    n_before <- nrow(dt_chunk)
    dt_chunk <- dt_chunk[complete.cases(dt_chunk)]
    n_after <- nrow(dt_chunk)
    removed <- n_before - n_after
    total_removed <- total_removed + removed
    
    cat(sprintf("Chunk %d/%d：原始列數 %d，移除 %d 列 NA，剩下 %d 列\n",
                i, num_chunks, n_before, removed, n_after))
    
    result_list[[i]] <- dt_chunk
    rm(dt_chunk)
    gc()
  }
  
  cat("[合併區塊] 正在合併所有區塊...\n")
  final_dt <- rbindlist(result_list)
  
  cat(sprintf("總共移除缺失資料列數：%d\n", total_removed))
  cat(sprintf("[9/9] 寫出結果至 %s ...\n", outputpath))
  write_fst(as.data.frame(final_dt), outputpath)
  
  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  cat(sprintf("完成！總耗時：%s 秒。\n", elapsed))
  
  return(final_dt)
}

merge_stopuid_fast_chunk_dropsamestopname <- function(inputfile, stopuid, startwith, outputpath, chunk_size = 10000000) {
  library(data.table)
  library(fst)
  
  start_time <- Sys.time()
  
  cat("[1/9] 載入主資料...\n")
  dt <- as.data.table(inputfile)
  
  cat("[2/9] 處理 stopuid...\n")
  stopuid <- as.data.table(stopuid)
  stopuid[, Id := as.character(Id)]
  stopuid[!startsWith(Id, startwith), Id := paste0(startwith, Id)]
  stopuid <- unique(stopuid, by = "Id")
  
  cat("[3/9] 建立 stopuid_B / stopuid_D...\n")
  safe_prefix_rename <- function(dt, prefix) {
    cols <- setdiff(names(dt), "Id")
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
    
    setkey(dt_chunk, BoardingStopUID)
    setkey(stopuid_B, Id)
    dt_chunk[stopuid_B, (B_cols) := mget(paste0("i.", B_cols)), on = .(BoardingStopUID = Id)]
    
    setkey(dt_chunk, DeboardingStopUID)
    setkey(stopuid_D, Id)
    dt_chunk[stopuid_D, (D_cols) := mget(paste0("i.", D_cols)), on = .(DeboardingStopUID = Id)]
    
    if (!("BLabel" %in% names(dt_chunk))) dt_chunk[, BLabel := NA]
    if (!("DLabel" %in% names(dt_chunk))) dt_chunk[, DLabel := NA]
    
    cat("[6/9] 刪除滿足條件的資料列...\n")
    dt_chunk <- dt_chunk[!( !is.na(BoardingStopName) & 
                              !is.na(DeboardingStopName) & 
                              BoardingStopName == DeboardingStopName & 
                              BoardingStopName != BLabel & 
                              DeboardingStopName != DLabel &
                              BoardingStopName != "" &
                              DeboardingStopName != "")]
    
    required_fields <- c("Blongitude", "Blatitude", "Dlongitude", "Dlatitude")
    missing_fields <- setdiff(required_fields, names(dt_chunk))
    if (length(missing_fields) > 0) {
      stop(paste("缺少必要欄位：", paste(missing_fields, collapse = ", ")))
    }
    dt_chunk <- dt_chunk[! (is.na(Blongitude) | is.na(Blatitude) | is.na(Dlongitude) | is.na(Dlatitude))]
    
    cat("[7/9] 清除不必要欄位、移除 NA...\n")
    remove_cols <- intersect(names(dt_chunk), c("Baddress", "Daddress", "BLabel", "DLabel"))
    if (length(remove_cols) > 0) dt_chunk[, (remove_cols) := NULL]
    
    n_before <- nrow(dt_chunk)
    dt_chunk <- dt_chunk[complete.cases(dt_chunk)]
    n_after <- nrow(dt_chunk)
    removed <- n_before - n_after
    total_removed <- total_removed + removed
    
    cat(sprintf("Chunk %d/%d：原始列數 %d，移除 %d 列 NA，剩下 %d 列\n",
                i, num_chunks, n_before, removed, n_after))
    
    result_list[[i]] <- dt_chunk
    rm(dt_chunk)
    gc()
  }
  
  cat("[合併區塊] 正在合併所有區塊...\n")
  final_dt <- rbindlist(result_list)
  
  cat(sprintf("總共移除缺失資料列數：%d\n", total_removed))
  cat(sprintf("[9/9] 寫出結果至 %s ...\n", outputpath))
  write_fst(as.data.frame(final_dt), outputpath)
  
  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  cat(sprintf("完成！總耗時：%s 秒。\n", elapsed))
  
  return(final_dt)
}

merge_stopuid_fast_chunk_dropsamestopname2 <- function(inputfile, stopuid, startwith, outputpath, chunk_size = 10000000) {
  library(data.table)
  library(fst)
  
  start_time <- Sys.time()
  
  cat("[1/9] 載入主資料...\n")
  dt <- as.data.table(inputfile)
  
  cat("[2/9] 處理 stopuid...\n")
  stopuid <- as.data.table(stopuid)
  stopuid[, Id := as.character(Id)]
  stopuid[!startsWith(Id, startwith), Id := paste0(startwith, Id)]
  stopuid <- unique(stopuid, by = "Id")
  
  cat("[3/9] 建立 stopuid_B / stopuid_D...\n")
  safe_prefix_rename <- function(dt, prefix) {
    cols <- setdiff(names(dt), "Id")
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
    
    setkey(dt_chunk, BoardingStopUID)
    setkey(stopuid_B, Id)
    dt_chunk[stopuid_B, (B_cols) := mget(paste0("i.", B_cols)), on = .(BoardingStopUID = Id)]
    
    dt_chunk <- dt_chunk[!is.na(BLabel) & BLabel != ""]
    
    setkey(dt_chunk, DeboardingStopUID)
    setkey(stopuid_D, Id)
    dt_chunk[stopuid_D, (D_cols) := mget(paste0("i.", D_cols)), on = .(DeboardingStopUID = Id)]
    
    dt_chunk <- dt_chunk[!is.na(DLabel) & DLabel != ""]
    
    dt_chunk[, BoardingStopName := ifelse(is.na(BoardingStopName) | BoardingStopName == "", "", BoardingStopName)]
    dt_chunk[, DeboardingStopName := ifelse(is.na(DeboardingStopName) | DeboardingStopName == "", "", DeboardingStopName)]
    
    if (!("BLabel" %in% names(dt_chunk))) dt_chunk[, BLabel := NA]
    if (!("DLabel" %in% names(dt_chunk))) dt_chunk[, DLabel := NA]
    
    cat("[6/9] 刪除滿足條件的資料列...\n")
    dt_chunk <- dt_chunk[ !((is.na(BoardingStopName) | BoardingStopName == "") |
                              (is.na(DeboardingStopName) | DeboardingStopName == "") |
                              (BoardingStopName == DeboardingStopName) |
                              (BoardingStopName != BLabel) |
                              (DeboardingStopName != DLabel))]
    
    required_fields <- c("Blongitude", "Blatitude", "Dlongitude", "Dlatitude")
    missing_fields <- setdiff(required_fields, names(dt_chunk))
    if (length(missing_fields) > 0) {
      stop(paste("缺少必要欄位：", paste(missing_fields, collapse = ", ")))
    }
    dt_chunk <- dt_chunk[! (is.na(Blongitude) | is.na(Blatitude) | is.na(Dlongitude) | is.na(Dlatitude))]
    
    cat("[7/9] 清除不必要欄位、移除 NA...\n")
    remove_cols <- intersect(names(dt_chunk), c("Baddress", "Daddress", "BLabel", "DLabel"))
    if (length(remove_cols) > 0) dt_chunk[, (remove_cols) := NULL]
    
    n_before <- nrow(dt_chunk)
    dt_chunk <- dt_chunk[complete.cases(dt_chunk)]
    n_after <- nrow(dt_chunk)
    removed <- n_before - n_after
    total_removed <- total_removed + removed
    
    cat(sprintf("Chunk %d/%d：原始列數 %d，移除 %d 列 NA，剩下 %d 列\n",
                i, num_chunks, n_before, removed, n_after))
    
    result_list[[i]] <- dt_chunk
    rm(dt_chunk)
    gc()
  }
  
  
  cat("[合併區塊] 正在合併所有區塊...\n")
  final_dt <- rbindlist(result_list)
  
  cat(sprintf("總共移除缺失資料列數：%d\n", total_removed))
  cat(sprintf("[9/9] 寫出結果至 %s ...\n", outputpath))
  write_fst(as.data.frame(final_dt), outputpath)
  
  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  cat(sprintf("完成！總耗時：%s 秒。\n", elapsed))
  
  return(final_dt)
}

merge_stopuid_fast_chunk_dropsamestopname3 <- function(inputfile, stopuid, startwith, outputpath, chunk_size = 10000000) {
  library(data.table)
  library(fst)
  
  start_time <- Sys.time()
  cat("[1/9] 載入主資料...\n")
  dt <- as.data.table(inputfile)
  
  cat("[2/9] 處理 stopuid...\n")
  stopuid <- as.data.table(stopuid)
  stopuid[, Id := as.character(Id)]
  stopuid[!startsWith(Id, startwith), Id := paste0(startwith, Id)]
  stopuid <- unique(stopuid, by = "Id")
  
  cat("[3/9] 建立 stopuid_B / stopuid_D...\n")
  safe_prefix_rename <- function(dt, prefix) {
    cols <- setdiff(names(dt), "Id")
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
    
    # 合併 Boarding 標籤資訊
    setkey(dt_chunk, BoardingStopUID)
    setkey(stopuid_B, Id)
    dt_chunk[stopuid_B, (B_cols) := mget(paste0("i.", B_cols)), on = .(BoardingStopUID = Id)]
    
    # 合併 Deboarding 標籤資訊
    setkey(dt_chunk, DeboardingStopUID)
    setkey(stopuid_D, Id)
    dt_chunk[stopuid_D, (D_cols) := mget(paste0("i.", D_cols)), on = .(DeboardingStopUID = Id)]
    
    # 如果標籤與現有名稱不符，使用標籤取代名稱
    dt_chunk[(BLabel != BoardingStopName | BoardingStopName=="")  & !is.na(BLabel), BoardingStopName := BLabel]
    dt_chunk[(DLabel != DeboardingStopName | DeboardingStopName=="") & !is.na(DLabel), DeboardingStopName := DLabel]
    
    # 將 NA 或空字串轉回空字串，保留 address 欄位
    dt_chunk[, BoardingStopName   := ifelse(is.na(BoardingStopName)   | BoardingStopName == "", "", BoardingStopName)]
    dt_chunk[, DeboardingStopName := ifelse(is.na(DeboardingStopName) | DeboardingStopName == "", "", DeboardingStopName)]
    
    cat("[6/9] 刪除缺失或同站資料列...\n")
    # 只刪除缺少名稱或上下站相同的列
    dt_chunk <- dt_chunk[ !(BoardingStopName == DeboardingStopName)]
    
    # 確認必要座標欄位存在
    required_fields <- c("Blongitude", "Blatitude", "Dlongitude", "Dlatitude")
    missing_fields <- setdiff(required_fields, names(dt_chunk))
    if (length(missing_fields) > 0) {
      stop(paste("缺少必要欄位：", paste(missing_fields, collapse = ", ")))
    }
    # 刪除缺少座標的列
    dt_chunk <- dt_chunk[! (is.na(Blongitude) | is.na(Blatitude) | is.na(Dlongitude) | is.na(Dlatitude))]
    
    cat("[7/9] 清除不必要欄位並移除 NA (保留 address)...\n")
    dt_chunk[, c("BLabel","DLabel") := NULL]
    
    # 定義「必須 non???NA」的欄位
    keep_cols <- c("BoardingStopName","DeboardingStopName",
                   "Blongitude","Blatitude","Dlongitude","Dlatitude")
    
    # 只對這些欄位檢查
    n_before <- nrow(dt_chunk)
    dt_chunk <- dt_chunk[ complete.cases(dt_chunk[, ..keep_cols]) ]
    n_after  <- nrow(dt_chunk)
    removed <- n_before - n_after
    total_removed <- total_removed + removed
    
    cat(sprintf("Chunk %d/%d：原始列數 %d，移除 %d 列 NA，剩下 %d 列\n",
                i, num_chunks, n_before, removed, n_after))
    
    result_list[[i]] <- dt_chunk
    rm(dt_chunk)
    gc()
  }
  
  cat("[合併區塊] 正在合併所有區塊...\n")
  final_dt <- rbindlist(result_list)
  
  cat(sprintf("總共移除缺失資料列數：%d\n", total_removed))
  cat(sprintf("[9/9] 寫出結果至 %s ...\n", outputpath))
  write_fst(as.data.frame(final_dt), outputpath)
  
  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  cat(sprintf("完成！總耗時：%s 秒。\n", elapsed))
  return(final_dt)
}

merge_stopuid_track_deletions_removed_only <- function(inputfile,
                                                       stopuid,
                                                       startwith,
                                                       removed_path = NULL,
                                                       chunk_size = 1e6) {
  # 載入套件
  library(data.table)
  library(fst)
  
  # --- 1. 處理 stopuid 對照表
  stopuid_dt <- as.data.table(stopuid)
  stopuid_dt[, Id := as.character(Id)]
  stopuid_dt[!startsWith(Id, startwith), Id := paste0(startwith, Id)]
  stopuid_dt <- unique(stopuid_dt, by = "Id")
  
  prefix_copy <- function(dt, prefix) {
    dt2 <- copy(dt)
    setnames(dt2,
             old = setdiff(names(dt2), "Id"),
             new = paste0(prefix, setdiff(names(dt2), "Id")))
    dt2
  }
  stopuid_B <- prefix_copy(stopuid_dt, "B")
  stopuid_D <- prefix_copy(stopuid_dt, "D")
  
  # --- 2. 讀入主資料並計算分塊數
  dt <- as.data.table(inputfile)
  total_rows   <- nrow(dt)
  num_chunks   <- ceiling(total_rows / chunk_size)
  removed_chunks <- vector("list", num_chunks)
  
  # --- 3. 分塊處理
  for (i in seq_len(num_chunks)) {
    start_idx <- (i - 1) * chunk_size + 1
    end_idx   <- min(i * chunk_size, total_rows)
    cur       <- copy(dt[start_idx:end_idx])
    removed_list <- list()
    
    # (1) 無對應 BLabel
    B_cols <- setdiff(names(stopuid_B), "Id")
    dropB  <- intersect(B_cols, names(cur))
    if (length(dropB)) cur[, (dropB) := NULL]
    setkey(cur, BoardingStopUID); setkey(stopuid_B, Id)
    cur[stopuid_B, (B_cols) := mget(paste0("i.", B_cols)), on = .(BoardingStopUID = Id)]
    cond <- is.na(cur$BLabel) | cur$BLabel == ""
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "no matching BLabel"]
      cur <- cur[!cond]
    }
    
    # (2) 無對應 DLabel
    D_cols <- setdiff(names(stopuid_D), "Id")
    dropD  <- intersect(D_cols, names(cur))
    if (length(dropD)) cur[, (dropD) := NULL]
    setkey(cur, DeboardingStopUID); setkey(stopuid_D, Id)
    cur[stopuid_D, (D_cols) := mget(paste0("i.", D_cols)), on = .(DeboardingStopUID = Id)]
    cond <- is.na(cur$DLabel) | cur$DLabel == ""
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "no matching DLabel"]
      cur <- cur[!cond]
    }
    
    # (3) BoardingStopName 遺失
    cond <- is.na(cur$BoardingStopName) | cur$BoardingStopName == ""
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "empty BoardingStopName"]
      cur <- cur[!cond]
    }
    
    # (4) DeboardingStopName 遺失
    cond <- is.na(cur$DeboardingStopName) | cur$DeboardingStopName == ""
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "empty DeboardingStopName"]
      cur <- cur[!cond]
    }
    
    # (5) 上下車站名相同
    cond <- cur$BoardingStopName == cur$DeboardingStopName
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "same stop name"]
      cur <- cur[!cond]
    }
    
    # (6) 站名與 Label 不一致
    cond <- cur$BoardingStopName != cur$BLabel
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "BoardingStopName ≠ BLabel"]
      cur <- cur[!cond]
    }
    cond <- cur$DeboardingStopName != cur$DLabel
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "DeboardingStopName ≠ DLabel"]
      cur <- cur[!cond]
    }
    
    # (7) 經緯度遺失
    cond <- is.na(cur$Blongitude) | is.na(cur$Blatitude) |
      is.na(cur$Dlongitude) | is.na(cur$Dlatitude)
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "missing coordinates"]
      cur <- cur[!cond]
    }
    
    # (8) 其他欄位 NA
    cond <- !complete.cases(cur)
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "other NA"]
      cur <- cur[!cond]
    }
    
    # 收集本 chunk 的刪除紀錄
    if (length(removed_list) > 0) {
      removed_chunks[[i]] <- rbindlist(removed_list, use.names = TRUE, fill = TRUE)
    }
  }
  
  # --- 4. 合併並（可選）匯出
  removed_dt <- if (length(removed_chunks) > 0)
    rbindlist(removed_chunks, use.names = TRUE, fill = TRUE)
  else
    data.table()
  
  if (!is.null(removed_path)) {
    write_fst(as.data.frame(removed_dt), removed_path)
  }
  
  # 回傳只含被刪除資料的 data.table
  return(removed_dt)
}

rm(list = ls())
gc()
#2023
{
  TPEbus2023fst <- "E:/brain/解壓縮data/fst/2023/2023臺北市公車.fst"
  NTWbus2023fst <- "E:/brain/解壓縮data/fst/2023/2023新北市公車.fst"
  TAObus2023fst <- "E:/brain/解壓縮data/fst/2023/2023桃園市公車.fst"
  KEEbus2023fst <- "E:/brain/解壓縮data/fst/2023/2023基隆市公車.fst"
  nrow(fst(TPEbus2023fst))
  nrow(fst(NTWbus2023fst))
  nrow(fst(TAObus2023fst))
  nrow(fst(KEEbus2023fst))
  
  TPEbus2023df <- process_bus_data(TPEbus2023fst,col_for_read,"2023")
  NTWbus2023df <- process_bus_data(NTWbus2023csv,col_for_read,"2023")
  TAObus2023df <- process_bus_data(TAObus2023csv,col_for_read,"2023")
  KEEbus2023df <- process_bus_data(KEEbus2023csv,col_for_read,"2023")
  KEEbus2023 <- read_fst(KEEbus2023fst,as.data.table = TRUE)%>%
    dplyr::select(BoardingTime)%>%
    mutate(Year = year(BoardingTime)) %>%
    count(Year)
  KEEbus2023D <- read_fst(KEEbus2023fst,as.data.table = TRUE)%>%
    dplyr::select(DeboardingTime)%>%
    mutate(Year = year(DeboardingTime)) %>%
    count(Year)
  nrow(TAObus2023df)
  nrow(KEEbus2023df) #相同站名站碼:21322734-6357248=14965486
  
  TPEbus2023df <- fill_missing_stops(TPEbus2023df,"E:/brain/解壓縮data/資料處理/2023/公車處理/臺北市公車(遺失值填補).fst")
  NTWbus2023df <- fill_missing_stops(NTWbus2023df,"E:/brain/解壓縮data/資料處理/2023/公車處理/新北市公車(遺失值填補).fst")
  TAObus2023df <- fill_missing_stops(TAObus2023df,"E:/brain/解壓縮data/資料處理/2023/公車處理/桃園市公車(遺失值填補).fst")
  KEEbus2023df <- fill_missing_stops(KEEbus2023df,"E:/brain/解壓縮data/資料處理/2023/公車處理/基隆市公車(遺失值填補).fst")
  
  NTPbus2023df <- process_stop_codes("E:/brain/解壓縮data/資料處理/2023/公車處理/新北市公車(遺失值填補).fst",
                                     "E:/brain/解壓縮data/資料處理/2023/公車處理/新北市公車(遺失值刪除)2.fst", drop_na_rows = TRUE, verbose = TRUE) 
  TYCbus2023df <- process_stop_codes("E:/brain/解壓縮data/資料處理/2023/公車處理/桃園市公車(遺失值填補).fst",
                                     "E:/brain/解壓縮data/資料處理/2023/公車處理/桃園市公車(遺失值刪除)2.fst", drop_na_rows = TRUE, verbose = TRUE) 
  KLCbus2023df <- process_stop_codes("E:/brain/解壓縮data/資料處理/2023/公車處理/基隆市公車(遺失值填補).fst",
                                     "E:/brain/解壓縮data/資料處理/2023/公車處理/基隆市公車(遺失值刪除)2.fst", drop_na_rows = TRUE, verbose = TRUE) 
  TPCbus2023df <- process_stop_codes("E:/brain/解壓縮data/資料處理/2023/公車處理/臺北市公車(遺失值填補).fst", 
                                     "E:/brain/解壓縮data/資料處理/2023/公車處理/臺北市公車(遺失值刪除)2.fst", drop_na_rows = TRUE, verbose = TRUE) 
  
  update_stop_codes("E:/brain/解壓縮data/資料處理/2023/公車處理/新北市公車(遺失值刪除)2.fst",
                    "E:/brain/解壓縮data/資料處理/2023/公車處理/新北市公車(刪除相同站名站碼)3.fst", verbose = TRUE)
  update_stop_codes("E:/brain/解壓縮data/資料處理/2023/公車處理/桃園市公車(遺失值刪除)2.fst",
                    "E:/brain/解壓縮data/資料處理/2023/公車處理/桃園市公車(刪除相同站名站碼)3.fst", verbose = TRUE)
  update_stop_codes("E:/brain/解壓縮data/資料處理/2023/公車處理/基隆市公車(遺失值刪除)2.fst",
                    "E:/brain/解壓縮data/資料處理/2023/公車處理/基隆市公車(刪除相同站名站碼)3.fst", verbose = TRUE)
  update_stop_codes_in_chunks("E:/brain/解壓縮data/資料處理/2023/公車處理/臺北市公車(遺失值刪除)2.fst",
                    "E:/brain/解壓縮data/資料處理/2023/公車處理/臺北市公車(刪除相同站名站碼)3.fst", verbose = TRUE,chunk_dates = list(1:6, 7:12))
  
}

#2022
{
  TPEbus2022fst <- "E:/brain/解壓縮data/fst/2022/2022臺北市公車.fst"
  NTWbus2022fst <- "E:/brain/解壓縮data/fst/2022/2022新北市公車.fst"
  TAObus2022fst <- "E:/brain/解壓縮data/fst/2022/2022桃園市公車.fst"
  KEEbus2022fst <- "E:/brain/解壓縮data/fst/2022/2022基隆市公車.fst"
  nrow(fst(TPEbus2022fst))
  nrow(fst(NTWbus2022fst))
  nrow(fst(TAObus2022fst))
  nrow(fst(KEEbus2022fst))
  
  TPEbus2022df <- process_bus_data(TPEbus2022_1_6csv ,col_for_read,"2022")
  TPEbus2022df <- process_bus_data(TPEbus2022_7_12csv ,col_for_read,"2022")
  NTWbus2022df <- process_bus_data(NTWbus2022csv ,col_for_read,"2022")
  TAObus2022df <- process_bus_data(TAObus2022csv ,col_for_read,"2022")
  KEEbus2022df <- process_bus_data(KEEbus2022csv ,col_for_read,"2022")
  KEEbus2022 <- read_fst(KEEbus2022fst,as.data.table = TRUE)%>%
    dplyr::select(BoardingTime)%>%
    mutate(Year = year(BoardingTime)) %>%
    count(Year)
  KEEbus2022D <- read_fst(KEEbus2022fst,as.data.table = TRUE)%>%
    dplyr::select(DeboardingTime)%>%
    mutate(Year = year(DeboardingTime)) %>%
    count(Year)
  nrow(TAObus2022df)
  nrow(KEEbus2022df) #相同站名站碼:21322734-6357248=14965486
  
  TPEbus2022df <- fill_missing_stops(TPEbus2022df,"E:/brain/解壓縮data/資料處理/2022/公車處理/臺北市公車1-6月(遺失值填補).fst")
  TPEbus2022df <- fill_missing_stops(TPEbus2022df,"E:/brain/解壓縮data/資料處理/2022/公車處理/臺北市公車7-12月(遺失值填補).fst")
  NTWbus2022df <- fill_missing_stops(NTWbus2022df,"E:/brain/解壓縮data/資料處理/2022/公車處理/新北市公車(遺失值填補).fst")
  TAObus2022df <- fill_missing_stops(TAObus2022df,"E:/brain/解壓縮data/資料處理/2022/公車處理/桃園市公車(遺失值填補).fst")
  KEEbus2022df <- fill_missing_stops(KEEbus2022df,"E:/brain/解壓縮data/資料處理/2022/公車處理/基隆市公車(遺失值填補).fst")
  
  NTPbus2022df <- process_stop_codes("E:/brain/解壓縮data/資料處理/2022/公車處理/新北市公車(遺失值填補).fst",
                                     "E:/brain/解壓縮data/資料處理/2022/公車處理/新北市公車(遺失值刪除)2.fst", drop_na_rows = TRUE, verbose = TRUE) 
  TYCbus2022df <- process_stop_codes("E:/brain/解壓縮data/資料處理/2022/公車處理/桃園市公車(遺失值填補).fst",
                                     "E:/brain/解壓縮data/資料處理/2022/公車處理/桃園市公車(遺失值刪除)2.fst", drop_na_rows = TRUE, verbose = TRUE) 
  KLCbus2022df <- process_stop_codes("E:/brain/解壓縮data/資料處理/2022/公車處理/基隆市公車(遺失值填補).fst",
                                     "E:/brain/解壓縮data/資料處理/2022/公車處理/基隆市公車(遺失值刪除)2.fst", drop_na_rows = TRUE, verbose = TRUE) 
  TPCbus2022df <- process_stop_codes("E:/brain/解壓縮data/資料處理/2022/公車處理/臺北市公車1-6月(遺失值填補).fst", 
                                     "E:/brain/解壓縮data/資料處理/2022/公車處理/臺北市公車1-6月(遺失值刪除)2.fst", drop_na_rows = TRUE, verbose = TRUE) 
  TPCbus2022df <- process_stop_codes("E:/brain/解壓縮data/資料處理/2022/公車處理/臺北市公車7-12月(遺失值填補).fst", 
                                     "E:/brain/解壓縮data/資料處理/2022/公車處理/臺北市公車7-12月(遺失值刪除)2.fst", drop_na_rows = TRUE, verbose = TRUE) 
  
  update_stop_codes("E:/brain/解壓縮data/資料處理/2022/公車處理/新北市公車(遺失值刪除)2.fst",
                    "E:/brain/解壓縮data/資料處理/2022/公車處理/新北市公車(刪除相同站名站碼)3.fst", verbose = TRUE)
  update_stop_codes("E:/brain/解壓縮data/資料處理/2022/公車處理/桃園市公車(遺失值刪除)2.fst",
                    "E:/brain/解壓縮data/資料處理/2022/公車處理/桃園市公車(刪除相同站名站碼)3.fst", verbose = TRUE)
  update_stop_codes("E:/brain/解壓縮data/資料處理/2022/公車處理/基隆市公車(遺失值刪除)2.fst",
                    "E:/brain/解壓縮data/資料處理/2022/公車處理/基隆市公車(刪除相同站名站碼)3.fst", verbose = TRUE)
  update_stop_codes("E:/brain/解壓縮data/資料處理/2022/公車處理/臺北市公車1-6月(遺失值刪除)2.fst",
                    "E:/brain/解壓縮data/資料處理/2022/公車處理/臺北市公車1-6月(刪除相同站名站碼)3.fst", verbose = TRUE)
  update_stop_codes("E:/brain/解壓縮data/資料處理/2022/公車處理/臺北市公車7-12月(遺失值刪除)2.fst",
                    "E:/brain/解壓縮data/資料處理/2022/公車處理/臺北市公車7-12月(刪除相同站名站碼)3.fst", verbose = TRUE)
  
}



base_path <- "E:/brain/解壓縮data/資料處理/2024"
busstoppath <- "E:/brain/解壓縮data/資料處理/公車站點資料"
NTPbusstoppath <- file.path(busstoppath,"新北市公車站點.csv")
TYCbusstoppath <- file.path(busstoppath,"桃園市公車站點.csv")
KLCbusstoppath <- file.path(busstoppath,"基隆市公車站點.csv")
TPCbusstoppath <- file.path(busstoppath,"臺北市公車站點.csv")
NTPbusoutputpath <- file.path(base_path,"新北市公車(經緯度_刪除相同站名).fst")
TYCbusoutputpath <- file.path(base_path,"桃園市公車(經緯度_刪除相同站名).fst")
KLCbusoutputpath <- file.path(base_path,"基隆市公車(經緯度_刪除相同站名).fst")
TPCbusoutputpath <- file.path(base_path,"臺北市公車(經緯度_刪除相同站名).fst")
NTPbusoutputpath2 <- file.path(base_path,"新北市公車(經緯度_刪除相同站名)v2.fst")
TYCbusoutputpath2 <- file.path(base_path,"桃園市公車(經緯度_刪除相同站名)v2.fst")
KLCbusoutputpath2 <- file.path(base_path,"基隆市公車(經緯度_刪除相同站名)v2.fst")
TPCbusoutputpath2 <- file.path(base_path,"臺北市公車(經緯度_刪除相同站名)v2.fst")

NTPbusdeloutputpath <- file.path(base_path,"新北市公車(經緯度_被刪除資料).fst")
TYCbusdeloutputpath <- file.path(base_path,"桃園市公車(經緯度_被刪除資料).fst")
KLCbusdeloutputpath <- file.path(base_path,"基隆市公車(經緯度_被刪除資料).fst")
TPCbusdeloutputpath <- file.path(base_path,"臺北市公車(經緯度_被刪除資料).fst")

NTPbus2024df <- read_fst(file.path(base_path,"新北市公車(經緯度_刪除相同站名).fst"))
nrow(NTPdf)
colSums(is.na(NTPdf))

TPCbus2024df <- read_fst(file.path(base_path,"臺北市公車(經緯度_刪除相同站名).fst"))
nrow(TPCdf)
colSums(is.na(TPCdf))

KLCbus2024df <- read_fst(file.path(base_path,"基隆市公車(經緯度_刪除相同站名).fst"))
nrow(KLCdf)
colSums(is.na(KLCdf))

TYCbus2024df <- read_fst(file.path(base_path,"桃園市公車(經緯度_刪除相同站名).fst"))
nrow(TYCdf)
colSums(is.na(TYCdf))


NTPbus2024df_head <- read_fst(file.path(base_path, "新北市公車(刪除相同站名站碼).fst"), from = 1, to = 5)
names(NTPbus2024df_head)  
NTPbus2024df <- read_fst(file.path(base_path, "新北市公車(刪除相同站名站碼).fst"),
                         columns=c("Authority","IDType","HolderType","TicketType",
                                   "SubTicketType","BoardingStopUID","BoardingStopName",
                                   "BoardingTime","DeboardingStopUID","DeboardingStopName",
                                   "DeboardingTime","TransferCode"))

NTPbus2024df <- read_fst(file.path(base_path, "新北市公車(刪除相同站名站碼).fst"),
                         columns=c("Authority","BoardingStopUID","BoardingStopName",
                                   "DeboardingStopUID","DeboardingStopName"))

TYCbus2024df <- read_fst(file.path(base_path, "桃園市公車(刪除相同站名站碼).fst"),
                         columns=c("Authority","IDType","HolderType","TicketType",
                                   "SubTicketType","BoardingStopUID","BoardingStopName",
                                   "BoardingTime","DeboardingStopUID","DeboardingStopName",
                                   "DeboardingTime","TransferCode"))

TYCbus2024df <- read_fst(file.path(base_path, "桃園市公車(刪除相同站名站碼).fst"),
                         columns=c("Authority","BoardingStopUID","BoardingStopName",
                                   "DeboardingStopUID","DeboardingStopName"))

KLCbus2024df <- read_fst(file.path(base_path, "基隆市公車(刪除相同站名站碼).fst"),
                         columns=c("Authority","IDType","HolderType","TicketType",
                                   "SubTicketType","BoardingStopUID","BoardingStopName",
                                   "BoardingTime","DeboardingStopUID","DeboardingStopName",
                                   "DeboardingTime","TransferCode"))

KLCbus2024df <- read_fst(file.path(base_path, "基隆市公車(刪除相同站名站碼).fst"),
                         columns=c("Authority","BoardingStopUID","BoardingStopName",
                                   "DeboardingStopUID","DeboardingStopName"))

TPCbus2024df <- read_fst(file.path(base_path, "臺北市公車(刪除相同站名站碼).fst"),
                         columns=c("Authority","IDType","HolderType","TicketType",
                                   "SubTicketType","BoardingStopUID","BoardingStopName",
                                   "BoardingTime","DeboardingStopUID","DeboardingStopName",
                                   "DeboardingTime","TransferCode"))
TPCbus2024df <- read_fst(file.path(base_path, "臺北市公車(刪除相同站名站碼).fst"),
                         columns=c("Authority","BoardingStopUID","BoardingStopName",
                                   "DeboardingStopUID","DeboardingStopName"))

NTPbusstop <- fread(NTPbusstoppath, encoding = "UTF-8")
TYCbusstop <- fread(TYCbusstoppath, encoding = "UTF-8")
KLCbusstop <- fread(KLCbusstoppath, encoding = "UTF-8")
TPCbusstop <- fread(TPCbusstoppath, encoding = "UTF-8")

NTPbus2024df <- merge_stopuid_fast(NTPbus2024df, NTPbusstop,"NWT", NTPbusoutputpath)
merge_stopuid_fast_chunk_dropsamestopname2(NTPbus2024df, NTPbusstop,"NWT", NTPbusoutputpath)
merge_stopuid_fast_chunk_dropsamestopname3(NTPbus2024df, NTPbusstop,"NWT", NTPbusoutputpath2)
nrow(fst(NTPbusoutputpath2))
NTPbus2024deldf <- merge_stopuid_track_deletions_removed_only (NTPbus2024df, NTPbusstop, "NWT",NTPbusdeloutputpath )
NTPbus2024deldf <- setDT(read_fst(NTPbusdeloutputpath))
NTPbus2024deldf[, .N, by = DeletionReason]

TYCbus2024df2 <- merge_stopuid(TYCbus2024df, TYCbusstop,"TAO", TYCbusoutputpath )
merge_stopuid_fast_chunk_dropsamestopname2(TYCbus2024df, TYCbusstop,"TAO", TYCbusoutputpath )
merge_stopuid_fast_chunk_dropsamestopname3(TYCbus2024df, TYCbusstop,"TAO", TYCbusoutputpath2)
nrow(fst(TYCbusoutputpath2))
TYCbus2024deldf <- merge_stopuid_track_deletions_removed_only (TYCbus2024df, TYCbusstop, "TAO",TYCbusdeloutputpath )
TYCbus2024deldf <- setDT(read_fst(TYCbusdeloutputpath))
TYCbus2024deldf[, .N, by = DeletionReason]
TYCONA <- TYCbus2024deldf[DeletionReason=="other NA"]
colSums(is.na(TYCONA))

KLCbus2024df2 <- merge_stopuid(KLCbus2024df, KLCbusstop,"KEE", KLCbusoutputpath)
merge_stopuid_fast_chunk_dropsamestopname2(KLCbus2024df, KLCbusstop,"KEE", KLCbusoutputpath)
merge_stopuid_fast_chunk_dropsamestopname3(KLCbus2024df, KLCbusstop,"KEE", KLCbusoutputpath2)
nrow(fst(KLCbusoutputpath2))
KLCbus2024deldf <- merge_stopuid_track_deletions_removed_only (KLCbus2024df, KLCbusstop, "KEE",KLCbusdeloutputpath )
KLCbus2024deldf <- setDT(read_fst(KLCbusdeloutputpath))
KLCbus2024deldf[, .N, by = DeletionReason]

merge_stopuid_fast_chunk(TPCbus2024df, TPCbusstop,"TPE", TPCbusoutputpath )
merge_stopuid_fast_chunk_dropsamestopname2(TPCbus2024df, TPCbusstop,"TPE", TPCbusoutputpath )
merge_stopuid_fast_chunk_dropsamestopname3(TPCbus2024df, TPCbusstop,"TPE", TPCbusoutputpath2)
nrow(fst(TPCbusoutputpath2))
TPCbus2024deldf <- merge_stopuid_track_deletions_removed_only (TPCbus2024df, TPCbusstop, "TPE",TPCbusdeloutputpath )
TPCbus2024deldf <- setDT(read_fst(TPCbusdeloutputpath))
TPCbus2024deldf[, .N, by = DeletionReason]

