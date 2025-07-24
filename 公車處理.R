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

#路徑設定
base_path <- "E:/brain/解壓縮data"
NTPbus2024df_output_fst <- file.path(base_path, "fst", "2024", "2024新北市公車.fst")
TPCbus2024df_output_fst <- file.path(base_path, "fst", "2024", "2024臺北市公車.fst")
TYCbus2024df_output_fst <- file.path(base_path, "fst", "2024", "2024桃園市公車.fst")
KLCbus2024df_output_fst <- file.path(base_path, "fst", "2024", "2024基隆市公車.fst")
NTPbus2024df_cleaned_fst <- file.path(base_path, "fst", "2024", "2024新北市公車(cleaned).fst")
TPCbus2024df_cleaned_fst <- file.path(base_path, "fst", "2024", "2024臺北市公車(cleaned).fst")
TYCbus2024df_cleaned_fst <- file.path(base_path, "fst", "2024", "2024桃園市公車(cleaned).fst")
KLCbus2024df_cleaned_fst <- file.path(base_path, "fst", "2024", "2024基隆市公車(cleaned).fst")
NTPbus2024df_removed_fst <- file.path(base_path, "fst", "2024", "2024新北市公車(removed).fst")
TPCbus2024df_removed_fst <- file.path(base_path, "fst", "2024", "2024臺北市公車(removed).fst")
TYCbus2024df_removed_fst <- file.path(base_path, "fst", "2024", "2024桃園市公車(removed).fst")
KLCbus2024df_removed_fst <- file.path(base_path, "fst", "2024", "2024基隆市公車(removed).fst")

busstoppath <- "E:/brain/解壓縮data/資料處理/公車站點資料"
NTPbusstoppath <- file.path(busstoppath,"新北市公車站點.csv")
TYCbusstoppath <- file.path(busstoppath,"桃園市公車站點.csv")
KLCbusstoppath <- file.path(busstoppath,"基隆市公車站點.csv")
TPCbusstoppath <- file.path(busstoppath,"臺北市公車站點.csv")
NTPbusstop <- fread(NTPbusstoppath, encoding = "UTF-8")
TYCbusstop <- fread(TYCbusstoppath, encoding = "UTF-8")
KLCbusstop <- fread(KLCbusstoppath, encoding = "UTF-8")
TPCbusstop <- fread(TPCbusstoppath, encoding = "UTF-8")

colnames(fst(NTPbus2024df_output_fst))
light_col <- c("Authority","HolderType","TicketType","SubTicketType","RouteUID","RouteName","Direction",             
               "BoardingStopUID","BoardingStopName","BoardingStopSequence","BoardingTime","DeboardingStopUID",      
               "DeboardingStopName","DeboardingStopSequence","DeboardingTime","TransferCode","IsAbnormal","ErrorCode",             
               "Result")
#處理資料
##刪除異常值
process_bus_data <- function(fst_path, columns) {
  
  df <- read_fst(fst_path, columns = columns)
  df <- df %>% mutate(
    DeboardingStopName = iconv(DeboardingStopName, from = "", to = "UTF-8"),
    DeboardingStopUID  = iconv(DeboardingStopUID,  from = "", to = "UTF-8"),
    BoardingStopName   = iconv(BoardingStopName,   from = "", to = "UTF-8"),
    BoardingStopUID    = iconv(BoardingStopUID,    from = "", to = "UTF-8")
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
  
  
  
  df <- df %>% filter(IsAbnormal == 0)
  rows_to_remove <- df %>% filter(BoardingStopName == DeboardingStopName & BoardingStopUID == DeboardingStopUID)
  removed_count <- nrow(rows_to_remove)
  cat("將移除", removed_count, "筆資料 (BoardingStopName==DeboardingStopName 和 BoardingStopUID==DeboardingStopName)\n")
  
  df <- df %>% filter(!(BoardingStopName == DeboardingStopName & BoardingStopUID == DeboardingStopUID))
  return(df)
  return(df)
}

NTPbus2024df <- process_bus_data(NTPbus2024df_output_fst,all_col)

##填補遺失值
fill_missing_stops <- function(df,df_output, verbose = TRUE) {
  df <- df %>% mutate(
    DeboardingStopName = iconv(DeboardingStopName, from = "", to = "UTF-8"),
    DeboardingStopUID  = iconv(DeboardingStopUID,  from = "", to = "UTF-8"),
    BoardingStopName   = iconv(BoardingStopName,   from = "", to = "UTF-8"),
    BoardingStopUID    = iconv(BoardingStopUID,    from = "", to = "UTF-8")
  )
  
  
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
  
  df <- df %>% 
    left_join(most_common_b_names, by = "BoardingStopUID") %>% 
    mutate(BoardingStopName = coalesce(BoardingStopName, modeBName)) %>% 
    select(-modeBName)
  write_fst(df, df_output)
  return(df)
}

NTPbus2024df <- fill_missing_stops(NTPbus2024df,"E:\\brain\\解壓縮data\\資料處理\\2024\\新北市公車(遺失值填補).fst")

##清除遺失值
process_stop_codes <- function(df, df_input, df_output, drop_na_rows = TRUE, verbose = TRUE) {
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

NTPbus2024df <- process_stop_codes(NTPbus2024df, file.path(base_path, "新北市公車(遺失值填補).fst"),
                                   file.path(base_path, "新北市公車(遺失值刪除).fst"), drop_na_rows = TRUE, verbose = TRUE) 

##更新相同名稱站點
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

update_stop_codes(file.path(base_path, "新北市公車(遺失值刪除).fst"),
                  file.path(base_path, "新北市公車(刪除相同站名站碼).fst"), verbose = TRUE)

##合併站點經緯度，並刪除異常值
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

merge_stopuid_fast_chunk_dropsamestopname2(NTPbus2024df, NTPbusstop,"NWT", NTPbusoutputpath)

##合併站點經緯度(檢查異常值)
merge_stopuid_track_deletions_removed_only <- function(inputfile,
                                                       stopuid,
                                                       startwith,
                                                       removed_path = NULL,
                                                       chunk_size = 1e6) {
  library(data.table)
  library(fst)
  
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
  
  dt <- as.data.table(inputfile)
  total_rows   <- nrow(dt)
  num_chunks   <- ceiling(total_rows / chunk_size)
  removed_chunks <- vector("list", num_chunks)
  
  for (i in seq_len(num_chunks)) {
    start_idx <- (i - 1) * chunk_size + 1
    end_idx   <- min(i * chunk_size, total_rows)
    cur       <- copy(dt[start_idx:end_idx])
    removed_list <- list()
    
    #無對應 BLabel
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
    
    #無對應 DLabel
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
    
    #BoardingStopName 遺失
    cond <- is.na(cur$BoardingStopName) | cur$BoardingStopName == ""
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "empty BoardingStopName"]
      cur <- cur[!cond]
    }
    
    #DeboardingStopName 遺失
    cond <- is.na(cur$DeboardingStopName) | cur$DeboardingStopName == ""
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "empty DeboardingStopName"]
      cur <- cur[!cond]
    }
    
    #上下車站名相同
    cond <- cur$BoardingStopName == cur$DeboardingStopName
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "same stop name"]
      cur <- cur[!cond]
    }
    
    #站名與 Label 不一致
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
    
    #經緯度遺失
    cond <- is.na(cur$Blongitude) | is.na(cur$Blatitude) |
      is.na(cur$Dlongitude) | is.na(cur$Dlatitude)
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "missing coordinates"]
      cur <- cur[!cond]
    }
    
    #其他欄位 NA
    cond <- !complete.cases(cur)
    if (any(cond)) {
      removed_list[[length(removed_list)+1]] <-
        cur[cond][, DeletionReason := "other NA"]
      cur <- cur[!cond]
    }
    
    if (length(removed_list) > 0) {
      removed_chunks[[i]] <- rbindlist(removed_list, use.names = TRUE, fill = TRUE)
    }
  }
  
  removed_dt <- if (length(removed_chunks) > 0)
    rbindlist(removed_chunks, use.names = TRUE, fill = TRUE)
  else
    data.table()
  
  if (!is.null(removed_path)) {
    write_fst(as.data.frame(removed_dt), removed_path)
  }
  
  return(removed_dt)
}
NTPbus2024deldf <- merge_stopuid_track_deletions_removed_only (NTPbus2024df, NTPbusstop, "NWT",NTPbusdeloutputpath )
NTPbus2024deldf[, .N, by = DeletionReason]
  out_fst = NTPbus2024df_cleaned_fst    # 若給路徑就把乾淨資料寫出

