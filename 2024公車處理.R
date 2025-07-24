install.packages('readr')
install.packages('data.table')
install.packages("fst")
install.packages("dplyr")
install.packages("doParallel")
install.packages("tidyr")
install.packages("ggplot2")
install.packages("forcats")
install.packages("scales")
install.packages("disk.frame")
install.packages("geosphere")
install.packages("psych") 
install.packages("smacof")
install.packages("lubridate")
install.packages("tibble")
install.packages("arrow")
install.packages("transport")
install.packages("proxy")
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
NTPbus2024df_input_csv <- file.path(base_path, "csv", "2024", "新北市公車電子票證資料(TO2A)2024-01-01 ~ 2024-12-31", "新北市公車電子票證資料(TO2A).csv")
NTPbus2024df_output_fst <- file.path(base_path, "fst", "2024", "2024新北市公車.fst")
NTPbus2024_light_path <- file.path(base_path,"fst","2024","2024新北市公車(未處理資料輕量版).fst")
names(fst(NTPbus2024df_output_fst))
head(fst(NTPbus2024df_output_fst))
df <- read_fst(NTPbus2024df_output_fst, from = 1, to = 10000)
names(df)
df <- as.data.table(df)
filtered <- df[BoardingStopUID == "NWT213893"]

TPCbus2024df_input_csv <- file.path(base_path, "csv", "2024", "臺北市公車電子票證資料(TO1A)2024-01-01 ~ 2024-11-30\\臺北市公車電子票證資料(TO1A).csv")
TPCbus2024df_output_fst <- file.path(base_path, "fst", "2024", "2024臺北市公車.fst")
TPCbus2024_light_path <- file.path(base_path,"fst","2024","2024臺北市公車(未處理資料輕量版).fst")

TYCbus2024df_input_csv <- file.path(base_path, "csv", "2024", "桃園市公車電子票證資料(TO2A)2024-01-01 ~ 2024-11-30\\桃園市公車電子票證資料(TO2A).csv")
TYCbus2024df_output_fst <- file.path(base_path, "fst", "2024", "2024桃園市公車.fst")
TYCbus2024_light_path <- file.path(base_path,"fst","2024","2024桃園市公車(未處理資料輕量版).fst")

KLCbus2024df_input_csv <- file.path(base_path, "csv", "2024", "基隆市公車電子票證資料(TO1A)2024-01-01 ~ 2024-11-30\\基隆市公車電子票證資料(TO1A).csv")
KLCbus2024df_output_fst <- file.path(base_path, "fst", "2024", "2024基隆市公車.fst")
KLCbus2024_light_path <- file.path(base_path,"fst","2024","2024基隆市公車(未處理資料輕量版).fst")

NTPmrt2024df_input_csv <- file.path(base_path, "csv", "2024", "新北捷運電子票證資料(TO2A)2024-01-01 ~ 2024-12-31\\新北捷運電子票證資料(TO2A).csv")
NTPmrt2024df_output_fst <- file.path(base_path, "fst", "2024", "2024新北市捷運.fst")

rail2024df_input_csv <- file.path(base_path, "csv", "2024", "臺鐵電子票證資料(TO2A)2024-01-01 ~ 2024-12-31\\臺鐵電子票證資料(TO2A).csv")
rail2024df_output_fst <- file.path(base_path, "fst", "2024", "2024臺鐵.fst")

TPCKLCbus2024df_output_fst <- file.path(base_path, "fst", "2024", "2024臺北基隆公車.fst")

combined_df_output <- file.path(base_path, "資料處理", "2024", "2024公車(去除價格).fst")

busstoppath <- "E:/brain/解壓縮data/資料處理/公車站點資料"
NTPbusstoppath <- file.path(busstoppath,"新北市公車站點.csv")
TYCbusstoppath <- file.path(busstoppath,"桃園市公車站點.csv")
KLCbusstoppath <- file.path(busstoppath,"基隆市公車站點.csv")
TPCbusstoppath <- file.path(busstoppath,"臺北市公車站點.csv")
NTPbusstop <- fread(NTPbusstoppath)
  
#2024新北市公車
#檢視
first_line <- readLines(NTPbus2024df_input_csv, n = 1, encoding = "UTF-8")
cat(strsplit(first_line, ",")[[1]], sep = "\n")

NTPbus2024df <- fread(NTPbus2024df_input_csv, skip = 1, header = TRUE, encoding = "UTF-8")
head(NTPbus2024df)
write_fst(NTPbus2024df, NTPbus2024df_output_fst)

#2024臺北市公車
first_line <- readLines(TPCbus2024df_input_csv, n = 1, encoding = "UTF-8")
cat(strsplit(first_line, ",")[[1]], sep = "\n")
TPCbus2024df <- fread(TPCbus2024df_input_csv, skip = 1, header = TRUE, encoding = "UTF-8")
head(TPCbus2024df)
write_fst(TPCbus2024df, TPCbus2024df_output_fst)

#2024桃園市公車
first_line <- readLines(TYCbus2024df_input_csv, n = 1, encoding = "UTF-8")
cat(strsplit(first_line, ",")[[1]], sep = "\n")
TYCbus2024df <- fread(TYCbus2024df_input_csv, skip = 1, header = TRUE, encoding = "UTF-8")
head(TYCbus2024df)
write_fst(TYCbus2024df, TYCbus2024df_output_fst)

#2024基隆市公車
first_line <- readLines(KLCbus2024df_input_csv, n = 1, encoding = "UTF-8")
cat(strsplit(first_line, ",")[[1]], sep = "\n")
KLCbus2024df <- fread(KLCbus2024df_input_csv, skip = 1, header = TRUE, encoding = "UTF-8")
KLCbus2024df <- read.csv(KLCbus2024df_input_csv, skip = 1, header = TRUE, fileEncoding = "UTF-8-BOM")
head(KLCbus2024df)
write_fst(KLCbus2024df, KLCbus2024df_output_fst)

sapply(KLCbus2024df$DeboardingStopName, Encoding)%>%unique()
encodings <- sapply(KLCbus2024df$DeboardingStopName, Encoding)
unknown_vals <- unique(KLCbus2024df$DeboardingStopName[encodings == "unknown"])
print(unknown_vals)

sapply(KLCbus2024df$BoardingStopName, Encoding)%>%unique()
encodings <- sapply(KLCbus2024df$BoardingStopName, Encoding)
unknown_vals <- unique(KLCbus2024df$BoardingStopName[encodings == "unknown"])
print(unknown_vals)

lines <- readLines(KLCbus2024df_input_csv, encoding = "BIG5")
lines_utf8 <- iconv(lines, from = "BIG5", to = "UTF-8", sub = "byte")
writeLines(lines_utf8, "converted_utf8.csv", useBytes = TRUE)

KLCbus2024df <- fread("converted_utf8.csv", skip = 1, encoding = "UTF-8")
head(KLCbus2024df)
write_fst(KLCbus2024df, KLCbus2024df_output_fst)

#2024新北市捷運
first_line <- readLines(NTPmrt2024df_input_csv, n = 1, encoding = "UTF-8")
cat(strsplit(first_line, ",")[[1]], sep = "\n")
NTPmrt2024df <- fread(NTPmrt2024df_input_csv, skip = 1, header = TRUE, encoding = "UTF-8")
head(NTPmrt2024df)
write_fst(NTPmrt2024df, NTPmrt2024df_output_fst)

#2024臺鐵
first_line <- readLines(rail2024df_input_csv, n = 1, encoding = "UTF-8")
cat(strsplit(first_line, ",")[[1]], sep = "\n")
rail2024df <- fread(rail2024df_input_csv, skip = 1, header = TRUE, encoding = "UTF-8")
head(rail2024df)
write_fst(rail2024df, rail2024df_output_fst)

#站點匯入
NTPbusstop <- fread(NTPbusstoppath, encoding = "UTF-8")
TYCbusstop <- fread(TYCbusstoppath, encoding = "UTF-8")
KLCbusstop <- fread(KLCbusstoppath, encoding = "UTF-8")
TPCbusstop <- fread(TPCbusstoppath, encoding = "UTF-8")

#檢查第一行
file_names <- c("NTP", "TPC", "TYC", "KLC")
files <- c(NTPbus2024df_input_csv, TPCbus2024df_input_csv, 
           TYCbus2024df_input_csv, KLCbus2024df_input_csv)
first_lines <- lapply(files, function(f) {
  line <- readLines(f, n = 1, encoding = "UTF-8")
  strsplit(line, ",")[[1]]
})
names(first_lines) <- file_names

sec_lines <- lapply(files, function(f) {
  line <- readLines(f, n = 2, encoding = "UTF-8")
  strsplit(line, ",")[[2]]
})

max_len <- max(sapply(first_lines, length))

compare_df <- data.frame(matrix(NA, nrow = length(first_lines), ncol = max_len),
                         stringsAsFactors = FALSE)
rownames(compare_df) <- file_names

for(i in seq_along(first_lines)) {
  n <- length(first_lines[[i]])
  compare_df[i, 1:n] <- first_lines[[i]]
}
print(compare_df)

#合併公車資料
KLCbus2024df <- read_fst(KLCbus2024df_output_fst)
all_col = c("ID",KLCbus2024df %>% select(-Price,-PaymentPrice,-Discount,-DiscountInfo,
                                  -FarePricingType, -TicketCount, -SrcUpdateTime, -UpdateTime, -InfoDate) %>% names())

NTPbus2024df <- read_fst(NTPbus2024df_output_fst, columns = all_col)
write_fst(NTPbus2024df, )
TYCbus2024df <- read_fst(TYCbus2024df_output_fst, columns = all_col)

all_col_noID <- KLCbus2024df %>% 
  select(-Price, -PaymentPrice, -Discount, -DiscountInfo, 
         -FarePricingType, -TicketCount, -SrcUpdateTime, -UpdateTime, -InfoDate) %>% 
  names()

KLCbus2024df <- read_fst(KLCbus2024df_output_fst, columns = all_col_noID)
TPCbus2024df <- read_fst(TPCbus2024df_output_fst, columns = all_col_noID)

combined_df <- NTPbus2024df
rm(NTPbus2024df); gc()

# 合併TYCbus2024df
combined_df <- rbindlist(list(combined_df, TYCbus2024df), fill = TRUE)
rm(TYCbus2024df); gc()

# 合併KLCbus2024df
combined_df <- rbindlist(list(combined_df, KLCbus2024df), fill = TRUE)
rm(KLCbus2024df); gc()

# 合併TPCbus2024df
combined_df <- rbindlist(list(combined_df, TPCbus2024df), fill = TRUE)
rm(TPCbus2024df); gc()

print(nrow(combined_df))
write_fst(combined_df, TPCKLCbus2024df_output_fst)


#測試儲存
NTPbus2024df <- read_fst(NTPbus2024df_output_fst)
str(NTPbus2024df)
TPCbus2024df <- read_fst(TPCbus2024df_output_fst)
str(TPCbus2024df)
TYCbus2024df <- read_fst(TYCbus2024df_output_fst)
str(TYCbus2024df)
KLCbus2024df <- read_fst(KLCbus2024df_output_fst)
str(KLCbus2024df)

#處理資料
KLCbus2024df <- read_fst(KLCbus2024df_output_fst)
all_col = c("ID",KLCbus2024df %>% select(-Price,-PaymentPrice,-Discount,-DiscountInfo,
                                         -FarePricingType, -TicketCount, -SrcUpdateTime, -UpdateTime, -InfoDate) %>% names())

all_col_noID <- KLCbus2024df %>% 
  select(-Price, -PaymentPrice, -Discount, -DiscountInfo, 
         -FarePricingType, -TicketCount, -SrcUpdateTime, -UpdateTime, -InfoDate) %>% 
  names()

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

#填補遺失值
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

#清除遺失值
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

# 檢查捷運相同站點上下車
NTPbus2024df <- read_fst(NTPbus2024df_output_fst)


NTPbus2024df <- process_bus_data(NTPbus2024df_output_fst,all_col)
TYCbus2024df <- process_bus_data(TYCbus2024df_output_fst,all_col)
KLCbus2024df <- process_bus_data(KLCbus2024df_output_fst,all_col_noID)
TPCbus2024df <- process_bus_data(TPCbus2024df_output_fst,all_col_noID)

NTPbus2024df <- fill_missing_stops(NTPbus2024df,"E:\\brain\\解壓縮data\\資料處理\\2024\\新北市公車(遺失值填補).fst")
TYCbus2024df <- fill_missing_stops(TYCbus2024df,"E:\\brain\\解壓縮data\\資料處理\\2024\\桃園市公車(遺失值填補).fst")
KLCbus2024df <- fill_missing_stops(KLCbus2024df,"E:\\brain\\解壓縮data\\資料處理\\2024\\基隆市公車(遺失值填補).fst")
TPCbus2024df <- fill_missing_stops(TPCbus2024df,"E:\\brain\\解壓縮data\\資料處理\\2024\\臺北市公車(遺失值填補).fst")

base_path <- "E:/brain/解壓縮data/資料處理/2024"
NTPbus2024df <- process_stop_codes(NTPbus2024df, file.path(base_path, "新北市公車(遺失值填補).fst"),
                                   file.path(base_path, "新北市公車(遺失值刪除).fst"), drop_na_rows = TRUE, verbose = TRUE) 
TYCbus2024df <- process_stop_codes(TYCbus2024df, file.path(base_path, "桃園市公車(遺失值填補).fst"),
                                   file.path(base_path, "桃園市公車(遺失值刪除).fst"), drop_na_rows = TRUE, verbose = TRUE) 
KLCbus2024df <- process_stop_codes(KLCbus2024df, file.path(base_path, "基隆市公車(遺失值填補).fst"),
                                   file.path(base_path, "基隆市公車(遺失值刪除).fst"), drop_na_rows = TRUE, verbose = TRUE) 
TPCbus2024df <- process_stop_codes(TPCbus2024df, file.path(base_path, "臺北市公車(遺失值填補).fst"), 
                                   file.path(base_path, "臺北市公車(遺失值刪除).fst"), drop_na_rows = TRUE, verbose = TRUE) 

update_stop_codes(file.path(base_path, "新北市公車(遺失值刪除).fst"),
                  file.path(base_path, "新北市公車(刪除相同站名站碼).fst"), verbose = TRUE)
update_stop_codes(file.path(base_path, "桃園市公車(遺失值刪除).fst"),
                  file.path(base_path, "桃園市公車(刪除相同站名站碼).fst"), verbose = TRUE)
update_stop_codes(file.path(base_path, "基隆市公車(遺失值刪除).fst"),
                  file.path(base_path, "基隆市公車(刪除相同站名站碼).fst"), verbose = TRUE)
update_stop_codes(file.path(base_path, "臺北市公車(遺失值刪除).fst"),
                  file.path(base_path, "臺北市公車(刪除相同站名站碼).fst"), verbose = TRUE)

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

#檢查ID
all_ids <- unique(TYCbus2024df$ID)
random_id <- sample(unique(TYCbus2024df$ID), 1)
result_df <- TYCbus2024df %>% filter(ID == random_id)
print(result_df)

id_counts <- TYCbus2024df %>%
  group_by(ID) %>%
  summarise(freq = n()) %>%
  arrange(desc(freq))

most_freq_id <- id_counts$ID[1]

result_df <- TYCbus2024df %>%
  filter(ID == most_freq_id)

print(result_df)

#繪圖
base_path <- "E:/brain/解壓縮data/資料處理/2024"
NTPbus2024df <- read.fst(file.path(base_path, "新北市公車(經緯度).fst"))
names(NTPbus2024df)
NTPbus2024df <- NTPbus2024df %>%
  select(-c(ID, OperatorNo, SubRouteUID,SubRouteName,Direction,
            IsAbnormal,ErrorCode,Result,Label,Code))

NTPbus2024df_operator <- read.fst(file.path(base_path, "新北市公車(經緯度).fst"),columns="OperatorNo")
NTP_authority_routes <- NTPbus2024df_operator %>%
  reframe(OperatorNo = unique(OperatorNo))


TYCbus2024df <- read.fst(file.path(base_path, "桃園市公車(經緯度).fst"))
names(TYCbus2024df)
TYCbus2024df <- TYCbus2024df %>%
  select(-c(ID, OperatorNo, SubRouteUID,SubRouteName,Direction,
            IsAbnormal,ErrorCode,Result,Label,Code))

TYCbus2024df_operator <- read.fst(file.path(base_path, "桃園市公車(經緯度).fst"),columns="OperatorNo")
TYC_authority_routes <- TYCbus2024df_operator %>%
  reframe(OperatorNo = unique(OperatorNo))

KLCbus2024df <- read.fst(file.path(base_path, "基隆市公車(經緯度).fst"))
names(KLCbus2024df)
KLCbus2024df <- KLCbus2024df %>%
  select(-c(OperatorNo, SubRouteUID,SubRouteName,Direction,
            IsAbnormal,ErrorCode,Result,Label,Code))

KLCbus2024df_operator <- read.fst(file.path(base_path, "基隆市公車(經緯度).fst"),columns="OperatorNo")
KLC_authority_routes <- KLCbus2024df_operator %>%
  reframe(OperatorNo = unique(OperatorNo))

TPCbus2024df <- read.fst(file.path(base_path, "臺北市公車(經緯度).fst"))
names(TPCbus2024df)
TPCbus2024df <- TPCbus2024df %>%
  select(-c(OperatorNo, SubRouteUID,SubRouteName,Direction,
            IsAbnormal,ErrorCode,Result,Label,Code,BBLabel,BBaddress,
            DDLabel,DDaddress))
TPCbus2024df <- TPCbus2024df %>%
  rename(Blongitude = BBlongitude,
         Dlongitude = DDlongitude,
         Blatitude = BBlatitude,
         Dlatitude = DDlatitude)

TPCbus2024df_operator <- read.fst(file.path(base_path, "臺北市公車(經緯度).fst"),columns="OperatorNo")
TPC_authority_routes <- TPCbus2024df_operator %>%
  reframe(OperatorNo = unique(OperatorNo))

names(fst(NTPbusoutputpath2))
NTPbus2024df <- setDT(read_fst(NTPbusoutputpath2,
                               columns = c("Authority","IDType","HolderType",
                                           "TicketType","SubTicketType","BoardingStopUID",
                                           "BoardingStopName","BoardingTime","DeboardingStopUID",
                                           "DeboardingStopName","DeboardingTime","TransferCode",
                                           "Blongitude","Blatitude","Dlongitude","Dlatitude")))
TYCbus2024df <- setDT(read_fst(TYCbusoutputpath2,
                               columns = c("Authority","IDType","HolderType",
                                           "TicketType","SubTicketType","BoardingStopUID",
                                           "BoardingStopName","BoardingTime","DeboardingStopUID",
                                           "DeboardingStopName","DeboardingTime","TransferCode",
                                           "Blongitude","Blatitude","Dlongitude","Dlatitude")))
KLCbus2024df <- setDT(read_fst(KLCbusoutputpath2,
                               columns = c("Authority","IDType","HolderType",
                                           "TicketType","SubTicketType","BoardingStopUID",
                                           "BoardingStopName","BoardingTime","DeboardingStopUID",
                                           "DeboardingStopName","DeboardingTime","TransferCode",
                                           "Blongitude","Blatitude","Dlongitude","Dlatitude")))
TPCbus2024df <- setDT(read_fst(TPCbusoutputpath2,
                               columns = c("Authority","IDType","HolderType",
                                           "TicketType","SubTicketType","BoardingStopUID",
                                           "BoardingStopName","BoardingTime","DeboardingStopUID",
                                           "DeboardingStopName","DeboardingTime","TransferCode",
                                           "Blongitude","Blatitude","Dlongitude","Dlatitude")))
NTPbus2024df$BoardingTime <- as.POSIXct(NTPbus2024df$BoardingTime, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
TPCbus2024df$BoardingTime <- as.POSIXct(TPCbus2024df$BoardingTime, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
KLCbus2024df$BoardingTime <- as.POSIXct(KLCbus2024df$BoardingTime, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
TYCbus2024df$BoardingTime <- as.POSIXct(TYCbus2024df$BoardingTime, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
NTPbus2024df$DeboardingTime <- as.POSIXct(NTPbus2024df$DeboardingTime, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
TPCbus2024df$DeboardingTime <- as.POSIXct(TPCbus2024df$DeboardingTime, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
KLCbus2024df$DeboardingTime <- as.POSIXct(KLCbus2024df$DeboardingTime, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
TYCbus2024df$DeboardingTime <- as.POSIXct(TYCbus2024df$DeboardingTime, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
gc()

bus2024df <- bind_rows(NTPbus2024df, TPCbus2024df, KLCbus2024df, TYCbus2024df)
names(bus2024df)
bus_path <- "E:/brain/解壓縮data/資料處理/2024/2024公車(初步合併)1.fst"
write_fst(bus2024df,bus_path)
bus_path2 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(初步合併)1v2.fst"
write_fst(bus2024df,bus_path2)
rm(list = setdiff(ls(), "bus2024df")) 

fst_info <- fst(bus_path2)
colnames(fst_info)
nrow(fst(bus_path2))

bus2024df <- read.fst(bus_path2)
bus2024df_route <- read.fst(bus_path, columns=c("Authority","RouteName"))
authority_routes <- bus2024df_route %>%
  group_by(Authority) %>%
  reframe(RouteName = unique(RouteName))

bus2024df <- bus2024df %>%
  mutate(Distance = distHaversine(
    cbind(Blongitude, Blatitude),  
    cbind(Dlongitude, Dlatitude)  
  ) / 1000)
names(bus2024df)

bus_path2 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(直線距離)2.fst"
bus_path2v2 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(直線距離)2v2.fst"
stopdfpath <- "E:/brain/解壓縮data/資料處理/公車站點資料/北北基桃公車站點(加入鄉政市區數位發展分類).parquet"
stopdfpath2 <- "E:/brain/解壓縮data/資料處理/公車站點資料/北北基桃公車站點(加入鄉政市區數位發展分類與氣象站).parquet"
stopdfpath2_v2 <- "E:/brain/解壓縮data/資料處理/公車站點資料/北北基桃公車站點(加入鄉政市區數位發展分類與氣象站_Voronoi_v2).parquet"
stopdfpath2_v2 <- "E:/brain/解壓縮data/資料處理/公車站點資料/北北基桃公車站點(加入鄉政市區數位發展分類與氣象站_voronoi_v3).parquet"

write_fst(bus2024df,bus_path2)
write_fst(bus2024df,bus_path2v2)
fst_info <- fst(bus_path2)
colnames(fst_info)

bus2024df <- read.fst(bus_path2, columns = c("Authority", "HolderType", "TicketType", "SubTicketType",
                                             "BoardingTime","DeboardingTime","Distance"))
bus2024df <- read.fst(bus_path2v2, columns = c("Authority", "HolderType", "TicketType", "SubTicketType",
                                             "BoardingTime","DeboardingTime","BoardingStopName","DeboardingStopName",
                                             "BoardingStopUID","DeboardingStopUID","Distance"))

bus2024df <- read.fst(bus_path2, columns = c("IDType", "Authority", "HolderType", "TicketType", "SubTicketType",
                                             "BoardingTime","DeboardingTime","BoardingStopName",
                                             "DeboardingStopName","RouteName","Distance"))
stoplonlat <- read.fst(bus_path2, columns = c("BoardingStopName","DeboardingStopName",
                                              "BoardingStopUID", "DeboardingStopUID",
                                             "Blongitude","Blatitude",
                                             "Dlongitude","Dlatitude"))
stopdf <- read_parquet(stopdfpath2,  col_select = c("UID", "town_name", "county_name", "development_level",
                                                   "nearest_StationID","nearest_StationName"))
stopdf <- read_parquet(stopdfpath2_v2)
names(stopdf)


# 所有站名
{
DT <- as.data.table(stoplonlat)
DT <- as.data.table(NTPdf)
DT <- as.data.table(TPCdf)
DT <- as.data.table(KLCdf)

sum(is.na(DT$BoardingStopName) | DT$BoardingStopName == "")
sum(is.na(DT$BoardingStopUID) | DT$BoardingStopUID == "")
sum(is.na(DT$DeboardingStopName) | DT$DeboardingStopName == "")
sum(is.na(DT$DeboardingStopUID) | DT$DeboardingStopUID == "")

all_stops <- unique(rbind(
  DT[, .(StopName = BoardingStopName, longitude = Blongitude, latitude = Blatitude, UID = BoardingStopUID)],
  DT[, .(StopName = DeboardingStopName, longitude = Dlongitude, latitude = Dlatitude, UID = DeboardingStopUID)]
))

head(all_stops)

pairs <- CJ(i = 1:nrow(all_stops), j = 1:nrow(all_stops))[i < j]

pairs[, `:=`(
  StopName1 = all_stops[i]$StopName,
  lon1 = all_stops[i]$longitude,
  lat1 = all_stops[i]$latitude,
  StopName2 = all_stops[j]$StopName,
  lon2 = all_stops[j]$longitude,
  lat2 = all_stops[j]$latitude
)]

pairs[, distance := distHaversine(cbind(lon1, lat1), cbind(lon2, lat2))]

result <- pairs[distance < 10 & StopName1 != StopName2]

result[, .(StopName1, StopName2, distance)]
}

#合併Stopdf和bus2024df
board_info <- stopdf %>%
  rename_with(~ paste0("B", .), .cols = -UID)

deboard_info <- stopdf %>%
  rename_with(~ paste0("D", .), .cols = -UID)

bus2024df <- bus2024df %>%
  left_join(board_info, by = c("BoardingStopUID" = "UID")) 

bus2024df <- bus2024df %>%
  left_join(deboard_info, by = c("DeboardingStopUID" = "UID"))

head(bus2024df)

rm(list = setdiff(ls(), "bus2024df")) 
gc() 
bus_path4 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(加入發展程度)4.fst"
fst_info <- fst(bus_path4)
colnames(fst_info)
write_fst(bus2024df,bus_path4)
bus_path4v2 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(加入發展程度)4v2.fst"
write_fst(bus2024df,bus_path4v2)
names(fst(bus_path4v2))
bus_path4v3 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(加入發展程度_Voronoi)4v3.fst"
write_fst(bus2024df,bus_path4v3)
bus_path4v4 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(加入發展程度_Voronoi)4v4.fst"
write_fst(bus2024df,bus_path4v4)

rm(list = setdiff(ls(), "all_stops")) 
gc() 
write_parquet(all_stops, "E:/brain/解壓縮data/資料處理/公車站點資料/北北基桃公車站點_UID.parquet")

bus_path4v3 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(加入發展程度_Voronoi)4v3.fst"
names(fst(bus_path4v3))
bus2024df <- read.fst(bus_path4)
bus2024df <- read.fst(bus_path4v2,columns = c("Authority","HolderType","TicketType","SubTicketType",       
                                              "BoardingTime","DeboardingTime","BoardingStopName","DeboardingStopName",  
                                              "BoardingStopUID","DeboardingStopUID","Distance","Bdevelopment_level",
                                              "Bnearest_StationID","Bnearest_StationName","Ddevelopment_level"))
bus2024dt <- as.data.table(read.fst(bus_path4v4,columns = c("Authority","HolderType","TicketType","SubTicketType",       
                                              "BoardingTime","DeboardingTime","BoardingStopName","DeboardingStopName",  
                                              "BoardingStopUID","DeboardingStopUID","Distance","Bdevelopment_level",
                                              "BStationID","Ddevelopment_level")))

bus2024df %>% head()
names(bus2024df)

bus2024dt %>%
  group_by(Authority) %>%
  summarise(Count_Distance_0 = sum(Distance < 0.01, na.rm = TRUE))
bus2024dt <- as.data.table(bus2024df)
rm(list = setdiff(ls(), "bus2024dt")) 
gc()

colSums(is.na(bus2024dt))
level_change <- function(df){
  df[, Distance := as.numeric(Distance)]
  df <- df[!is.na(Distance) & Distance > 0.01]

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
bus2024dt <- level_change(bus2024dt)

bus_path5 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(發展程度移動)5.fst"
bus_path52 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(發展程度移動)5v2.fst"
bus_path53 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(發展程度移動_voronoi)5v3.fst"
bus_path53_trancated <- "E:/brain/解壓縮data/資料處理/2024/2024公車(發展程度移動_voronoi_truncated)5v3.fst"
bus_path54 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(發展程度移動_voronoi)5v4.fst"
bus_path54_trancated <- "E:/brain/解壓縮data/資料處理/2024/2024公車(發展程度移動_voronoi_truncated)5v4.fst"

fst_info <- fst(bus_path5)
colnames(fst_info)
nrow(fst(bus_path54))
nrow(fst(bus_path54_trancated))

write_fst(bus2024dt,bus_path5)
write_fst(bus2024dt,bus_path52)
write_fst(bus2024dt,bus_path53)
write_fst(bus2024dt,bus_path54)

bus2024dt <- as.data.table(read.fst(bus_path53))
bus2024dt <- bus2024dt %>% drop_na()
write_fst(bus2024dt,bus_path54_trancated)

bus2024dt <- as.data.table(read_fst(bus_path5, 
                                    columns=c("Authority","HolderType","TicketType",
                                              "SubTicketType","DeboardingTime",
                                              "Distance","dev_movement","movement_level",
                                              "Bdevelopment_level", "Ddevelopment_level")))
bus2024dt <- as.data.table(read_fst(bus_path5, 
                                    columns=c("Authority","HolderType","TicketType",
                                              "SubTicketType","Distance","dev_movement","movement_level",
                                              "Bdevelopment_level","Ddevelopment_level")))
bus2024dt <- as.data.table(read_fst(bus_path5, 
                                    columns=c("Authority","HolderType",
                                              "SubTicketType","Distance","movement_level")))

#計算鄉政市區比例
bus2024dt <- as.data.table(read_fst(bus_path5, 
                                    columns = c("Bcounty_name", "Dcounty_name", 
                                                "Btown_name", "Dtown_name", 
                                                "Bdevelopment_level", "Ddevelopment_level")))
B_dt <- bus2024dt[, .(county_name = Bcounty_name,
                      town_name = Btown_name,
                      development_level = Bdevelopment_level)]
D_dt <- bus2024dt[, .(county_name = Dcounty_name,
                      town_name = Dtown_name,
                      development_level = Ddevelopment_level)]
combined_dt <- rbind(B_dt, D_dt)
unique_dt <- unique(combined_dt[, .(county_name, town_name, development_level)])
result <- unique_dt[, .(Count = .N), by = development_level]
result[, Percent := round(Count / sum(Count) * 100, 2)]
print(result)

rm(list = setdiff(ls(), "bus2024dt")) 
gc()
head(bus2024dt)
bus2024dt[, movement_level_abs := abs(movement_level)]

psych::describe
describe(bus2024dt$Distance)
describeBy(bus2024dt$Distance, group=bus2024dt$Authority)
describeBy(bus2024dt$Distance, group=bus2024dt$HolderType)
describeBy(bus2024dt$Distance, group=bus2024dt$TicketType)
describeBy(bus2024dt$Distance, group=bus2024dt$SubTicketType)
describeBy(bus2024dt$Distance, group=bus2024dt$movement_level)
describeBy(bus2024dt$Distance, group=bus2024dt$dev_movement)
describeBy(bus2024dt$Distance, group=bus2024dt$movement_level_abs)
rm(list = setdiff(ls(), "bus2024dt")) 

ticket_summary <- bus2024dt[, .(
  Count = .N,
  Count_TicketType4 = sum(TicketType == 4)
), by = .(Bdevelopment_level, Ddevelopment_level)]

ticket_summary[, Percent_TicketType4 := paste0(round(Count_TicketType4 / Count * 100, 2), "%")]
print(ticket_summary)

movement_sum <- bus2024dt[, .(
  Count = .N,
  Count_TicketType4 = sum(TicketType == 4)
), by = movement_level_abs]

movement_sum[, Percent := paste0(round(Count / sum(Count) * 100, 2), "%")]
movement_sum[, Percent_TicketType4 := paste0(round(Count_TicketType4 / Count * 100, 2), "%")]

print(movement_sum)

bdev_summary <- bus2024dt[, .(
  Count = .N,
  Count_TicketType4 = sum(TicketType == 4)
), by = Bdevelopment_level]

bdev_summary[, Percent := paste0(round(Count / sum(Count) * 100, 2), "%")]
bdev_summary[, Percent_TicketType4 := paste0(round(Count_TicketType4 / Count * 100, 2), "%")]

print(bdev_summary)

ddev_summary <- bus2024dt[, .(
  Count = .N,
  Count_TicketType4 = sum(TicketType == 4)
), by = Ddevelopment_level]

ddev_summary[, Percent := paste0(round(Count / sum(Count) * 100, 2), "%")]
ddev_summary[, Percent_TicketType4 := paste0(round(Count_TicketType4 / Count * 100, 2), "%")]

print(ddev_summary)

tickettype4_pct <- paste0(round(nrow(bus2024dt[TicketType == 4])/nrow(bus2024dt)*100,2),"%")
nrow(bus2024dt[TicketType == 4])
print(tickettype4_pct)

paste0(round(nrow(bus2024dt[(movement_level_abs == 0)&(TicketType == 4)])/nrow(bus2024dt[TicketType == 4])*100,2),"%")
paste0(round(nrow(bus2024dt[(movement_level_abs == 1)&(TicketType == 4)])/nrow(bus2024dt[TicketType == 4])*100,2),"%")
paste0(round(nrow(bus2024dt[(movement_level_abs == 2)&(TicketType == 4)])/nrow(bus2024dt[TicketType == 4])*100,2),"%")
paste0(round(nrow(bus2024dt[(movement_level_abs == 3)&(TicketType == 4)])/nrow(bus2024dt[TicketType == 4])*100,2),"%")

subdata_A <- bus2024dt[HolderType == "A"]
describe(subdata_A$Distance)
describeBy(subdata_A$Distance, group = subdata_A$movement_level)
describeBy(subdata_A$Distance, group = subdata_A$dev_movement)
describeBy(subdata_A$Distance, group = subdata_A$movement_level_abs)

subdata_B <- bus2024dt[HolderType == "B"]
paste0(round(nrow(subdata_B[TicketType == 4])/nrow(subdata_B)*100,2),"%")
paste0(round(nrow(subdata_B[(movement_level_abs == 0)&(TicketType == 4)])/nrow(subdata_B[TicketType == 4])*100,2),"%")
paste0(round(nrow(subdata_B[(movement_level_abs == 1)&(TicketType == 4)])/nrow(subdata_B[TicketType == 4])*100,2),"%")
paste0(round(nrow(subdata_B[(movement_level_abs == 2)&(TicketType == 4)])/nrow(subdata_B[TicketType == 4])*100,2),"%")
paste0(round(nrow(subdata_B[(movement_level_abs == 3)&(TicketType == 4)])/nrow(subdata_B[TicketType == 4])*100,2),"%")
nrow(bus2024dt[TicketType == 4])
describe(subdata_B$Distance)
movement_sum <- subdata_B[, .(
  Count = .N,
  Count_TicketType4 = sum(TicketType == 4)
), by = movement_level_abs]

movement_sum[, Percent := paste0(round(Count / sum(Count) * 100, 2), "%")]
movement_sum[, Percent_TicketType4 := paste0(round(Count_TicketType4 / Count * 100, 2), "%")]

print(movement_sum)

ticket_summary <- subdata_B[, .(
  Count = .N,
  Count_TicketType4 = sum(TicketType == 4)
), by = .(Bdevelopment_level, Ddevelopment_level)]

ticket_summary[, Percent_TicketType4 := paste0(round(Count_TicketType4 / Count * 100, 2), "%")]
print(ticket_summary)

bdev_summary <- subdata_B[, .(
  Count = .N,
  Count_TicketType4 = sum(TicketType == 4)
), by = Bdevelopment_level]

bdev_summary[, Percent := paste0(round(Count / sum(Count) * 100, 2), "%")]
bdev_summary[, Percent_TicketType4 := paste0(round(Count_TicketType4 / Count * 100, 2), "%")]

print(bdev_summary)

ddev_summary <- subdata_B[, .(
  Count = .N,
  Count_TicketType4 = sum(TicketType == 4)
), by = Ddevelopment_level]

ddev_summary[, Percent := paste0(round(Count / sum(Count) * 100, 2), "%")]
ddev_summary[, Percent_TicketType4 := paste0(round(Count_TicketType4 / Count * 100, 2), "%")]

print(ddev_summary)

tickettype4_pct <- paste0(round(nrow(subdata_B[TicketType == 4])/nrow(subdata_B)*100,2),"%")
nrow(subdata_B[TicketType == 4])
print(tickettype4_pct)

subdata_B_mp <- subdata_B[SubTicketType != "Single_Ticket"]
subdata_B_st <- subdata_B[SubTicketType == "Single_Ticket"]
describeBy(subdata_B$Distance, group = subdata_B$movement_level)
describeBy(subdata_B$Distance, group = subdata_B$dev_movement)
describeBy(subdata_B$Distance, group = subdata_B$movement_level_abs)
describeBy(subdata_B_mp$Distance, group = subdata_B_mp$movement_level_abs)
describeBy(subdata_B_st$Distance, group = subdata_B_st$movement_level_abs)

subdata_C <- bus2024dt[substr(HolderType, 1, 1) == "C"]
describeBy(subdata_C$Distance, group = subdata_C$movement_level)
describeBy(subdata_C$Distance, group = subdata_C$dev_movement)
describeBy(subdata_C$Distance, group = subdata_C$movement_level_abs)
gc()

top_n_per_authority <- bus2024dt %>%
  group_by(Authority) %>%                     
  arrange(desc(Distance), .by_group = TRUE) %>%  
  slice_head(n = 500)                           

top500_df <- bus2024dt[order(-bus2024dt$Distance), ][1:500, ]
bus2024dt$RouteName <- iconv(bus2024dt$RouteName, from = "unknown", to = "UTF-8")
bus2024dt$RouteName <- enc2utf8(bus2024dt$RouteName)

authority_routes <- bus2024dt %>%
  group_by(Authority) %>%
  reframe(RouteName = unique(RouteName))


plot_hist_box <- function(df, var,
                          group_value = NULL,
                          group_col = "Authority",
                          main_title_hist = NULL,
                          main_title_box = NULL,
                          xlab = NULL,
                          ylab = "數量") {
  var_sym <- rlang::ensym(var)
  group_sym <- rlang::ensym(group_col)
  var_label <- as.character(var_sym)
  
  if (!is.null(group_value)) {
    data_subset <- df[df[[as.character(group_sym)]] == group_value, ]
    group_label <- paste0("（", group_value, "）")
  } else {
    data_subset <- df
    group_label <- "(全部)"
  }

  if (is.null(main_title_hist)) {
    main_title_hist <- paste("直方圖", group_label, var_label)
  }
  if (is.null(main_title_box)) {
    main_title_box <- paste("箱型圖", group_label, var_label)
  }
  if (is.null(xlab)) {
    xlab <- var_label
  }
  
  par(mfrow = c(2, 1))
  
  hist(data_subset[[var_label]],
       xlab = xlab,
       ylab = ylab,
       main = main_title_hist,
       col = "gray")
  
  boxplot(data_subset[[var_label]],
          horizontal = TRUE,
          xlab = xlab,
          main = main_title_box,
          col = "skyblue")
}

plot_hist_box(bus2024dt, Distance,"Taipei",
              main_title_hist="臺北市公車直線距離直方圖", 
              main_title_box="臺北市公車直線距離箱型圖",
              xlab="距離(km)")
plot_hist_box(bus2024dt, Distance,"NewTaipei",
              main_title_hist="新北市公車直線距離直方圖", 
              main_title_box="新北市公車直線距離箱型圖",
              xlab="距離(km)")
plot_hist_box(bus2024dt, Distance,"Taoyuan",
              main_title_hist="桃園市公車直線距離直方圖", 
              main_title_box="桃園市公車直線距離箱型圖",
              xlab="距離(km)")
plot_hist_box(bus2024dt, Distance,"Keelung",
              main_title_hist="基隆市公車直線距離直方圖", 
              main_title_box="基隆市公車直線距離箱型圖",
              xlab="距離(km)")
plot_hist_box <- function(df, var,
                          group_value = NULL,
                          group_col = "movement_level_abs",
                          main_title_hist = NULL,
                          main_title_box = NULL,
                          xlab = NULL,
                          ylab = "數量") {
  var_sym <- rlang::ensym(var)
  group_sym <- rlang::ensym(group_col)
  var_label <- as.character(var_sym)
  
  if (!is.null(group_value)) {
    data_subset <- df[df[[as.character(group_sym)]] == group_value, ]
    group_label <- paste0("（", group_value, "）")
  } else {
    data_subset <- df
    group_label <- "(全部)"
  }
  
  if (is.null(main_title_hist)) {
    main_title_hist <- paste("直方圖", group_label, var_label)
  }
  if (is.null(main_title_box)) {
    main_title_box <- paste("箱型圖", group_label, var_label)
  }
  if (is.null(xlab)) {
    xlab <- var_label
  }
  
  par(mfrow = c(2, 1))
  
  hist(data_subset[[var_label]],
       xlab = xlab,
       ylab = ylab,
       main = main_title_hist,
       col = "gray")
  
  boxplot(data_subset[[var_label]],
          horizontal = TRUE,
          xlab = xlab,
          main = main_title_box,
          col = "skyblue")
}

plot_hist_box(bus2024dt, Distance,0,
              main_title_hist="移動層級0: 公車直線距離直方圖", 
              main_title_box="移動層級0: 公車直線距離箱型圖",
              xlab="距離(km)")
plot_hist_box(bus2024dt, Distance,1,
              main_title_hist="移動層級1: 公車直線距離直方圖", 
              main_title_box="移動層級1: 公車直線距離箱型圖",
              xlab="距離(km)")
plot_hist_box(bus2024dt, Distance,2,
              main_title_hist="移動層級2: 公車直線距離直方圖", 
              main_title_box="移動層級2: 公車直線距離箱型圖",
              xlab="距離(km)")
plot_hist_box(bus2024dt, Distance,3,
              main_title_hist="移動層級3: 公車直線距離直方圖", 
              main_title_box="移動層級3: 公車直線距離箱型圖",
              xlab="距離(km)")
plot_hist_box(subdata_B, Distance,0,
              main_title_hist="移動層級0: 學生身分公車直線距離直方圖", 
              main_title_box="移動層級0: 學生身分公車直線距離箱型圖",
              xlab="距離(km)")
plot_hist_box(subdata_B, Distance,1,
              main_title_hist="移動層級1: 學生身分公車直線距離直方圖", 
              main_title_box="移動層級1: 學生身分公車直線距離箱型圖",
              xlab="距離(km)")
plot_hist_box(subdata_B, Distance,2,
              main_title_hist="移動層級2: 學生身分公車直線距離直方圖", 
              main_title_box="移動層級2: 學生身分公車直線距離箱型圖",
              xlab="距離(km)")
plot_hist_box(subdata_B, Distance,3,
              main_title_hist="移動層級3: 學生身分公車直線距離直方圖", 
              main_title_box="移動層級3: 學生身分公車直線距離箱型圖",
              xlab="距離(km)")

plotTicketTypeChart <- function(df, Authority_filter = NULL, AuthorityName = NULL) {
  library(dplyr)
  library(tidyr)
  library(lubridate)
  
  if (!is.null(Authority_filter)) {
    df <- df %>% filter(Authority == Authority_filter)
  }
  
  df <- df %>%
    mutate(BYearMonth = format(as.POSIXct(BoardingTime), "%Y-%m"))
  
  count_table <- df %>%
    group_by(BYearMonth, TicketType) %>%
    summarise(Count = n(), .groups = "drop")
  print("進度1 - count_table 完成")
  
  pivot_table <- count_table %>%
    pivot_wider(names_from = TicketType, values_from = Count, values_fill = list(Count = 0)) %>%
    arrange(BYearMonth)
  
  pivot_table <- pivot_table %>%
    mutate(Total = `1` + `4`,
           Proportion_1 = `1` / Total,
           Proportion_4 = `4` / Total)
  
  print(pivot_table)
  print("進度2 - pivot_table 完成")
  
  # 繪圖
  counts_mat <- t(as.matrix(pivot_table %>% select(`1`, `4`)))
  
  if (!is.null(AuthorityName)) {
    bp <- barplot(counts_mat, beside = TRUE,
                  col = c("skyblue", "lightgreen"),
                  names.arg = pivot_table$BYearMonth,
                  las = 1,
                  cex.names = 0.9,
                  ylim = c(0, max(counts_mat) * 1.2),
                  xlab = "年-月", ylab = "人次",
                  main = paste(AuthorityName,"每月票種計數及比例折線圖"))
  } else{
    bp <- barplot(counts_mat, beside = TRUE,
                  col = c("skyblue", "lightgreen"),
                  names.arg = pivot_table$BYearMonth,
                  las = 1,
                  cex.names = 0.9,
                  ylim = c(0, max(counts_mat) * 1.2),
                  xlab = "年-月", ylab = "人次",
                  main = "每月票種計數及比例折線圖")
  }
  

  text(x = bp[1, ], y = counts_mat[1, ] + 1, labels = counts_mat[1, ], cex = 0.8)
  text(x = bp[2, ], y = counts_mat[2, ] + 1, labels = counts_mat[2, ], cex = 0.8)
  
  group_centers <- colMeans(bp)

  par(new = TRUE)
  plot(group_centers, pivot_table$Proportion_1, type = "b",
       axes = FALSE, xlab = "", ylab = "",
       col = "skyblue", pch = 21, bg = "white", ylim = c(0, 1), lwd = 2)
  lines(group_centers, pivot_table$Proportion_4, type = "b",
        col = "lightgreen", pch = 21, bg = "white", lwd = 2)
  axis(side = 4, at = seq(0, 1, by = 0.2))
  mtext("Proportion", side = 4, line = 3)
  
  legend("topright",
         legend = c("單次票", "月票"),
         fill = c("skyblue", "lightgreen"),
         border = c("skyblue", "lightgreen"),
         bty = "n")
  print(paste(AuthorityName,"每月票種計數及比例折線圖"))
}
plotTicketTypeChart(bus2024dt)

plotTicketTypeChart(bus2024dt, Authority_filter = "NewTaipei", AuthorityName = "新北市")
plotTicketTypeChart(bus2024dt, Authority_filter = "Taipei", AuthorityName = "臺北市")
plotTicketTypeChart(bus2024dt, Authority_filter = "Taoyuan", AuthorityName = "桃園市")
plotTicketTypeChart(bus2024dt, Authority_filter = "Keelung", AuthorityName = "基隆市")

transform_bus_data <- function(df) {
  library(dplyr)
  library(lubridate)
  
  df %>%
    mutate(
      BoardingTime = as.POSIXct(BoardingTime),
      DeboardingTime = as.POSIXct(DeboardingTime),

      BDayOfWeek = format(BoardingTime, "%A"),
 
      BWeekendOrWeekday = ifelse(wday(BoardingTime) %in% c(1, 7), "Weekend", "Weekday"),

      BHour = hour(BoardingTime),

      BMonth = month(BoardingTime),
      BYear = year(BoardingTime),
      DYear = year(DeboardingTime),

      Duration = DeboardingTime - BoardingTime
    )
}

bus2024dt <- transform_bus_data(bus2024dt)
bus_path3 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(加入時間)3.fst"
write_fst(bus2024dt,bus_path3)

plotWeeklyTicketTypeChart <- function(df, auth_filter = NULL, AuthorityName = NULL) {
  library(dplyr)
  library(ggplot2)
  
  if (!is.null(auth_filter)) {
    df <- df %>% filter(Authority == auth_filter)
  }
  
  count_table <- df %>%
    group_by(BDayOfWeek, TicketType) %>%
    summarise(Count = n(), .groups = "drop")
  
  ordered_days <- c("星期一", "星期二", "星期三", "星期四", "星期五",
                    "星期六", "星期日")
  
  count_table <- count_table %>%
    mutate(BDayOfWeek = factor(BDayOfWeek, levels = ordered_days, ordered = TRUE)) %>%
    arrange(BDayOfWeek)
  
  print(count_table)
  
  new_labels <- function(x) {
    sapply(x, function(lab) {
      if (as.character(lab) == "1") {
        "單次票"
      } else if (as.character(lab) == "4") {
        "月票"
      } else {
        lab
      }
    })
  }
  
  p <- ggplot(count_table, aes(x = BDayOfWeek, y = Count, fill = as.factor(TicketType))) +
    geom_bar(stat = "identity", position = "dodge") +
    scale_x_discrete(drop = FALSE, limits = ordered_days) +
    scale_fill_discrete(name = "TicketType", labels = new_labels) +
    labs(title = "各星期票種分佈情形", x = "星期幾", y = "人次") +
    theme_minimal() +
    theme(plot.title = element_text(hjust = 0.5))
  
  if (!is.null(AuthorityName)) {
    p <- p + labs(title = paste(AuthorityName, "各類票券的每週分佈"))
  }
  
  print(p)
  print(paste0(AuthorityName, "各星期票種分佈情形"))
}
plotWeeklyTicketTypeChart(bus2024dt)
plotWeeklyTicketTypeChart(bus2024dt, auth_filter = "NewTaipei", AuthorityName = "新北市")
plotWeeklyTicketTypeChart(bus2024dt, auth_filter = "Taipei", AuthorityName = "臺北市")
plotWeeklyTicketTypeChart(bus2024dt, auth_filter = "Taoyuan", AuthorityName = "桃園市")
plotWeeklyTicketTypeChart(bus2024dt, auth_filter = "Keelung", AuthorityName = "基隆市")

plotHourlyTicketTypeChart <- function(df, auth_filter = NULL, AuthorityName = NULL) {
  library(dplyr)
  library(ggplot2)

  if (!is.null(auth_filter)) {
    df <- df %>% filter(Authority == auth_filter)
  }

  count_table <- df %>%
    group_by(BHour, TicketType) %>%
    summarise(Count = n(), .groups = "drop")

  ordered_hours <- as.character(0:23)

  count_table <- count_table %>%
    mutate(BHour = factor(as.character(BHour), levels = ordered_hours, ordered = TRUE)) %>%
    arrange(BHour)
  
  print(count_table,n=46)
  total_count <- sum(count_table$Count, na.rm = TRUE)
  print(total_count)

  new_labels <- function(x) {
    sapply(x, function(lab) {
      if (as.character(lab) == "1") {
        "單次票"
      } else if (as.character(lab) == "4") {
        "月票"
      } else {
        lab
      }
    })
  }
  
  p <- ggplot(count_table, aes(x = BHour, y = Count, fill = as.factor(TicketType))) +
    geom_bar(stat = "identity", position = "dodge") +
    scale_x_discrete(drop = FALSE, limits = ordered_hours) +
    scale_fill_discrete(name = "TicketType", labels = new_labels) +
    labs(title = "各小時票種分佈情形", x = "小時", y = "人次") +
    theme_minimal() +
    theme(plot.title = element_text(hjust = 0.5))
  
  if (!is.null(AuthorityName)) {
    p <- p + labs(title = paste(AuthorityName, "各小時票種分佈情形"))
  }
  
  print(p)
  print(paste0(AuthorityName, "各小時票種分佈情形"))
}
plotHourlyTicketTypeChart(bus2024dt)
plotHourlyTicketTypeChart(bus2024dt, auth_filter = "NewTaipei", AuthorityName = "新北市")
plotHourlyTicketTypeChart(bus2024dt, auth_filter = "Taipei", AuthorityName = "臺北市")
plotHourlyTicketTypeChart(bus2024dt, auth_filter = "Taoyuan", AuthorityName = "桃園市")
plotHourlyTicketTypeChart(bus2024dt, auth_filter = "Keelung", AuthorityName = "基隆市")

pic_path <- "E:/brain/解壓縮data/資料視覺化/2024"
IDTYPE <- function(df,name){
  full_title <- paste(name, "公車電子票證卡種")
  p <- ggplot(df, aes(x = fct_infreq(IDType))) +
    geom_bar() +
    labs(title = full_title, x = "電子票證卡種", y = "計數") +
    theme_minimal()
  print(p)
  
  summary_table <- df %>%
    count(IDType, name = "count") %>%
    mutate(
      percent = count / sum(count),
      percent_label = percent(percent, accuracy = 0.01)
    ) %>%
    arrange(desc(count)) %>%
    mutate(
      count = comma(count)  
    ) %>%
    select(IDType, count, percent_label)
  
  print(summary_table)
  print(full_title)
}
IDTYPE(NTPbus2024df,"新北市")
IDTYPE(TYCbus2024df,"桃園市")
IDTYPE(KLCbus2024df,"基隆市")
IDTYPE(TPCbus2024df,"臺北市")

HOLDERTYPE <- function(df,name){
  full_title <- paste(name, "公車持卡身分")
  p <- ggplot(df, aes(x = fct_infreq(HolderType))) +
    geom_bar() +
    labs(title = full_title, x = "持卡身分", y = "計數") +
    theme_minimal()
  print(p)
  
  summary_table <- df %>%
    count(HolderType, name = "count") %>%
    mutate(
      percent = count / sum(count),
      percent_label = percent(percent, accuracy = 0.01)
    ) %>%
    arrange(desc(count)) %>%
    mutate(
      count = comma(count)
    ) %>%
    select(HolderType, count, percent_label)
  
  print(summary_table)
  print(full_title)
}
HOLDERTYPE(NTPbus2024df,"新北市")
HOLDERTYPE(TYCbus2024df,"桃園市")
HOLDERTYPE(KLCbus2024df,"基隆市")
HOLDERTYPE(TPCbus2024df,"臺北市")

TICKETTYPE <- function(df, name) {
  full_title <- paste(name, "公車月票/單次票")
  
  ticket_counts <- df %>%
    count(TicketType, name = "count") %>%
    mutate(
      TicketType = case_when(
        TicketType == 1 ~ "單次票",
        TicketType == 4 ~ "月票",
        TRUE ~ as.character(TicketType)
      ),
      percent = count / sum(count),
      percent_label = percent(percent, accuracy = 0.01),
      ypos = cumsum(count) - 0.5 * count  # 用來決定標籤位置
    )
  
  p <- ggplot(ticket_counts, aes(x = "", y = count, fill = TicketType)) +
    geom_bar(stat = "identity", width = 1, color = "white") +
    coord_polar(theta = "y") +
    geom_text(aes(label = percent_label, y = ypos), color = "white", size = 5) +
    scale_fill_manual(values = c("單次票" = "#FF7F0E", "月票" = "#1F77B4")) +
    labs(title = full_title) +
    theme_void() +
    theme(
      plot.title = element_text(size = 18, hjust = 0.5),
      legend.title = element_blank(),
      legend.text = element_text(size = 14)
    )
  
  summary_table <- df %>%
    count(TicketType, name = "count") %>%
    mutate(
      percent = count / sum(count),
      percent_label = percent(percent, accuracy = 0.01)
    ) %>%
    arrange(desc(count)) %>%
    mutate(
      count = comma(count)  
    ) %>%
    select(TicketType, count, percent_label)
  
  print(summary_table)
  print(p)
  print(full_title)
}
TICKETTYPE(NTPbus2024df,"新北市")
TICKETTYPE(TYCbus2024df,"桃園市")
TICKETTYPE(KLCbus2024df,"基隆市")
TICKETTYPE(TPCbus2024df,"臺北市")

CROSSTAB_HOLDER <- function(df, desired_order = c("A", "B", "C01", "C02", "C09", "X")) {
  library(dplyr)
  library(tidyr)
  library(scales)

  df_tt4 <- df %>% filter(TicketType == 4)

  holder_counts <- df_tt4 %>%
    count(HolderType, name = "count")

  holder_counts <- holder_counts %>%
    mutate(HolderType = factor(HolderType, levels = desired_order)) %>%
    complete(HolderType = desired_order, fill = list(count = 0)) %>%
    arrange(HolderType)

  total_count <- sum(holder_counts$count)
  holder_counts <- holder_counts %>%
    mutate(percentage = round(count / total_count * 100, 2))

  holder_counts <- holder_counts %>%
    mutate(count_formatted = comma(count))

  holder_counts <- holder_counts %>%
    mutate(combined = paste0(count_formatted, " (", percentage, "%)"))
  
  final_table <- holder_counts %>% select(HolderType, combined)
  
  return(final_table)
}
CROSSTAB_HOLDER(NTPbus2024df)
CROSSTAB_HOLDER(TYCbus2024df)
CROSSTAB_HOLDER(KLCbus2024df)
CROSSTAB_HOLDER(TPCbus2024df)

TICKETTYPE_HOLDER <- function(df, holder_type, title_label) {
  full_title <- paste(title_label, "公車月票/單次票")
  
  ticket_counts <- df %>%
    filter(HolderType == holder_type) %>%
    count(TicketType, name = "count") %>%
    mutate(
      TicketType = case_when(
        TicketType == 1 ~ "單次票",
        TicketType == 4 ~ "月票",
        TRUE ~ as.character(TicketType)
      ),
      percent = count / sum(count),
      percent_label = percent(percent, accuracy = 0.1),
      ypos = cumsum(count) - 0.5 * count
    )
  
  p <- ggplot(ticket_counts, aes(x = "", y = count, fill = TicketType)) +
    geom_bar(stat = "identity", width = 1, color = "white") +
    coord_polar(theta = "y") +
    geom_text(aes(label = percent_label, y = ypos), color = "white", size = 5) +
    scale_fill_manual(values = c("單次票" = "#FF7F0E", "月票" = "#1F77B4")) +
    labs(title = full_title) +
    theme_void() +
    theme(
      plot.title = element_text(size = 18, hjust = 0.5),
      legend.title = element_blank(),
      legend.text = element_text(size = 14)
    )
  
  print(p)
  print(full_title)
}
TICKETTYPE_HOLDER(NTPbus2024df,"A","新北市普通身分")
TICKETTYPE_HOLDER(NTPbus2024df,"B","新北市學生身分")
TICKETTYPE_HOLDER(TYCbus2024df,"A","桃園市普通身分")
TICKETTYPE_HOLDER(TYCbus2024df,"B","桃園市學生身分")
TICKETTYPE_HOLDER(KLCbus2024df,"A","基隆市普通身分")
TICKETTYPE_HOLDER(KLCbus2024df,"B","基隆市學生身分")
TICKETTYPE_HOLDER(TPCbus2024df,"A","臺北市普通身分")
TICKETTYPE_HOLDER(TPCbus2024df,"B","臺北市學生身分")

NTPbus2024df %>%
  count(SubTicketType, name = "count") %>%
  arrange(desc(count)) 
TYCbus2024df %>%
  count(SubTicketType, name = "count") %>%
  arrange(desc(count)) 
KLCbus2024df %>%
  count(SubTicketType, name = "count") %>%
  arrange(desc(count)) 
TPCbus2024df %>%
  count(SubTicketType, name = "count") %>%
  arrange(desc(count)) 


TYCbus2024df <- read.fst(file.path(base_path, "桃園市公車(刪除相同站名站碼).fst"))
ggplot(TYCbus2024df, aes(x = fct_infreq(IDType))) +
  geom_bar()+
  labs(title = "桃園市電子票證卡種", x = "電子票證卡種", y = "計數") 

data_viz(NTPbus2024df,"新北市公車電子票證.png")

# MDS-SMACOF 時間 使用比例
bus_path3 <- "E:/brain/解壓縮data/資料處理/2024/2024公車(加入時間)3.fst"
bus2024dt <- read.fst(bus_path3, columns=c("IDType", "Authority", "HolderType", 
                                             "TicketType","SubTicketType", "BoardingTime",
                                             "DeboardingTime", "Distance", "BDayOfWeek",
                                             "BHour", "BMonth"))
bus2024dt <- read.fst(bus_path3, columns=c("IDType", "Authority", "HolderType", 
                                           "TicketType","SubTicketType",
                                           "Distance", "BDayOfWeek",
                                           "BHour", "BMonth"))
bus2024dt <- read.fst(bus_path3, columns=c("BoardingStopName","DeboardingStopName","Blongitude",
                                           "Blatitude","Dlongitude","Dlatitude"))
names(bus2024dt)

bus2024dt$SubTicketType[bus2024dt$SubTicketType == ""] <- "Single_Ticket"
unique(bus2024dt$SubTicketType)
table(bus2024dt$SubTicketType)
pct <- round(prop.table(table(bus2024dt$SubTicketType)) * 100,2)
names(pct) <- paste0(names(pct), " (", pct, "%)")
pct

setDT(bus2024dt) 
bus2024dt <- as.data.table(bus2024dt)
filtered_df <- bus2024dt %>%
  filter(SubTicketType %in% c("Single_Ticket", "#NOR-1200","#KEE-288",
                              "#HSZ-1200","#HSZ-799"))
filtered_df <- filtered_df %>%
  filter(HolderType %in% c("A", "B","C01","C02","C09"))

rm(list = setdiff(ls(), "filtered_df"))

fdf <- as.data.frame.matrix(addmargins(table(filtered_df$SubTicketType, filtered_df$HolderType)))

filtered_df <- filtered_df %>%
  mutate(time_slot = paste(BDayOfWeek, BHour, sep = "_"))

grouped_counts <- filtered_df %>%
  group_by(HolderType, SubTicketType, time_slot) %>%
  summarise(count = n(), .groups = "drop")

grouped_counts <- grouped_counts %>%
  group_by(HolderType, SubTicketType) %>%
  mutate(total = sum(count),
         proportion = count / total) %>%
  ungroup()

prop_matrix <- grouped_counts %>%
  select(HolderType, SubTicketType, time_slot, proportion) %>%
  pivot_wider(names_from = time_slot, values_from = proportion, values_fill = list(proportion = 0))

print(prop_matrix)

prop_matrix <- prop_matrix %>%
  mutate(group = paste(HolderType, SubTicketType, sep = "_")) %>%
  relocate(group) %>%   
  column_to_rownames(var = "group")

prop_matrix <- prop_matrix %>% select(-HolderType, -SubTicketType)

prop_matrix_mat <- as.matrix(prop_matrix)

print(prop_matrix)

dist_matrix <- dist((prop_matrix_mat), method = "manhattan")
dist_matrix2 <- dist((prop_matrix_mat), method = "euclidean")
print(as.matrix(dist_matrix))
print(as.matrix(dist_matrix2))

mds_result <- smacofSym(dist_matrix)
mds_result2 <- smacofSym(dist_matrix2)
mds_result_interval <- smacofSym(dist_matrix, type="interval")
mds_result2_interval <- smacofSym(dist_matrix2, type="interval")

perm_result <- permtest(object = mds_result,
                        data = as.matrix(dist_matrix), 
                        method.dat = "manhattan", 
                        nrep = 1000,         
                        verbose = TRUE)
print(perm_result)

perm_result2 <- permtest(object = mds_result2,
                        data = as.matrix(dist_matrix2), 
                        method.dat = "euclidean", 
                        nrep = 1000,         
                        verbose = TRUE)
print(perm_result2)

perm_result2_interval <- permtest(object = mds_result2_interval,
                         data = as.matrix(dist_matrix2), 
                         method.dat = "euclidean", 
                         nrep = 1000,         
                         verbose = TRUE)
print(perm_result2_interval)

plot(mds_result$conf, main = "不同票種使用者在時間行為相似性分析", xlab = "Dimension 1", ylab = "Dimension 2",
     pch = 19, col = "blue", cex = 1.2)
text(mds_result$conf, labels = rownames(mds_result$conf), pos = 3, cex = 0.8)

plot(mds_result2_interval$conf, main = "不同票種使用者在時間行為相似性分析", xlab = "Dimension 1", ylab = "Dimension 2",
     pch = 19, col = "blue", cex = 1.2)
text(mds_result2_interval$conf, labels = rownames(mds_result2_interval$conf), pos = 3, cex = 0.8)

# MDS-SMACOF 組別(持卡身分、票種、上下車發展程度)在距離上的投影
bus2024dt$SubTicketType[bus2024dt$SubTicketType == ""] <- "Single_Ticket"
rm(list = setdiff(ls(), "bus2024dt"))
gc()

unique(bus2024dt$SubTicketType)
table(bus2024dt$SubTicketType)
pct <- round(prop.table(table(bus2024dt$SubTicketType)) * 100,2)
names(pct) <- paste0(names(pct), " (", pct, "%)")
pct

setDT(bus2024dt) 
bus2024dt <- as.data.table(bus2024dt)
names(filtered_df)
head(filtered_df$dev_movement)

filtered_df <- bus2024dt %>%
  filter(SubTicketType %in% c("Single_Ticket", "#NOR-1200","#KEE-288",
                              "#HSZ-1200","#HSZ-799"))
filtered_df <- filtered_df %>%
  filter(HolderType %in% c("A", "B","C01","C02","C09"))

rm(list = setdiff(ls(), "filtered_df"))
gc()

fdf <- as.data.frame.matrix(addmargins(table(filtered_df$SubTicketType, filtered_df$HolderType)))

names(filtered_df)

dt <- as.data.table(filtered_df)

rm(list = setdiff(ls(), "dt"))
gc()

names(dt)

dt[, .N, by = movement_level][order(movement_level)]
dt[, movement_level_abs := abs(movement_level)]
dt[, .N, by = movement_level_abs][order(movement_level_abs)]

quantile_probs <- seq(0, 1, length.out = 101)

group_quantiles_dt <- dt[, .(quant_vals = list(quantile(Distance, 
                                                        probs = quantile_probs, 
                                                        type = 1))),
                         by = .(HolderType, movement_level_abs)]
group_quantiles_dt[, group := paste(HolderType, movement_level_abs, sep = "_")]

quant_mat <- do.call(rbind, group_quantiles_dt$quant_vals)
rownames(quant_mat) <- group_quantiles_dt$group

w2_distance <- function(x, y) {
  sqrt(sum((x - y)^2) / (length(x) - 1))
}

ws_mat <- as.matrix(proxy::dist(quant_mat, method = w2_distance))

mds_result <- smacofSym(ws_mat, type = "interval")

perm_result_interval <- permtest(object = mds_result,
                                  data = as.matrix(ws_mat), 
                                  method.dat = "Wasserstein", 
                                  nrep = 1000,         
                                  verbose = TRUE)
print(perm_result_interval)

plot(mds_result$conf, main = "不同身分使用者在數位發展程度移動距離相似性分析", xlab = "Dimension 1", ylab = "Dimension 2",
     pch = 19, col = "blue", cex = 1.2)
text(mds_result$conf, labels = rownames(mds_result$conf), pos = 3, cex = 0.8)
