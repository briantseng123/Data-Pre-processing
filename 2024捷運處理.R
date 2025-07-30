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
}

base_path <- "E:/brain/解壓縮data"
TPCmrt2024df_input_csv_1_6 <- file.path(base_path, "csv", "2024", "臺北捷運電子票證資料(TO2A)_2024年1~6月\\臺北捷運電子票證資料(TO2A).csv")
TPCmrt2024df_output_parquet_1_6 <- file.path(base_path, "fst", "2024", "2024臺北市捷運1-6月.parquet")
TPCmrt2024df_output_fst_1_6 <- file.path(base_path, "fst", "2024", "2024臺北市捷運1-6月.fst")
TPCmrt2024df_output_fst_1_6_2<- file.path(base_path, "資料處理", "2024", "2024臺北市捷運1-6月(去除異常值)2.fst")
TPCmrt2024df_output_fst_1_6_3_chunkv3 <- file.path(base_path, "資料處理", "2024", "2024臺北市捷運1-6月(加入鄉政市區數位發展分類與氣象站_kriging_v3)chunk")
names(fst("E:/brain/解壓縮data/fst/2024/2024臺北市捷運1-6月.fst"))
nrow(fst(TPCmrt2024df_output_fst_1_6_2))
read_fst("E:/brain/解壓縮data/fst/2024/2024臺北市捷運1-6月.fst",columns = "ExitStationID")%>%
  distinct()%>%arrange(ExitStationID)
read_fst("E:/brain/解壓縮data/fst/2024/2024臺北市捷運7-12月.fst",columns = "ExitStationID")%>%
  distinct()%>%arrange(ExitStationID)

TPCmrt2024df_input_csv_7_12 <- file.path(base_path, "csv", "2024", "臺北捷運電子票證資料(TO2A)_2024年7~12月\\臺北捷運電子票證資料(TO2A).csv")
TPCmrt2024df_output_parquet_7_12 <- file.path(base_path, "fst", "2024", "2024臺北市捷運7-12月.parquet")
TPCmrt2024df_output_fst_7_12 <- file.path(base_path, "fst", "2024", "2024臺北市捷運7-12月.fst")
TPCmrt2024df_output_fst_7_12_2<- file.path(base_path, "資料處理", "2024", "2024臺北市捷運7-12月(去除異常值)2.fst")
TPCmrt2024df_output_fst_7_12_3_chunkv3<- file.path(base_path, "資料處理", "2024", "2024臺北市捷運7-12月(加入鄉政市區數位發展分類與氣象站_kriging_v3)3chunk")
nrow(fst(TPCmrt2024df_output_fst_7_12))
nrow(fst(TPCmrt2024df_output_fst_7_12_2))

NTPmrt2024df_output_fst <- file.path(base_path, "fst", "2024", "2024新北市捷運.fst")
NTPmrt2024df_output_fst2 <- file.path(base_path, "資料處理", "2024", "2024新北市捷運(去除異常值)2.fst")
NTPmrt2024df_output_fst3v3 <- file.path(base_path, "資料處理", "2024", "2024新北市捷運(加入鄉政市區數位發展分類與氣象站_kriging_v3)3.fst")
NTPmrt2024df_output_fst4v3 <- file.path(base_path, "資料處理", "2024", "2024新北市捷運(加入直線距離_kriging_v3)4.fst")
NTPmrt2024df_output_fst5v3 <- file.path(base_path, "資料處理", "2024", "2024新北市捷運(發展程度移動_kriging_v3)5.fst")
names(fst(NTPmrt2024df_output_fst5v3))

rail2024df_output_fst <- "E:/brain/解壓縮data/fst/2024/2024臺鐵.fst"
rail2024df_output_fst2 <- file.path(base_path, "資料處理", "2024", "2024臺鐵(去除異常值)2.fst")
rail2024df_output_fst3 <- file.path(base_path, "資料處理", "2024", "2024臺鐵(加入鄉政市區數位發展分類與氣象站)3.fst")
rail2024df_output_fst4 <- file.path(base_path, "資料處理", "2024", "2024臺鐵(加入直線距離_kriging)4.fst")
rail2024df_output_fst5 <- file.path(base_path, "資料處理", "2024", "2024臺鐵(發展程度移動)5.fst")
rail2024df_output_fst5_truncated <- file.path(base_path, "資料處理", "2024", "2024臺鐵(發展程度移動_truncated)5.fst")

rail2024df <- read_fst(rail2024df_output_fst, as.data.table = TRUE)
rail2024df[as.Date(rail2024df$EntryTime)=="2024-09-21"& hour(rail2024df$EntryTime)==20]

nrow(fst(rail2024df_output_fst))
nrow(fst(rail2024df_output_fst2))
nrow(fst(rail2024df_output_fst3))
nrow(fst(rail2024df_output_fst4))
nrow(fst(rail2024df_output_fst5))
names(fst(rail2024df_output_fst5_truncated))

railstop <- read_fst(rail2024df_output_fst5,
                     columns=c("EntryStationID","EntryStationName","ExitStationID","ExitStationName",
                               "BLatitude","BLongitude","DLatitude","DLongitude"),as.data.table=TRUE)
unique_stops <- unique(
  rbind(
    railstop[, .(StationID = EntryStationID, StationName = EntryStationName,
                 Latitude = BLatitude, Longitude = BLongitude)],
    railstop[, .(StationID = ExitStationID,  StationName = ExitStationName,
                 Latitude = DLatitude, Longitude = DLongitude)]
  )
)
write_parquet(unique_stops,"E:/brain/解壓縮data/資料處理/臺鐵站點資料/全臺臺鐵站點(站碼經緯度).parquet")
nrow(fst(rail2024df_output_fst2))
names(fst(rail2024df_output_fst5))

mrtstop_path <- "E:/brain/解壓縮data/資料處理/交通站點資料/Kriging格點/北台灣捷運站點(加入鄉政市區數位發展分類與Kriging天氣格點).csv"
rail_path <- "E:/brain/解壓縮data/資料處理/交通站點資料/Kriging格點/全臺臺鐵站點(加入鄉鎮市區數位發展分類與Kriging天氣格點).csv"

mrt <- fread(mrtstop_path, encoding = "UTF-8")
rail <- fread(rail_path, encoding = "UTF-8")

mrt <- fread(mrtstop_path_TPC)
mrt <- read_parquet(mrtstop_path)

csventry1_6 <- fread(TPCmrt2024df_input_csv_1_6 , skip = 1, header = TRUE, encoding = "UTF-8", select = "EntryTime")
csventry7_12 <- fread(TPCmrt2024df_input_csv_7_12 , skip = 1, header = TRUE, encoding = "UTF-8", select = "EntryTime")
TPCmrt2024df_fst_1_6_entrytime <- read_fst(TPCmrt2024df_output_fst_1_6v2, columns = "EntryTime", as.data.table = TRUE)
TPCmrt2024df_fst_7_12_entrytime <- read_fst(TPCmrt2024df_output_fst_7_12, columns = "EntryTime", as.data.table = TRUE)

TPCmrt2024df <- read_fst(TPCmrt2024df_output_fst_1_6, as.data.table = TRUE)
TPCmrt2024df[, `:=`(
  EntryTime = EntryTime - hours(8),
  ExitTime  = ExitTime  - hours(8)
)]
write_fst(TPCmrt2024df, TPCmrt2024df_output_fst_1_6v2,compress0)
#檢查分布
{
  files <- list(
    TPCmrt2024df_output_fst_1_6,
    TPCmrt2024df_output_fst_7_12
  )
  
  plot_dt <- rbindlist(
    lapply(files, function(f){
      dt <- read_fst(f, columns = "EntryTime", as.data.table = TRUE)
      
      message(paste("Processing file:", f))
      message("Data type of EntryTime: ", class(dt$EntryTime)[1]) 
      message("First few values of EntryTime:")
      print(head(dt$EntryTime))

      if (is.character(dt$EntryTime)) {
        message("EntryTime is character. Attempting to parse with as.POSIXct().")
        dt[, EntryTimePOSIX := as.POSIXct(EntryTime, format="%Y-%m-%d %H:%M:%S", tz="Asia/Taipei")]
        
        num_na_after_posix <- sum(is.na(dt$EntryTimePOSIX) & !is.na(dt$EntryTime))
        if (num_na_after_posix > 0) {
          message(paste("Warning:", num_na_after_posix, 
                        "EntryTime values could not be parsed into POSIXct and became NA. Examples:"))
          print(head(dt[is.na(EntryTimePOSIX) & !is.na(EntryTime), .(OriginalEntryTime = EntryTime)]))
        }
        
        dt[, EntryTimeNumeric := as.integer(EntryTimePOSIX)]
        
      } else if (is.numeric(dt$EntryTime) || inherits(dt$EntryTime, "POSIXt")) {
        message("EntryTime is already numeric or POSIXt. Converting directly to integer.")
        dt[, EntryTimeNumeric := as.integer(EntryTime)]
      } else {
        message(paste("Unexpected EntryTime type:", class(dt$EntryTime)[1], 
                      ". Attempting direct as.integer() conversion (NAs are likely)."))
        dt[, EntryTimeNumeric := as.integer(EntryTime)] 
      }
      message("Number of NAs in EntryTimeNumeric after conversion: ", sum(is.na(dt$EntryTimeNumeric)))
      if (any(is.na(dt$EntryTimeNumeric)) && sum(is.na(dt$EntryTimeNumeric)) == nrow(dt[!(is.na(EntryTime) | EntryTime == "")])) { # 檢查是否所有非空/非NA的原始值都變NA了
        message("WARNING: All (or most) convertible EntryTime values became NA in EntryTimeNumeric. This suggests a systematic failure in parsing.")
        message("Please double-check the 'format' and 'tz' arguments in as.POSIXct() against your actual data.")
      } else if (any(is.na(dt$EntryTimeNumeric))) {
        message("Some EntryTimeNumeric values are NA. Original EntryTime values that led to NA (first few):")
        print(head(dt[is.na(EntryTimeNumeric), .(OriginalEntryTime = EntryTime)]))
      }
      
      
      t0 <- as.integer(as.POSIXct("2024-01-01 00:00:00", tz = "Asia/Taipei"))
      t1 <- as.integer(as.POSIXct("2025-01-01 00:00:00", tz = "Asia/Taipei"))
      
      dt_filtered <- dt[!is.na(EntryTimeNumeric) & EntryTimeNumeric >= t0 & EntryTimeNumeric < t1]
      
      if (nrow(dt_filtered) == 0) {
        message("No data after filtering for this file.")
        return(data.table(DayIndex = integer(0), Hour = integer(0), Count = integer(0)))
      }
      
      dt_filtered[, `:=`(
        DayIndex = EntryTimeNumeric %/% 86400L, 
        Hour     = (EntryTimeNumeric %% 86400L) %/% 3600L
      )]
      
      dt_filtered[, .(Count = .N), by = .(DayIndex, Hour)]
    }),
    use.names = TRUE, fill = TRUE 
  )
  
  if (nrow(plot_dt) > 0 && !"Count" %in% names(plot_dt)) {
    plot_dt <- data.table(Date = as.Date(character(0)), Hour = integer(0), Count = integer(0))
  } else if (nrow(plot_dt) > 0) {
    plot_dt <- plot_dt[, .(Count = sum(Count, na.rm = TRUE)), by = .(DayIndex, Hour) 
    ][, Date := as.Date("1970-01-01") + DayIndex
    ][, DayIndex := NULL]
  } else { 
    plot_dt <- data.table(Date = as.Date(character(0)), Hour = integer(0), Count = integer(0))
  }
  
  
  
combined_entry <- rbind(TPCmrt2024df_fst_1_6_entrytime, TPCmrt2024df_fst_7_12_entrytime)
combined_entry <- rbind(csventry1_6, csventry7_12)
combined_entry <- rbind(TPC16parquet,TPC712parquet)

combined_entry <- as.data.table(combined_entry)
combined_entry[, Hour := hour(EntryTime)]
combined_entry <- combined_entry[year(EntryTime)==2024]
names(combined_entry)
plot_dt <- combined_entry[, .(Count = .N), by = .(EntryTime, Hour)]

ggplot(plot_dt, aes(x = factor(Hour), y = Count)) +
  geom_col(position = "dodge") +
  scale_x_discrete(breaks = as.character(0:23)) +
  labs(
    title = "2024 年度上下半年 MRT 進站次數時段分布",
    x = "小時 (0–23)",
    y = "進站次數"
  ) +
  theme_minimal()

TPCmrt2024df_fst_1_6_entrytime[, Hour := hour(EntryTime)]
TPCmrt2024df_fst_1_6_entrytime <- as.data.table(TPCmrt2024df_fst_1_6_entrytime)
TPCmrt2024df_fst_1_6_entrytime <- TPCmrt2024df_fst_1_6_entrytime[year(EntryTime)==2024]
plot_dt16 <- TPCmrt2024df_fst_1_6_entrytime[, .(Count = .N), by = .(EntryTime, Hour)]
ggplot(plot_dt16, aes(x = factor(Hour), y = Count)) +
  geom_col(position = "dodge") +
  scale_x_discrete(breaks = as.character(0:23)) +
  labs(
    title = "2024年上半年臺北捷運搭乘人次時段分布(原檔)",
    x = "小時 (0–23)",
    y = "人次"
  ) +
  theme_minimal()
TPCmrt2024df_fst_7_12_entrytime[, Hour := hour(EntryTime)]
TPCmrt2024df_fst_7_12_entrytime <- as.data.table(TPCmrt2024df_fst_7_12_entrytime)
TPCmrt2024df_fst_7_12_entrytime <- TPCmrt2024df_fst_7_12_entrytime[year(EntryTime)==2024]
plot_dt712 <- TPCmrt2024df_fst_7_12_entrytime[, .(Count = .N), by = .(EntryTime, Hour)]
ggplot(plot_dt712, aes(x = factor(Hour), y = Count)) +
  geom_col(position = "dodge") +
  scale_x_discrete(breaks = as.character(0:23)) +
  labs(
    title = "2024年上半年臺北捷運搭乘人次時段分布(原檔)",
    x = "小時 (0–23)",
    y = "人次"
  ) +
  theme_minimal()

csventry7_12[, Hour := hour(EntryTime)]
csventry7_12 <- as.data.table(csventry7_12)
csventry7_12 <- csventry7_12[year(EntryTime)==2024]
plot_dt712 <- csventry7_12[, .(Count = .N), by = .(EntryTime, Hour)]
ggplot(plot_dt712, aes(x = factor(Hour), y = Count)) +
  geom_col(position = "dodge") +
  scale_x_discrete(breaks = as.character(0:23)) +
  labs(
    title = "2024年下半年臺北捷運搭乘人次時段分布(原檔)",
    x = "小時 (0–23)",
    y = "人次"
  ) +
  theme_minimal()

TPC16parquet <- read_parquet(TPCmrt2024df_output_parquet_1_6, col_select = "EntryTime")
TPC712parquet <- read_parquet(TPCmrt2024df_output_parquet_7_12, col_select = "EntryTime")
TPC16parquet <- as.data.table(TPC16parquet)
TPC16parquet[, Hour := hour(EntryTime)]
TPC16parquet <- TPC16parquet[year(EntryTime)==2024]
plot_dt162 <- TPC16parquet[, .(Count = .N), by = .(EntryTime, Hour)]
ggplot(plot_dt162, aes(x = factor(Hour), y = Count)) +
  geom_col(position = "dodge") +
  scale_x_discrete(breaks = as.character(0:23)) +
  labs(
    title = "2024年上半年臺北捷運搭乘人次時段分布",
    x = "小時 (0–23)",
    y = "人次"
  ) +
  theme_minimal()

TPC712parquet <- as.data.table(TPC712parquet)
TPC712parquet[, Hour := hour(EntryTime)]
TPC712parquet <- TPC712parquet[year(EntryTime)==2024]
plot_dt7122 <- TPC712parquet[, .(Count = .N), by = .(EntryTime, Hour)]
ggplot(plot_dt7122, aes(x = factor(Hour), y = Count)) +
  geom_col(position = "dodge") +
  scale_x_discrete(breaks = as.character(0:23)) +
  labs(
    title = "2024年下半年臺北捷運搭乘人次時段分布",
    x = "小時 (0–23)",
    y = "人次"
  ) +
  theme_minimal()
}

#轉成fst
#2024臺北市捷運
checklines <- function(df){
first_line <- readLines(df, n = 1, encoding = "UTF-8")
cat(strsplit(first_line, ",")[[1]], sep = "\n")
second_line <- readLines(df, n = 2, encoding = "UTF-8")
cat(strsplit(second_line, ",")[[2]], sep = "\n")
second_line <- readLines(df, n = 3, encoding = "UTF-8")
cat(strsplit(second_line, ",")[[3]], sep = "\n")
}
checklines(TPCmrt2024df_input_csv_1_6)
checklines(TPCmrt2024df_input_csv_7_12)

readcsvdatatoparquet <- function(df, df_output){
open_dataset(df, format = "csv", skip_rows = 1) |>
  select(
    Authority, ID, IDType, HolderType, TicketType, SubTicketType,
    EntryStationID, EntryStationName, EntryTime,
    ExitStationID, ExitStationName, ExitTime,
    TransferCode, Result
  ) |>
  write_dataset(
    path   = df_output,           
    format = "parquet"             
  )
}
readcsvdatatoparquet(TPCmrt2024df_input_csv_1_6,TPCmrt2024df_output_parquet_1_6)
readcsvdatatoparquet(TPCmrt2024df_input_csv_7_12,TPCmrt2024df_output_parquet_7_12)

head(TPCmrt2024df_1_6)
#write_fst(TPCmrt2024df_1_6, TPCmrt2024df_output_fst_1_6)

parquettofst <- function(df,parquet_input, fst_output){
ds <- open_dataset(parquet_input, format = "parquet")
df <- ds %>%
  collect()
write_fst(df, fst_output, compress = 0)
}
parquettofst(TPCmrt2024df, TPCmrt2024df_output_parquet_1_6, TPCmrt2024df_output_fst_1_6)
parquettofst(TPCmrt2024df, TPCmrt2024df_output_parquet_7_12, TPCmrt2024df_output_fst_7_12)

readLines(TPCmrt2024df_input_csv_1_6, n = 5, encoding="UTF-8")

csv_to_fst <- function(csv_path, fst_output) {
  cols_to_select <- c(
    "Authority", "ID", "IDType", "HolderType", "TicketType", "SubTicketType",
    "EntryStationID", "EntryStationName", "EntryTime",
    "ExitStationID", "ExitStationName", "ExitTime",
    "TransferCode"
  )
  
  df <- data.table::fread(
    file = csv_path,
    header = TRUE,
    skip = 1,
    encoding = "UTF-8",
    data.table = TRUE,
    select = cols_to_select
  )
  write_fst(df,fst_output,compress=0)
    
}
csv_to_fst <- function(csv_path, fst_output) {
  ds <- open_dataset(csv_path, format = "csv", skip_rows = 1)
  
  df <- ds %>%
    select(
      Authority, ID, IDType, HolderType, TicketType, SubTicketType,
      EntryStationID, EntryStationName, EntryTime,
      ExitStationID,  ExitStationName,  ExitTime,
      TransferCode
    ) %>%
    collect() 
  
  setDT(df)
  df[, EntryTime := as.POSIXct(EntryTime, format = "%Y-%m-%d %H:%M:%OS", tz = "Asia/Taipei")]
  df[, ExitTime  := as.POSIXct(ExitTime,  format = "%Y-%m-%d %H:%M:%OS", tz = "Asia/Taipei")]
  
  write_fst(df, fst_output, compress = 0)
}
csv_to_fst(TPCmrt2024df_input_csv_1_6, TPCmrt2024df_output_fst_1_6)
csv_to_fst(TPCmrt2024df_input_csv_7_12, TPCmrt2024df_output_fst_7_12)

rm(list = ls(all.names = TRUE))
gc()

head(TPCmrt2024df)

NTPmrt2024df <- setDT(read.fst(NTPmrt2024df_output_fst))
NTPmrt2024df%>%head()
NTPmrt2024df%>%nrow()

TPCmrt2024df_1_6 <- read.fst(TPCmrt2024df_output_fst_1_6,as.data.table = TRUE)

threads_fst(12)  
timing <- system.time({
  TPCmrt2024df_1_6 <- read_fst(
    TPCmrt2024df_output_fst_1_6,
    as.data.table = TRUE
  )
})
print(timing)

threads_fst(12)  
timing <- system.time({
  TPCmrt2024df_7_12 <- read_fst(
    TPCmrt2024df_output_fst_7_12,
    as.data.table = TRUE
  )
})
print(timing)
gc()

TPCmrt2024df_7_12 <- read.fst(TPCmrt2024df_output_fst_7_12, as.data.table = TRUE)

rail2024df <- read.fst(rail2024df_output_fst ,as.data.table = TRUE)

checkneg99 <- function(df){
char_counts <- df %>%
  summarize(across(
    where(is.character),
    ~ sum(. == "-99", na.rm = TRUE)
  )) %>%
  pivot_longer(
    cols      = everything(),
    names_to  = "variable",
    values_to = "count_neg99"
  )

num_counts <- df %>%
  summarize(across(
    where(is.numeric),
    ~ sum(. == -99, na.rm = TRUE)
  )) %>%
  pivot_longer(
    cols      = everything(),
    names_to  = "variable",
    values_to = "count_neg99"
  )

counts_all <- bind_rows(char_counts, num_counts)

print(counts_all)
}
checkneg99(NTPmrt2024df)
checkneg99(TPCmrt2024df_1_6)
checkneg99(TPCmrt2024df_7_12)
checkneg99(rail2024df)
colSums(is.na(TPCmrt2024df_1_6))
colSums(is.na(TPCmrt2024df_7_12))
colSums(is.na(rail2024df))
nrow(rail2024df)

nrow(rail2024df[rail2024df$EntryStationName==rail2024df$ExitStationName])

cleanproblemmrt <- function(df){
parse_flexible <- function(x) {
  parse_date_time(
    x,
    orders = c(
      "Ymd HMS OS",    # 年-月-日 時:分:秒.毫秒  (OS = optional seconds fraction)
      "Ymd HMS",       # 年-月-日 時:分:秒
      "Ymd HM",        # 年-月-日 時:分
      "Ymd"            # 年-月-日（純日期）
    ),
    tz = "UTC",        # 如有需求可指定時區
  )
}
df <- df %>%
  # 三欄都轉成字串再填 "-99"
  mutate(across(
    c(SubTicketType, TransferCode),
    ~ replace_na(as.character(.), "-99")
  )) %>%
  
  mutate(
    EntryTime = parse_flexible(EntryTime),
    ExitTime  = parse_flexible(ExitTime)
  ) %>%
  
  # 衍生時間欄位
  mutate(
    BDayOfWeek        = weekdays(EntryTime),                                # Monday…Sunday
    BWeekendOrWeekday = if_else(
      wday(EntryTime) %in% c(1, 7),
      "Weekend", 
      "Weekday"
    ),
    BHour    = hour(EntryTime),                                            # 0–23
    BMonth   = month(EntryTime),                                           # 1–12
    BYear    = year(EntryTime),                                            # 年份
    DYear    = year(ExitTime),                                             # 離站年份
    Duration = ExitTime - EntryTime                                       # difftime
  ) %>%
  filter(
    Duration <= dhours(6),
    EntryStationID != "-99",
    EntryStationID != ExitStationID,
    EntryStationName != ExitStationName
  )
}
cleanproblemmrt_dt <- function(df) {
  if (!is.data.table(df)) setDT(df)
  
  df[, SubTicketType := fifelse(
    is.na(SubTicketType), "-99", as.character(SubTicketType)
  )]
  df[, TransferCode := fifelse(
    is.na(TransferCode), "-99", as.character(TransferCode)
  )]
  
  df[, EntryTime := as.POSIXct(EntryTime,
                               format = "%Y-%m-%d %H:%M:%S",
                               tz     = "Asia/Taipei")]
  df[, ExitTime := as.POSIXct(ExitTime,
                              format = "%Y-%m-%d %H:%M:%S",
                              tz     = "Asia/Taipei")]
  
  df[, `:=`(
    BDayOfWeek        = weekdays(EntryTime),
    BWeekendOrWeekday = fifelse(wday(EntryTime) %in% c(1, 7), "Weekend", "Weekday"),
    BHour             = hour(EntryTime),
    BMonth            = month(EntryTime),
    BYear             = year(EntryTime),
    DYear             = year(ExitTime),
    Duration          = difftime(ExitTime, EntryTime, units = "hour")
  )]
  
  keep_idx <- df[
    Duration <= 6 &               
      EntryStationID != "-99" &
      EntryStationID != ExitStationID &
      EntryStationName != ExitStationName,
    .I
  ]
  df <- df[keep_idx]
  
  invisible(df)
}

col_fr=c("Authority","HolderType","TicketType","SubTicketType",
      "EntryStationID","EntryStationName","EntryTime",
      "ExitStationID","ExitStationName","ExitTime","TransferCode")
cleanproblemmrt_dt_opti <- function(df) {
  if (!is.data.table(df)) setDT(df)
  
  df[, SubTicketType := fifelse(is.na(SubTicketType), "-99", as.character(SubTicketType))]
  df[, TransferCode := fifelse(is.na(TransferCode), "-99", as.character(TransferCode))]
  
  df[, EntryTime := as.POSIXct(EntryTime, format = "%Y-%m-%d %H:%M:%S", tz = "Asia/Taipei")]
  df[, ExitTime := as.POSIXct(ExitTime, format = "%Y-%m-%d %H:%M:%S", tz = "Asia/Taipei")]
 
  df[, `:=`(
    BHour             = hour(EntryTime),
    BMonth            = month(EntryTime),
    BYear             = year(EntryTime),
    DYear             = year(ExitTime),
    Duration          = as.numeric(difftime(ExitTime, EntryTime, units = "hours")) # 確保為數值
  )]

  df_filtered <- df[!is.na(Duration) & Duration <= 12 &
                      EntryStationID != "-99" &
                      EntryStationID != ExitStationID &
                      EntryStationName != ExitStationName]
  
  return(df_filtered) 
}
process_fst_chunks_direct <- function(fst_file_path, 
                                      processing_function, 
                                      chunk_size = 10000000L,col=col_fr) {

  if (!file.exists(fst_file_path)) {
    stop(paste("FST 檔案不存在:", fst_file_path))
  }
  if (!grepl("\\.fst$", fst_file_path, ignore.case = TRUE)) {
    warning(paste("輸入檔案可能不是 FST 檔案 (副檔名非 .fst):", fst_file_path))
  }
  if (!requireNamespace("fst", quietly = TRUE)) {
    stop("請先安裝並載入 'fst' 套件: install.packages('fst'); library(fst)")
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
                                   as.data.table = TRUE,
                                   columns = col) 
    
    processed_chunk <- processing_function(current_chunk_data) 
    processed_list[[i]] <- processed_chunk
    gc()
  }
  
  message("所有分塊處理完畢。正在合併結果...")
  final_result <- rbindlist(processed_list, use.names = TRUE, fill = TRUE)
  message("合併完成。")
  
  return(final_result)
}

TPCmrt2024df_1_6 <- process_fst_chunks_direct(TPCmrt2024df_output_fst_1_6,cleanproblemmrt_dt_opti)
TPCmrt2024df_7_12 <- process_fst_chunks_direct(TPCmrt2024df_output_fst_7_12,cleanproblemmrt_dt_opti)
rail2024df <- process_fst_chunks_direct(rail2024df_output_fst ,cleanproblemmrt_dt_opti)


cleanproblemmrt(NTPmrt2024df)
rail2024df <- cleanproblemrail_dt(rail2024df)
nrow(rail2024df)
nrow(fst(TPCmrt2024df_output_fst_1_6))
nrow(fst(TPCmrt2024df_output_fst_1_6_2))
nrow(fst(TPCmrt2024df_output_fst_7_12))
nrow(fst(TPCmrt2024df_output_fst_7_12_2))
write.fst(NTPmrt2024df,NTPmrt2024df_output_fst2)
write.fst(TPCmrt2024df_1_6,TPCmrt2024df_output_fst_1_6_2, compress=0)
write.fst(TPCmrt2024df_7_12,TPCmrt2024df_output_fst_7_12_2, compress=0)
write.fst(rail2024df ,rail2024df_output_fst2, compress=0)
rm(TPCmrt2024df_1_6)
rm(TPCmrt2024df_7_12)
gc()

head(fst(rail2024df_output_fst2))
rail2024df <- read_fst(rail2024df_output_fst2,
                       columns=c("Authority","HolderType","TicketType",
                                 "SubTicketType","EntryStationName","EntryStationID",
                                 "EntryTime","ExitStationName","ExitStationID","ExitTime","TransferCode"), 
                       as.data.table = TRUE)

NTPmrt2024df <- setDT(read.fst(NTPmrt2024df_output_fst2))
names(fst(NTPmrt2024df_output_fst2))
names(mrtstop)
TPCmrt2024df_1_6 <- setDT(read.fst(TPCmrt2024df_output_fst_1_6_2))
names(railstop)

TPCmrtstop <- read_parquet(mrtstop_path) %>%
  mutate(
    MRT_StationID = case_when(
      MRT_StationID == "BL11" ~ "BL11 / G12",
      MRT_StationID == "G09" ~ "G09 / O05",
      MRT_StationID == "G10" ~ "G10 / R08",
      MRT_StationID == "O06" ~ "O06 / R07",
      TRUE                ~ MRT_StationID  
    )
  )
write.csv(TPCmrtstop,mrtstop_path_TPC)

merge_stopuid_fast_chunk_rail <- function(inputfile, stopuid, temp_dir, outputpath, chunk_size = 10000000) {
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
  
  if (!dir.exists(temp_dir)) {
    dir.create(temp_dir)
  }
  
  #result_list <- vector("list", num_chunks)
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
    dt_chunk <- dt_chunk%>%filter(EntryStationName!=ExitStationName)
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
    
    if (nrow(dt_chunk) > 0) {
      chunk_file_path <- file.path(temp_dir, sprintf("chunk_%d.fst", i))
      write_fst(dt_chunk, chunk_file_path)
    }
    
    rm(dt_chunk)
    gc()
  }
  gc()
  cat("[合併區塊] 正在從暫存檔合併所有區塊...\n")
  chunk_files <- list.files(temp_dir, pattern = "chunk_.*\\.fst", full.names = TRUE)
  
  final_dt <- rbindlist(lapply(chunk_files, read_fst, as.data.table = TRUE))
  gc()
  
  cat(nrow(final_dt),"\n")
  cat(sprintf("總共移除缺失資料列數：%d\n", total_removed))
  cat(sprintf("[9/9] 寫出結果至 %s ...\n", outputpath))
  write_fst(as.data.frame(final_dt), outputpath) 
  
  unlink(temp_dir, recursive = TRUE)
  
  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  cat(sprintf("完成！總耗時：%s 秒。\n", elapsed))
  
  #return(final_dt)
}
merge_stopuid_fast_chunk_rail(rail2024df,rail,rail2024df_temp_fst3 ,rail2024df_output_fst3)
head(fst(rail2024df_output_fst3))
names(rail2024df)

merge_stopuid_fast_chunk_dropsamestopname3 <- function(inputfile, stopuid, outputpath, chunk_size = 10000000) {
  library(data.table)
  library(fst)
  
  start_time <- Sys.time()
  cat("[1/9] 載入主資料...\n")
  dt <- as.data.table(inputfile)
  
  cat("[2/9] 處理 stopuid...\n")
  stopuid <- as.data.table(stopuid)
  stopuid[, MRT_StationID := as.character(MRT_StationID)]
  stopuid <- unique(stopuid, by = "MRT_StationID")
  
  cat("[3/9] 建立 stopuid_B / stopuid_D...\n")
  safe_prefix_rename <- function(dt, prefix) {
    cols <- setdiff(names(dt), "MRT_StationID")
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
    setkey(stopuid_B, MRT_StationID)
    dt_chunk[stopuid_B, (B_cols) := mget(paste0("i.", B_cols)), on = .(EntryStationID = MRT_StationID)]
    
    # 合併 Deboarding 標籤資訊
    setkey(dt_chunk, ExitStationID)
    setkey(stopuid_D, MRT_StationID)
    dt_chunk[stopuid_D, (D_cols) := mget(paste0("i.", D_cols)), on = .(ExitStationID = MRT_StationID)]
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
    # 定義「必須 non‑NA」的欄位
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
merge_stopuid_fast_chunk_dropsamestopname3(NTPmrt2024df,mrt,NTPmrt2024df_output_fst3v3)
mega_preprocess_fst <- function(fst_path,
                                stopuid_path,
                                out_dir,
                                final_path,
                                chunk_size = 10000000,
                                compress   = 60) {
  library(data.table)
  library(fst)
  library(fs)
  
  message("[1] 讀取 stopuid …")
  stopuid <- fread(stopuid_path, colClasses = list(character = "MRT_StationID"), encoding = "UTF-8")
  setkey(stopuid, MRT_StationID)
  
  stopuid_B <- copy(stopuid)
  setnames(stopuid_B,
           old = setdiff(names(stopuid_B), "MRT_StationID"),
           new = paste0("B", setdiff(names(stopuid_B), "MRT_StationID")))
  
  stopuid_D <- copy(stopuid)
  setnames(stopuid_D,
           old = setdiff(names(stopuid_D), "MRT_StationID"),
           new = paste0("D", setdiff(names(stopuid_D), "MRT_StationID")))
  
  meta     <- fst::fst.metadata(fst_path)
  n_rows   <- meta$nrOfRows
  parts    <- ceiling(n_rows / chunk_size)
  message("[2] 檔案總列數: ", format(n_rows, big.mark = ","), 
          " | chunk 數: ", parts)
  
  dir_create(out_dir, recurse = TRUE)
  
  for (part in seq_len(parts)) {
    from <- (part - 1) * chunk_size + 1L
    to   <- min(part * chunk_size, n_rows)
    msg <- sprintf("%s | Part %d [%d-%d] 讀檔…",
                   Sys.time(),
                   part, from, to)
    message(msg)
    
    dt <- as.data.table(fst::read_fst(fst_path, from = from, to = to))
    
    dt[stopuid_B, on = .(EntryStationID = MRT_StationID),
       names(stopuid_B)[-1] := mget(paste0("i.", names(stopuid_B)[-1]))]
    dt[stopuid_D, on = .(ExitStationID  = MRT_StationID),
       names(stopuid_D)[-1] := mget(paste0("i.", names(stopuid_D)[-1]))]

    dt[(BStationNameCh != EntryStationName | EntryStationName == "") & 
         !is.na(BStationNameCh), EntryStationName := BStationNameCh]
    dt[(DStationNameCh != ExitStationName  | ExitStationName  == "") & 
         !is.na(DStationNameCh), ExitStationName  := DStationNameCh]
    
    dt <- dt[EntryStationName != ExitStationName]
    dt <- dt[!is.na(BLongitude) & !is.na(BLatitude) &
               !is.na(DLongitude) & !is.na(DLatitude)]
    head(dt)%>%print()
    out_file <- file.path(out_dir, sprintf("chunk_%03d.fst", part))
    fst::write_fst(dt, out_file, compress = compress)

    rm(dt); gc()
  }
  
  message("[4] 串流合併 chunk → ", final_path)
  chunk_files <- dir_ls(out_dir, glob = "*.fst", recurse = FALSE)
  if (!length(chunk_files)) stop("找不到任何 chunk 檔，流程疑似失敗。")
  
  
  first <- TRUE
  for (f in chunk_files) {
    if (first) {
      file_copy(f, final_path, overwrite = TRUE)
      first <- FALSE
    } else {
      fst::write_fst(read_fst(f), final_path, append = TRUE, compress = compress)
    }
  }
  message("✅  DONE.   Rows written: ", format(n_rows, big.mark = ","), 
          " → ", final_path)
}

mega_preprocess_fst(TPCmrt2024df_output_fst_1_6_2,
                    mrtstop_path,
                    TPCmrt2024df_output_fst_1_6_3_chunkv3,
                    TPCmrt2024df_output_fst_1_6_3_v3)
mega_preprocess_fst(TPCmrt2024df_output_fst_7_12_2,
                    mrtstop_path,
                    TPCmrt2024df_output_fst_7_12_3_chunkv3,
                    TPCmrt2024df_output_fst_7_12_3v3)

Fnrow(fst(NTPmrt2024df_output_fst3))
NTPmrt2024df <- read.fst(NTPmrt2024df_output_fst3v3)
colSums(is.na(NTPmrt2024df))
print(NTPmrt2024df[!complete.cases(NTPmrt2024df), ])

NTPmrt2024df <- NTPmrt2024df %>%
  mutate(Distance = distHaversine(
    cbind(BLongitude, BLatitude),  
    cbind(DLongitude, DLatitude)  
  ) / 1000)
names(NTPmrt2024df)
write.fst(NTPmrt2024df,NTPmrt2024df_output_fst4v3)

rail2024df<- read.fst(rail2024df_output_fst3, as.data.table = TRUE)
print(rail2024df[!complete.cases(rail2024df), ])
rail2024df <- rail2024df %>%
  mutate(Distance = distHaversine(
    cbind(BLongitude, BLatitude),  
    cbind(DLongitude, DLatitude)  
  ) / 1000)
write.fst(rail2024df,rail2024df_output_fst4 )

names(fst(NTPmrt2024df_output_fst4))
top_id <- NTPmrt2024df[, .N, by = ID][order(-N)][1, ID]
top_row <- NTPmrt2024df[ID == top_id]
names(fst(NTPmrt2024df_output_fst4v2))
head(fst(NTPmrt2024df_output_fst4v2))
NTPmrt2024df <- data.table(read.fst(NTPmrt2024df_output_fst4v3,
                                    columns=c("Authority","IDType","HolderType","TicketType","SubTicketType",
                                              "EntryStationID","EntryStationName","EntryTime","ExitStationID",
                                              "ExitStationName","ExitTime","TransferCode","Bdevelopment_level",      
                                              "BStationID","Ddevelopment_level","Distance")))
describe(NTPmrt2024df$Distance)

rail2024df <- read_fst(rail2024df_output_fst4, as.data.table=TRUE)
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
NTPmrt2024df <- level_change(NTPmrt2024df)
rail2024df <- level_change(rail2024df)

write.fst(NTPmrt2024df,NTPmrt2024df_output_fst5v3)
write.fst(rail2024df,rail2024df_output_fst5)

rail2024df <- read_fst(rail2024df_output_fst5,as.data.table = TRUE)
head(rail2024df)
names(rail2024df)
rail2024df<- rail2024df[rail2024df$Bcounty_name%in%c("臺北市","新北市","基隆市","桃園市")&
                          rail2024df$Dcounty_name%in%c("臺北市","新北市","基隆市","桃園市")]
write_fst(rail2024df,rail2024df_output_fst5_truncated)

nrow(fst(rail2024df_output_fst5))
nrow(fst(rail2024df_output_fst5_truncated))

#一次做step4 step5
TPCmrt2024_1_6_chunk_dir    <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運1-6月(加入鄉政市區數位發展分類與氣象站_kriging_v3)chunk"              
TPCmrt2024_1_6_final_fst    <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運1-6月(發展程度移動_kriging_v3)5chunk" 
TPCmrt2024_7_12_chunk_dir    <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運7-12月(加入鄉政市區數位發展分類與氣象站_kriging_v3)3chunk"              
TPCmrt2024_7_12_final_fst    <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運7-12月(發展程度移動_kriging_v3)5chunk" 
head(fst("E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運1-6月(發展程度移動_kriging_v3)5chunk/chunk_001.fst"))
head(fst("E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運7-12月(發展程度移動_kriging_v3)5chunk/chunk_001.fst"))


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
lonlatdevelop_fst(TPCmrt2024_1_6_chunk_dir,TPCmrt2024_1_6_final_fst)
lonlatdevelop_fst(TPCmrt2024_7_12_chunk_dir,TPCmrt2024_7_12_final_fst)

nrow(fst(NTPmrt2024df_output_fst))
nrow(fst(NTPmrt2024df_output_fst2))
nrow(fst(NTPmrt2024df_output_fst3))
nrow(fst(NTPmrt2024df_output_fst4))
nrow(fst(NTPmrt2024df_output_fst5))

TPCmrt2024df_1_6 <- setDT(read.fst(TPCmrt2024df_output_fst_1_6_2,columns = c("EntryStationID")))
unique(TPCmrt2024df_1_6$EntryStationID)%>%sort()

merge_and_write_fst <- function(dir_path, vars, out_file) {
  fst_files <- list.files(path = dir_path,
                          pattern = "\\.fst$",
                          full.names = TRUE)
  if (length(fst_files) == 0) {
    stop("目錄中沒有找到任何 .fst 檔案：", dir_path)
  }
  
  dt_list <- lapply(fst_files, function(f) {
    cat("讀取檔案",f,"\n")
    read.fst(f, columns = vars, as.data.table = TRUE)
  })
  cat("合併","\n")
  merged_dt <- rbindlist(dt_list, use.names = TRUE, fill = TRUE)
  cat("輸出","\n")
  write_fst(merged_dt, out_file)
  message("已完成合併並寫出：", out_file)
}
mrt_var_select <- c("Authority","IDType","HolderType","TicketType","SubTicketType",
                    "EntryStationID","EntryStationName","EntryTime","ExitStationID",
                    "ExitStationName","ExitTime","TransferCode","Bdevelopment_level",
                    "BStationID","Ddevelopment_level","Distance",
                    "dev_movement","movement_level")
TPCmrt2024_1_6_final_fst    <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運1-6月(發展程度移動_kriging_v3)5chunk" 
TPCmrt2024_7_12_final_fst    <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運7-12月(發展程度移動_kriging_v3)5chunk" 
TPCmrt2024_1_6_final_fst_path <- "E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/2024臺北市捷運1-6月(發展程度移動_kriging_v3)5.fst"
TPCmrt2024_7_12_final_fst_path    <- "E:/brain/解壓縮data/資料處理/2024/2024公車捷運合併/2024臺北市捷運7-12月(發展程度移動_kriging_v3)5.fst"
TPCmrt2024_final_fst_chunk_path    <- "E:/brain/解壓縮data/資料處理/2024/2024臺北市捷運(發展程度移動_kriging_v3)5"
TPCmrt2024_final_fst_path    <- "E:/brain/解壓縮data/資料處理/2024/2024整年臺北市捷運(發展程度移動_kriging_v3)5.fst"
merge_and_write_fst(TPCmrt2024_1_6_final_fst,mrt_var_select,TPCmrt2024_1_6_final_fst_path)
merge_and_write_fst(TPCmrt2024_7_12_final_fst,mrt_var_select,TPCmrt2024_7_12_final_fst_path)
merge_and_write_fst(TPCmrt2024_final_fst_chunk_path,mrt_var_select,TPCmrt2024_final_fst_path)
