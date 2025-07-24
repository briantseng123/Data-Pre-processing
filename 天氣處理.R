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
library(naniar)
library(MissMech)
library(maps)
library(deldir)
library(sf)
library(sp)
library(gstat)
library(progress)
library(rnaturalearth)
library(rnaturalearthdata)
library(units)
}

combined_daily <- read_fst("E:/brain/解壓縮data/資料處理/天氣資料/有人無人合併後資料2/2024年每小時氣象資料.fst",as.data.table=TRUE)
combined_daily <- read_fst("E:/brain/解壓縮data/資料處理/天氣資料/有人無人合併後資料2/2023年每小時氣象資料.fst",as.data.table=TRUE)
combined_daily <- read_fst("E:/brain/解壓縮data/資料處理/天氣資料/有人無人合併後資料2/2022年每小時氣象資料.fst",as.data.table=TRUE)
uniqueN(
  combined_daily[
    substr(weather_station_ID,1,1) == "4",
    weather_station_ID
  ]
)
uniqueN(
  combined_daily[
    substr(weather_station_ID,1,1) == "C",
    weather_station_ID
  ]
)

#計算Stats2&good_ids
combined_daily <- combined_daily%>%
  rename("datetime"="yyyymmddhh")
setnames(combined_daily, "datetime", "datetime")      
combined_daily[, datetime := as.POSIXct(datetime)]
names(combined_daily)
all_ids <- combined_daily %>%
  pull(weather_station_ID) %>%
  as.character() %>%
  unique()

names(combined_daily)
describe(combined_daily$temperature_c)
describe(combined_daily$wind_speed_m_s)
describe(combined_daily$relative_humidity_percent)
describe(combined_daily$precipitation_mm)
describe(combined_daily$uv_index)


vars <- c(
  "temperature_c",
  "precipitation_mm",
  "wind_speed_m_s",
  "relative_humidity_percent",
  "uv_index"
)

combined_daily[, (vars) := lapply(.SD, as.numeric), .SDcols = vars]

colSums(is.na(combined_daily))


problematic_rows <- combined_daily[na_indices]
library(gridExtra)
sapply(combined_daily[, vars, with = FALSE], class)
do.call(grid.arrange, c(par_list, ncol = 5))

# 定義變數與異常條件
#2024
{
vars_defs <- list(
  temperature_c              = function(x) x < -19 | x > 41,
  precipitation_mm           = function(x) x < 0 | x > 215,
  relative_humidity_percent = function(x) x < 0 | x > 100,
  wind_speed_m_s = function(x) x<0 | x>75,
  uv_index = function(x) x<0
)
special_value_map <- list(
  instr_fail = list(codes = c(-999.1,-99.1), label = "儀器故障待修"),
  data_acc_later = list(codes = c(-9.6,-99.6, -999.6), label = "資料累計於後"),
  no_data_malfunction = list(codes = c(-9.5, -99.5, -999.5, -9999.5, -9995.0), label = "因故障而無資料"),
  no_data_unknown = list(codes = c(-9.7, -99.7, -999.7, -9999.7, -9997.0), label = "因不明原因而無資料"),
  trace_precip = list(codes = c(-9.8), label = "雨跡(Trace)")
)

dt_sub <- combined_daily[ , c("weather_station_ID", names(vars_defs)), with = FALSE]
missing_wide <- dt_sub[ , 
                        lapply(.SD, function(x) sum(is.na(x))) ,
                        by = weather_station_ID,
                        .SDcols = names(vars_defs)
]

# 計算每站異常／遺失率與計數
setorder(combined_daily, weather_station_ID, datetime)
special_cats   <- names(special_value_map)
special_codes  <- lapply(special_value_map, `[[`, "codes")
stats2 <- combined_daily[ , {
  out    <- list()
  n_rows <- .N
  
  for (var in names(vars_defs)) {
    v    <- get(var)
    cond <- vars_defs[[var]]
    
    # 1) 各 special_value 類別：計數 & 比例
    for (cat in special_cats) {
      codes <- special_value_map[[cat]]$codes
      cnt   <- sum(v %in% codes, na.rm = TRUE)
      pct   <- if (n_rows > 0) cnt / n_rows * 100 else 0
      out[[paste(var, cat, "cnt", sep = "_")]] <- cnt
      out[[paste(var, cat, "pct", sep = "_")]] <- pct
    }
    
    # 2) 遺失值
    miss_cnt <- sum(is.na(v))
    miss_pct <- if (n_rows > 0) miss_cnt / n_rows * 100 else 0
    out[[paste(var, "miss_cnt", sep = "_")]] <- miss_cnt
    out[[paste(var, "miss_pct", sep = "_")]] <- miss_pct
    
    # 3) 異常值（排除特殊值 & NA）
    valid_idx <- !is.na(v) & !(v %in% unlist(special_codes))
    anom_cnt  <- sum(cond(v[valid_idx]), na.rm = TRUE)
    anom_pct  <- if (sum(valid_idx) > 0) anom_cnt / sum(valid_idx) * 100 else 0
    out[[paste(var, "anom_cnt", sep = "_")]] <- anom_cnt
    out[[paste(var, "anom_pct", sep = "_")]] <- anom_pct
    
    # 4) 最長「問題時段」（NA／特殊／異常）
    issue     <- is.na(v) | cond(v)
    run_issue <- rleid(issue)
    segs_i    <- data.table(datetime, issue, run = run_issue)[
      issue == TRUE,
      .(start = min(datetime), end = max(datetime)),
      by = run
    ]
    max_issue_h <- if (nrow(segs_i) > 0)
      as.numeric(max(segs_i$end - segs_i$start), units = "hours")
    else 0
    out[[paste(var, "max_issue_hours", sep = "_")]] <- max_issue_h
    
    # 5) 最長「正常持續時段」
    valid     <- !issue
    same      <- valid & (v == shift(v, fill = NA))
    run_pers  <- rleid(same)
    segs_p    <- data.table(datetime, same, run = run_pers)[
      same == TRUE,
      .(start = min(datetime), end = max(datetime)),
      by = run
    ]
    max_persist_h <- if (nrow(segs_p) > 0)
      as.numeric(max(segs_p$end - segs_p$start), units = "hours")
    else 0
    out[[paste(var, "max_persist_hours", sep = "_")]] <- max_persist_h
    
  } 
  
  out
}, by = weather_station_ID]
station_N <- combined_daily[, .N, by = weather_station_ID]
stats2     <- merge(stats2, station_N, by = "weather_station_ID")

# 計算總問題率
rate_cols <- grep("_rate_pct$", names(stats2), value=TRUE)
stats2[, total_issue_rate_pct := rowSums(.SD), .SDcols=rate_cols]

# 分出有人站（ID 開頭 4）與自動站（ID 開頭 C）
manned <- stats2[substr(weather_station_ID,1,1)=="4"]
auto   <- stats2[substr(weather_station_ID,1,1)=="C"]

persistence_limits <- list(
  temperature_c              = 24,
  wind_speed_m_s              = 24,
  precipitation_mm           = Inf,
  uv_index              = 24,
  relative_humidity_percent = 24
)
#各自篩選異常率+缺失率 <= 25%
base_vars_manned <- c("temperature_c","precipitation_mm","wind_speed_m_s","relative_humidity_percent","uv_index")
for (var in base_vars_manned) {
  manned[, paste0(var, "_sum") := get(paste0(var,"_anom_pct")) +
           get(paste0(var,"_miss_pct"))]
}
filtered_manned <- manned[
  Reduce(`&`, lapply(base_vars_manned, function(var)
    get(paste0(var, "_anom_pct")) +
      get(paste0(var, "_miss_pct")) <= 25
  )) &
    Reduce(`&`, lapply(base_vars_manned, function(var)
      get(paste0(var, "_max_issue_hours")) <= 168
    )) &
    Reduce(`&`, lapply(base_vars_manned, function(var)
      get(paste0(var, "_max_persist_hours")) <= persistence_limits[[var]]
    ))
]

base_vars_auto <- c("temperature_c","precipitation_mm","wind_speed_m_s","relative_humidity_percent")
for (var in base_vars_auto) {
  auto[, paste0(var, "_sum") := get(paste0(var,"_anom_pct")) +
         get(paste0(var,"_miss_pct"))]
}
filtered_auto <- auto[
  Reduce(`&`, lapply(base_vars_auto, function(var)
    get(paste0(var, "_anom_pct")) +
      get(paste0(var, "_miss_pct")) <= 25
  )) &
    Reduce(`&`, lapply(base_vars_auto, function(var)
      get(paste0(var, "_max_issue_hours")) <= 168
    )) &
    Reduce(`&`, lapply(base_vars_manned, function(var)
      get(paste0(var, "_max_persist_hours")) <= persistence_limits[[var]]
    ))
]

# 合併 ID，篩出 clean_combined_daily
all_ids <- union(filtered_manned$weather_station_ID,
                 filtered_auto$weather_station_ID)
clean_combined_daily <- combined_daily[weather_station_ID %in% all_ids]

# 剔除筆數 < 8786*0.9 的站
good_ids <- clean_combined_daily[, .N, by=weather_station_ID][N >= 8786*0.9, weather_station_ID]
bad_ids <- clean_combined_daily[, .N, by=weather_station_ID][N < 8786*0.9, weather_station_ID]
clean_combined_filtered <- clean_combined_daily[weather_station_ID %in% good_ids]
clean_combined_filtered
nrow(clean_combined_filtered)
length(good_ids)
uniqueN(
  clean_combined_filtered[
    substr(weather_station_ID,1,1) == "4",
    weather_station_ID
  ]
)
uniqueN(
  clean_combined_filtered[
    substr(weather_station_ID,1,1) == "C",
    weather_station_ID
  ]
)
}
#2023
{
  vars_defs <- list(
    temperature_c              = function(x) x < -19 | x > 41,
    precipitation_mm           = function(x) x < 0 | x > 215,
    relative_humidity_percent = function(x) x < 0 | x > 100,
    wind_speed_m_s = function(x) x<0 | x>75,
    uv_index = function(x) x<0
  )
  
  special_value_map <- list(
    instr_fail = list(codes = c(-9991,-999.1), label = "儀器故障待修"),
    data_acc_later = list(codes = c(-9996,-9.6, -999.6,-9996.0), label = "資料累計於後"),
    no_data_malfunction = list(codes = c(-9997,-9.5, -99.5, -999.5, -9999.5, -9995.0), label = "因不明原因或故障而無資料"),
    no_data_unknown = list(codes = c(-9.7, -99.7, -999.7, -9999.7, -9997.0), label = "因不明原因而無資料"),
    trace_precip = list(codes = c(-9998), label = "雨跡(Trace)"),
    no_obs = list(codes = c(-9999,-9.8,-9998.0), label = "未觀測而無資料")
  )
  
  dt_sub <- combined_daily[ , c("weather_station_ID", names(vars_defs)), with = FALSE]
  missing_wide <- dt_sub[ , 
                          lapply(.SD, function(x) sum(is.na(x))) ,
                          by = weather_station_ID,
                          .SDcols = names(vars_defs)
  ]
  
  # 計算每站異常／遺失率與計數
  setorder(combined_daily, weather_station_ID, datetime)
  special_cats   <- names(special_value_map)
  special_codes  <- lapply(special_value_map, `[[`, "codes")
  stats2 <- combined_daily[ , {
    out    <- list()
    n_rows <- .N
    
    for (var in names(vars_defs)) {
      v    <- get(var)
      cond <- vars_defs[[var]]
      
      # 1) 各 special_value 類別：計數 & 比例
      for (cat in special_cats) {
        codes <- special_value_map[[cat]]$codes
        cnt   <- sum(v %in% codes, na.rm = TRUE)
        pct   <- if (n_rows > 0) cnt / n_rows * 100 else 0
        out[[paste(var, cat, "cnt", sep = "_")]] <- cnt
        out[[paste(var, cat, "pct", sep = "_")]] <- pct
      }
      
      # 2) 遺失值
      miss_cnt <- sum(is.na(v))
      miss_pct <- if (n_rows > 0) miss_cnt / n_rows * 100 else 0
      out[[paste(var, "miss_cnt", sep = "_")]] <- miss_cnt
      out[[paste(var, "miss_pct", sep = "_")]] <- miss_pct
      
      # 3) 異常值（排除特殊值 & NA）
      valid_idx <- !is.na(v) & !(v %in% unlist(special_codes))
      anom_cnt  <- sum(cond(v[valid_idx]), na.rm = TRUE)
      anom_pct  <- if (sum(valid_idx) > 0) anom_cnt / sum(valid_idx) * 100 else 0
      out[[paste(var, "anom_cnt", sep = "_")]] <- anom_cnt
      out[[paste(var, "anom_pct", sep = "_")]] <- anom_pct
      
      # 4) 最長「問題時段」（NA／特殊／異常）
      issue     <- is.na(v) | cond(v)
      run_issue <- rleid(issue)
      segs_i    <- data.table(datetime, issue, run = run_issue)[
        issue == TRUE,
        .(start = min(datetime), end = max(datetime)),
        by = run
      ]
      max_issue_h <- if (nrow(segs_i) > 0)
        as.numeric(max(segs_i$end - segs_i$start), units = "hours")
      else 0
      out[[paste(var, "max_issue_hours", sep = "_")]] <- max_issue_h
      
      # 5) 最長「正常持續時段」
      valid     <- !issue
      same      <- valid & (v == shift(v, fill = NA))
      run_pers  <- rleid(same)
      segs_p    <- data.table(datetime, same, run = run_pers)[
        same == TRUE,
        .(start = min(datetime), end = max(datetime)),
        by = run
      ]
      max_persist_h <- if (nrow(segs_p) > 0)
        as.numeric(max(segs_p$end - segs_p$start), units = "hours")
      else 0
      out[[paste(var, "max_persist_hours", sep = "_")]] <- max_persist_h
      
    } 
    
    out
  }, by = weather_station_ID]
  station_N <- combined_daily[, .N, by = weather_station_ID]
  stats2     <- merge(stats2, station_N, by = "weather_station_ID")
  
  # 計算總問題率
  rate_cols <- grep("_rate_pct$", names(stats2), value=TRUE)
  stats2[, total_issue_rate_pct := rowSums(.SD), .SDcols=rate_cols]
  
  # 分出有人站（ID 開頭 4）與自動站（ID 開頭 C）
  manned <- stats2[substr(weather_station_ID,1,1)=="4"]
  auto   <- stats2[substr(weather_station_ID,1,1)=="C"]
  
  persistence_limits <- list(
    temperature_c              = 24,
    wind_speed_m_s              = 24,
    precipitation_mm           = Inf,
    uv_index              = 24,
    relative_humidity_percent = 24
  )
  #各自篩選異常率+缺失率 <= 25%
  base_vars_manned <- c("temperature_c","precipitation_mm","wind_speed_m_s","relative_humidity_percent","uv_index")
  for (var in base_vars_manned) {
    manned[, paste0(var, "_sum") := get(paste0(var,"_anom_pct")) +
             get(paste0(var,"_miss_pct"))]
  }
  filtered_manned <- manned[
    Reduce(`&`, lapply(base_vars_manned, function(var)
      get(paste0(var, "_anom_pct")) +
        get(paste0(var, "_miss_pct")) <= 25
    )) &
      Reduce(`&`, lapply(base_vars_manned, function(var)
        get(paste0(var, "_max_issue_hours")) <= 168
      )) &
      Reduce(`&`, lapply(base_vars_manned, function(var)
        get(paste0(var, "_max_persist_hours")) <= persistence_limits[[var]]
      ))
  ]
  
  base_vars_auto <- c("temperature_c","precipitation_mm","wind_speed_m_s","relative_humidity_percent")
  for (var in base_vars_auto) {
    auto[, paste0(var, "_sum") := get(paste0(var,"_anom_pct")) +
           get(paste0(var,"_miss_pct"))]
  }
  filtered_auto <- auto[
    Reduce(`&`, lapply(base_vars_auto, function(var)
      get(paste0(var, "_anom_pct")) +
        get(paste0(var, "_miss_pct")) <= 25
    )) &
      Reduce(`&`, lapply(base_vars_auto, function(var)
        get(paste0(var, "_max_issue_hours")) <= 168
      )) &
      Reduce(`&`, lapply(base_vars_manned, function(var)
        get(paste0(var, "_max_persist_hours")) <= persistence_limits[[var]]
      ))
  ]
  
  # 合併 ID，篩出 clean_combined_daily
  all_ids <- union(filtered_manned$weather_station_ID,
                   filtered_auto$weather_station_ID)
  clean_combined_daily <- combined_daily[weather_station_ID %in% all_ids]
  
  # 剔除筆數 < 8786*0.9 的站
  good_ids <- clean_combined_daily[, .N, by=weather_station_ID][N >= 8786*0.9, weather_station_ID]
  bad_ids <- clean_combined_daily[, .N, by=weather_station_ID][N < 8786*0.9, weather_station_ID]
  clean_combined_filtered <- clean_combined_daily[weather_station_ID %in% good_ids]
  clean_combined_filtered
  nrow(clean_combined_filtered)
  length(good_ids)
  uniqueN(
    clean_combined_filtered[
      substr(weather_station_ID,1,1) == "4",
      weather_station_ID
    ]
  )
  uniqueN(
    clean_combined_filtered[
      substr(weather_station_ID,1,1) == "C",
      weather_station_ID
    ]
  )
}
#2022
{
  vars_defs <- list(
    temperature_c              = function(x) x < -19 | x > 41,
    precipitation_mm           = function(x) x < 0 | x > 215,
    relative_humidity_percent = function(x) x < 0 | x > 100,
    wind_speed_m_s = function(x) x<0 | x>75,
    uv_index = function(x) x<0
  )
  
  special_value_map <- list(
    instr_fail = list(codes = c(-9991), label = "儀器故障待修"),
    data_acc_later = list(codes = c(-9996), label = "資料累計於後"),
    no_data_malfunction = list(codes = c(-9997), label = "因不明原因或故障而無資料"),
    trace_precip = list(codes = c(-9998), label = "雨跡(Trace)"),
    no_obs = list(codes = c(-9999), label = "未觀測而無資料")
  )
  
  dt_sub <- combined_daily[ , c("weather_station_ID", names(vars_defs)), with = FALSE]
  missing_wide <- dt_sub[ , 
                          lapply(.SD, function(x) sum(is.na(x))) ,
                          by = weather_station_ID,
                          .SDcols = names(vars_defs)
  ]
  
  # 計算每站異常／遺失率與計數
  setorder(combined_daily, weather_station_ID, datetime)
  special_cats   <- names(special_value_map)
  special_codes  <- lapply(special_value_map, `[[`, "codes")
  stats2 <- combined_daily[ , {
    out    <- list()
    n_rows <- .N
    
    for (var in names(vars_defs)) {
      v    <- get(var)
      cond <- vars_defs[[var]]
      
      # 1) 各 special_value 類別：計數 & 比例
      for (cat in special_cats) {
        codes <- special_value_map[[cat]]$codes
        cnt   <- sum(v %in% codes, na.rm = TRUE)
        pct   <- if (n_rows > 0) cnt / n_rows * 100 else 0
        out[[paste(var, cat, "cnt", sep = "_")]] <- cnt
        out[[paste(var, cat, "pct", sep = "_")]] <- pct
      }
      
      # 2) 遺失值
      miss_cnt <- sum(is.na(v))
      miss_pct <- if (n_rows > 0) miss_cnt / n_rows * 100 else 0
      out[[paste(var, "miss_cnt", sep = "_")]] <- miss_cnt
      out[[paste(var, "miss_pct", sep = "_")]] <- miss_pct
      
      # 3) 異常值（排除特殊值 & NA）
      valid_idx <- !is.na(v) & !(v %in% unlist(special_codes))
      anom_cnt  <- sum(cond(v[valid_idx]), na.rm = TRUE)
      anom_pct  <- if (sum(valid_idx) > 0) anom_cnt / sum(valid_idx) * 100 else 0
      out[[paste(var, "anom_cnt", sep = "_")]] <- anom_cnt
      out[[paste(var, "anom_pct", sep = "_")]] <- anom_pct
      
      # 4) 最長「問題時段」（NA／特殊／異常）
      issue     <- is.na(v) | cond(v)
      run_issue <- rleid(issue)
      segs_i    <- data.table(datetime, issue, run = run_issue)[
        issue == TRUE,
        .(start = min(datetime), end = max(datetime)),
        by = run
      ]
      max_issue_h <- if (nrow(segs_i) > 0)
        as.numeric(max(segs_i$end - segs_i$start), units = "hours")
      else 0
      out[[paste(var, "max_issue_hours", sep = "_")]] <- max_issue_h
      
      # 5) 最長「正常持續時段」
      valid     <- !issue
      same      <- valid & (v == shift(v, fill = NA))
      run_pers  <- rleid(same)
      segs_p    <- data.table(datetime, same, run = run_pers)[
        same == TRUE,
        .(start = min(datetime), end = max(datetime)),
        by = run
      ]
      max_persist_h <- if (nrow(segs_p) > 0)
        as.numeric(max(segs_p$end - segs_p$start), units = "hours")
      else 0
      out[[paste(var, "max_persist_hours", sep = "_")]] <- max_persist_h
      
    } 
    
    out
  }, by = weather_station_ID]
  station_N <- combined_daily[, .N, by = weather_station_ID]
  stats2     <- merge(stats2, station_N, by = "weather_station_ID")
  
  # 計算總問題率
  rate_cols <- grep("_rate_pct$", names(stats2), value=TRUE)
  stats2[, total_issue_rate_pct := rowSums(.SD), .SDcols=rate_cols]
  
  # 分出有人站（ID 開頭 4）與自動站（ID 開頭 C）
  manned <- stats2[substr(weather_station_ID,1,1)=="4"]
  auto   <- stats2[substr(weather_station_ID,1,1)=="C"]
  
  persistence_limits <- list(
    temperature_c              = 24,
    wind_speed_m_s              = 24,
    precipitation_mm           = Inf,
    uv_index              = 24,
    relative_humidity_percent = 24
  )
  #各自篩選異常率+缺失率 <= 25%
  base_vars_manned <- c("temperature_c","precipitation_mm","wind_speed_m_s","relative_humidity_percent","uv_index")
  for (var in base_vars_manned) {
    manned[, paste0(var, "_sum") := get(paste0(var,"_anom_pct")) +
             get(paste0(var,"_miss_pct"))]
  }
  filtered_manned <- manned[
    Reduce(`&`, lapply(base_vars_manned, function(var)
      get(paste0(var, "_anom_pct")) +
        get(paste0(var, "_miss_pct")) <= 25
    )) &
      Reduce(`&`, lapply(base_vars_manned, function(var)
        get(paste0(var, "_max_issue_hours")) <= 168
      )) &
      Reduce(`&`, lapply(base_vars_manned, function(var)
        get(paste0(var, "_max_persist_hours")) <= persistence_limits[[var]]
      ))
  ]
  
  base_vars_auto <- c("temperature_c","precipitation_mm","wind_speed_m_s","relative_humidity_percent")
  for (var in base_vars_auto) {
    auto[, paste0(var, "_sum") := get(paste0(var,"_anom_pct")) +
           get(paste0(var,"_miss_pct"))]
  }
  filtered_auto <- auto[
    Reduce(`&`, lapply(base_vars_auto, function(var)
      get(paste0(var, "_anom_pct")) +
        get(paste0(var, "_miss_pct")) <= 25
    )) &
      Reduce(`&`, lapply(base_vars_auto, function(var)
        get(paste0(var, "_max_issue_hours")) <= 168
      )) &
      Reduce(`&`, lapply(base_vars_manned, function(var)
        get(paste0(var, "_max_persist_hours")) <= persistence_limits[[var]]
      ))
  ]
  
  # 合併 ID，篩出 clean_combined_daily
  all_ids <- union(filtered_manned$weather_station_ID,
                   filtered_auto$weather_station_ID)
  clean_combined_daily <- combined_daily[weather_station_ID %in% all_ids]
  
  # 剔除筆數 < 8786*0.9 的站
  good_ids <- clean_combined_daily[, .N, by=weather_station_ID][N >= 8786*0.9, weather_station_ID]
  bad_ids <- clean_combined_daily[, .N, by=weather_station_ID][N < 8786*0.9, weather_station_ID]
  clean_combined_filtered <- clean_combined_daily[weather_station_ID %in% good_ids]
  clean_combined_filtered
  nrow(clean_combined_filtered)
  length(good_ids)
  uniqueN(
    clean_combined_filtered[
      substr(weather_station_ID,1,1) == "4",
      weather_station_ID
    ]
  )
  uniqueN(
    clean_combined_filtered[
      substr(weather_station_ID,1,1) == "C",
      weather_station_ID
    ]
  )
}

{
  stats <- combined_daily[, {
    n_rows <- .N
    out <- list()
    for (var in names(vars_defs)) {
      v        <- get(var)
      cond     <- vars_defs[[var]]
      n_not_na <- sum(!is.na(v))
      anom_cnt <- sum(cond(v), na.rm=TRUE)
      anom_pct <- if (n_not_na>0) anom_cnt/n_not_na*100 else 0
      miss_cnt <- n_rows - n_not_na
      miss_pct <- miss_cnt/n_rows*100
      
      out[[paste0(var, "_anom_cnt")]]      <- anom_cnt
      out[[paste0(var, "_anom_rate_pct")]] <- anom_pct
      out[[paste0(var, "_miss_cnt")]]      <- miss_cnt
      out[[paste0(var, "_miss_rate_pct")]] <- miss_pct
    }
    out
  }, by=weather_station_ID]
  setorder(combined_daily, weather_station_ID, datetime)
  stats2 <- combined_daily[, {
    
    n_rows <- .N
    out <- list()
    
    for (var in names(vars_defs)) {
      v    <- get(var)
      cond <- vars_defs[[var]]
      
      n_not_na  <- sum(!is.na(v))
      anom_cnt  <- sum(cond(v), na.rm = TRUE)
      anom_pct  <- if (n_not_na > 0) anom_cnt / n_not_na * 100 else 0
      miss_cnt  <- n_rows - n_not_na
      miss_pct  <- miss_cnt / n_rows * 100
      
      out[[paste0(var, "_anom_rate_pct")]] <- anom_pct
      out[[paste0(var, "_miss_rate_pct")]] <- miss_pct
      out[[paste0(var, "_anom_cnt")]]      <- anom_cnt
      out[[paste0(var, "_miss_cnt")]]      <- miss_cnt
      
      issue    <- is.na(v) | cond(v)
      run_issue <- rleid(issue)
      segs_i   <- data.table(datetime, issue, run = run_issue)[
        issue == TRUE,
        .(start = min(datetime), end = max(datetime)),
        by = run
      ]
      max_issue_h <- if (nrow(segs_i) > 0) {
        as.numeric(max(segs_i$end - segs_i$start), units = "hours")
      } else {
        0
      }
      out[[paste0(var, "_max_issue_hours")]] <- max_issue_h
      
      valid   <- !is.na(v)
      same    <- valid & (v == shift(v, fill = NA))
      run_pers <- rleid(same)
      segs_p  <- data.table(datetime, same, run = run_pers)[
        same == TRUE,
        .(start = min(datetime), end = max(datetime)),
        by = run
      ]
      max_persist_h <- if (nrow(segs_p) > 0) {
        as.numeric(max(segs_p$end - segs_p$start), units = "hours")
      } else {
        0
      }
      out[[paste0(var, "_max_persist_hours")]] <- max_persist_h
    }
    
    out
  }, by = weather_station_ID]
  
  setorder(combined_daily, weather_station_ID, datetime)
  special_cats   <- names(special_value_map)
  special_codes  <- lapply(special_value_map, `[[`, "codes")
}

nrow(clean_combined_filtered)
length(good_ids)
uniqueN(
  clean_combined_filtered[
    substr(weather_station_ID,1,1) == "4",
    weather_station_ID
  ]
)
uniqueN(
  clean_combined_filtered[
    substr(weather_station_ID,1,1) == "C",
    weather_station_ID
  ]
)

write.fst(clean_combined_filtered,"E:/brain/解壓縮data/資料處理/天氣資料/2024每小時天氣資料(篩選後v2)2.fst")
write.fst(clean_combined_filtered,"E:/brain/解壓縮data/資料處理/天氣資料/2023每小時天氣資料(篩選後v2)2.fst")
write.fst(clean_combined_filtered,"E:/brain/解壓縮data/資料處理/天氣資料/2022每小時天氣資料(篩選後v2)2.fst")

combined_daily_filtered <- read_fst("E:/brain/解壓縮data/資料處理/天氣資料/2024每小時天氣資料(篩選後v2)2.fst",as.data.table = TRUE)
combined_daily_filtered <- read_fst("E:/brain/解壓縮data/資料處理/天氣資料/2023每小時天氣資料(篩選後v2)2.fst",as.data.table = TRUE)
combined_daily_filtered <- read_fst("E:/brain/解壓縮data/資料處理/天氣資料/2022每小時天氣資料(篩選後v2)2.fst",as.data.table = TRUE)

unique(
  combined_daily[!(weather_station_ID %in% good_ids)][
    , weather_station_ID
  ]
)
# 比較篩選前和篩選後
{
  names(stats2)
  stats2[, type := ifelse(substr(weather_station_ID,1,1)=="4","manned","auto")]
  orig_stats2     <- stats2
  filtered_stats2 <- stats2[weather_station_ID %in% good_ids]
  
  anom_cols <- grep("_anom_pct$", names(stats2), value = TRUE)
  miss_cols <- grep("_miss_pct$", names(stats2), value = TRUE)
  acc_later_cols <- grep("_data_acc_later_pct$", names(stats2), value = TRUE)
  instr_fail_cols <- grep("_instr_fail_pct$", names(stats2), value = TRUE)
  no_data_unknown_cols <- grep("_no_data_unknown_pct$", names(stats2), value = TRUE)
  no_data_malfunction_cols <- grep("_no_data_malfunction_pct$", names(stats2), value = TRUE)
  trace_precip_cols <- grep("_trace_precip_pc$", names(stats2), value = TRUE)
}
clean_combined_filtered %>%
  filter(temperature_c  < 0) %>% 
  pull(temperature_c ) %>%       
  unique() 

pre_type <- function(cols){
orig_stats2[
  , lapply(.SD, function(p) weighted.mean(p, w = N, na.rm = TRUE)),
  by      = type,
  .SDcols = cols
]
}
pre_type(anom_cols)
pre_type(miss_cols)
pre_type(acc_later_cols)
pre_type(instr_fail_cols)
pre_type(no_data_unknown_cols)
pre_type(no_data_malfunction_cols)
pre_type(trace_precip_cols)

post_type <- function(cols){
  filtered_stats2[
    , lapply(.SD, function(p) weighted.mean(p, w = N, na.rm = TRUE)),
    by      = type,
    .SDcols = cols
  ]
}
post_type(anom_cols)
post_type(miss_cols)
post_type(acc_later_cols)
post_type(instr_fail_cols)
post_type(no_data_unknown_cols)
post_type(no_data_malfunction_cols)
post_type(trace_precip_cols)

metrics <- list(
  temperature_c = c(
    instr_fail           = "temperature_c_instr_fail_pct",
    data_acc_later       = "temperature_c_data_acc_later_pct",
    no_data_malfunction  = "temperature_c_no_data_malfunction_pct",
    no_data_unknown      = "temperature_c_no_data_unknown_pct",
    miss                 = "temperature_c_miss_pct",
    anom                 = "temperature_c_anom_pct"
  ),
  precipitation_mm = c(
    instr_fail           = "precipitation_mm_instr_fail_pct",
    data_acc_later       = "precipitation_mm_data_acc_later_pct",
    no_data_malfunction  = "precipitation_mm_no_data_malfunction_pct",
    no_data_unknown      = "precipitation_mm_no_data_unknown_pct",
    miss                 = "precipitation_mm_miss_pct",
    anom                 = "precipitation_mm_anom_pct"
  )
)

build_summary <- function(cols){
  pre <- orig_stats2[
    , lapply(.SD, function(p) weighted.mean(p, w = N, na.rm=TRUE)),
    by      = type,
    .SDcols = cols
  ]
  
  post <- filtered_stats2[
    , lapply(.SD, function(p) weighted.mean(p, w = N, na.rm=TRUE)),
    by      = type,
    .SDcols = cols
  ]
  
  setorder(pre,  type)
  setorder(post, type)
  
  dt <- data.table(
    metric      = names(cols),
    pre_manned  = as.numeric(pre[type=="manned",  ..cols]),
    post_manned = as.numeric(post[type=="manned", ..cols]),
    pre_auto    = as.numeric(pre[type=="auto",    ..cols]),
    post_auto   = as.numeric(post[type=="auto",   ..cols])
  )
  return(dt)
}

build_summary(metrics$temperature_c)
build_summary(metrics$precipitation_mm)

names(clean_combined_filtered)
clean_combined_filtered %>%
  filter(substr(weather_station_ID, 1, 1) == "4") %>%
  pull(weather_station_ID) %>%
  unique() %>%
  length()

clean_combined_filtered %>%
  filter(substr(weather_station_ID, 1, 1) == "C") %>%
  pull(weather_station_ID) %>%
  unique() %>%
  length()

names(stats2)

table(clean_combined_filtered$weather_station_ID)
print(bad_ids)

#繪圖比較篩選前和篩選後
weather_station_info_path <- "E:/brain/解壓縮data/資料處理/天氣資料/氣象站點/all_weather_stations_info.fst"
weather_station_info <- as.data.table(read_fst(weather_station_info_path))
names(weather_station_info)
weather_station_info <- weather_station_info %>%
  mutate(
    StationLongitude = as.numeric(as.character(StationLongitude)),
    StationLatitude  = as.numeric(as.character(StationLatitude))
  )
weather_station_info <- weather_station_info[StationID %in% all_ids]
filtered_weather_station_info <- weather_station_info[StationID %in% good_ids]
nrow(filtered_weather_station_info)
# 繪圖
names(weather_station_info)
plot_compare_stations <- function(all_data, filtered_data,
                                  id_prefix = NULL,
                                  id_title = NULL,
                                  region = "Taiwan",
                                  point_size = 2,
                                  col_all = "red",
                                  col_filtered = "blue",
                                  title_all = NULL,
                                  title_filtered = NULL) {
  map_df <- map_data("world", region = region)
  
  if (!is.null(id_prefix)) {
    all_data     <- all_data     %>% filter(startsWith(StationID, id_prefix))
    filtered_data<- filtered_data%>% filter(startsWith(StationID, id_prefix))
  }
  
  if (is.null(title_all)) {
    title_all <- if (is.null(id_prefix)) "所有氣象站" else paste0("所有", id_title, "氣象站")
  }
  if (is.null(title_filtered)) {
    title_filtered <- if (is.null(id_prefix)) "篩選後氣象站" else paste0("篩選後", id_title, "氣象站")
  }
  
  p1 <- ggplot() +
    geom_polygon(data = map_df, aes(long, lat, group = group),
                 fill = "gray90", color = "black") +
    geom_point(data = all_data,
               aes(x = StationLongitude, y = StationLatitude),
               color = col_all, size = point_size) +
    coord_quickmap() +
    theme_minimal() +
    labs(title = title_all, x = NULL, y = NULL)
  
  p2 <- ggplot() +
    geom_polygon(data = map_df, aes(long, lat, group = group),
                 fill = "gray90", color = "black") +
    geom_point(data = filtered_data,
               aes(x = StationLongitude, y = StationLatitude),
               color = col_filtered, size = point_size) +
    coord_quickmap() +
    theme_minimal() +
    labs(title = title_filtered, x = NULL, y = NULL)
  
  grid.arrange(p1, p2, ncol = 2)
}
plot_compare_stations(weather_station_info,
                      filtered_weather_station_info)
plot_compare_stations(weather_station_info,
                      filtered_weather_station_info,
                      id_prefix = "4","有人")
plot_compare_stations(weather_station_info,
                      filtered_weather_station_info,
                      id_prefix = "C","無人")

# 填補氣象站異常值、遺失值
good_ids <- combined_daily_filtered%>%
  dplyr::select(weather_station_ID)%>%
  distinct()
weather_station_info_path <- "E:/brain/解壓縮data/資料處理/天氣資料/氣象站點/all_weather_stations_info.fst"
weather_station_info <- as.data.table(read_fst(weather_station_info_path))
filtered_weather_station_info <- weather_station_info%>%
  filter(StationID%in%good_ids$weather_station_ID)
names(filtered_weather_station_info)

clean_combined_filtered_path <- "E:/brain/解壓縮data/資料處理/天氣資料/2024每小時天氣資料(篩選後v2)2.fst"
clean_combined_filtered <- as.data.table(read.fst(clean_combined_filtered_path))
names(clean_combined_filtered)

coords <- filtered_weather_station_info %>%
  select(StationID, StationLongitude, StationLatitude)
clean_combined_geo <- combined_daily_filtered %>%
  left_join(coords,
            by = c("weather_station_ID" = "StationID"))

all_special_codes <- unlist(lapply(special_value_map, `[[`, "codes"))

colSums(is.na(clean_combined_geo))
for (var in names(vars_defs)) {
  v <- clean_combined_geo[[var]]
  idx <- vars_defs[[var]](v) | (v %in% all_special_codes)
  for (i in which(idx)) {
    set(clean_combined_geo, i = i, j = var, value = NA)
  }
}
colSums(is.na(clean_combined_geo))


fill_idw_missing_and_anomaly <- function(df, vars_defs, params,
                                         crs_in = 4326, crs_out = 32651,
                                         nmax = 8) {
  df2 <- df
  for (var in names(vars_defs)) {
    cond <- vars_defs[[var]] | special_value_map[[var]]
    df2[[var]][ cond(df2[[var]]) ] <- NA
  }

  df2 <- df2 %>% mutate(datetime = as.POSIXct(datetime))
  
  total_to_fill <- sum(sapply(names(vars_defs), function(var) {
    sum(is.na(df2[[var]]))
  }))
  
  pb <- progress_bar$new(
    format = "  填補進度 [:bar] :percent (已填補 :current/:total 筆)",
    total  = total_to_fill,
    clear  = FALSE, width = 60
  )
  df_filled <- df2 %>%
    group_by(datetime) %>%
    group_modify(~ {
      dat <- .x
    
      sf_pts <- dat %>%
        st_as_sf(coords = c("StationLongitude","StationLatitude"), crs = crs_in) %>%
        st_transform(crs_out)
      sp_pts <- as(sf_pts, "Spatial")
      
      for (var in names(vars_defs)) {
        missing_idx <- which(is.na(sp_pts[[var]]))
        to_fill_n   <- length(missing_idx)
        if (to_fill_n == 0) next
        known   <- sp_pts[!is.na(sp_pts[[var]]), ]
        missing <- sp_pts[ is.na(sp_pts[[var]]), ]
        if (nrow(known) == 0) next
        
        if (nrow(missing) > 0 && nrow(known) > 0) {
          idw_out <- idw(
            formula   = as.formula(paste(var, "~1")),
            locations = known,
            newdata   = missing,
            idp       = params[[var]]$p,
            nmax      = nmax,
            maxdist   = params[[var]]$radius
          )
          dat[[var]][ is.na(dat[[var]]) ] <- idw_out@data$var1.pred
          pb$tick(to_fill_n)
        }
      }
      dat
    }) %>%
    ungroup()
  
  return(df_filled)
}

vars_defs <- list(
  temperature_c              = function(x) x < -19  | x > 41,
  wind_speed_m_s             = function(x) x < 0    | x > 75,
  max_instant_wind_speed_m_s = function(x) x < 0    | x > 96,
  precipitation_mm           = function(x) x < 0    | x > 215,
  visibility_km              = function(x) x < 0,
  uv_index                   = function(x) x < 0
)

params <- list(
  temperature_c              = list(radius = 20000, p = 2),
  wind_speed_m_s             = list(radius = 20000, p = 2),
  max_instant_wind_speed_m_s = list(radius = 20000, p = 2),
  precipitation_mm           = list(radius = 20000, p = 2),
  visibility_km              = list(radius = 20000, p = 2),
  uv_index                   = list(radius = 20000, p = 2)
)

filled_data <- fill_idw_missing_and_anomaly(
  df        = clean_combined_geo,
  vars_defs = vars_defs,
  params    = params
)
colSums(is.na(filled_data))
colSums(is.na(clean_combined_geo))
names(filled_data)
filled_data_sub <- filled_data %>%
  select(datetime, weather_station_ID, temperature_c, wind_speed_m_s,
         max_instant_wind_speed_m_s, precipitation_mm, visibility_km,
         uv_index)

filled_data_path <- "E:/brain/解壓縮data/資料處理/天氣資料/2024每小時氣象資料(DTW補值v2)2.fst"
write.fst(filled_data_sub,filled_data_path)

clean_combined_geo_path <- "E:/brain/解壓縮data/資料處理/天氣資料/2024每小時氣象資料(將特殊值轉為NA_v3)2.fst"
clean_combined_geo_path <- "E:/brain/解壓縮data/資料處理/天氣資料/2023每小時氣象資料(將特殊值轉為NA_v3)2.fst"
clean_combined_geo_path <- "E:/brain/解壓縮data/資料處理/天氣資料/2022每小時氣象資料(將特殊值轉為NA_v3)2.fst"

write.fst(clean_combined_geo,clean_combined_geo_path)

# 劃分Voronoi Polygon
filtered_weather_station_info_path <- "E:/brain/解壓縮data/資料處理/天氣資料/氣象站點/篩選後氣象站點.fst"
filtered_weather_station_info <- as.data.table(read.fst(filtered_weather_station_info_path))
filtered_weather_station_info%>%
  count(StationID)%>%
  filter(n>1)

tw_moi_sf <- st_read("E:/brain/解壓縮data/資料處理/縣市經緯度shp/OFiles_46d6e741-0817-439f-9c21-de305a8c3ff9/COUNTY_MOI_1140318.shp",
                     options = "ENCODING=UTF-8")  
# 檢查 CRS，並先轉成 WGS84 (EPSG:4326)
st_crs(tw_moi_sf)
tw_wgs84 <- st_transform(tw_moi_sf, 4326)

# 合併成單一本島多邊形
island_main <- st_union(tw_wgs84)
island_main_sf <- st_sf(geometry = island_main, crs = 4326)

# 轉投影到 TWD97 / TM2 (EPSG:3826)
island_main_proj <- st_transform(island_main_sf, 3826)

# 在平面座標上每 200 m 插點以平滑輪廓
island_main_dense <- st_segmentize(
  island_main_proj,
  dfMaxLength = set_units(200, "m")
)
nrow(filtered_weather_station_info)
# 讀取並轉換氣象站點至 sf
pts_sf   <- st_as_sf(
  filtered_weather_station_info,
  coords = c("StationLongitude","StationLatitude"),
  crs    = 4326,
  remove = FALSE
)
pts_proj <- st_transform(pts_sf, 3826)

# 在平面投影下計算 Voronoi
multi_pts      <- st_union(pts_proj)
vor_all_proj   <- st_voronoi(multi_pts)
vor_polys_proj <- st_collection_extract(vor_all_proj, "POLYGON")


# 把每塊 Voronoi cell 和 StationID 對應
vor_sf <- st_sf(geometry = vor_polys_proj, crs = 3826)
vor_proj_corrected <- st_join(vor_sf, pts_proj)


# 裁切到本島範圍
vor_clip_proj <- st_intersection(vor_proj_corrected, island_main_dense)

# 加密 Voronoi 邊界
vor_clip_dense <- st_segmentize(
  vor_clip_proj,
  dfMaxLength = set_units(200, "m")
)

all_ids      <- pts_proj$StationID
clipped_ids  <- vor_clip_proj$StationID
missing_ids <- setdiff(all_ids, clipped_ids)
length(missing_ids)       
filtered_weather_station_info[StationID%in%missing_ids]

vor_final <- st_collection_extract(vor_clip_proj, "POLYGON")
st_write(
  vor_final,            
  "E:/brain/解壓縮data/資料處理/天氣資料/天氣站點Voronoi Polygon/天氣站點Voronoi_Polygon.gpkg",       
  delete_layer = TRUE        
)
# 上下界
{
bbjitao <- tw_moi_sf[ tw_moi_sf$COUNTYNAME %in% 
                        c("臺北市","新北市","基隆市","桃園市"), ]

# 用 WGS84 座標系算出經緯度範圍
bbox_wgs84 <- st_bbox(bbjitao)
print(bbox_wgs84)
# 先轉投影再算
bbjitao_3826 <- st_transform(bbjitao, 3826)
bbox_3826 <- st_bbox(bbjitao_3826)
print(bbox_3826)
tw_main_cnty_3826 <- st_transform(tw_main_cnty, 3826)
st_bbox(tw_main_cnty_3826)
}
# 繪圖
ggplot() +
  geom_sf(
    data  = island_main_dense,
    fill  = "gray90",
    color = "black"
  ) +
  geom_sf(
    data      = vor_final,
    aes(fill = StationID),
    color     = "red",
    alpha     = 0.3,
    show.legend = FALSE
  ) +
  geom_sf(
    data = pts_proj,
    color = "blue",
    size  = 1
  ) +
  coord_sf(
    crs   = 3826,
    xlim  = c(130000, 362000),
    ylim  = c(2400000, 2800000),
    expand = FALSE
  ) +
  theme_minimal() +
  labs(
    title = "全臺氣象站",
    x     = "Longitude",
    y     = "Latitude"
  )
ggplot() +
  geom_sf(
    data  = island_main_dense,
    fill  = "gray90",
    color = "black"
  ) +
  geom_sf(
    data      = vor_final,
    aes(fill = StationID),
    color     = "red",
    alpha     = 0.3,
    show.legend = FALSE
  ) +
  geom_sf(
    data = pts_proj,
    color = "blue",
    size  = 1
  ) +
  coord_sf(
    crs   = 3826,
    xlim  = c(248000, 362000),
    ylim  = c(2710000, 2800000),
    expand = FALSE
  ) +
  theme_minimal() +
  labs(
    title = "北北基桃地區氣象站",
    x     = "Longitude",
    y     = "Latitude"
  )

# 確認Voronoi Polygon劃分
vor_readback <- st_read(
  "E:/brain/解壓縮data/資料處理/天氣資料/天氣站點Voronoi Polygon/天氣站點Voronoi_Polygon.shp",
  options = "ENCODING=UTF-8"
)

print(vor_readback)
st_crs(vor_readback)
unique(st_geometry_type(vor_readback))

#補值
weather2024_path <- "E:/brain/解壓縮data/資料處理/天氣資料/2024每小時氣象資料(將特殊值轉為NA_v3)2.fst"
weather2024_output_path <- "E:/brain/解壓縮data/資料處理/天氣資料/2024每小時氣象資料(加上警報_將特殊值轉為NA_v3)3.fst"
weather2024 <- as.data.table(read.fst(weather2024_path))
weather2024%>%head()

filtered_weather_station_info_path <- "E:/brain/解壓縮data/資料處理/天氣資料/氣象站點/篩選後氣象站點.fst"
filtered_weather_station_info <- as.data.table(read.fst(filtered_weather_station_info_path))
filtered_weather_station_info_ID <- filtered_weather_station_info$StationID
altitude_weather_station_info_path <- "E:/brain/解壓縮data/資料處理/天氣資料/氣象站點/all_weather_stations_info_altitude.parquet"
altitude_weather_station_info <- as.data.table(read_parquet(altitude_weather_station_info_path))
names(altitude_weather_station_info)
filtered_altitude_weather_station_info <- altitude_weather_station_info[StationID%in%filtered_weather_station_info_ID]

altitude <- filtered_altitude_weather_station_info %>%
  select(StationID, StationAltitude)
names(weather2024)
weather2024_altitude <- weather2024 %>%
  left_join(altitude,
            by = c("weather_station_ID" = "StationID"))

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
names(weather2024)
#station_pressure_hpa <- describegroup(weather2024, "station_pressure_hpa", "weather_station_ID")
temperature_c <- describegroup(weather2024, "temperature_c", "weather_station_ID")
#relative_humidity_percent <- describegroup(weather2024, "relative_humidity_percent", "weather_station_ID")
#vapor_pressure_hpa <- describegroup(weather2024, "vapor_pressure_hpa", "weather_station_ID")
wind_speed_m_s <- describegroup(weather2024, "wind_speed_m_s", "weather_station_ID")
#max_avg_wind_speed_m_s <- describegroup(weather2024, "max_avg_wind_speed_m_s", "weather_station_ID")
max_instant_wind_speed_m_s <- describegroup(weather2024, "max_instant_wind_speed_m_s", "weather_station_ID")
precipitation_mm <- describegroup(weather2024, "precipitation_mm", "weather_station_ID")
#precipitation_duration_hr <- describegroup(weather2024, "precipitation_duration_hr", "weather_station_ID")
visibility_km <- describegroup(weather2024, "visibility_km", "weather_station_ID")
uv_index <- describegroup(weather2024, "uv_index", "weather_station_ID")


names(weather2024)
weather_warning <- function(df, 
                            id_col      = "weather_station_ID",
                            time_col    = "datetime",
                            wind_col    = "wind_speed_m_s",
                            gust_col    = "max_instant_wind_speed_m_s",
                            temp_col    = "temperature_c",
                            rain_col    = "precipitation_mm",
                            vis_col     = "visibility_km",
                            uv_col      = "uv_index") {
  dt <- as.data.table(df)
  Sys.setlocale("LC_CTYPE", "UTF-8")
  # 必要欄位檢查
  required <- c(id_col, time_col, wind_col, gust_col, temp_col, 
                rain_col, vis_col, uv_col)
  miss <- setdiff(required, names(dt))
  if (length(miss)) stop("缺少欄位：", paste(miss, collapse = ", "))
  
  setkeyv(dt, c(id_col, time_col))  # 便於 by= 分組後 rolling
  
  # 風速：先轉為蒲福風級
  beaufort_cut <- c(
    0, 1.6, 3.4, 5.5, 8, 10.8, 13.9, 17.2,     # 0–7 級
    20.8, 24.5, 28.5, 32.7, 37.0, 41.5, 46.2,  # 8–14 級
    51.0, 56.1, 61.2, Inf                      # 15–17+
  )
  dt[, `:=`(
    bf_avg  = findInterval(get(wind_col), beaufort_cut) - 1L,
    bf_gust = findInterval(get(gust_col), beaufort_cut) - 1L
  )]
  
  dt[, wind_alert := fcase(
    bf_avg  >= 12 | bf_gust >= 14, "紅色燈號",
    bf_avg  >=  9 | bf_gust >= 11, "橙色燈號",
    bf_avg  >=  6 | bf_gust >=  8, "黃色燈號",
    default = NA_character_
  )]
  
  # 高溫警報（38°C / 36°C；72 h = 3 天）
  dt[, hi38 := get(temp_col) >= 38]
  dt[, hi36 := get(temp_col) >= 36]
  dt[, `:=`(
    hi38_3d = frollapply(hi38, 72, function(x) all(x), fill=FALSE, align="right"),
    hi36_3d = frollapply(hi36, 72, function(x) all(x), fill=FALSE, align="right")
  ), by = id_col]
  
  dt[, high_temp_alert := fcase(
    hi38_3d,                     "紅色燈號",
    hi38 | hi36_3d,              "橙色燈號",
    hi36,                        "黃色燈號",
    default = NA_character_
  )]
  
  # 低溫警報（<=6℃ / <=10℃；24 h）
  dt[, min24 := frollapply(get(temp_col), 24, min,
                           fill = Inf, align = "right"),
     by = id_col]
  dt[, all_le12 := frollapply(get(temp_col) <= 12, 24, all,
                              fill = FALSE, align = "right"),
     by = id_col]
  dt[, all_le6 := frollapply(get(temp_col) <= 6, 24, all,
                              fill = FALSE, align = "right"),
     by = id_col]
  dt[, low_temp_alert := fcase(
    all_le6,                        "紅色燈號",
    (get(temp_col) <= 6) | (min24 <= 10 & all_le12), "橙色燈號",
    get(temp_col) <= 10,                       "黃色燈號",
    default = NA_character_            
  )]
  
  dt[, lo6  := get(temp_col) <= 6]
  dt[, lo10 := get(temp_col) <= 10]
  dt[, `:=`(
    lo6_24  = frollapply(lo6,  24, all,  fill = FALSE, align = "right"),
    mean24  = frollmean(get(temp_col), 24, na.rm = TRUE, align = "right")
  ), by = id_col]
  
  dt[, low_temp_alert := fcase(
    lo6_24,                                          "紅色燈號",
    lo6 | (lo10 & mean24 <= 12),                     "橙色燈號",
    lo10,                                            "黃色燈號",
    default = NA_character_
  )]
  
  # 豪雨特報（80 / 200 / 350 / 500 mm；3 h / 24 h）
  dt[, `:=`(
    rain_3h  = frollsum(get(rain_col),  3, na.rm = TRUE, align = "right"),
    rain_24h = frollsum(get(rain_col), 24, na.rm = TRUE, align = "right")
  ), by = id_col]
  
  dt[, rain_alert := fcase(
    rain_24h >= 500,                                     "超大豪雨",
    rain_24h >= 350 | rain_3h >= 200,                    "大豪雨",
    rain_24h >= 200 | rain_3h >= 100,                    "豪雨",
    rain_24h >=  80 | get(rain_col) >= 40,               "大雨",
    default = NA_character_
  )]
  
  # 紫外線分級
  dt[, uv_level := fcase(
    get(uv_col) <= 2,  "低量級",
    get(uv_col) <= 5,  "中量級",
    get(uv_col) <= 7,  "高量級",
    get(uv_col) <= 10, "過量級",
    get(uv_col) > 10,  "危險級",
    default = NA_character_
  )]
  
  # 濃霧特報（能見度 < 0.2 km）
  dt[, fog_alert := get(vis_col) < 0.2]
  
  # 清理暫存欄位
  dt[, c("bf_avg", "bf_gust", "hi38", "hi36", "hi38_3d",
         "hi36_3d", "min24", "all_le6","all_le12",
         "rain_3h", "rain_24h") := NULL]
  
  return(dt[])
}
weather_warning2 <- function(df,
                            id_col   = "weather_station_ID",
                            time_col = "datetime",
                            wind_col = "wind_speed_m_s",
                            gust_col = "max_instant_wind_speed_m_s",
                            temp_col = "temperature_c",
                            alti_col = "StationAltitude",
                            rain_col = "precipitation_mm",
                            vis_col  = "visibility_km",
                            uv_col   = "uv_index") {
  
  require(data.table)
  dt <- as.data.table(copy(df))                    
  
  # sanity checks 
  req <- c(id_col, time_col, wind_col, gust_col,
           temp_col, alti_col, rain_col, vis_col, uv_col)
  miss <- setdiff(req, names(dt))
  if (length(miss)) stop("缺少欄位: ", paste(miss, collapse = ", "))
  
  setkeyv(dt, c(id_col, time_col))
  
  # 風速
  beaufort <- c(0, 1.6, 3.4, 5.5, 8, 10.8, 13.9, 17.2,
                20.8, 24.5, 28.5, 32.7, 37.0, 41.5,
                46.2, 51.0, 56.1, 61.2, Inf)
  
  dt[, `:=`(
    bf_avg  = findInterval(get(wind_col),  beaufort) - 1L,
    bf_gust = findInterval(get(gust_col),  beaufort) - 1L
  )]
  
  dt[, wind_alert := fcase(
    bf_avg >= 12 | bf_gust >= 14, "紅色燈號",
    bf_avg >=  9 | bf_gust >= 11, "橙色燈號",
    bf_avg >=  6 | bf_gust >=  8, "黃色燈號",
    default = NA_character_
  )]
  
  # 高溫 
  dt[, `:=`(hi38 = get(temp_col) >= 38,
            hi36 = get(temp_col) >= 36)]
  
  dt[, `:=`(
    hi38_3d = as.logical(frollapply(hi38, 72, all,  fill = FALSE, align = "right")),
    hi36_3d = as.logical(frollapply(hi36, 72, all,  fill = FALSE, align = "right"))
  ), by = id_col]
  
  dt[, high_temp_alert := fcase(
    hi38_3d,               "紅色燈號",
    hi38 | hi36_3d,        "橙色燈號",
    hi36,                  "黃色燈號",
    default = NA_character_
  )]
  
  # 低溫 
  dt[, `:=`(
    lo6  = (get(temp_col) <= 6)  & (get(alti_col) <= 200),
    lo10 = (get(temp_col) <= 10) & (get(alti_col) <= 200)
  )]
  
  dt[, `:=`(
    lo6_24 = as.logical(frollapply(lo6, 24, all, fill = FALSE, align = "right")),

    mean24 = frollmean(
      fifelse(get(alti_col) <= 200, get(temp_col), NA_real_),
      24, na.rm = TRUE, align = "right"
    )
  ), by = id_col]
  
  dt[, low_temp_alert := fcase(
    lo6_24,                              "紅色燈號",
    lo6    | (lo10 & mean24 <= 12),      "橙色燈號",
    lo10,                                "黃色燈號",
    default = NA_character_
  )]
  
  # 降雨 
  dt[, `:=`(
    rain_3h  = frollsum(get(rain_col),  3, na.rm = TRUE, align = "right"),
    rain_24h = frollsum(get(rain_col), 24, na.rm = TRUE, align = "right")
  ), by = id_col]
  
  dt[, rain_alert := fcase(
    rain_24h >= 500,                       "超大豪雨",
    rain_24h >= 350 | rain_3h >= 200,      "大豪雨",
    rain_24h >= 200 | rain_3h >= 100,      "豪雨",
    rain_24h >=  80 | get(rain_col) >= 40, "大雨",
    default = NA_character_
  )]
  
  # 紫外線 & 濃霧 
  dt[, uv_level := fcase(
    get(uv_col) <=  2, "低量級",
    get(uv_col) <=  5, "中量級",
    get(uv_col) <=  7, "高量級",
    get(uv_col) <= 10, "過量級",
    get(uv_col)  > 10, "危險級",
    default = NA_character_
  )]
  
  dt[, fog_alert := get(vis_col) < 0.2]
  
  # cleanup
  dt[, c("bf_avg","bf_gust","hi38","hi36","hi38_3d","hi36_3d",
         "lo6","lo10","lo6_24","mean24",
         "rain_3h","rain_24h") := NULL]
  
  dt[]
}

weather_warning3 <- function(df,
                             id_col   = "weather_station_ID",
                             time_col = "datetime",
                             temp_col = "temperature_c",
                             alti_col = "StationAltitude",
                             rain_col = "precipitation_mm") {
  
  require(data.table)
  dt <- as.data.table(copy(df))                    
  
  # sanity checks 
  req <- c(id_col, time_col,temp_col, alti_col, rain_col)
  miss <- setdiff(req, names(dt))
  if (length(miss)) stop("缺少欄位: ", paste(miss, collapse = ", "))
  
  setkeyv(dt, c(id_col, time_col))
  
  
  # 高溫 
  dt[, `:=`(hi38 = get(temp_col) >= 38,
            hi36 = get(temp_col) >= 36)]
  
  dt[, `:=`(
    hi38_3d = as.logical(frollapply(hi38, 72, all,  fill = FALSE, align = "right")),
    hi36_3d = as.logical(frollapply(hi36, 72, all,  fill = FALSE, align = "right"))
  ), by = id_col]
  
  dt[, high_temp_alert := fcase(
    hi38_3d,               "紅色燈號",
    hi38 | hi36_3d,        "橙色燈號",
    hi36,                  "黃色燈號",
    default = NA_character_
  )]
  
  # 低溫 
  dt[, `:=`(
    lo6  = (get(temp_col) <= 6)  & (get(alti_col) <= 200),
    lo10 = (get(temp_col) <= 10) & (get(alti_col) <= 200)
  )]
  
  dt[, `:=`(
    lo6_24 = as.logical(frollapply(lo6, 24, all, fill = FALSE, align = "right")),
    
    mean24 = frollmean(
      fifelse(get(alti_col) <= 200, get(temp_col), NA_real_),
      24, na.rm = TRUE, align = "right"
    )
  ), by = id_col]
  
  dt[, low_temp_alert := fcase(
    lo6_24,                              "紅色燈號",
    lo6    | (lo10 & mean24 <= 12),      "橙色燈號",
    lo10,                                "黃色燈號",
    default = NA_character_
  )]
  
  # 降雨 
  dt[, `:=`(
    rain_3h  = frollsum(get(rain_col),  3, na.rm = TRUE, align = "right"),
    rain_24h = frollsum(get(rain_col), 24, na.rm = TRUE, align = "right")
  ), by = id_col]
  
  dt[, rain_alert := fcase(
    rain_24h >= 500,                       "超大豪雨",
    rain_24h >= 350 | rain_3h >= 200,      "大豪雨",
    rain_24h >= 200 | rain_3h >= 100,      "豪雨",
    rain_24h >=  80 | get(rain_col) >= 40, "大雨",
    default = NA_character_
  )]
  
  # cleanup
  dt[, c("hi38","hi36","hi38_3d","hi36_3d",
         "lo6","lo10","lo6_24","mean24",
         "rain_3h","rain_24h") := NULL]
  
  dt[]
}

weather2024_2 <- weather_warning2(weather2024_altitude)
weather2024_3 <- weather_warning3(weather2024_altitude)
head(weather2024_2)
names(weather2024_2)

cols <- c( "rain_alert", "high_temp_alert","low_temp_alert")
freq_tbl <- melt(
  weather2024_3,
  measure.vars = cols,
  variable.name = "indicator",
  value.name    = "value"
)[
  , .N, by = .(indicator, value)
][
  order(indicator, -N)
]

print(freq_tbl)

write.fst(weather2024_3, weather2024_output_path)

weather <- as.data.table(read.fst(weather2024_output_path))
length(unique(weather$weather_station_ID))
stopdfpath2 <- "E:/brain/解壓縮data/資料處理/公車站點資料/北北基桃公車站點(加入鄉政市區數位發展分類與氣象站).parquet"
stopdf <- as.data.table(read_parquet(stopdfpath2,  col_select = c("UID", "town_name", "county_name", "development_level",
                                                    "nearest_StationID","nearest_StationName")))
stop_ids <- unique(stopdf[, .(nearest_StationID)])
weather_sub1 <- weather[weather_station_ID %in% stop_ids$nearest_StationID]
names(weather_sub1)
table(weather_sub1$source_file)
NNKT_UniqueID <- as.data.table(unique(weather_sub1$weather_station_ID))
counts <- NNKT_UniqueID[ , .(
  auto = sum(grepl("C", V1)),             
  cwa  = .N - sum(grepl("C", V1))         
)]
print(counts)

length(unique(weather_sub1$datetime))
table(weather_sub1$high_temp_alert)
table(weather_sub1$low_temp_alert)
table(weather_sub1$wind_alert)
table(weather_sub1$rain_alert)
table(weather_sub1$uv_level)
table(weather_sub1$fog_alert)

table(weather_sub1$weather_station_ID)

# 公車站點
busstop_path <- "E:/brain/解壓縮data/資料處理/公車站點資料/北北基桃公車站點(加入鄉政市區數位發展分類).parquet"
busstop <- as.data.table(read_parquet(busstop_path))
busstop <- busstop[county_name%in%c("新北市","臺北市","桃園市","基隆市")]
busstop_sf <- st_as_sf(
  busstop,
  coords = c("longitude", "latitude"),
  crs    = 4326,
  remove = FALSE       
)

weather_voronoi <- "E:/brain/解壓縮data/資料處理/天氣資料/天氣站點Voronoi Polygon/天氣站點Voronoi_Polygon.gpkg"
vor_polygons <- st_read(weather_voronoi) 
vor_polygons <- st_transform(vor_polygons, 4326)

names(vor_polygons)
busstop_with_station <- st_join(
  busstop_sf,
  vor_polygons[, "StationID"],  
  join = st_intersects,
  left = TRUE
)
head(busstop_with_station)
colSums(is.na(busstop_with_station))
busstop_df <- st_drop_geometry(busstop_with_station)
colSums(is.na(busstop_df))
busstopcheck <- busstop_df%>%
  left_join(filtered_weather_station_info, by = c("StationID" = "StationID"))

busstop_df <- busstop_df%>%
  dplyr::select(all_of(c("StopName","longitude","latitude","UID",
                             "development_level","StationID")))
busstop_df_path <- "E:/brain/解壓縮data/資料處理/公車站點資料/北北基桃公車站點(加入鄉政市區數位發展分類與氣象站_Voronoi_v3).parquet"
write_parquet(busstop_df,busstop_df_path)

# 捷運站點
mrtstop_path <- "E:/brain/解壓縮data/資料處理/捷運站點資料/北台灣捷運站點(加入鄉政市區數位發展分類).parquet"
mrtstop <- as.data.table(read_parquet(mrtstop_path))
names(mrtstop)
mrtstop <- mrtstop %>%
  rename(
    MRT_StationID=StationID
  )
mrtstop_sf <- st_as_sf(
  mrtstop,
  coords = c("Longitude", "Latitude"),
  crs    = 4326,
  remove = FALSE       
)
weather_voronoi <- "E:/brain/解壓縮data/資料處理/天氣資料/天氣站點Voronoi Polygon/天氣站點Voronoi_Polygon.gpkg"
vor_polygons <- st_read(weather_voronoi) 
vor_polygons <- st_transform(vor_polygons, st_crs(mrtstop_sf))
names(vor_polygons)
mrtstop_with_station <- st_join(
  mrtstop_sf,
  vor_polygons[, "StationID"],  
  join = st_intersects,
  left = TRUE
)
head(mrtstop_with_station)
colSums(is.na(mrtstop_with_station))
mrtstop_df <- st_drop_geometry(mrtstop_with_station)
mrtstopcheck <- mrtstop_df%>%
  left_join(filtered_weather_station_info, by = c("StationID" = "StationID"))

colSums(is.na(mrtstop_df))
mrtstop_df <- mrtstop_df%>%
  dplyr::select(all_of(c("MRT_StationID","StationNameCh","Latitude","Longitude",
                                           "development_level","StationID")))
mrtstop_df_path <- "E:/brain/解壓縮data/資料處理/捷運站點資料/北台灣捷運站點(加入鄉政市區數位發展分類與氣象站_voronoi_v3).parquet"
write_parquet(mrtstop_df,mrtstop_df_path)

# 臺鐵站點
railstop_path <- "E:/brain/解壓縮data/資料處理/臺鐵站點資料/全臺臺鐵站點(加入鄉鎮市區數位發展分類).parquet"
railstop <- as.data.table(read_parquet(railstop_path))
names(railstop)
railstop_sf <- st_as_sf(
  railstop,
  coords = c("Longitude", "Latitude"),
  crs    = 4326,
  remove = FALSE       
)
weather_voronoi <- "E:/brain/解壓縮data/資料處理/天氣資料/天氣站點Voronoi Polygon/天氣站點Voronoi_Polygon.gpkg"
vor_polygons <- st_read(weather_voronoi) 
vor_polygons <- st_transform(vor_polygons, st_crs(railstop_sf))
names(vor_polygons)
railstop_with_station <- st_join(
  railstop_sf,
  vor_polygons[, "StationID"],  
  join = st_intersects,
  left = TRUE
)
head(railstop_with_station)
colSums(is.na(railstop_with_station))
railstop_df <- st_drop_geometry(railstop_with_station)
colSums(is.na(railstop_df))
railstopcheck <- railstop_df%>%
  left_join(filtered_weather_station_info, by = c("StationID" = "StationID"))

railstop_df <- railstop_df%>%
  dplyr::select(all_of(c("StopName","Latitude","Longitude","county_name",
                                           "development_level","StationID")))
rail_df_path <- "E:/brain/解壓縮data/資料處理/臺鐵站點資料/全臺臺鐵站點(加入鄉政市區數位發展分類與氣象站_voronoi_v3).parquet"
write_parquet(railstop_df,rail_df_path)

#繪圖
weather2024_output_path <- "E:/brain/解壓縮data/資料處理/天氣資料/2024每小時氣象資料(加上警報DTW v2)3.fst"
dt <- read.fst(weather2024_output_path,as.data.table = TRUE)
freq_dt <- dt[, .N, by = .(temperature_c)][order(temperature_c)]
ggplot(dt, aes(x = temperature_c)) +
  geom_histogram(bins = 30, fill = "#D55E00", color = "white") +
  labs(title = "天氣站溫度直方圖",
       x     = "溫度 (°C)",
       y     = "數量") +
  theme_minimal()
