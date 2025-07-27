library(fst)
library(data.table)
library(dplyr)
library(lattice)
library(gstat)
library(geoR)
library(sp)
library(sf)
library(rnaturalearth)
library(rnaturalearthdata)
library(devtools)
library(parallel)
library(doParallel)
library(progress)
library(progressr)
library(tidyverse) 
library(stars)     
library(viridis)
library(stars)
library(magick)
library(av)
library(arrow)
library(tidyr) 
library(lubridate)

# A5000
{
  df <- read_fst("E:/brain/解壓縮data/資料處理/天氣資料/2024每小時氣象資料(將特殊值轉為NA_v3)2.fst",as.data.table = TRUE)
  df <- read_fst("E:/brain/解壓縮data/資料處理/天氣資料/2023每小時氣象資料(將特殊值轉為NA_v3)2.fst",as.data.table = TRUE)
  df <- read_fst("E:/brain/解壓縮data/資料處理/天氣資料/2022每小時氣象資料(將特殊值轉為NA_v3)2.fst",as.data.table = TRUE)
}
# PC
{
  df <- read_fst("F:/淡江/研究實習生/天氣資料/2024每小時氣象資料(將特殊值轉為NA_v3)2.fst",as.data.table = TRUE)
  df <- read_fst("F:/淡江/研究實習生/天氣資料/2023每小時氣象資料(將特殊值轉為NA_v3)2.fst",as.data.table = TRUE)
  df <- read_fst("F:/淡江/研究實習生/天氣資料/2022每小時氣象資料(將特殊值轉為NA_v3)2.fst",as.data.table = TRUE)
}

df$StationLongitude <- as.numeric(df$StationLongitude)
df$StationLatitude  <- as.numeric(df$StationLatitude)
coordinates(df) <- ~ StationLongitude + StationLatitude
proj4string(df) <- CRS("+proj=longlat +datum=WGS84 +no_defs")
df_utm <- spTransform(df, CRS("+proj=utm +zone=51 +datum=WGS84 +units=m +no_defs"))

# 載入台灣邊界 (Natural Earth) 並投影
#devtools::install_github("ropensci/rnaturalearthhires")
taiwan_ll <- ne_countries(scale = "large", country = "Taiwan", returnclass = "sf")
taiwan_utm <- st_transform(taiwan_ll, crs = st_crs(df_utm))
taiwan_sp <- as(taiwan_utm, "Spatial")

taiwan_islands <- taiwan_utm %>%
  st_cast("POLYGON")

# --- 計算每個獨立島嶼的面積，並找出最大的那個 ---
taiwan_main_island <- taiwan_islands %>%
  mutate(area = st_area(.)) %>% 
  arrange(desc(area)) %>%      
  slice(1)      

# 建立 5km 等距網格
grid_sf <- st_make_grid(
  taiwan_main_island,     
  cellsize = 5000,  
  what = "centers" 
)

# 轉成完整的 sf 物件
grid_sf <- st_sf(geometry = grid_sf)

#匯入交通工具站點
{
  railstop <- read_parquet("F:/淡江/研究實習生/台鐵站位資訊/全臺臺鐵站點(加入鄉鎮市區數位發展分類).parquet")
  busstop <- read_fst("F:/淡江/研究實習生/公車站位資訊/站牌、站位、組站位/北北基桃站群(添加鄉政市區&發展程度)3.fst")
  mrtstop <- read_parquet("F:/淡江/研究實習生/捷運站位資訊/北台灣捷運站點(加入鄉政市區數位發展分類).parquet")
  rail_stop_sf_ll <- st_as_sf(railstop, 
                              coords = c("Longitude", "Latitude"), 
                              crs = 4326, remove = FALSE)
  bus_stop_sf_ll <- st_as_sf(busstop, 
                              coords = c("Longitude", "Latitude"), 
                              crs = 4326, remove = FALSE)
  mrt_stop_sf_ll <- st_as_sf(mrtstop, 
                              coords = c("Longitude", "Latitude"), 
                              crs = 4326, remove = FALSE)
  
  #將經緯度座標轉換成UTM
  rail_stops_sf_utm <- st_transform(rail_stop_sf_ll, crs = st_crs(grid_sf))
  bus_stops_sf_utm <- st_transform(bus_stop_sf_ll, crs = st_crs(grid_sf))
  mrt_stops_sf_utm <- st_transform(mrt_stop_sf_ll, crs = st_crs(grid_sf))
  
  # 為grid_sf加上編號
  if (!"grid_id" %in% names(grid_sf)) {
    grid_sf <- grid_sf %>%
      mutate(grid_id = 1:nrow(.))
  }
  
  #找到屬於站點的編號
  nearest_grid_index_for_rail <- st_nearest_feature(rail_stops_sf_utm, grid_sf)
  nearest_grid_index_for_bus <- st_nearest_feature(bus_stops_sf_utm, grid_sf)
  nearest_grid_index_for_mrt <- st_nearest_feature(mrt_stops_sf_utm, grid_sf)
  
  # 站點網格對應表
  rail_stop_to_grid_map <- rail_stops_sf_utm %>%
    mutate(
      weather_grid_id = grid_sf$grid_id[nearest_grid_index_for_rail])%>% 
    dplyr::select(StopName,county_name,development_level,geometry,weather_grid_id)%>%
    st_drop_geometry()
  
  bus_stop_to_grid_map <- bus_stops_sf_utm %>%
    mutate(
      weather_grid_id = grid_sf$grid_id[nearest_grid_index_for_bus]
    ) %>%
    st_drop_geometry()
  mrt_stop_to_grid_map <- mrt_stops_sf_utm %>%
    mutate(
      weather_grid_id = grid_sf$grid_id[nearest_grid_index_for_mrt]
    ) %>%
    st_drop_geometry()
  
  #輸出
  write_csv(rail_stop_to_grid_map,"F:/淡江/研究實習生/台鐵站位資訊/全臺臺鐵站點(加入鄉鎮市區數位發展分類與Kriging天氣格點).csv")
  write_csv(bus_stop_to_grid_map,"F:/淡江/研究實習生/公車站位資訊/站牌、站位、組站位/北北基桃公車站群(添加鄉政市區&發展程度與Kriging天氣格點)3.csv")
  write_csv(mrt_stop_to_grid_map,"F:/淡江/研究實習生/捷運站位資訊/北台灣捷運站點(加入鄉政市區數位發展分類與Kriging天氣格點).csv")
}

#kriging grid_id
kriging <- read_rds("F:/淡江/研究實習生/天氣資料/天氣Kriging資料/2022天氣站格點(Kriging).rds")
first_hour_key <- names(kriging)[1]
first_var_key <- names(kriging[[first_hour_key]])[1]
grid_total_points <- nrow(kriging[[first_hour_key]][[first_var_key]])
grid_ids_vector <- 1:grid_total_points
for (h_key in names(kriging)) {
  if (length(kriging[[h_key]]) > 0) {
    for (v_key in names(kriging[[h_key]])) {
      kriging[[h_key]][[v_key]]@data$grid_id <- grid_ids_vector
    }
  }
}

grid_utm_from_sf <- as(grid_sf, "Spatial")

df_utm_sf <- st_as_sf(df_utm)

# 台灣grid劃分
ggplot() +
  geom_sf(data = grid_sf, color = "grey80", size = 0.5) +
  geom_sf(data = taiwan_main_island, fill = NA, color = "black") +
  geom_sf(data = df_utm_sf, color = "red", size = 1.5) +
  labs(
    title = "網格覆蓋範圍檢查",
    subtitle = "新網格 (灰色) 完整覆蓋台灣，而原始資料點 (紅色) 則沒有"
  ) +
  theme_bw()

df@data <- df@data %>%
  mutate(datetime = as.POSIXct(datetime),
         hour = format(datetime, "%Y-%m-%d %H"))

df_utm@data$datetime <- df@data$datetime
df_utm@data$hour     <- df@data$hour

all_hours <- sort(unique(df_utm$hour))
names(df_utm)
vars <- c("temperature_c", "relative_humidity_percent", "wind_speed_m_s",
          "precipitation_mm","uv_index") 
# 初始化儲存容器
filled_results <- list()
#non parallel
for (h in all_hours) {
  message("Processing hour: ", h)
  sub_hr <- df_utm[df_utm$hour == h, ]
  if (nrow(sub_hr) == 0) next
  
  h_str <- gsub("[: ]", "_", h)
  filled_results[[h_str]] <- list()
  
  for (v in vars) {
    if (!v %in% names(sub_hr@data)) {
      warning("Variable ", v, " not found in data columns. Skipping.")
      next
    }
    
    obs <- sub_hr[!is.na(sub_hr[[v]]), ]
    if (nrow(obs) < 5) {
      warning("Hour ", h, ", var ", v, ": <5 observations, skip.")
      next
    }
    
    coords_obs <- coordinates(obs)
    dmat <- sp::spDists(coords_obs, longlat = FALSE)
    max_dist <- max(dmat, na.rm = TRUE) / 2
    
    vario_obj <- variog(data = obs@data[[v]], coords = coords_obs, max.dist = max_dist)
    reml  <- likfit(data = obs@data[[v]], coords = coords_obs,
                    ini.cov.pars = c(var(obs@data[[v]]), max_dist), fix.nug = TRUE,
                    lik.met = "REML")
    
    vgm_cloud <- variogram(as.formula(paste0(v, " ~ 1")), obs)
    vgm_fit   <- fit.variogram(vgm_cloud,
                               vgm(psill = var(obs@data[[v]]), model = "Exp",
                                   range = max_dist, nugget = 0))
    
    # Kriging 整張 5km 網格
    kr <- krige(as.formula(paste0(v, " ~ 1")), locations = obs,
                newdata = grid_utm_from_sf, model = vgm_fit)
    filled_results[[h_str]][[v]] <- kr
  }
}

saveRDS(filled_results,"E:/brain/解壓縮data/資料處理/天氣資料/Kriging 天氣站格點/2024天氣站格點(Kriging).rds")
saveRDS(filled_results,"E:/brain/解壓縮data/資料處理/天氣資料/Kriging 天氣站格點/2023天氣站格點(Kriging).rds")
saveRDS(filled_results,"E:/brain/解壓縮data/資料處理/天氣資料/Kriging 天氣站格點/2022天氣站格點(Kriging).rds")
filled_results <- readRDS("E:/brain/解壓縮data/資料處理/天氣資料/Kriging 天氣站格點/2022天氣站格點(Kriging).rds")
filled_results <- readRDS("F:/淡江/研究實習生/天氣資料/天氣Kriging資料/2024天氣站格點(Kriging).rds")
filled_results <- readRDS("F:/淡江/研究實習生/天氣資料/天氣Kriging資料/2023天氣站格點(Kriging).rds")
filled_results <- readRDS("F:/淡江/研究實習生/天氣資料/天氣Kriging資料/2022天氣站格點(Kriging).rds")

filled_results_clean <- purrr::compact(filled_results)
names(filled_results_clean)

hourly_ranges_df <- imap_dfr(filled_results_clean, ~{
  imap_dfr(.x, ~{
    pred_range <- range(.x@data$var1.pred, na.rm = TRUE)
    tibble(min_pred = pred_range[1], max_pred = pred_range[2])
  }, .id = "variable")
}, .id = "hour")
global_ranges_df <- hourly_ranges_df %>%
  group_by(variable) %>%
  summarise(global_min = min(min_pred, na.rm = TRUE), global_max = max(max_pred, na.rm = TRUE)) %>%
  ungroup()

#檢查filled_result結果
{
  complete_grid <- expand_grid(
    hour_str = all_hours,
    variable = vars
  )
  
  # 檢查 filled_results 中是否存在對應的結果
  status_list <- sapply(1:nrow(complete_grid), function(i) {
    h_key <- gsub("[: ]", "_", complete_grid$hour_str[i])
    v_key <- complete_grid$variable[i]
    !is.null(filled_results[[h_key]][[v_key]])
  })
  status_df <- complete_grid %>%
    mutate(
      kriging_status = ifelse(status_list, "Success", "Missing"),
      datetime = ymd_hms(hour_str)
    )
}

plot_kriging_map <- function(v_to_plot, h_to_plot, 
                             kriging_results, global_ranges, map_outline) {
  
  # --- 建立一個變數名稱與中文標籤的對照表 ---
  label_map <- c(
    "temperature_c" = "溫度 (°C)",
    "relative_humidity_percent" = "相對溼度 (%)",
    "wind_speed_m_s" = "風速 (m/s)",
    "precipitation_mm" = "降雨量 (mm)",
    "uv_index" = "紫外線指數"
  )
  # 使用對照表來取得正確的標籤，如果找不到，就用原始變數名稱
  v_label <- ifelse(v_to_plot %in% names(label_map), label_map[v_to_plot], v_to_plot)
  
  h_key <- gsub("[: ]", "_", h_to_plot)
  var_limits <- dplyr::filter(global_ranges, variable == v_to_plot)
  
  if (nrow(var_limits) == 1 && h_key %in% names(kriging_results)) {
    
    min_val <- var_limits$global_min
    max_val <- var_limits$global_max
    kr_result <- kriging_results[[h_key]][[v_to_plot]]
    
    if (is.null(kr_result) || length(kr_result) == 0) {
      return(invisible(NULL)) 
    }
    
    # === 關鍵修正 1：穩健的資料準備，強制命名座標 ===
    plot_data_values <- kr_result@data
    plot_coords <- as.data.frame(sp::coordinates(kr_result))
    names(plot_coords)[1:2] <- c("x", "y") # 強制命名
    plot_df <- dplyr::bind_cols(plot_data_values, plot_coords) %>%
      dplyr::rename(prediction = var1.pred)
    
    p <- ggplot2::ggplot() +
      ggplot2::geom_tile(
        data = plot_df, 
        ggplot2::aes(x = x, y = y, fill = prediction),
        width = 5000, height = 5000
      ) +
      ggplot2::geom_sf(data = map_outline, fill = NA, color = "black", linewidth = 0.5) +
      viridis::scale_fill_viridis(
        option = "plasma", 
        name = v_label, # 直接使用 v_label 作為圖例名稱
        limits = c(min_val, max_val),
        na.value = "transparent"
      ) +
      # === 關鍵修正 2：使用修正後的 v_label ===
      ggplot2::labs(
        title = paste("Kriging -", v_label),
        subtitle = paste("時間:", h_to_plot)
      ) +
      ggplot2::theme_void() +
      ggplot2::theme(
        plot.background = ggplot2::element_rect(fill = "white", color = NA),
        plot.title = ggplot2::element_text(hjust = 0.5, size = 16, face = "bold"),
        plot.subtitle = ggplot2::element_text(hjust = 0.5, size = 12),
        legend.title = ggplot2::element_text(size = 10) # 讓圖例標題清楚些
      ) +
      ggplot2::coord_sf(datum = sf::st_crs(map_outline))
    
    return(p) # 注意：在迴圈中儲存時，我們只回傳物件，不 print
    
  } else {
    message("找不到資料: ", h_to_plot, " for variable ", v_to_plot)
    return(invisible(NULL))
  }
}
create_kriging_gif <- function(target_date, v_to_plot, 
                               kriging_results, global_ranges, map_outline,
                               output_filename = "kriging_animation.gif",
                               fps = 2) { # fps = 每秒顯示的圖片張數
  
  # a. 建立一個暫存資料夾來存放單張圖片
  temp_dir <- file.path(tempdir(), "kriging_frames")
  if (!dir.exists(temp_dir)) {
    dir.create(temp_dir)
  } else {
    # 清理舊的圖片
    unlink(file.path(temp_dir, "*.png"))
  }
  
  message("暫存資料夾建立於: ", temp_dir)
  
  # b. 產生該日期 24 小時的時間點字串
  hourly_timestamps <- paste0(target_date, " ", sprintf("%02d", 0:23))
  
  # c. 迴圈產生並儲存每一張圖片
  frame_files <- c()
  for (i in seq_along(hourly_timestamps)) {
    h <- hourly_timestamps[i]
    message("正在產生圖片: ", h)
    
    # 呼叫舊函數來產生 ggplot 物件
    plot_object <- plot_kriging_map(
      v_to_plot = v_to_plot,
      h_to_plot = h,
      kriging_results = kriging_results,
      global_ranges = global_ranges,
      map_outline = map_outline
    )
    
    # 如果成功產生圖片，就存檔
    if (!is.null(plot_object)) {
      frame_path <- file.path(temp_dir, sprintf("frame_%03d.png", i))
      ggsave(filename = frame_path, plot = plot_object, width = 8, height = 8, dpi = 96)
      frame_files <- c(frame_files, frame_path)
    }
  }
  
  # d. 使用 magick 套件將所有圖片組合成 GIF
  if (length(frame_files) > 0) {
    message("\n所有圖片產生完畢，開始合成 GIF...")
    
    animation <- image_read(frame_files) %>%
      image_animate(fps = fps)
    
    image_write(animation, path = output_filename)
    
    message("GIF 成功儲存至: ", normalizePath(output_filename))
  } else {
    message("沒有任何有效的圖片被產生，無法建立 GIF。")
  }
  
  # e. 清理暫存資料夾
  unlink(temp_dir, recursive = TRUE)
  message("暫存資料夾已清理。")
}

create_kriging_gif_range <- function(start_datetime, end_datetime, v_to_plot, 
                                     kriging_results, global_ranges, map_outline,
                                     output_filename = "kriging_range_animation.gif",
                                     fps = 2) {
  
  # a. 建立暫存資料夾
  temp_dir <- file.path(tempdir(), "kriging_frames")
  if (!dir.exists(temp_dir)) dir.create(temp_dir) else unlink(file.path(temp_dir, "*.png"))
  message("暫存資料夾建立於: ", temp_dir)
  
  # b. *** 關鍵修改：產生指定時間區間內的每小時時間序列 ***
  start_time <- as.POSIXct(start_datetime, tz = "UTC")
  end_time <- as.POSIXct(end_datetime, tz = "UTC")
  
  # 產生從開始時間到結束時間，每隔一小時的時間序列
  time_sequence <- seq(from = start_time, to = end_time, by = "hour")
  
  # 將時間序列格式化成我們需要的字串格式
  hourly_timestamps <- format(time_sequence, "%Y-%m-%d %H")
  
  # c. 迴圈產生並儲存每一張圖片
  frame_files <- c()
  for (i in seq_along(hourly_timestamps)) {
    h <- hourly_timestamps[i]
    message("正在產生圖片: ", h)
    
    plot_object <- plot_kriging_map(
      v_to_plot = v_to_plot, h_to_plot = h,
      kriging_results = kriging_results, global_ranges = global_ranges,
      map_outline = map_outline
    )
    
    if (!is.null(plot_object)) {
      frame_path <- file.path(temp_dir, sprintf("frame_%03d.png", i))
      ggsave(filename = frame_path, plot = plot_object, width = 8, height = 8, dpi = 96)
      frame_files <- c(frame_files, frame_path)
    }
  }
  
  # d. 使用 magick 套件將所有圖片組合成 GIF
  if (length(frame_files) > 0) {
    message("\n所有圖片產生完畢，開始合成 GIF...")
    animation <- image_read(frame_files) %>%
      image_animate(fps = fps)
    image_write(animation, path = output_filename)
    message("GIF 成功儲存至: ", normalizePath(output_filename))
  } else {
    message("沒有任何有效的圖片被產生，無法建立 GIF。")
  }
  
  # e. 清理暫存資料夾
  unlink(temp_dir, recursive = TRUE)
  message("暫存資料夾已清理。")
}

create_kriging_video <- function(start_datetime, end_datetime, v_to_plot, 
                                 kriging_results, global_ranges, map_outline,
                                 output_filename = "kriging_animation.mp4",
                                 framerate = 2) { # framerate = 影格率(幀率)
  
  # a. 建立暫存資料夾 (這部分不變)
  temp_dir <- file.path(tempdir(), "kriging_frames")
  if (!dir.exists(temp_dir)) dir.create(temp_dir) else unlink(file.path(temp_dir, "*.png"))
  message("暫存資料夾建立於: ", temp_dir)
  
  # b. 產生時間序列 (這部分不變)
  start_time <- as.POSIXct(start_datetime, tz = "UTC")
  end_time <- as.POSIXct(end_datetime, tz = "UTC")
  time_sequence <- seq(from = start_time, to = end_time, by = "hour")
  hourly_timestamps <- format(time_sequence, "%Y-%m-%d %H")
  
  # c. 迴圈產生並儲存每一張圖片 (這部分不變)
  frame_files <- c()
  for (i in seq_along(hourly_timestamps)) {
    h <- hourly_timestamps[i]
    message("正在產生圖片: ", h)
    plot_object <- plot_kriging_map(
      v_to_plot = v_to_plot, h_to_plot = h,
      kriging_results = kriging_results, global_ranges = global_ranges,
      map_outline = map_outline
    )
    if (!is.null(plot_object)) {
      frame_path <- file.path(temp_dir, sprintf("frame_%03d.png", i))
      ggsave(filename = frame_path, plot = plot_object, width = 8, height = 8, dpi = 96)
      frame_files <- c(frame_files, frame_path)
    }
  }
  
  # d. *** 關鍵修改：使用 av 套件將所有圖片組合成 MP4 ***
  if (length(frame_files) > 0) {
    message("\n所有圖片產生完畢，開始合成 MP4...")
    
    output_dir <- dirname(output_filename) # 取得輸出路徑的資料夾部分
    # 如果資料夾不存在，就遞迴地建立它
    if (!dir.exists(output_dir)) {
      dir.create(output_dir, recursive = TRUE)
    }
    
    # 呼叫 av_encode_video 來合成影片
    av::av_encode_video(
      input = frame_files,      
      output = output_filename, 
      framerate = framerate     
    )
    
    message("MP4 影片成功儲存至: ", normalizePath(output_filename))
  } else {
    message("沒有任何有效的圖片被產生，無法建立 MP4。")
  }
  
  # e. 清理暫存資料夾 (這部分不變)
  unlink(temp_dir, recursive = TRUE)
  message("暫存資料夾已清理。")
}
target_variable <- "temperature_c"
target_date_str <- "2022-01-01"

plot_kriging_map(
  v_to_plot = target_variable, 
  h_to_plot = target_date_str,
  kriging_results = filled_results_clean, # 使用清理過的 results list
  global_ranges = global_ranges_df,
  map_outline = taiwan_main_island
)

gif_file <- "E:/brain/weather_kriging_mp4/"
gif_file <- "F:/淡江/研究實習生/天氣Kriging_mp4/"
create_kriging_video(
  start_datetime = "2023-01-01 01",
  end_datetime = "2023-12-31 23",
  v_to_plot = "temperature_c",
  kriging_results = filled_results_clean,
  global_ranges = global_ranges_df,
  map_outline = taiwan_main_island,
  output_filename = paste0(gif_file,"temperature_c", "_", "2023", ".mp4"), 
  framerate = 50
)
gc()
create_kriging_video(
  start_datetime = "2023-01-01 01",
  end_datetime = "2023-12-31 23",
  v_to_plot = "relative_humidity_percent",
  kriging_results = filled_results_clean,
  global_ranges = global_ranges_df,
  map_outline = taiwan_main_island,
  output_filename = paste0(gif_file,"relative_humidity_percent", "_", "2023", ".mp4"), 
  framerate = 50 
)
gc()
create_kriging_video(
  start_datetime = "2023-01-01 01",
  end_datetime = "2023-12-31 23",
  v_to_plot = "wind_speed_m_s",
  kriging_results = filled_results_clean,
  global_ranges = global_ranges_df,
  map_outline = taiwan_main_island,
  output_filename = paste0(gif_file,"wind_speed_m_s", "_", "2023", ".mp4"), 
  framerate =  50
)
gc()
create_kriging_video(
  start_datetime = "2023-01-01 01",
  end_datetime = "2023-12-31 23",
  v_to_plot = "precipitation_mm",
  kriging_results = filled_results_clean,
  global_ranges = global_ranges_df,
  map_outline = taiwan_main_island,
  output_filename = paste0(gif_file,"precipitation_mm", "_", "2023", ".mp4"), 
  framerate = 50
)
gc()
create_kriging_video(
  start_datetime = "2023-01-01 01",
  end_datetime = "2023-12-31 23",
  v_to_plot = "uv_index",
  kriging_results = filled_results_clean,
  global_ranges = global_ranges_df,
  map_outline = taiwan_main_island,
  output_filename = paste0(gif_file,"uv_index", "_", "2023", ".mp4"), 
  framerate = 50
)
gc()

gif_file <- "F:/淡江/研究實習生/天氣Kriging_mp4/"
create_kriging_video(
  start_datetime = "2024-01-01 01",
  end_datetime = "2024-12-31 23",
  v_to_plot = "temperature_c",
  kriging_results = filled_results_clean,
  global_ranges = global_ranges_df,
  map_outline = taiwan_main_island,
  output_filename = paste0(gif_file,"temperature_c", "_", "2024", ".mp4"), 
  framerate = 50
)
gc()
create_kriging_video(
  start_datetime = "2024-01-01 01",
  end_datetime = "2024-12-31 23",
  v_to_plot = "relative_humidity_percent",
  kriging_results = filled_results_clean,
  global_ranges = global_ranges_df,
  map_outline = taiwan_main_island,
  output_filename = paste0(gif_file,"relative_humidity_percent", "_", "2024", ".mp4"), 
  framerate = 50 
)
gc()
create_kriging_video(
  start_datetime = "2024-01-01 01",
  end_datetime = "2024-12-31 23",
  v_to_plot = "wind_speed_m_s",
  kriging_results = filled_results_clean,
  global_ranges = global_ranges_df,
  map_outline = taiwan_main_island,
  output_filename = paste0(gif_file,"wind_speed_m_s", "_", "2024", ".mp4"), 
  framerate =  50
)
gc()
create_kriging_video(
  start_datetime = "2024-01-01 01",
  end_datetime = "2024-12-31 23",
  v_to_plot = "precipitation_mm",
  kriging_results = filled_results_clean,
  global_ranges = global_ranges_df,
  map_outline = taiwan_main_island,
  output_filename = paste0(gif_file,"precipitation_mm", "_", "2024", ".mp4"), 
  framerate = 50
)
gc()
create_kriging_video(
  start_datetime = "2024-01-01 01",
  end_datetime = "2024-12-31 23",
  v_to_plot = "uv_index",
  kriging_results = filled_results_clean,
  global_ranges = global_ranges_df,
  map_outline = taiwan_main_island,
  output_filename = paste0(gif_file,"uv_index", "_", "2024", ".mp4"), 
  framerate = 50
)
gc()
