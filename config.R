host <- Sys.info()[["nodename"]]

data_dir <- switch(host,
                   "DESKTOP-3E1J9LG" = "F:/淡江/研究實習生/",
                   "Brian-PC" = "E:/Research/weather/",
                   "Lab-PC" = "C:/Users/Researcher/Data/",
                   stop("請加入此電腦的資料路徑到 config.R")
)