library(readr)
part_r_00000 <- read_csv("~/Desktop/part-r-00000.csv", col_names = FALSE)
dataOne <- part_r_00000[,1:2]

ds = dataOne$X1[which(dataOne$X2 == sort(dataOne$X2, decreasing = TRUE)[1])]
sn <- ds

for (year in 2:10){
  ds = dataOne$X1[which(dataOne$X2 == sort(dataOne$X2, decreasing = TRUE)[year])]
  sn <- rbind(sn, ds)
  }

write.csv(sn, file = "top10.csv",row.names=FALSE)

