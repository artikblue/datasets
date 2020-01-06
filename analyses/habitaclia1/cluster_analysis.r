
library(mongolite)
library(plyr)
library(dplyr)
library(ggplot2)

con <- mongo("alquiler", url = "mongodb://127.0.0.1:27017/habitaclia2")
mydata <- con$find('{}')

ds <- select(mydata, 4,5,6,7,9) # numeric feats

# KMEANS
dcluster <- kmeans(ds, 6, nstart = 1)

dcluster

# CLUSTER ANALYSIS

group1 <- filter(mydata, price >250 & price < 1600)

mean(data.matrix(group1["price"]))

sd(data.matrix(group1["price"]))

plot(density(data.matrix(group1["price"])))

shapiro.test(data.matrix(group1["price"]))
pairs(~price + surface + rooms + toilets,data=group1,
      main="correlation matrix") #Matríz de correlaciones

regresion = lm(data.matrix(group1["price"]) ~ data.matrix(group1["surface"])) 
plot(price ~ surface, group1)
abline (regresion, lwd=1, col ="red" )   
regresion
cor.test(data.matrix(group1["surface"]), data.matrix(group1["price"]), method=c("pearson", "kendall", "spearman"))

group2 <- filter(mydata, price > 1600 & price < 3200)
mean(data.matrix(group2["price"]))

sd(data.matrix(group2["price"]))

plot(density(data.matrix(group2["price"])))

shapiro.test(data.matrix(group2["price"]))
pairs(~price + surface + rooms + toilets,data=group2,
      main="correlation matrix")

regresion = lm(data.matrix(group2["price"]) ~ data.matrix(group2["surface"])) 
plot(price ~ surface, group2)
abline (regresion, lwd=1, col ="red" )   
regresion
cor.test(data.matrix(group2["price"]), data.matrix(group2["price"]), method=c("pearson", "kendall", "spearman"))

mydata["price_category"] <- NA
mydata$price_category<-ifelse(mydata$price <500, "VERYCHEAP", ifelse(mydata$price <1000,"NORMAL", ifelse(mydata$price < 5000,"EXPENSIVE","HIGHEXPENSIVE")))
nrow(subset(mydata,price_category == "VERYCHEAP")) / nrow(mydata)
nrow(subset(mydata,price_category == "NORMAL")) / nrow(mydata)
nrow(subset(mydata,price_category == "EXPENSIVE")) / nrow(mydata)
nrow(subset(mydata,price_category == "HIGHEXPENSIVE")) / nrow(mydata)



