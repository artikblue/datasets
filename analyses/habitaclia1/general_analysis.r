install.packages("openssl")
install.packages("mongolite")
install.packages("MASS")
install.packages("ISLR")
install.packages("plyr")
install.packages("dplyr")

library(mongolite)
library(MASS)
library(ISLR)
library(plyr)
library(dplyr)

con <- mongo("alquiler", url = "mongodb://127.0.0.1:27017/habitaclia2")
con$count()


mydata <- con$find('{}')
print("Studied areas")
unique(mydata["subzone"])
print("Studied districts")
unique(mydata["zone"])
print("Studied villages")
unique(mydata["village"])

print("general means")
# GENERAL MEANS (relevant data)
sapply(mydata["price"],mean)
sapply(mydata["surface"],mean)
sapply(mydata["price"],median)

print("box plots")
# BOX PLOTS
sapply(mydata["price"],boxplot)
sapply(mydata["surface"],boxplot)
sapply(mydata["rooms"],boxplot)
sapply(mydata["toilets"],boxplot)
print("density graphs")
# DENSITY GRAPHS
plot(density(data.matrix(mydata["price"])))
plot(density(data.matrix(mydata["surface"])))
plot(density(data.matrix(mydata["rooms"])))
plot(density(data.matrix(mydata["toilets"])))
print("data summaries")
# SUMMARIES
summary(mydata$price)
summary(mydata$surface)
summary(mydata$numpics)
summary(mydata$rooms)
summary(mydata$toilets)

print("grouping by zone, village and renting company")
str(mydata %>% group_by(village))
zones <- mydata %>%
  group_by(village) %>%
  summarize(price_mean=mean(price), price_dev=sd(price), price_var=var(price),
            price_meanna=median(price), total_vals=length(price),price_max=max(price),
            price_mino=min(price),price_rango=max(price)-min(price), rooms_mean=mean(rooms), 
            toilets_mean=mean(toilets), pics_mean=mean(numpics),
            surfaces_max=max(surface), surfaces_mino=min(surface), surface_mean=mean(surface),
            surface_meanna=median(surface), surfaces_dev=sd(surface), surfaces_varianza=var(surface),
            surfaces_rango=max(surface)-min(surface), price_metro=sum(price)/sum(surface))



zonas <- mydata %>%
  group_by(subzone) %>%
  summarize(price_mean=mean(price), price_dev=sd(price), price_var=var(price),
            price_meanna=median(price), total_vals=length(price),price_max=max(price),
            price_mino=min(price),price_rango=max(price)-min(price), rooms_mean=mean(rooms), 
            toilets_mean=mean(toilets), pics_mean=mean(numpics),
            surfaces_max=max(surface), surfaces_mino=min(surface), surface_mean=mean(surface),
            surface_meanna=median(surface), surfaces_dev=sd(surface), surfaces_varianza=var(surface),
            surfaces_rango=max(surface)-min(surface), price_metro=sum(price)/sum(surface))

subzonas <- mydata %>%
  group_by(zone) %>%
  summarize(price_mean=mean(price), price_dev=sd(price), price_var=var(price),
            price_meanna=median(price), total_vals=length(price),price_max=max(price),
            price_mino=min(price),price_rango=max(price)-min(price), rooms_mean=mean(rooms), 
            toilets_mean=mean(toilets), pics_mean=mean(numpics),
            surfaces_max=max(surface), surfaces_mino=min(surface), surface_mean=mean(surface),
            surface_meanna=median(surface), surfaces_dev=sd(surface), surfaces_varianza=var(surface),
            surfaces_rango=max(surface)-min(surface), price_metro=sum(price)/sum(surface))

companies <- mydata %>%
  group_by(company) %>%
  summarize(price_mean=mean(price), price_dev=sd(price), price_var=var(price),
            price_meanna=median(price), total_vals=length(price),price_max=max(price),
            price_mino=min(price),price_rango=max(price)-min(price), rooms_mean=mean(rooms), 
            toilets_mean=mean(toilets), pics_mean=mean(numpics),
            surfaces_max=max(surface), surfaces_mino=min(surface), surface_mean=mean(surface),
            surface_meanna=median(surface), surfaces_dev=sd(surface), surfaces_varianza=var(surface),
            surfaces_rango=max(surface)-min(surface), price_metro=sum(price)/sum(surface))




zones <- mydata %>%
  group_by(subzone) %>%
  summarize(price_mean=mean(price), price_dev=sd(price), price_var=var(price),
            price_meanna=median(price), total_vals=length(price),price_max=max(price),
            price_mino=min(price),price_rango=max(price)-min(price), rooms_mean=mean(rooms), 
            toilets_mean=mean(toilets), pics_mean=mean(numpics),
            surfaces_max=max(surfaces), surfaces_mino=min(surfaces), surface_mean=mean(surfaces),
            surface_meanna=median(surfaces), surfaces_dev=sd(surfaces), surfaces_varianza=var(surfaces),
            surfaces_rango=max(surfaces)-min(surfaces), price_metro=sum(price)/sum(surfaces))



zonas <- mydata %>%
  group_by(poblacion) %>%
  summarize(price_mean=mean(price), price_dev=sd(price), price_var=var(price),
            price_meanna=median(price), total_vals=length(price),price_max=max(price),
            price_mino=min(price),price_rango=max(price)-min(price), rooms_mean=mean(rooms), 
            toilets_mean=mean(toilets), pics_mean=mean(numpics),
            surfaces_max=max(surfaces), surfaces_mino=min(surfaces), surface_mean=mean(surfaces),
            surface_meanna=median(surfaces), surfaces_dev=sd(surfaces), surfaces_varianza=var(surfaces),
            surfaces_rango=max(surfaces)-min(surfaces), price_metro=sum(price)/sum(surfaces))


companies <- mydata %>%
  group_by(inmobiliaria) %>%
  summarize(price_mean=mean(price), price_dev=sd(price), price_var=var(price),
            price_meanna=median(price), total_vals=length(price),price_max=max(price),
            price_mino=min(price),price_rango=max(price)-min(price), rooms_mean=mean(rooms), 
            toilets_mean=mean(toilets), pics_mean=mean(numpics),
            surfaces_max=max(surfaces), surfaces_mino=min(surfaces), surface_mean=mean(surfaces),
            surface_meanna=median(surfaces), surfaces_dev=sd(surfaces), surfaces_varianza=var(surfaces),
            surfaces_rango=max(surfaces)-min(surfaces), price_metro=sum(price)/sum(surfaces))




print("basic general correlation graph")
pairs(~price + surface + rooms + toilets,data=mydata,
      main="correlation matrix") 
select(zonas %>% arrange(-total_vals), subzone, total_vals)

select(zonas %>% arrange(-price_mean), subzone, price_mean)

select(zonas %>% arrange(price_mean), subzone, price_mean)

select(zonas %>% arrange(-surface_mean), subzone, surface_mean)


# AREAS
select(zones %>% arrange(-total_vals), village, total_vals)

select(zones %>% arrange(-price_mean), village, price_mean)

select(zones %>% arrange(price_mean), village, price_mean)

select(zones %>% arrange(-surface_mean), village, surface_mean)

# SUBAREAS
select(zones %>% arrange(-total_vals), zone, total_vals)

select(zones %>% arrange(-price_mean), zone, price_mean)

select(zones %>% arrange(price_mean), zone, price_mean)

select(zones %>% arrange(-surface_mean), zone, surface_mean)

select(companies %>% arrange(-total_vals), company, total_vals)

select(companies %>% arrange(-price_mean), company, price_mean)

select(companies %>% arrange(price_mean), company, price_mean)

select(companies %>% arrange(-surface_mean), company, surface_mean)


# MOST COMMON VALS
print("most common values")
sort(table(mydata["price"]),decreasing=TRUE)[1:5]

sort(table(mydata["company"]),decreasing=TRUE)[1:5]

sort(table(mydata["surface"]),decreasing=TRUE)[1:5] 

sort(table(mydata["rooms"]),decreasing=TRUE)[1:3]

sort(table(mydata["subzone"]),decreasing=TRUE)[1:3]





