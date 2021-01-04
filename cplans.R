library('dplyr')
library('googlesheets4')
library('sf')


BOUNDARY_DIR < "F:\\Farm\\FarmDataAutomation\\boundaries"
CROPPLANS_DIR <- "F:\\Farm\FarmDataAutomation\\CropPlans"
#define this function to use to match up boundaries with master sheet rows later
match_boundaries <- function(x) {
  re <- paste(x,".*shp",sep="");
  y <- boundary_files[grepl(re, boundary_files)]
  y <- ifelse(length(y) > 0, y, "NONE")
  return (y)
}

# make sure current directory is cropplans folder
setwd(CROPPLANS_DIR)


####### ENTER YEAR IN BELOW #########
####### i.e. "2020" => FALL20 and SP21 maps #########
year <- 2020
#Calculates the fall and spring column names. Says join these two things with a space
spring_season <- paste(toString(year+1),"SPRING",sep=" ")
fall_season <- paste(toString(year), "FALL", sep=" ")


ms <- read_sheet('1AUMOHvJnfGT5Ve1Ep-ECslC9bhj8sP0DAI64CmQ-iWw',sheet='Master Sheet')
# this one is here to select the google account if prompted to refresh credentials
1

# Filter the master sheet to just the necessary columns
ms_reduced <- ms%>% select('Farm Name', 'Field','CROPPING ACRES', 'POTATO ACRES', 
                        all_of(fall_season), all_of(spring_season))

# make column names lowercase so easier to work with
names(ms_reduced) <- tolower(make.names(names(ms_reduced)))

# store the new names of the rotation columns in variables
spring_col <- tolower(make.names(c(spring_season)))
fall_col <- tolower(make.names(c(fall_season)))

# remove the rows that have NA in both fall and spring
ms_reduced <- ms_reduced %>% filter(!(is.na(ms_reduced[[fall_col]]) 
                                      & is.na(ms_reduced[[spring_col]])))              
# remove CREP
ms_reduced <- ms_reduced %>% filter(!(ms_reduced[[fall_col]] =="CREP" & ms_reduced[[spring_col]]=="CREP"))


# gets a list of all the files in the boundaries folder
boundary_files <- dir(boundary_dir, full.names = TRUE)
#Filters out only the shape files
boundary_files <- boundary_files[grepl(".shp$", boundary_files)]

# pull out the grower characters
ms_reduced$grower <- str_extract(ms_reduced$farm.name, "^\\w{2}(?=-)")

# apply the match function from above to all the rows of the dataframe
# Pulls the boundary file path into the dataframe for each field
ms_reduced$boundary.file <- vapply(ms_reduced$id,match_boundaries,character(1L))

# convert the dataframe to characters then back again (type business thats needed to make things work right)
ms_reduced <- apply(ms_reduced,2,as.character)

ms_reduced <- as.data.frame(ms_reduced)
ms_reduced$id <- paste(ms_reduced$farm, ms_reduced$field, sep="_")

missing_boundaries <- ms_reduced %>% filter(boundary.file=="NONE")
ms_reduced <- ms_reduced %>% filter(boundary.file!="NONE")

write.csv(missing_boundaries, file="no_boundary.csv")

# loop over the dataframe and create the fall and spring crop plans where necessary
for (x in 1:nrow(ms_reduced)) {
  folder_num <- ceil(500/(x/2))
  if (!is.na(ms_reduced[x, fall_col])) {
    bound <- st_read(ms_reduced[x,"boundary.file"])
    bound$product <- ms_reduced[x,fall_col]
    bound$grower <- ms_reduced[x,"grower"]
    bound$farm <- ms_reduced[x,"farm.name"]
    folder_name <- paste("CropPlans", folder_num, sep="")
    file_name <- paste(year,"FALL",ms_reduced[x,"id"],".shp",sep="_")
    full_path <- paste(folder_name, file_name)
    st_write(bound, file_name, driver="ESRI Shapefile")
  }
  if (!is.na(ms_reduced[x, spring_col])) {
    bound <- st_read(ms_reduced[x,"boundary.file"])
    bound$product <- ms_reduced[x,spring_col]
    bound$grower <- ms_reduced[x,"grower"]
    bound$farm <- ms_reduced[x,"farm.name"]
    file_name <- paste(year+1,"SPRING",ms_reduced[x,"id"],".shp",sep="_")
    st_write(bound, file_name, driver="ESRI Shapefile")
  }
}
