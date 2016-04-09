notes = read.csv("noteevents.csv", header = TRUE, sep=",")

library(stringr)
library(tm)

notes$text = gsub("[^[:alpha:][:space:]]", " ", notes$text)
notes$text = gsub("\\n", " ", notes$text)
notes$text = gsub("\\s+", " ", str_trim(tolower(notes$text)))

write.csv(notes, file="notesprocess.csv")
