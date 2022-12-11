
Sudo = Super User Do
su ferman # to connect to ferman 




## the root : / 
## the home directory: ~ 


# To know where you are
PWD 


# look at the list 
LS 

#look at the list and get details of the files 
    -ls -l 


# wildcard * is a sign used to find similar files
ls ba* #to find every folders starting with ba
ls *ba* 


#Change repertory to      
CD 
cd + letter + (click on tab)

# to return to the precedent repertory
CD ..  

#to create a directory
MKDIR namedirectory 


#To create a focument
TOUCH name.txt 

# to display the number of line in a file follow by number of words
wc name.txt 

# use to print text file on the terminal
# can also concatenate 
cat name.txt
cat name.txt name.txt pour concatener 

#to display the first line and last line of a file 
HEAD 
TAIL 
HEAD -n 10 
to display the first line and last line of a file 
head name.txt
head -n 4 
the n option followed by the number of line you want to see 

#to print something OR to append something in a document

echo "My name is Firat" 
echo "My name is Firat" > name.txt 
#or add something in the last of a file 
    echo "My name is Firat" >> name.txt 


NANO 
Text editor
nano name.txt 



# MIGRATION + DESTRUCTION 
#to move file or to rename file 
MV 

# move my journal entry journal-2017-01-24.txt into the Journal directory
mv journal-2017-01-24.txt Journal
#move the Journal directory into the Documents folder
mv Journal Documents

mv file* folder/ 

# to copy file 
cp echo-out.txt Desktop
    #Be aware that there is one difference between copying files
    #and folders, when copying folders you need to specify the -r
    #option :
    cp -r Documents Desktop

cp /fd/fdf/  .   #the point means this current directory
#To drop documents and file  
RM 
rm -r Desktop/Documents to drop directory


###Difference between ECHO and EXPORT
>>> vech=bus
>>> echo "$vech"
bus
>>> bash #to start a new shell instance
>>> echo "$vech"
output is empty
#Empty line as the variable vech is not exported to new process. 
#But to make the variable known to child process use the export command.
>>> export vech="bus"
>>> echo $backup
bus
>>> bash 
>>> echo $backup 
bus 

#SHEBANG(shabang)
#The first line of a shell/bash/dash script is known as "shebang" because first line and begin by "#!"
#/bin/bash VS /bin/sh is an executable that represensting the system shell 
# "#" in a script is read as a comment but "#!" is read as an executable it is called a she-bang it helps to identify the interpreter

