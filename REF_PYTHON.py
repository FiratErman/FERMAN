### PYTHON CONCEPTUAL HIERARCHY
# Python programs are decomposed: 
#- modules > statements > expressions > objects(it is data structure)

### STRING OPERATIONS
from pandas.core.frame import DataFrame


>>>S='SPAM'
>>>S.find("pa")

>>>S.replace('pa','XYZ') 
>>>S[1:3]="XYZ"

###SPLIT
>>> line= 'aaa,bbb,cc'
>>> line.split(',')


### NUMBER OPERATIONS
>>> s=[1,3,4]
>>> s[0]=2


### FORMATTING 
>>>'{}, eggs, and {}'.format('spam','SPAM!')
 
 >>>'%.2f | %+05d' % (3.14159, −42)


#Other Ways to Code Strings
>>>S="A\nB\tC"
>>>len(S)
#n for going to the line and \t for tab

### PATTERN MATCHING 
# module re calls for searching, splitting and replacement. 
>>> import re
>>> match = re.match('Hello[ \t]*(.*)world', 'Hello Python world')
>>> match.group(1)
#This example searches for a substring that begins with the word “Hello,” followed by
#zero or more tabs or spaces, followed by arbitrary characters to be saved as a matched
#group, terminated by the word “world.” 

#split
>>>re.split('l','Hello world')


###LISTS
# list object are ordrederd collections of typed objects= 
    - no fix size 
    - mutable (can be modified)

L = [123, 'spam', 1.23]
#to add
>>>>>L + [4, 5, 6] 
>>>L.append(2)
#To sort only same types of objects (numerical or string)
>>> L.sort


#TYPE-SPECIFIC OPERATIONS
>>>L.append('NI')


###LIST COMPREHENSION EXPRESSION
#powerful way to process structurs
#There are a way to build a new list by running an axpression on ach item in a sequecnce
# one at a time from left to right
>>> M=[[1,2,3],[4,5,6],[7,8,9]]
>>> col2=[row[1] for row in M] # Give me row[1] for each row in matrix M in a new list 
>>> diag= [M[i][i] for i in [0,1,2]]
>>> doubles = [c*2 for c in 'spam']
>>> [[x ** 2, x ** 3] for x in range(4)]

>>> {sum(row) for row in M}

>>> squares = [x ** 2 for x in [1, 2, 3, 4, 5]]
>>> squares
[1, 4, 9, 16, 25]

>>> squares = []
>>>for x in [1, 2, 3, 4, 5]:
    squares.append(x ** 2)
>>> squares
[1, 4, 9, 16, 25]

#create a column period that is concatenating year + semester
df=pd.DataFrame({'Year':[2013,2012],'Semester':['s1','s2']})
DataFrame	Year	Semester	
0	        2013	   s1	        
1	        2012	   s2	        
df['period']=[str(x)+y for x,y in zip(df.Year,df.Semester)]
df
DataFrame	Year	Semester	period
0	        2013	  s1	    2013s1
1	        2012	  s2	    2012s2


### DICTIONARIES
#Dictionaires are different, are not sequences and are instead known as mappings.
#They store object by KEY instead of positions.
#MUTABLE like lists they may be changed in place and can grow and shrink on demand
>>> D = {'food': 'Spam', 'quantity': 4, 'color': 'pink'}
>>> D['food']
'Spam'
D['quantity'] += 1
>>> D
{'color': 'pink', 'food': 'Spam', 'quantity': 5}
#with an empty dictionary
>>>D = {}
>>> D['name'] = 'Bob'
>>>D['job'] = 'dev'
>>> D['age'] = 40
# Create keys by assignment
>>> D
{'age': 40, 'job': 'dev', 'name': 'Bob'}
#dictonaries are NOT sequences, keys may come back in a different order than in which we typed them


###TUPLES
#Tuples are sequences like list but are IMMUTABLE