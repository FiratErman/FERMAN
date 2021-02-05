



#Importation d'un fichier csv
data=	pd.read_csv(gama)
 

#Importation d'un fichier excell 
import pandas as pd
results=pd.read_excel("D:\FErman\Desktop\pythonxx.xlsx")
results.head()
#Si plusieurs feuille
results=pd.read_excel("D:\FErman\Desktop\pythonxx.xlsx",sheetname="..")
results.head()

#Manipule des data
data.shape 		#donne nombre de lignes et de colonnes

data.columns 	#donne le nom des colonnes

print(data["nomVariable"]) # donne le nom de la variable

#Faire une concaténation entre plusieurs tables
In [4]: frames = [df1, df2, df3]

In [5]: result = pd.concat(frames)

############################################################################
#Select options
Selecting data by row numbers (.iloc)
Selecting data by label or by a conditional statment (.loc)
############################################################################

###Création d'une table avec ajout d'une colonne
############################################################################
###Création d'une table avec ajout d'une colonne
import pyspark.sql.functions as func 
from pyspark.sql.window import Window 
from pyspark.sql.types import IntegerType

spark.sql("use default")
sqlContext.sql("show tables").show()

df=spark.sql("select * from default.fer_ref_regionfrance")
df

#On crée une boucle 
list_code=["Bretagne","Occitanie"]
for i in range(len(list_code)):
    if i==0:
        df = df.withColumn('flag',func.when(df.region_de_france==list_code[i],"ok").otherwise("0"))
    else:
        df = df.withColumn('flag',func.when((df.region_de_france==list_code[i])|(df.flag=="ok") ,"ok"))

#On est obligé de créer une table temporaire à partir de notre table existante df
df.registerTempTable("dataframe")


#On a crée une table dans le schema default
spark.sql("use default")
spark.sql("create table FER_dataframe as select * from dataframe")
#################################################################################






#################################################################################
### exemple trouve sur stackoverflow : If these are RDDs you can use SparkContext.union method:

rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([4, 5, 6])
rdd3 = sc.parallelize([7, 8, 9])

rdd = sc.union([rdd1, rdd2, rdd3])
rdd.collect()

## [1, 2, 3, 4, 5, 6, 7, 8, 9]

There is no DataFrame equivalent but it is just a matter of a simple one-liner:

from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

df1 = sqlContext.createDataFrame([(1, "foo1"), (2, "bar1")], ("k", "v"))
df2 = sqlContext.createDataFrame([(3, "foo2"), (4, "bar2")], ("k", "v"))
df3 = sqlContext.createDataFrame([(5, "foo3"), (6, "bar3")], ("k", "v"))

unionAll(df1, df2, df3).show()

## +---+----+
## |  k|   v|
## +---+----+
## |  1|foo1|
## |  2|bar1|
## |  3|foo2|
## |  4|bar2|
## |  5|foo3|
## |  6|bar3|
## +---+----+
#################################################################################







##################################################################################
#Creation d'une table 

from pyspark.sql import DataFrame
df1=sqlContext.createDataFrame([("pharmacie",7608),("vpc",4578),("hut",8954),("electromenager",4567),("electromenager",4561)] , ("famille_mcc","code_mcc"))
####################################################################################







##################################################################################
#Pour #filtrer
>>> df.filter(df.age > 3).collect()
[Row(age=5, name=u'Bob')]
>>> df.where(df.age == 2).collect()
[Row(age=2, name=u'Alice')]

>>> df.filter("age > 3").collect()
[Row(age=5, name=u'Bob')]
>>> df.where("age = 2").collect()
[Row(age=2, name=u'Alice')]
#################################################################################

 





###############################################################################
#faire une #jointure, à noter que le inner peut changer. 
(df=spark.sql('select * from default.fer_datawall_famillemcc_pdm_proxia')
df.count()
df.distinct().count()
df.select('mcc').distinct().show()

df2=spark.table("default.FER_FAMILLE_MCC")

df.join(df2,df['mcc']==df2['mcc'],"inner")
join()
#################################################################################




################################################################################
#pour rechercher une valeur dans un champ de valeur

df1.show()
+--------------+--------+
|   famille_mcc|code_mcc|
+--------------+--------+
|     pharmacie|    7608|
|           vpc|    4578|
|           hut|    8954|
|electromenager|    4567|
|electromenager|    4561|
+--------------+--------+


import pyspark.sql.functions as func
df1.where(func.col("famille_mcc").like("%harm%")).show()
+-----------+--------+
|famille_mcc|code_mcc|
+-----------+--------+
|  pharmacie|    7608|
+-----------+--------+
###############################################################################






#############################################################################
(df=spark.sql('select * from default.fer_datawall_famillemcc_pdm_proxia')
df.count()
df.distinct().count()
df.select('mcc').distinct().show

df2=spark.table("default.FER_FAMILLE_MCC")

df.join(df2,df['mcc']==df2['mcc'],"inner")
join()


)

#############################################################################
Pour Python: 
#Pour convertir la valeur d'une colonne vers le integer
print (pd.to_numeric(df.b, errors='coerce'))
#############################################################################


############################################################################
#Pour filtrer avec plusieurs conditions
f = pd.DataFrame({'Def':[True] *2 + [False]*4,'days since':[7,8,9,14,2,13],'bin':[1,3,5,3,3,3]})


temp2 = df[~df["Def"] & (df["days since"] > 7) & (df["bin"] == 3)]
print (temp2)

     Def  bin  days since
3  False    3          14
5  False    3          13




temp2 = f[ (f["days since"] > 7) & (f["bin"] == 3)]
print (temp2)
temp2 = f[ (f["days since"] > 7) & (f["bin"] == 3)]

print (temp2)

     Def  bin  days since
1   True    3           8
3  False    3          14
5  False    3          13

############################################################################




############################################################################
#equivalent du select in list like('%zer%')
df.select('c43_nom_et_adresse_de_laccepteur_de_carte').where(func.col('c43_nom_et_adresse_de_laccepteur_de_carte').like("%CDISCOUNT%")).show()

############################################################################



#############################################################################

In [6]: df=pd.DataFrame({'Year':['2014', '2015'], 'quarter': ['q1', 'q2']})

In [7]: df
Out[7]:
   Year quarter
0  2014      q1
1  2015      q2

In [8]: df['period'] = df[['Year', 'quarter']].apply(lambda x: ''.join(x), axis=1)

In [9]: df
Out[9]:
   Year quarter  period
0  2014      q1  2014q1
1  2015      q2  2015q2


#############################################################################

###jointure python###

person_likes = [{'person_id': '1', 'food': 'ice_cream', 'pastimes': 'swimming'},
                {'person_id': '2', 'food': 'paella', 'pastimes': 'banjo'}]

person_accounts = [{'person_id': '1', 'blogs': ['swimming digest', 'cooking puddings']},
                   {'person_id': '2', 'blogs': ['learn flamenca']}]

### Effectuer la jointure ###
import pandas as pd
accounts = pd.DataFrame(person_accounts)
likes = pd.DataFrame(person_likes)


pd.merge(accounts, likes, on='person_id',how='inner')

#Il faut définir la Dataframe avec pandas pour pouvoir faire la jointure entre deux tables 
#on peut soit la définir au moment de création de table soit apres.

How much does it cost to write the real investment 


#Pour le toPandas(), cela ramene les données en local et les sort de spark. 
#Il faut donc les sortir de Spark

94

#################
#conversion d'une table en dataframe et creation de table sur default de big data
from pyspark.sql import SparkSession
from pyspark import SparkContext
df2=spark.createDataFrame(df)  pyspark
df2.registerTempTable("test")
spark.sql("use default")
spark.sql ('create table fer_donnees_meteo as select * from test')
# Ensuite dans impala faire un INVALIDATE METADATA default.fer_donnees_meteo pour valider la création de la donnée

###################################

#Conversion 
#Sur Python on a soit une dataframe soit une serie 

###################################
#Pour renommer une colonne :
df.rename(index=str, columns={"A": "a", "B": "c"})




############################################
import pandas as pd
df = pd.DataFrame({ 'gene':["1 // foo // blabla",
                                   "2 // bar // lalala",
                                   "3 // qux // trilil",
                                   "4 // woz // hohoho"], 'cell1':[5,9,1,7], 'cell2':[12,90,13,87]})
df = source_df[["gene","cell1","cell2"]]


#Pour créer trois colonnes
In [13]:
df['gene'] = df['gene'].str.split('//').str[1]
df

Out[13]:
   cell1  cell2   gene
0      5     12   foo 
1      9     90   bar 
2      1     13   qux 
3      7     87   woz 

#######################################
#Pour renommer dans une dataframe pyspark :
df = df1.selectExpr("Name as name")

#Pour renommer dans une dataframe pandas :
df1.rename(index=str, columns={"c43_nom_et_adresse_de_laccepteur_de_carte":A})



#######
#Il existe des dataframe pandas et des dataframe pyspark/sparK faire attention à ne pas confondre
# les deux 

#Pour changer format d'une variable en dataframe pyspark.
df['name']=df['name'].astype(str)

#Pour créer une colonne à partir des valeurs des autres colonnes
def label_race (row):
   if row['eri_hispanic'] == 1 :
      return 'Hispanic'
   if row['eri_afr_amer'] + row['eri_asian'] + row['eri_hawaiian'] + row['eri_nat_amer'] + row['eri_white'] > 1 :
      return 'Two Or More'
   if row['eri_nat_amer'] == 1 :
      return 'A/I AK Native'
   if row['eri_asian'] == 1:
      return 'Asian'
   if row['eri_afr_amer']  == 1:
      return 'Black/AA'
   if row['eri_hawaiian'] == 1:
      return 'Haw/Pac Isl.'
   if row['eri_white'] == 1:
      return 'White'
   return 'Other'

   df.apply (lambda row: label_race (row),axis=1)

  # puis si on veut créer une colonne avec 
   df['race_label'] = df.apply (lambda row: label_race (row),axis=1)


   #je converties en dataframe pyspark
spark.createDataFrame(df1).show()




############################################################
#get dummies for event columns
calendar=pd.get_dummies(calendar,columns=['event_type_1',"event_type_2"],dummy_na=True)
############################################################

############################################################
# create new features
def label_race(row):
    if row['event_type_1_nan'] + row['event_type_2_nan']>=1:
        return '1'
    if row['event_type_1_nan'] + row['event_type_2_nan']==0:
        return "0"

def label_roll(row):
    if row['event_type_1_Cultural'] + row['event_type_2_Cultural']>=1:
        return "1"
    if row['event_type_1_Cultural'] + row['event_type_2_Cultural']==0:
        return "0"

def label_step(row):
    if row['event_type_1_Religious'] + row['event_type_2_Religious']>=1:
        return "1"
    if row['event_type_1_Religious'] + row['event_type_2_Religious']==0:
        return "0"


calendar["No_event"]=calendar.apply(lambda row: label_race(row),axis=1)
calendar["event_type_Cultural"]=calendar.apply(lambda row: label_roll(row),axis=1)
calendar["event_type_Religious"]=calendar.apply(lambda row: label_step(row),axis=1)
############################################################


############################################################
#rename columns
calendar.rename(columns={"event_type_1_Sporting":"event_type_Sporting"},inplace=True)
############################################################

############################################################
# The number of weekday has been replaced by a week number day beginning with monday as 0 and sunday as 6
calendar.loc[:,'date']=pd.to_datetime(calendar["date"],format="%Y-%m-%d")
calendar["wday"]=calendar["date"].dt.weekday
calendar
############################################################



#####################################################
#To avoid high use of resources we change the name of the days columns and replace it by date
# En gros on remplace nom des colonnes par nom dans une liste 
a=calendar["date"]
b=list(a)

#%%

c=sales_train_validation
c.head(2)

#%%

c=c.iloc[:, 6:]


#%%




#Convert the day columns to rows. Contrary of dummies. First attempt on the 5 first row.
sales_train_validation_5=sales_train_validation_5.melt(id_vars=["id", "item_id","dept_id","cat_id","store_id", "state_id"],
        var_name="d",
        value_name="Value")
############################################################


############################################################
#convert d to datetime
sales_train_validation.loc[:,'d']=pd.to_datetime(sales_train_validation["d"],format="%Y-%m-%d")
#get month and year of each sample
sales_train_validation['month']=sales_train_validation['d'].dt.month
sales_train_validation['year']=sales_train_validation['d'].dt.year


#create weekday column
sales_train_validation['wday']=sales_train_validation['d'].dt.weekday
############################################################



############################################################
#merge
WI_1=pd.merge(WI,calendar_x2,how="left",on="d")
############################################################

############################################################
#Add id feature
sell_prices1["id"] =sell_prices1["item_id"] +"_"+ sell_prices1["store_id"]+"_validation"
sell_prices1.head(2)
############################################################

############################################################
#avoir les valeurs uniques d'une colonne equivalent select distinct
sell_prices['id'].nunique()
############################################################

############################################################
##Convert dummies
WI_4=pd.get_dummies(WI_3,columns=["store_id","cat_id","dept_id","state_id"])
############################################################

############################################################
#Calcul of the missing values
x1_missing_values=WI_4.isnull()
#No missing values
for column in x1_missing_values.columns.values:
    print(column)
    print (x1_missing_values[column].value_counts())
    print("")
############################################################




############################################################
    # KNN IMPUTER 
    import numpy as np
import pandas as pd
dict = {'First':[np.nan, np.nan,np.nan, np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,
                 np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,9,12,65,75],
        'Second': [30, 45, 56, 45,35,78,52,61,14,102,102,30, 45, 56, 45,35,78,52,61,14],
        'Third':[50, 40, 80, 98,54,62,14,75,35,150,40, 80, 98,54,62,14,102,102,30, 45]}
df = pd.DataFrame(dict)
# creating a dataframe from list
df
#%%
imputer = KNNImputer(n_neighbors=4)
dfx=imputer.fit_transform(df)
dfx
#%%
d fx1=pd.DataFrame(dfx)
dfx1
#%%
WI_4=pd.DataFrame(WI_4)
#%%
dfx=imputer.fit_transform(WI_4)
###########################################################################


############################################################
#faire un group by 
WI_4[WI_4["year"]==2014] .groupby(['year'])['sell_price'].mean()
############################################################

############################################################
# est dans une liste
years=['2011','2012']
WI_4=WI_4.loc[WI_4['year'].isin (years)]
#est pas dans une liste
years=['2011','2012']
WI_4=WI_4.loc[~WI_4['year'].isin (years)]
############################################################

############################################################
#transform : créer directement une colonne en faisant par exemple ici la moyenne par rapport 
#au group by
WI_4["sell_prices_mean_month_year"]=WI_4.groupby(["month","year"])["sell_price"].transform("mean")
############################################################

############################################################
#correlation
#%%
corr_matrix=WI_4.corr()
corr_matrix
#%%
corr_matrix["Value"].sort_values(ascending=False)
############################################################


############################################################
#Exporter une dataframe en csv sur Pycharm
sales_train_validation.to_csv("venv/data/sales_train_validation.csv")
############################################################


############################################################
#Find the maximum among the given numbers
#The max() function has two forms:
#// to find the largest item in an iterable
#max(iterable, *iterables, key, default)

#// to find the largest item between two or more objects
#max(arg1, arg2, *args, key)
 #EXAMPLE 1 : plus grosse valeur 
max(4, -5, 23, 5)
result = 23 

#EXAMPLE 2: the largest string in a list
languages = ["Python", "C Programming", "Java", "JavaScript"]
largest_string = max(languages);
print("The largest string is:", largest_string)
The largest string is: Python
############################################################


############################################################
#f-Strings: A New and Improved Way to Format Strings in Python
>>> name = "Eric"
>>> age = 74
>>> f"Hello, {name}. You are {age}."
'Hello, Eric. You are 74.'

>>> f"{2 * 37}"
'74'

>>> numcols = [f"d_{day}" for day in range(start_day,tr_last+1)]
############################################################


############################################################
#update() Method
#nsert an item to the dictionary
car = {
  "brand": "Ford",
  "model": "Mustang",
  "year": 1964
}

car.update({"color": "White"})

print(car)
############################################################


############################################################
#The zip() function takes iterables (can be zero or more), aggregates them in a tuple,
# and return it.
number_list = [1, 2, 3]
str_list = ['one', 'two', 'three']

# No iterables are passed
result = zip()

# Converting itertor to list
result_list = list(result)
print(result_list)
results :[]
# Two iterables are passed
result = zip(number_list, str_list)

# Converting itertor to set
result_set = set(result)
print(result_set)
results: 
{(2, 'two'), (3, 'three'), (1, 'one')}

# replace old name by new name (here c by b)
c.rename(columns={i:j for i,j in zip(c,b)}, inplace=True)
############################################################


############################################################
#To show a progress bar USE TQDM
from tqdm.notebook import tqdm
tqdm(print("hello"))

for i in tqdm(range(50)):
    print(i)
############################################################

############################################################
#Create a Pipeline with Pandas

def testA2 (df,col):
    return df.groupby(col).mean()

def uppercase (df):
    df.columns=df.columns.str.upper()
    return df.groupby(col).mean()

(df.pipe(testA2,col="employed"))
.pipe(uppercase)
############################################################

############################################################
#np.random.seed
>> numpy.random.seed(0) ; numpy.random.rand(4)
array([ 0.55,  0.72,  0.6 ,  0.54])
>>> numpy.random.seed(0) ; numpy.random.rand(4)
array([ 0.55,  0.72,  0.6 ,  0.54])
#With the seed reset (every time), the same set of numbers will
#appear every time.

#If the random seed is not reset, different numbers appear with 
#every invocation:
>>> numpy.random.rand(4)
array([ 0.42,  0.65,  0.44,  0.89])
>>> numpy.random.rand(4)
array([ 0.96,  0.38,  0.79,  0.53])
############################################################

############################################################
#************************** JOBLIB**************************
#Joblib is a set of tools to provide lightweight pipelining in Python.
# In particular :
    # 1.transparent disk-caching of functions and lazy re-evaluation (memorize pattern)
    # 2.easy simple parallel computing
#Joblib is optimized to be fast and robust on large data in particular and has
#specific optimizations for numpy arrays.

#Joblib is optimized to be fast and robust on large data in particular and has 
#specific optimizations for numpy arrays.


#To know where is the cache dir
import pip
from distutils.version import LooseVersion

if LooseVersion(pip.__version__) < LooseVersion('10'):
    # older pip version
    from pip.utils.appdirs import user_cache_dir
else:
    # newer pip version
    from pip._internal.utils.appdirs import user_cache_dir

print(user_cache_dir('pip'))
print(user_cache_dir('wheel'))




#
cachedir = 'your_cache_location_directory'
#
>>> from joblib import Memory
>>> memory = Memory(cachedir, verbose=0)

>>> @memory.cache
... def f(x):
...     print('Running f(%s)' % x)
...     return x
#alling this function twice with the same argument does not
# execute it the second time, the output is just reloaded from a
# pickle file in the cache directory:
>>> print(f(1))
Running f(1)
1
>>> print(f(1))
1


#The original motivation behind the Memory context was to have a
# memoize-like pattern on numpy arrays. Memory uses
# fast cryptographic hashing of the input arguments 
# to check if they have been computed;


#JOBLIB PARALLEL
class joblib.Parallel(n_jobs=None, backend=None, verbose=0,
 timeout=None, pre_dispatch='2 * n_jobs',
 batch_size='auto', temp_folder=None, max_nbytes='1M',
  mmap_mode='r', prefer=None, require=None)
############################################################

############################################################
his module provides an interface to the optional garbage collector.
 It provides the ability to disable the collector, tune the collection
  frequency, and set debugging options.
 It also provides access to unreachable objects that the collector 
 found but cannot free
 ############################################################

############################################################
#numpy.random.choice
#Generates a random sample from a given 1-D array
>>> np.random.choice(5, 3)
array([0, 3, 4])

>>> np.random.choice(5, 3, p=[0.1, 0, 0.3, 0.6, 0])
array([3, 3, 0])
 ############################################################

 ############################################################
 #numpy.setdiff1d
#numpy.setdiff1d(ar1, ar2, assume_unique=False)[source]
#Find the set difference of two arrays.

#Return the unique values in ar1 that are not in ar2.

>>> a = np.array([1, 2, 3, 2, 4, 1])
>>> b = np.array([3, 4, 5, 6])
>>> np.setdiff1d(a, b)
array([1, 2])
 ############################################################

 ############################################################
 '''#*******************GARBAGE COLLECTION***********************
 #Memory management
    #A programming language uses objects in its programs to
    #perform operations. Objects include simple variables,like
    #strings, integers, or booleans. They also include more
    # complex data structures like lists, hashes, or classes.

    #The values of your program’s objects are stored in memory 
    #for quick access. In many programming languages, a variable
    #in your program code is simply a pointer to the address of
    #the object in memory. When a variable is used in a program,
    #the process will read the value from memory and operate on it.

    #In early programming languages, developers were responsible for all
    #memory management in their programs. This meant before creating a
    #list or an object, you first needed to allocate the memory for
    #your variable. After you were done with your variable, you then
    #"needed to deallocate it to “free” that memory for other users.


#This led to 2 PROBLEMS:

    #FORGETTING TO FREE YOUR MEMORY.
        #If you don’t free your memory when you’re done using it, it can resuly
        #in memory leaks. This can lead to your program using too much memory 
        #over time. For long-running applications, this can cause serious problems.
    #FREEING YOUR MEMORY TO SOON. The second type of problem consists of 
        #freeing your memory while it’s still in use. This can cause your program
        #to crash if it tries to access a value in memory that doesn’t exist,
        #or it can corrupt your data. A variable that refers to memory that 
        #has been freed is called a dangling pointer.


:#These problems were undesirable, and so newer languages added AUTOMATIC MEMORY
 #MANAGEMENT.
#With automatic memory management, programmers no longer needed to manage memory
#themselves. Rather, the runtime handled this for them.
#There are a few different methods for automatic memory management, but one of the
#more popular ones uses reference counting. With reference counting, the runtime
#keeps track of all of the references to an object. When an object has zero 
#references to it, it’s unusable by the program code and thus able to be deleted.

#For programmers, automatic memory management adds a number of benefits. It’s faster
#to develop programs without thinking about low-level memory details. Further, it can
#help avoid costly memory leaks or dangerous dangling pointers.

#However, automatic memory management comes at a cost. Your program will need to use
#additional memory and computation to track all of its references. What’s more, many
#programming languages with automatic memory management use a “stop-the-world” process
#for garbage collection where all execution stops while the garbage collector looks 
#for and deletes objects to be collected.'''
'''This section assumes you’re using the CPython implementation of Python

There are two aspects to memory management and garbage collection in CPython:

    -Reference counting
    -Generational garbage collection


REFERENCE COUNTING
    The main garbage collection mechanism in CPython is through reference counts.
    Whenever you create an object in Python, the underlying C object has both a 
    Python type (such as list, dict, or function) and a reference count.

    At a very basic level, a Python object’s reference count is incremented whenever
    the object is referenced, and it’s decremented when an object is dereferenced.
    If an object’s reference count is 0, the memory for the object is deallocated.

    Your program’s code can’t disable Python’s reference counting. This is in contrast
    to the generational garbage collector discussed below.

    Some people claim reference counting is a poor man’s garbage collector. It does have
    some downsides, including an inability to detect cyclic references as discussed below.
    However, reference counting is nice because you can immediately remove an object when 
    it has no references.
    
    VIEWING REFERENCE COUNT IN PYTHON
    let’s use a Python REPL and the sys module to see how reference counts are handled.
    First, in your terminal, type python to enter into a Python REPL.
    Second, import the sys module into your REPL. Then, create a variable and check its
    REFERENCE COUNT:'''
        >>> import sys
        >>> a = 'my-string'
        >>> sys.getrefcount(a)
        2   
        there are two references to our variable a. One is from creating the variable.
        The second is when we pass the variable a to the sys.getrefcount() function.
    
        If you add the variable to a data structure, such as a list or a dictionary, 
        you’ll see the reference count increase:
            >>> import sys
            >>> a = 'my-string'
            >>> b = [a] # Make a list with a as an element.
            >>> c = { 'key': a } # Create a dictionary with a as one of the values.
            >>> sys.getrefcount(a)
            4
            """The reference count of a increases when added to a list or a dictionary."""
    
    
    
""" GENERATIONAL GARBAGE COLLECTION
    In addition to the reference counting strategy for memory management, Python also uses
    a method called a generational garbage collector.  
    The easiest way to understand why we need a generational garbage collector is by way of example.
    In the previous section, we saw that adding an object to an array or object increased its reference count.
    But what happens if you add an object to itself?"""
        >>> class MyClass(object):
        ...     pass
        ...
        >>> a = MyClass()
        >>> a.obj = a
        >>> del a
    """we defined a new class. We then created an instance of the class and assigned the
    instance to be a property on itself. Finally, we deleted the instance.

    By deleting the instance, it’s no longer accessible in our Python program. However,
    Python didn’t destroy the instance from memory. The instance doesn’t have a reference
    count of zero because it has a reference to itself.

    We call this type of problem a reference cycle, and you can’t solve it by reference 
    counting. This is the point of the generational garbage collector, which is accessible by 
    the gc module in the standard library."""

    Generational garbage collector terminology
    There are two key concepts to understand with the generational garbage collector. The first concept
     is that of a generation.

"""The garbage collector is keeping track of all objects in memory. A new object starts its life in the
 first generation of the garbage collector. If Python executes a garbage collection process on a generation
and an object survives, it moves up into a second, older generation. The Python garbage collector has
three generations in total, and an object moves into an older generation whenever it survives a garbage
collection process on its current generation.

The second key concept is the threshold. For each generation, the garbage collector module has a threshold
number of objects. If the number of objects exceeds that threshold, the garbage collector will trigger a
collection process. For any objects that survive that process, they’re moved into an older generation.

Unlike the reference counting mechanism, you may change the behavior of the generational garbage collector
in your Python program. This includes changing the thresholds for triggering a garbage collection process
in your code, manually triggering a garbage collection process, or disabling the garbage collection 
process altogether.

Let’s see how you can use the gc module to check garbage collection statistics or change the behavior
of the garbage collector."""
"""sing the GC module
In your terminal, enter python to drop into a Python REPL.

Import the gc module into your session. You can then check the configured thresholds
 of your garbage collector with the get_threshold() method:
"""
>>> import gc
>>> gc.get_threshold()
(700, 10, 10)
By default, Python has a threshold of 700 for the youngest generation
 and 10 for each of the two older generations.

You can check the number of objects in each of your generations
 with the get_count() method:

>>> import gc
>>> gc.get_count()
(596, 2, 1)
In this example, we have 596 objects in our youngest generation,
 two objects in the next generation, and one object in the oldest generation.

As you can see, Python creates a number of objects by default
 before you even start executing your program. You can trigger
  a manual garbage collection process by using the gc.collect() method:

>>> gc.get_count()
(595, 2, 1)
>>> gc.collect()
57
>>> gc.get_count()
(18, 0, 0)
Running a garbage collection process cleans up a huge amount
 of objects—577 in the first generation and three more in the
  older generations.

You can alter the thresholds for triggering garbage collection 
by using the set_threshold() method in the gc module:

>>> import gc
>>> gc.get_threshold()
(700, 10, 10)
>>> gc.set_threshold(1000, 15, 15)
>>> gc.get_threshold()
(1000, 15, 15)
"""In the example above, we increase each of our thresholds from 
their defaults. Increasing the threshold will reduce the frequency 
at which the garbage collector runs. This will be less computationally
 expensive in your program at the expense of keeping dead objects
  around longer.

Now that you know how both reference counting and the garbage 
collector module work, let’s discuss how you should use this
 when writing Python applications.
"""

 ############################################################
 
 ############################################################
 #Shift 
"""function Shift index by desired number of periods with an optional
  time freq. This function takes a scalar parameter called period,
   which represents the number of shifts to be made over the desired
    axis. This function is very helpful when dealing with time-series data"""
 Pandas dataframe.shift()

Syntax:DataFrame.shift(periods=1, freq=None, axis=0)
#Parameters :
#periods : Number of periods to move, can be positive or negative
#freq : DateOffset, timedelta, or time rule string, optional Increment to use from the tseries module or time rule (e.g. ‘EOM’). See Notes
#axis : {0 or ‘index’, 1 or ‘columns’}

Return : shifted : DataFrame

# importing pandas as pd 
import pandas as pd 
   
# Creating row index values for our data frame 
# We have taken time frequency to be of 12 hours interval 
# We are generating five index value using "period = 5" parameter 
   
ind = pd.date_range('01 / 01 / 2000', periods = 5, freq ='12H') 
   
# Creating a dataframe with 4 columns 
# using "ind" as the index for our dataframe 
df = pd.DataFrame({"A":[1, 2, 3, 4, 5],  
                   "B":[10, 20, 30, 40, 50], 
                   "C":[11, 22, 33, 44, 55], 
                   "D":[12, 24, 51, 36, 2]},  
                    index = ind) 
  
# Print the dataframe 
df 
	                A	B	C	D
2000-01-01 00:00:00	1	10	11	12
2000-01-01 12:00:00	2	20	22	24
2000-01-02 00:00:00	3	30	33	51
2000-01-02 12:00:00	4	40	44	36
2000-01-03 00:00:00	5	50	55	2

# shift index axis by two periods in positive direction 
# axis = 0 is set by default 
df.shift(2, axis = 0) 

                    A	B	C	D
2000-01-01 00:00:00	NaN	NaN	NaN	NaN
2000-01-01 12:00:00	NaN	NaN	NaN	NaN
2000-01-02 00:00:00	1.0	10.0	11.0	12.0
2000-01-02 12:00:00	2.0	20.0	22.0	24.0
2000-01-03 00:00:00	3.0	30.0	33.0	51.0

si axis=1 alors ca decale les colonnes sur le meme principe

#exemple: on veut la previous value si on fait shift(4) cela va prendre 
# la 4èeme valeur précédantchaque object.ici comme on a rien mis entre parenthèse elle prend
#juste la précédente.
df['prev_value'] = df.groupby('object')['value'].shift()

    object  period  value  prev_value
0       1       1     24         NaN
1       1       2     67        24.0
2       1       4     89        67.0
3       2       4      5         NaN
4       2      23     23         5.0

  ############################################################


############################################################
#ROLLING 
DataFrame.rolling(self, window, min_periods=None, center=False,
                     win_type=None, on=None, axis=0, closed=None)

"""Parameters
window
Size of the moving window. This is the number of observations used for
 calculating the statistic. Each window will be a fixed size.

If its an offset then this will be the time period of each window.
 Each window will be a variable sized based on the observations included 
 in the time-period. This is only valid for datetimelike indexes.

If a BaseIndexer subclass is passed, calculates the window boundaries
 based on the defined get_window_bounds method. Additional rolling 
 keyword arguments, namely min_periods, center, and closed will be 
 passed to get_window_bounds.

min_periods:int, default None
Minimum number of observations in window required to have a value
 (otherwise result is NA). For a window that is specified by an offset,
  min_periods will default to 1. Otherwise, min_periods will default 
  to the size of the window.

center:bool, default False
Set the labels at the center of the window.

win_type:str, default None
Provide a window type. If None, all points are evenly weighted. See the notes
 below for further information.

on:str, optional
For a DataFrame, a datetime-like column or MultiIndex level on which to 
calculate the rolling window, rather than the DataFrame’s index. Provided
 integer column is ignored and excluded from result since an integer index
  is not used to calculate the rolling window.

axis:int or str, default 0
closedstr, default None
Make the interval closed on the ‘right’, ‘left’, ‘both’ or ‘neither’ endpoints.
 For offset-based windows, it defaults to ‘right’. For fixed windows, 
 defaults to ‘both’. Remaining cases not implemented for fixed windows."""

 df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
df
     B
0  0.0
1  1.0
2  2.0
3  NaN
4  4.0

df.rolling(2, win_type='triang').sum()
     B
0  NaN
1  0.5
2  1.5
3  NaN
4  NaN



df = pd.DataFrame({'B': [0, 1, 2, 5, 4,10,25,50]})
B
0	0
1	1
2	2
3	5
4	4
5	10
6	25
7	50

df.rolling(3).sum()
	B
0	NaN
1	NaN
2	3.0
3	8.0
4	11.0
5	19.0
6	39.0
7	85.0
############################################################

############################################################
#****************The getattr() method *********************
"""The getattr()returns the value of the named attribute
of an object. If not found, it returns the default value 
provided to the function."""
#example1
class Person:
    age = 23
    name = "Adam"

person = Person()
print('The age is:', getattr(person, "age"))
print('The age is:', person.age)

The age is: 23
The age is: 23

#example2
class Person:
    age = 23
    name = "Adam"

person = Person()
# when default value is provided
print('The sex is:', getattr(person, 'sex', 'Male'))
The sex is: Male
# when no default value is provided
print('The sex is:', getattr(person, 'sex'))
AttributeError: 'Person' object has no attribute 'sex'
############################################################

############################################################
#enumerate
my_list = ['apple', 'banana', 'grapes', 'pear']
for value in enumerate(my_list):
    print(value)
(0, 'apple')
(1, 'banana')
(2, 'grapes')
(3, 'pear')

 my_list = ['apple', 'banana', 'grapes', 'pear']
for c, value in enumerate(my_list):
    print(c, value)

0 apple
1 banana
2 grapes
3 pear
############################################################



###########################################################
#pour creer colonne en disant que si colA contient un nom particulier alrs on crée colB egale a tel nom
df['Activity_2'] = pd.np.where(df.Activity.str.contains("email"), "email",
                   pd.np.where(df.Activity.str.contains("conference"), "conference",
                   pd.np.where(df.Activity.str.contains("call"), "call", "task")))

df

#   Activity            Activity_2
#0  email personA       email
#1  attend conference   conference
#2  send email          email
#3  call Sam            call
#4  random text         task
#5  random text         task
#6  lwantto call        call
#########################################################