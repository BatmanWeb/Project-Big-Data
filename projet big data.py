#!/usr/bin/env python
# coding: utf-8

# In[4]:


pip install matplotlib


# In[5]:


pip install scikit-learn


# In[6]:


pip install numpy


# In[1]:


from mpl_toolkits.mplot3d import Axes3D
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt # plotting
import numpy as np # linear algebra
import os # accessing directory structure
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)


# In[2]:


import os

# Parcourir les fichiers dans le répertoire StormEvents
for dirname, _, filenames in os.walk("./Desktop/StormEvents"):
    for filename in filenames:
        print(os.path.join(dirname, filename))


# In[18]:


import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, to_timestamp, regexp_replace

# Initialiser Spark
spark = SparkSession.builder \
    .appName("Analyse des données climatiques") \
    .getOrCreate()

# Vérifier la version de Spark
print("Version de Spark :", spark.version)


# In[5]:


df = spark.read.csv("./Desktop/StormEvents/StormEvents_details-ftp_v1.0_d2020_c20201216.csv", header=True, inferSchema=True)


df.printSchema()


# In[6]:


# Afficher les 10 premières lignes
df.show(10)


# In[9]:


# Créer une liste pour les colonnes avec au moins une valeur vide
columns_with_empty_values = []

# Vérifier chaque colonne pour voir si elle a au moins une valeur vide
for column in df.columns:
    empty_count = df.filter(col(column).isNull() | (col(column) == '')).count()
    if empty_count > 0:  # Si au moins une valeur est nulle ou vide
        columns_with_empty_values.append(column)

print("Colonnes avec au moins une valeur vide :")
print(columns_with_empty_values)


# In[23]:


# Suppression de plusieurs colonnes à la main
df = df.drop("TOR_F_SCALE","TOR_LENGTH","TOR_WIDTH","TOR_OTHER_WFO","TOR_OTHER_CZ_STATE","TOR_OTHER_CZ_FIPS","TOR_OTHER_CZ_NAME","CZ_FIP","CZ_TYPE","STATE_FIPS")  

df.printSchema()


# In[10]:


#Suppresion de plusieurs colonnes manuellement

# Obtenir le nombre total de lignes dans le DataFrame
total_rows = df.count()

# Créer une liste pour les colonnes à supprimer
columns_to_drop = []

# Vérifier chaque colonne pour voir si elle a plus de 80% de valeurs manquantes
for column in df.columns:
    # Compter le nombre de valeurs non nulles dans la colonne
    non_null_count = df.filter(col(column).isNotNull()).count()
    
    # Calculer le pourcentage de valeurs manquantes
    missing_percentage = (total_rows - non_null_count) / total_rows
    
    # Si le pourcentage de valeurs manquantes est supérieur à 80%, ajouter à la liste des colonnes à supprimer
    if missing_percentage > 0.8:
        columns_to_drop.append(column)

# Supprimer les colonnes
df = df.drop(*columns_to_drop)

# Afficher les colonnes restantes
print("Colonnes après suppression :")
df.printSchema()


# In[21]:


# Créer une liste pour les colonnes avec au moins une valeur vide
columns_with_empty_values = []

# Vérifier chaque colonne pour voir si elle a au moins une valeur vide
for column in df.columns:
    empty_count = df.filter(col(column).isNull() | (col(column) == '')).count()
    if empty_count > 0:  # Si au moins une valeur est nulle ou vide
        columns_with_empty_values.append(column)

print("Colonnes avec au moins une valeur vide :")
print(columns_with_empty_values)


# In[12]:


# Gérer les valeurs manquantes restantes
df = df.na.fill({"DAMAGE_PROPERTY": 0, "DAMAGE_CROPS": 0})  # Remplacer par 0 pour les colonnes numériques


# In[15]:


# Convertir les colonnes de date/heure
df = df.withColumn("BEGIN_DATE_TIME", to_timestamp(col("BEGIN_DATE_TIME"), 'dd-MMM-yy HH:mm:ss'))
df = df.withColumn("END_DATE_TIME", to_timestamp(col("END_DATE_TIME"), 'dd-MMM-yy HH:mm:ss'))


# In[16]:


# Supprimer les doublons
df = df.dropDuplicates()


# In[19]:


# Convertir les colonnes numériques en types numériques
df = df.withColumn("DAMAGE_PROPERTY", regexp_replace(col("DAMAGE_PROPERTY"), "K", "000").cast("float"))
df = df.withColumn("DAMAGE_CROPS", regexp_replace(col("DAMAGE_CROPS"), "K", "000").cast("float"))


# In[20]:


# Afficher le schéma final après nettoyage
print("Schéma après nettoyage :")
df.printSchema()


# In[ ]:




