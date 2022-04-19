import pandas

#Read Files
fileName='C:/Users/laxmi/Downloads/8317_Age_and_sex_by_ethnic_group/Data8317.csv'
file_df=pandas.read_csv(filepath_or_buffer=fileName, nrows=1000)

file_df = file_df[['Year','Age','Ethnic','Sex','Area','count']]
print(file_df.head() )


# checking for data types and converting count to int
file_dt=file_df.dtypes
print(file_dt)
file_df=file_df.dropna(axis=0)


#file_dt=file_df.astype({'count':'int64'} , errors='ignore').dtypes
file_df['count'] = pandas.to_numeric(file_df['count'], errors='coerce')
file_dt=file_df.dtypes


print(file_dt)
file_df=file_df.dropna(axis=0)
file_df=file_df.filter(["Ethnic","Year","count"])
file_df
file_df=file_df.groupby(by=['Ethnic','Year'],axis=0)['Ethnic','Year','count'].agg({'count':'sum'}).reset_index()
file_df

#file_df.plot( x='Year',y='count' , kind='scatter', subplots=True, legend=True,sharey=True)


#TOPN
file_df_pivot = file_df.pivot(index='Year', columns='Ethnic', values='count')
print(file_df_pivot)
file_df_pivot.plot()
