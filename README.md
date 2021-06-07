Functional requirements
The application should provide the following functionality:

•	A means to load the initial data set (which consists of three CSV files) and translate it into a suitable format, either XML, or JSON or an entity relationship structure (not CSV) 

•	A means to back up the data in this format using either files or a database. This should preserve the current state of the data when the program is closed.

•	A process for cleaning and preparing the initial data set, managing inconsistences, errors, missing values and any specific changes required by the client (see below)

•	A graphical user interface(s) for interacting with the data enable the user to:
  o	Load and clean the initial data set
  o	Load and save the prepared data set
  o	Use the prepared data set to generate output and visualisations
  o	Manipulate the range of values used to generate output and visualisations
It should be assumed that this program will be able to handle other sets of data generated from the same source, i.e. data with the same column row headings but containing different values and anomalies. However, the application is not required to be generic (work with multiple unknown data sets). Given this best practice regarding code reuse, encapsulation and a well-defined programming interface should be applied where applicable.

Data manipulation and outputs 
The client initially wants the application to perform the following actions on the data:
  1.	Outputs should not include any data from vendors that have a ‘PROGRAM STATUS’ of INACTIVE.
  2.	The ‘PE DESCRIPTION’ column contains information on the number and type of seating available at the vendor. Extract this out into a new column, retain all other information within that column. E.g.: 
  a.	‘FOOD MKT RETAIL (1-1,999 SF) LOW RISK’, 
  b.	‘RESTAURANT (61-150) SEATS LOW RISK’.  
  c.	Extract the greyed area out and retain the rest in the examples
  d.	The client initially needs information to generate the following and output the results using appropriate representation:
  3.	Produce the mean, mode and median for the inspection score per year:
  a.	For each type of vendor’s seating
  b.	For each ‘zip code’
  4.	Produce a suitable graph that displays the number of establishments that have committed each type of violation. You may need to consider how you group this data to make visualisation feasible
  5.	Determine if there is any significant correlation between the number of violations committed per vendor and their zip code, ‘Is there a tendency for facilities in specific   locations to have more violations?’. You will need to select an appropriate visualisation to demonstrate this.
  Non-functional requirements
  •	The GUI interface provides appropriate feedback to confirm or deny a user’s actions
  •	The application manages internal and user-generated errors
Technical requirements 
  •	The application is built using Python 3.7.* 
  •	The application uses advanced APIs such as: NumPy, panda, Seaborn, Matplotlib 
  •	The application runs within the anaconda environment using a Jupyter notebook
  •	The application or its parts do not run concurrently, do NOT use Python threads
