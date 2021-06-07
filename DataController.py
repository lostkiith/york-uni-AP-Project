from concurrent.futures.thread import ThreadPoolExecutor

import pandas as pd
import pymongo
import seaborn as sns
from pymongo.errors import ServerSelectionTimeoutError


class DataController(object):

    @staticmethod
    def convert_csv_to_json(filename):
        """" opens the csv file converts it into a dictionary then returns it as a json file."""
        try:
            with open(filename, encoding="utf-8-sig", newline='') as inFile:
                data_reader = pd.read_csv(inFile, low_memory=False).to_json(orient="records")
                return data_reader
        except FileNotFoundError:
            raise FileNotFoundError
        except TypeError:
            raise TypeError
        except ValueError:
            raise ValueError

    @staticmethod
    def replace_database_collection(file, choice):
        """" replaces the database collection if it exists with the new collection."""
        client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=1000)
        try:
            db = client["DataCollection"]
            collection = db[choice]
            collection.drop()  # drops current collection
            collection.insert_many(file.reset_index().to_dict('records'))
        except ServerSelectionTimeoutError as exc:
            raise RuntimeError('Failed to open database') from exc

    @staticmethod
    def read_from_database(choice):
        """" returns the data stored in the database by the file name."""
        client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=1000)
        try:
            db = client["DataCollection"]
            collection = db[choice]  # violations, inspections or inventory
            return pd.DataFrame(collection.find({}, {'index': 0}))
        except ServerSelectionTimeoutError as exc:
            raise RuntimeError('Failed to open database') from exc

    @staticmethod
    def prep_data(file_name):
        """" reads the file as a json object into a dataframe then drops duplicate and incomplete rows."""
        try:
            data_frame = pd.read_json(file_name, convert_dates=True)
            data_frame = data_frame.dropna()  # drops incomplete rows
            data_frame = data_frame.drop_duplicates()  # drops duplicate rows from dataframe.
            return data_frame
        except TypeError as te:
            raise TypeError(f"Must be a JSON file. {te}")
        except ValueError as ve:
            raise ValueError(f"File not in correct format. {ve}")

    @staticmethod
    def clean_dataset(dataset):
        """" cleans the dataset based on the requirements."""
        violations = dataset[0]
        inspections = dataset[1]
        inventory = dataset[2]
        #  checks if the dataset has been cleaned
        if 'OWNER ID' not in inspections.columns:
            raise ValueError
        else:

            # Drop duplicated columns in inspections
            inspections = inspections.drop(columns=['OWNER ID', 'OWNER NAME', 'FACILITY NAME',
                                                    'RECORD ID', 'PROGRAM NAME', 'PROGRAM ELEMENT (PE)',
                                                    'FACILITY ADDRESS', 'FACILITY CITY',
                                                    'FACILITY STATE', 'Census Tracts 2010', 'Location',
                                                    '2011 Supervisorial District Boundaries (Official)',
                                                    'Board Approved Statistical Areas'])

            # set date values to the correct format
            inspections['ACTIVITY DATE'] = pd.to_datetime(inspections['ACTIVITY DATE'], infer_datetime_format=True)

            # add serial number to violations
            violations = violations.reset_index()
            violations = violations.merge(inspections[['Zip Codes', 'SERIAL NUMBER']])

            # create seat numbers
            inspections = DataController.create_new_col_for_seat_numbers(inspections)
            inventory = DataController.create_new_col_for_seat_numbers(inventory)

            # remove inactive records
            inactive_list = DataController.get_inactive_list(inspections)
            inactive_list_fid = inactive_list['FACILITY ID']
            inactive_list_fid = inactive_list_fid.drop_duplicates()
            inventory = DataController.del_by_facility_id(inactive_list_fid, inventory)

            inactive_list_sn = inactive_list['SERIAL NUMBER']
            violations = DataController.del_by_serial_number(inactive_list_sn, violations)
            inspections = DataController.remove_inactive(inspections)
            # by serial number
            return [violations, inspections, inventory]

    @staticmethod
    def clean_dataset_threads(dataset):

        violations = dataset[0]
        inspections = dataset[1]
        inventory = dataset[2]

        if 'OWNER ID' not in inspections.columns:
            raise ValueError
        else:

            with ThreadPoolExecutor() as executor:
                # Drop duplicated columns in inspections
                inspections = executor.submit(inspections.drop, columns=['OWNER ID', 'OWNER NAME', 'FACILITY NAME',
                                                                         'RECORD ID', 'PROGRAM NAME',
                                                                         'PROGRAM ELEMENT (PE)',
                                                                         'FACILITY ADDRESS', 'FACILITY CITY',
                                                                         'FACILITY STATE', 'Census Tracts 2010',
                                                                         'Location',
                                                                         '2011 Supervisorial District Boundaries (Official)',
                                                                         'Board Approved Statistical Areas']).result()
                # set date values to the correct format
                inspections['ACTIVITY DATE'] = executor.submit(pd.to_datetime, inspections['ACTIVITY DATE']).result()

                # add serial number to violations
                violations = violations.reset_index()
                violations = violations.merge(inspections[['Zip Codes', 'SERIAL NUMBER']])

                # create seat numbers
                inspections = executor.submit(DataController.create_new_col_for_seat_numbers, inspections).result()

                # remove inactive records
                inactive_list = executor.submit(DataController.get_inactive_list, inspections).result()
                inactive_list_fid = inactive_list['FACILITY ID']
                inactive_list_fid = inactive_list_fid.drop_duplicates()
                inventory = executor.submit(DataController.del_by_facility_id, inactive_list_fid, inventory)

                inactive_list_sn = inactive_list['SERIAL NUMBER']
                violations = executor.submit(DataController.del_by_serial_number, inactive_list_sn, violations)
                inspections = executor.submit(DataController.remove_inactive, inspections)

                return [violations.result(), inspections.result(), inventory.result()]

    @staticmethod
    def create_new_col_for_seat_numbers(data_to_edit):
        """" edits the PE description by removing the bracketed numbers into there own column."""
        if 'PE DESCRIPTION' in data_to_edit.columns:
            pe_description = data_to_edit['PE DESCRIPTION'].str.extract(r'\(([^)]+)\)')  # takes the pe description col
            data_to_edit['PE DESCRIPTION'] = data_to_edit['PE DESCRIPTION'].str.replace(r'[\(\[].*?[\)\]]', '')
            data_to_edit['SEAT NUMBERS'] = pe_description  # adds a new col to the dataframe
            return data_to_edit
        else:
            raise ValueError("no 'PE DESCRIPTION' column")

    @staticmethod
    def averages(choice, inspections):
        """" choices witch averages data to return."""
        try:
            #  check for cleaned data
            if 'SEAT NUMBERS' not in inspections.columns:
                raise TypeError
            else:
                if choice == "by type of vendor’s seating":
                    agg_data = DataController.avg_grouping(inspections, 'PE DESCRIPTION')
                    return agg_data.round(2)
                elif choice == "by zip code":
                    agg_data = DataController.avg_grouping(inspections, 'Zip Codes')
                    return agg_data.round(2)

        except RuntimeError:
            raise RuntimeError("Failed to open database.")

    @staticmethod
    def avg_grouping(inspections, group_by):
        """" create a new dataframe of years group by thr group_by variable."""
        agg_data = inspections[[group_by, 'SCORE', 'ACTIVITY DATE']].groupby(
            [pd.Grouper(key=group_by), pd.Grouper(key='ACTIVITY DATE', freq='Y')]).agg(
            'mean').reset_index()  # groups the mean of score by year
        agg_data = agg_data.rename(
            columns={"SCORE": "mean grouped by year"})  # renames the score column to mean by year
        pe_description_score_median = inspections[[group_by, 'SCORE', 'ACTIVITY DATE']].groupby(
            [pd.Grouper(key=group_by), pd.Grouper(key='ACTIVITY DATE', freq='Y')]).agg(
            'median').reset_index()
        pe_description_score_median = pe_description_score_median.rename(
            columns={"SCORE": "median grouped by year"})
        pe_description_score_mode = inspections[[group_by, 'SCORE', 'ACTIVITY DATE']].groupby(
            [pd.Grouper(key=group_by), pd.Grouper(key='ACTIVITY DATE', freq='Y')]) \
            .apply(pd.DataFrame.mode).dropna().set_index(group_by).reset_index()
        pe_description_score_mode = pe_description_score_mode.rename(columns={"SCORE": "mode grouped by year"})
        #  add the new columns for mean, median and mode to final dataframe.
        agg_data['median grouped by year'] = pe_description_score_median['median grouped by year']
        agg_data['mode grouped by year'] = pe_description_score_mode['mode grouped by year']
        return agg_data

    @staticmethod
    def del_by_facility_id(to_remove, data):
        """" delete all rows with the facility id given the the dataframe."""
        return data[~data['FACILITY ID'].isin(to_remove.values.tolist())]

    @staticmethod
    def del_by_serial_number(to_remove, data):
        """" delete all rows with the serial number given the the dataframe."""
        return data[~data['SERIAL NUMBER'].isin(to_remove.values.tolist())]

    @staticmethod
    def get_inactive_list(data):
        """" returns a dataframe with all inactive records."""
        if 'PROGRAM STATUS' in data.columns:
            return data[data['PROGRAM STATUS'].ne('ACTIVE')]
        else:
            raise ValueError("no 'PROGRAM STATUS' column")

    @staticmethod
    def remove_inactive(data):
        """" remove all rows with program status of inactive."""
        if 'PROGRAM STATUS' in data.columns:
            return data.drop(data[data['PROGRAM STATUS'].ne('ACTIVE')].index)
        else:
            raise ValueError("no 'PROGRAM STATUS' column")

    @staticmethod
    def violation_bar_graph(violations, number, ax):
        """" creates a bar graph from violation code and number of violations."""
        violation_code_count = violations['VIOLATION CODE'].value_counts().reset_index()
        violation_code_count = violation_code_count.rename(columns={
            "VIOLATION CODE": "number of violations", "index": "violation code"})
        violation_code_count = violation_code_count.sort_values(by=['number of violations'])
        violation_code_count = violation_code_count.tail(number)
        return sns.barplot(data=violation_code_count, x='violation code', y='number of violations', ax=ax)

    @staticmethod
    def violation_scatter_graph(violations, ax):
        """" creates a scatter graph from zip code and number of violations."""
        violation_zip_count = violations[['VIOLATION CODE', 'Zip Codes']]
        violation_zip_count['Zip Codes'] = violation_zip_count['Zip Codes'].astype('category')
        violation_zip_count = violation_zip_count['Zip Codes'].value_counts().reset_index()
        violation_zip_count = violation_zip_count.rename(columns={
            "Zip Codes": "number of violations", "index": "zip area"})
        violation_zip_count = violation_zip_count.sort_values(by=['number of violations'])
        return sns.scatterplot(data=violation_zip_count, x='zip area', y='number of violations', hue_norm=(0, 7), ax=ax)
