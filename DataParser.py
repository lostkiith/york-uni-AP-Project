import time
import tkinter as tk
from concurrent.futures.thread import ThreadPoolExecutor
from tkinter import messagebox
from tkinter import ttk, simpledialog
from tkinter.filedialog import askopenfilename

import matplotlib.figure as plt
import pandas as pd
import seaborn as sns
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

import DataController

global violations
global inspections
global inventory


def data_viewer():
    # Clears the data from the tree view
    def clear_tree():
        my_tree.delete(*my_tree.get_children())

    # Create a three view from the dataframe
    def setup_tree_view(data):
        """ populate the tree view with data """
        # Ref: https://stackoverflow.com/questions/18562123/how-to-make-ttk-treeviews-rows-editable

        # Clear old tree view
        clear_tree()
        # Set up new tree view
        my_tree["column"] = list(data.columns)
        my_tree["show"] = "headings"
        # Loop thru column list for headers
        for column in my_tree["column"]:
            my_tree.heading(column, text=column)

        # Put data in tree view
        for row in data.to_numpy().tolist():
            my_tree.insert("", "end", values=row)

    # Setup how data is displayed
    def set_pandas_display_options() -> None:
        """Set pandas display options."""
        # Ref: https://stackoverflow.com/a/52432757/
        display = pd.options.display
        display.max_columns = 1000
        display.max_rows = 1000
        display.max_colwidth = 199
        display.width = None
        display.precision = 2

    # cleans the dataset
    def clean_dataset():
        """ if dataset has been loaded clean it or throw error."""
        try:
            global violations
            global inspections
            global inventory

            dataset = DataController.clean_dataset([violations, inspections, inventory])

            violations = dataset[0]
            inspections = dataset[1]
            inventory = dataset[2]

            messagebox.showinfo("complete", "Data set has been cleaned")
        except RuntimeError:
            messagebox.showerror("error", "Failed to open database.")
        except KeyError as ex:
            messagebox.showerror("error", "problem with current dataset column. {}".format(ex))
        except ValueError as ex:
            messagebox.showerror("error", "current data set has already been cleaned. {}".format(ex))
        except NameError:
            messagebox.showerror("error", "No data to display, \n "
                                          "load the initial data set \n "
                                          "or load from the database. ")

    # Opens a csv file, cleans then converts it into a JSON file
    def prep_initial_dataset():
        """loads the initial data set and saves it to the database."""

        def open_file():
            """Open a file for editing."""
            set_pandas_display_options()
            filepath = askopenfilename(
                filetypes=[("Text Files", "*.csv"), ("All Files", "*.*")]
            )
            if not filepath:
                return
            else:
                return filepath

        try:
            global violations
            global inspections
            global inventory

            violations = None
            inspections = None
            inventory = None

            messagebox.showinfo("Info", "Please select the three CSV files")

            while inspections is None or violations is None or inventory is None:
                data = DataController.prep_data(DataController.convert_csv_to_json(open_file()))
                if 'ACTIVITY DATE' in data.columns:
                    inspections = data
                elif 'SERIAL NUMBER' in data.columns:
                    violations = data
                    violations = violations.set_index("SERIAL NUMBER")
                elif 'SERIAL NUMBER' not in data.columns and 'FACILITY ID' in data.columns:
                    inventory = data
                else:
                    raise ValueError

            messagebox.showinfo("Completed", "Initial data set prepared and loaded into memory.")

        except FileNotFoundError:
            messagebox.showerror("Warning", "No files or missing files. The "
                                            "violations, inspections and inventory files must all be present")
        except TypeError:
            messagebox.showerror("Warning", "Files must be in CSV format")
        except ValueError:
            messagebox.showerror("Warning", "Files not correctly formatted.")

    def pre_initial_dataset_threads():

        def pre_initial_dataset_thread(data):
            with ThreadPoolExecutor() as ex:
                data_file = ex.submit(DataController.convert_csv_to_json, data)
                data_file = ex.submit(DataController.prep_data, data_file.result())
            return data_file.result()

        try:
            global violations
            global inspections
            global inventory
            with ThreadPoolExecutor() as pool:

                violations = pool.submit(pre_initial_dataset_thread, 'violations.csv').result()
                inspections = pool.submit(pre_initial_dataset_thread, 'Inspections.csv').result()
                inventory = pool.submit(pre_initial_dataset_thread, 'Inventroy.csv').result()

            messagebox.showinfo("Completed", "Initial data set loaded into memory.")

        except FileNotFoundError:
            messagebox.showerror("Warning", "No files or missing files. The "
                                            "violations, inspections and inventory files must all be present")
        except TypeError:
            messagebox.showerror("Warning", "Files must be in CSV format")
        except ValueError:
            messagebox.showerror("Warning", "Files not correctly formatted.")

    # save dataset to database
    def save_dataset():
        """save the current dataset to the database."""
        try:
            DataController.replace_database_collection(violations, "violations")
            DataController.replace_database_collection(inspections, "inspections")
            DataController.replace_database_collection(inventory, "inventory")
            messagebox.showinfo("Completed", "dataset as been saved to the database.")
        except RuntimeError as ex:
            messagebox.showerror("Warning", "Failed to save to the database. \n"
                                            "check the database connection ({})".format(ex))
        except NameError:
            messagebox.showerror("error", "No data to clean, \n "
                                          "load the initial data set \n "
                                          "or load from the database. ")

    # save dataset to database
    def save_dataset_threads():
            """save the current dataset to the database."""
            try:
                global violations
                global inspections
                global inventory

                tic = time.perf_counter()
                with ThreadPoolExecutor() as executor:
                    executor.submit(DataController.replace_database_collection, violations, "violations")
                    executor.submit(DataController.replace_database_collection, inspections, "inspections")
                    executor.submit(DataController.replace_database_collection, inventory, "inventory")
                executor.shutdown()
                toc = time.perf_counter()
                print(f"in {toc - tic:0.4f}")
            except RuntimeError as ex:
                messagebox.showerror("Warning", "Failed to save to the database. \n"
                                                "check the database connection ({})".format(ex))
            except NameError:
                messagebox.showerror("error", "No data to save, \n "
                                              "load the initial data set \n "
                                              "or load from the database. ")

    # load dataset from database
    def load_dataset_from_database():
        global violations
        global inspections
        global inventory
        try:
            violations = DataController.read_from_database('violations')
            inspections = DataController.read_from_database('inspections')
            inventory = DataController.read_from_database('inventory')
        except RuntimeError as exc:
            messagebox.showerror("error", 'Failed to open database ({})'.format(exc))
        except NameError:
            messagebox.showerror("error", "No data to display, \n "
                                          "load the initial data set \n "
                                          "or load from the database. ")

    # Opens one of the databases into the tree view
    def display_dataset():
        """display dataset with a tree view."""

        # Creates a popup to select one of the databases
        def choice_database():
            global pop
            pop = tk.Toplevel(window)
            pop.title("Select Database")
            v = tk.StringVar()
            databases = ["inspections",
                         "inventory",
                         "violations"]

            def get_choice():
                try:
                    global violations
                    global inspections
                    global inventory
                    if v.get() == "inspections":
                        setup_tree_view(inspections)
                    elif v.get() == "inventory":
                        setup_tree_view(inventory)
                    if v.get() == "violations":
                        setup_tree_view(violations)

                except NameError:
                    pop.destroy()
                    messagebox.showerror("error", "No data to display, \n "
                                                  "load the initial data set \n "
                                                  "or load from the database. ")
                except TypeError:
                    pop.destroy()
                    messagebox.showerror("error", "Data has not been cleaned.")
                except AttributeError:
                    pop.destroy()
                    messagebox.showerror("error", "Data has not been cleaned.")
                pop.destroy()

            tk.Label(pop,
                     text="""Select the Database to use:""",
                     justify=tk.LEFT,
                     padx=20).pack()

            for language in databases:
                tk.Radiobutton(pop,
                               text=language,
                               indicatoron=0,
                               width=20,
                               padx=20,
                               variable=v,
                               command=get_choice, value=language).pack(anchor=tk.W)

        set_pandas_display_options()
        choice_database()

    # display averages by  type of vendor’s seating or by zip code per year
    def display_avg():

        # Creates a popup to select average type
        def choice_avg():
            global pop

            pop = tk.Toplevel(window)
            pop.title("Select averages ")
            v = tk.StringVar()
            databases = ["by type of vendor’s seating",
                         "by zip code"]

            def get_choice():
                try:
                    global inspections

                    if v.get() == "by type of vendor’s seating":
                        data = DataController.averages(v.get(), inspections)
                        setup_tree_view(data)
                        pop.destroy()
                    elif v.get() == "by zip code":
                        data = DataController.averages(v.get(), inspections)
                        setup_tree_view(data)
                        pop.destroy()

                except AttributeError:
                    pop.destroy()
                    messagebox.showerror("error", "Data has not been cleaned.")
                except NameError:
                    pop.destroy()
                    messagebox.showerror("error", "No data to display, \n "
                                                  "load the initial data set \n "
                                                  "or load from the database. ")
                except TypeError:
                    pop.destroy()
                    messagebox.showerror("error", "Data has not been cleaned.")

            tk.Label(pop,
                     text="""Select the averages to display:""",
                     justify=tk.LEFT,
                     padx=20).pack()

            for language in databases:
                tk.Radiobutton(pop,
                               text=language,
                               indicatoron=0,
                               width=20,
                               padx=20,
                               variable=v,
                               command=get_choice, value=language).pack(anchor=tk.W)

        set_pandas_display_options()
        choice_avg()

    # display data using graphs
    def display_data_graph():
        pop = tk.Toplevel(window)
        pop.title("Data Graphs")
        sns.set_theme(style="whitegrid")
        frame = tk.Frame(pop, relief=tk.RAISED, bd=2)
        frame.pack(anchor=tk.CENTER, fill=tk.BOTH)
        f1 = plt.Figure(figsize=(7, 7))
        ax1 = f1.subplots()
        f2 = plt.Figure(figsize=(7, 7))
        ax2 = f2.subplots()
        sns.set()

        try:
            global violations
            DataController.violation_bar_graph(violations, 14, ax1)
            DataController.violation_scatter_graph(violations, ax2)

            bar_graph_label = tk.Label(frame, text="A bar graph of the most common violations that have been \n"
                                                   "committed by the establishments")

            scatter_graph_label = tk.Label(frame, text="A scatter graph of the placement of violations grouped by zip "
                                                       "codes")
            bar_graph_label.pack(side=tk.TOP, anchor=tk.NW)
            scatter_graph_label.pack(side=tk.TOP, anchor=tk.NE)
            canvas1 = FigureCanvasTkAgg(f1, frame)
            canvas1.draw()
            canvas1.get_tk_widget().pack(side=tk.LEFT, expand=1)
            canvas2 = FigureCanvasTkAgg(f2, frame)
            canvas2.draw()
            canvas2.get_tk_widget().pack(side=tk.RIGHT, expand=1)
        except NameError:
            messagebox.showerror("error", "No data to display, \n "
                                          "load the initial data set \n "
                                          "or load from the database. ")
            pop.destroy()
        except KeyError:
            messagebox.showerror("error", "Data has not been cleaned")
            pop.destroy()

    def on_closing():
        if messagebox.askokcancel("Quit", "Do you want to quit? \n"
                                          "Any data not saved \n"
                                          "will be lost."):
            window.destroy()

    window = tk.Tk()
    window.title("Data Manager")

    window.rowconfigure(0, minsize=900, weight=1)
    window.columnconfigure(1, minsize=1200, weight=1)

    fr_buttons = tk.Frame(window, relief=tk.RAISED, bd=2)
    tree_frame = tk.Frame(window, relief=tk.RAISED, bd=2)

    load_buttons = tk.Frame(fr_buttons, relief=tk.RAISED, bd=2)
    save_buttons = tk.Frame(fr_buttons, relief=tk.RAISED, bd=2)
    clean_buttons = tk.Frame(fr_buttons, relief=tk.RAISED, bd=2)
    display_buttons = tk.Frame(fr_buttons, relief=tk.RAISED, bd=2)

    my_tree = ttk.Treeview(tree_frame, selectmode='browse')
    scroll_vertical = ttk.Scrollbar(tree_frame, orient='vertical', command=my_tree.yview)
    my_tree.configure(yscroll=scroll_vertical.set)
    scroll_vertical.pack(side=tk.LEFT, fill=tk.Y)

    scroll_horizontal = ttk.Scrollbar(tree_frame, orient='horizontal', command=my_tree.xview)
    my_tree.configure(xscroll=scroll_horizontal.set)
    scroll_horizontal.pack(side=tk.BOTTOM, fill=tk.X)

    # buttons relating to loading data
    load_labels = tk.Label(load_buttons, text="Loading the dataset")
    btn_open = tk.Button(load_buttons, text="Load dataset from csv files", command=prep_initial_dataset)
    # btn_open = tk.Button(load_buttons, text="Load initial dataset", command=pre_initial_dataset_threads)
    btn_load = tk.Button(load_buttons, text="Load dataset from database", command=load_dataset_from_database)
    load_labels.pack(anchor=tk.W, fill=tk.BOTH)
    btn_open.pack(anchor=tk.W, fill=tk.BOTH)
    btn_load.pack(anchor=tk.W, fill=tk.BOTH)

    # button relating to saving data
    save_labels = tk.Label(save_buttons, text="Saving the dataset")
    btn_save = tk.Button(save_buttons, text="save dataset to database", command=save_dataset)
    # btn_save = tk.Button(save_buttons, text="save dataset to database", command=save_dataset_threads)
    save_labels.pack(anchor=tk.W, fill=tk.BOTH)
    btn_save.pack(anchor=tk.W, fill=tk.BOTH)

    # button relating to cleaning data
    clean_labels = tk.Label(clean_buttons, text="Cleaning dataset")
    btn_clean_dataset = tk.Button(clean_buttons, text="clean dataset", command=clean_dataset)
    clean_labels.pack(anchor=tk.W, fill=tk.BOTH)
    btn_clean_dataset.pack(anchor=tk.W, fill=tk.BOTH)

    # button relating to displaying data
    display_labels = tk.Label(display_buttons, text="Displaying dataset")
    btn_display_dataset = tk.Button(display_buttons, text="Display dataset", command=display_dataset)
    btn_display_avg = tk.Button(display_buttons, text="Display dataset Averages", command=display_avg)
    btn_display_graph = tk.Button(display_buttons, text="Display graph Data", command=display_data_graph)
    btn_clear_tree = tk.Button(display_buttons, text="Clear window", command=clear_tree)
    display_labels.pack(anchor=tk.W, fill=tk.BOTH)
    btn_display_dataset.pack(anchor=tk.W, fill=tk.BOTH)
    btn_display_avg.pack(anchor=tk.W, fill=tk.BOTH)
    btn_display_graph.pack(anchor=tk.W, fill=tk.BOTH)
    btn_clear_tree.pack(anchor=tk.W, fill=tk.BOTH)

    fr_buttons.grid(row=0, column=0, sticky="ns")
    load_buttons.grid(row=1, column=0, sticky="nsew", padx=5, pady=2)
    save_buttons.grid(row=2, column=0, sticky="nsew", padx=5, pady=2)
    clean_buttons.grid(row=3, column=0, sticky="nsew", padx=5, pady=2)
    display_buttons.grid(row=4, column=0, sticky="nsew", padx=5, pady=2)

    tree_frame.grid(row=0, column=1, sticky="nsew")
    my_tree.pack(expand=tk.YES, fill=tk.BOTH)
    window.protocol("WM_DELETE_WINDOW", on_closing)
    window.mainloop()


data_viewer()
