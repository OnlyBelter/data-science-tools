import os
import pandas as pd


root_dir = r'.'
file_name = 'demo_input_file.csv'

file_path = os.path.join(root_dir, file_name)
COL_NAME = ["name", "mz", "rt", "Annotation.type", "annotated.from.ID", "annotated.from.peak", "ID", "compound.name",
               "isotope","adduct","Formula","score","peak.group","confidence"]

def flat_csv(f_path, save_path):
    file = pd.read_csv(f_path)
    file_flat = pd.DataFrame(columns=file.columns.values)
    # print(file_flat)
    (row_n, col_n) = file.shape
    # print(row_n, col_n)
    for i in range(row_n):
        _ = file.loc[i]
        ids = _.ID.split(';')
        for j in range(len(ids)):
            single_recode = []
            for col in COL_NAME:
                if col in COL_NAME[0:3]:
                    single_recode.append(_[col])
                else:
                    try:
                        single_recode.append(_[col].split(';')[j])
                    except:
                        # print(_)
                        single_recode.append('NA')
            # add a new line at the end of this dataFrame
            file_flat.loc[len(file_flat)] = single_recode
    # don't need to output index
    file_flat.to_csv(save_path, index=False)
    print('Flatting finished!')


def filter_flat_data(f_path, save_path):
    flat_data = pd.read_csv(f_path)
    # add a new column 'check'
    flat_data['check'] = False
    id2grade = {}  # each id's smallest grade level
    (row_n, col_n) = flat_data.shape
    for i in range(row_n):
        id = flat_data.loc[i].ID
        grade = flat_data.loc[i].confidence
        if id not in id2grade:
            id2grade[id] = grade
        elif grade != id2grade[id]:
            if grade < id2grade[id]:
                id2grade[id] = grade
    for i in range(row_n):
        id = flat_data.loc[i].ID
        grade = flat_data.loc[i].confidence
        # print(grade, id2grade[id])
        if grade == id2grade[id]:
            # change the value of column 'check' in this row
            flat_data.loc[i, 'check'] = True
            # print(flat_data.loc[i])
    # get a new df depending on the condition of 'check'
    # drop a whole column
    new_flat_data = flat_data[flat_data['check']==True].drop('check', axis=1)
    new_flat_data.to_csv(save_path, index=False)
    print('Filtering finished!')



def group_data(flat_data_file, save_path):
    flat_data = pd.read_csv(flat_data_file)
    # groupby through three columns
    gb = flat_data.groupby(['name', 'mz', 'rt'])
    # aggregate the result of groupby
    aggregated = gb.aggregate(lambda x: tuple(x))
    # defining a function for apply
    join = lambda x: [';'.join([str(j) for j in i]) for i in x]
    # grouped_data = aggregated.iloc[:, 1:].apply(join)
    grouped_data = aggregated.apply(join)
    # print(grouped_data)
    # sort by 'ID'
    grouped_data.sort_values(by='ID', ascending=True, inplace=True)
    # write to csv with the same column order as COL_NAME
    grouped_data.to_csv(save_path, columns=COL_NAME[3:])
    print('Grouping finished!')

flat_file_path = os.path.join(root_dir, 'flat_data.csv')
filtered_flat_file_path = os.path.join(root_dir, 'filtered_flat.csv')
result_path = os.path.join(root_dir, 'result.csv')

flat_csv(file_path, flat_file_path)
filter_flat_data(flat_file_path, filtered_flat_file_path)
group_data(filtered_flat_file_path, result_path)
