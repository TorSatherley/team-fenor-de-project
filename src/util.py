import json


def json_to_pg8000_output(filepath, include_cols_in_output=True):
    """
    Reads the json and returns is as a nested list (the output format of p8000)
    Cols are also outputed as standard, as they will be needed as well

    """

    # Opening JSON file
    f = open(
        filepath,
    )
    simulated_pg8000_output = []
    simulated_pg8000_output_cols = []
    # returns JSON object as
    # a dictionary
    data = json.load(f)

    for i in data:
        simulated_pg8000_output += [list(i.values())]

    for i in data[0].keys():
        simulated_pg8000_output_cols += [i]

    # Closing file
    f.close()

    return simulated_pg8000_output, simulated_pg8000_output_cols
