import pandas as pd

# Check if a value can be mapped to a float
def check_input(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

infile='introduction-to-the-zoo-tools-classifications.csv'
outfile='zoo_tools_results.csv'
workflow='workflow v4 - use this'
all_data=pd.read_csv(infile)
workflow_data=all_data.groupby('workflow_name').get_group(workflow)
results=pd.DataFrame({'Home Latitude':[],
                     'Home Longitude':[],
                     'Institution Latitude':[],
                     'Institution Longitude':[]
                     })
for i in range(len(workflow_data)):
    tasks=pd.read_json(workflow_data['annotations'].iloc[i])['value'].iloc[0]

    # Since tasks alternate numerical values / NSEW options,
    # even indices give numerical values, odd indices give NSEW choices
    home_lat,home_lon,inst_lat,inst_lon=[tasks[i]['value'] for i in (0,2,4,6)]
    home_lat_ns,home_lon_ew,inst_lat_ns,inst_lon_ew=[tasks[i]['value'][0]['label'] for i in (1,3,5,7)]

    # Since eye/hair color are optional, need to check that a response exists
    # before parsing.
    #eye_col_task=tasks[8]['value'][0]
    #if len(eye_col_task)>1:
    #    eye_col=eye_col_task['label']
    #else:
    #    eye_col='N/A'

    #hair_col_task=tasks[9]['value'][0]
    #if len(hair_col_task)>1:
    #    hair_col=hair_col_task['label']
    #else:
    #    hair_col='N/A'

    # Check that all numerical inputs are a single number that can be mapped to a float
    if all([check_input(x) for x in (home_lat,home_lon,inst_lat,inst_lon)]):
        home_lat,home_lon,inst_lat,inst_lon=map(float,[home_lat,home_lon,inst_lat,inst_lon])

        # Use the NSEW inputs to convert numerical values to positive/negative
        # Since South/West lat/lons could be input as negative values, also check if
        # the value is already the appropriate sign
        if home_lat >0 and home_lat_ns=='South':
            home_lat=-home_lat
        if home_lon >0 and home_lon_ew=='West':
            home_lon=-home_lon
        if inst_lat >0 and inst_lat_ns=='South':
            inst_lat=-inst_lat
        if inst_lon >0 and inst_lon_ew=='West':
            inst_lon=-inst_lon

        new_row=pd.DataFrame({'Home Latitude':[home_lat],
                     'Home Longitude':[home_lon],
                     'Institution Latitude':[inst_lat],
                     'Institution Longitude':[inst_lon]
                     })
        results=pd.concat([results,new_row])


# Correctly reorders columns and resets indices for neatness
results=results[['Home Latitude',
                 'Home Longitude',
                 'Institution Latitude',
                 'Institution Longitude']].reset_index(drop=True)
results.to_csv(outfile,index=False)
