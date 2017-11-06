import numpy as np
import pandas as pd
from astroquery.sdss import SDSS

def get_stats(choices_dict):
    # Input is a dict with classification options as the keys, and each option's count as integer values
    options=choices_dict.keys()
    n_tot=sum(choices_dict.values())
    stats_dict={}
    for option in options:
        stats_dict['p_'+option]=round(float(choices_dict[option])/float(n_tot),4)

    return stats_dict

# Output file from Panoptes
infile='galaxy-zoo-in-astronomy-101-classifications.csv'

# Name of the specific workflow to grab and export.
workflow_name = 'NU Highlights of Astronomy'

all_raw_data=pd.read_csv(infile)

# Grab data from our workflow
workflow_data=all_raw_data.groupby('workflow_name').get_group(workflow_name)

# This code chunk will print each user and how many classifications they did
#for usr in workflow_data.user_name.unique():
#    usr_data=workflow_data.groupby('user_name').get_group(usr)
#    print '{:>13}'.format(usr[0:13]), len(usr_data),

# Group galaxies by their galaxy ids
galaxy_groups=workflow_data.groupby('subject_ids')


# Create empty dataframes to fill, one for our classification data, and one for
# the data from the original Galaxy Zoo for comparison
workflow_results=pd.DataFrame({'Galaxy ID':[],
                        'N_Votes':[],
                        'p_ell':[],
                        'p_sp':[],
                        'p_mrg':[],
                        'p_oth':[],
                        'g_mag':[],
                        'r_mag':[],
                        'z':[]})

zoo_results=pd.DataFrame({'Galaxy ID':[],
                        'N_Votes':[],
                        'p_ell':[],
                        'p_sp':[],
                        'p_mrg':[],
                        'p_oth':[],
                        'g_mag':[],
                        'r_mag':[],
                        'z':[]})

# Iterate through each id to find that galaxy's stats
for gx in workflow_data.subject_ids.unique():
    subject_classifications=galaxy_groups.get_group(gx)

    # Get SDSS galaxy id from metadata, specifically image name
    image_file=pd.read_json(subject_classifications.iloc[0]['subject_data'])[gx]['image_file']
    gx_id=image_file[:-5]
    #display(Image(filename='Activity2Images/'+image_file, width=200, height=200))

    # Count number of each type of classification, and get stats based on those numbers
    choices={'Spiral':0,'Elliptical':0,'Merger':0,'Star/artifact':0}
    for index,row in subject_classifications.iterrows():
        choice=str(pd.read_json(row['annotations'])['value'].values[0])
        if choice != 'nan':
            choices[choice]+=1
    stats_dict=get_stats(choices)

    # Perform SDSS SQL queries to determine magnitude and redshift for this galaxy.
    mag_sql_query='select top 1 modelmag_g,modelmag_r from photoobjall where objid='+gx_id
    z_sql_query='select top 1 z from specobjall where bestobjid='+gx_id
    zooVotes_sql_query='select top 1 nvote_std,p_el,p_cs,p_mg,p_dk from zooVotes where objid='+gx_id

    mags=SDSS.query_sql(mag_sql_query).to_pandas()

    # In case galaxy doesn't have a measured redshift, need to catch resulting SQL error
    try:
        z=SDSS.query_sql(z_sql_query)[0][0]
    except:
        z=np.nan # If no redshift, flag value
        pass

    new_workflow_row=pd.DataFrame({'Galaxy ID':[gx_id],
                        'N_Votes'    :[sum(choices.values())],
                        'p_ell'      :[stats_dict['p_Elliptical']],
                        'p_sp'       :[stats_dict['p_Spiral']],
                        'p_mrg'      :[stats_dict['p_Merger']],
                        'p_oth'      :[stats_dict['p_Star/artifact']],
                        'g_mag'      :[round(mags['modelmag_g'].values[0],4)],
                        'r_mag'      :[round(mags['modelmag_r'].values[0],4)],
                        'z'          :[round(z,4)]
                         })
    workflow_results=pd.concat([workflow_results, new_workflow_row])


    # Grabs original Galaxy Zoo voting data for the galaxy
    zooVotes=SDSS.query_sql(zooVotes_sql_query).to_pandas()

    new_zoo_row=pd.DataFrame({'Galaxy ID':[gx_id],
                        'N_Votes'    :[zooVotes['nvote_std'].values[0]],
                        'p_ell'      :[zooVotes['p_el'].values[0]],
                        'p_sp'       :[zooVotes['p_cs'].values[0]],
                        'p_mrg'      :[zooVotes['p_mg'].values[0]],
                        'p_oth'      :[zooVotes['p_dk'].values[0]],
                        'g_mag'      :[round(mags['modelmag_g'].values[0],4)],
                        'r_mag'      :[round(mags['modelmag_r'].values[0],4)],
                        'z'          :[round(z,4)]
                         })
    zoo_results=pd.concat([zoo_results, new_zoo_row])

# Miscellaneous formatting stuff

#Rearrange table to group appropriate columns together
workflow_results=workflow_results[['Galaxy ID', 'N_Votes', 'p_ell', 'p_sp', 'p_mrg', 'p_oth', 'g_mag', 'r_mag', 'z']]
zoo_results=zoo_results[['Galaxy ID', 'N_Votes', 'p_ell', 'p_sp', 'p_mrg', 'p_oth', 'g_mag', 'r_mag', 'z']]

#Sort by Galaxy ID so we can quickly compare tables, and reset indices for neatness
workflow_results=workflow_results.sort_values('Galaxy ID').reset_index(drop=True)
zoo_results=zoo_results.sort_values('Galaxy ID').reset_index(drop=True)

# Typecast these columns as ints for neatness
for col in (['N_Votes']):
    workflow_results[col] = workflow_results[col].astype(int)
    zoo_results[col] = zoo_results[col].astype(int)

# Stores both results dataframes as .csv's
workflow_results.to_csv('Intro2Astro_GZ_workflow_output.csv')
zoo_results.to_csv('Intro2Astro_GZ_zoo_output.csv')
