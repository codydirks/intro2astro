import pandas as pd
import numpy as np


'''
1. Read in cvs
2. get classification information
3. get galaxy metadata (redshift, type)
'''

def get_classification(row):
    # Takes an entire row (i.e. a single classification) and grabs
    # the data relevant to the line identification
    annotation_json=row['annotations']
    metadata_json='['+row['metadata']+']'
    json_object=pd.read_json(annotation_json)['value'][0]
    metadata_object=pd.read_json(metadata_json)['subject_dimensions'][0][0]
    nw=metadata_object['naturalWidth'] # Need this value in case students rescaled the window
    if json_object and len(json_object[0])==7:# Error checks for a valid classification
        xleft = json_object[0]['x']
        width = json_object[0]['width']
    else:
        xleft = np.nan
        width = np.nan
    return {'xleft' : xleft, 'width' : width, 'nw': nw}

def get_galaxy_metadata(subject_data_json):
    json_object=pd.read_json(subject_data_json, orient='index')
    ra = json_object['RA'].values[0]
    dec = json_object['Dec'].values[0]
    z = json_object['Redshift'].values[0]
    galID = json_object['dr7objid'].values[0]
    elliptical = bool(json_object['elliptical'].values[0])
    return {'ra' : ra, 'dec' : dec, 'z' : z, 'galID' : galID, 'elliptical' : elliptical}

def calc_lambda_central(classification_dict):
    # Input is a classification dictionary with x_left, width, and natural window width
    xleft = classification_dict['xleft']
    width = classification_dict['width']
    nw = classification_dict['nw']
    xmin = int((108./1152.)*nw)# These hardcoded pixel values represent the default window sizes
    xmax = int((1081./1152.)*nw)# If the actual window was sized differently, the factor of 'nw' scales the result appropriately
    lambdamin = 380.
    lambdamax = 500.
    lamperpix = (lambdamax - lambdamin) / (xmax - xmin)
    lambdacen = (xleft + (width / 2.) - xmin) * lamperpix + lambdamin
    return lambdacen

# Raw input file exported from the project builder
infile = "intro2astro-hubbles-law-classifications.csv"

# Name of output csv file where results will be stored
outfile='NU_astro_120.csv'

# Name of workflow to be analyzed
workflow='NU Highlights of Astronomy'
all_raw_data = pd.read_csv(infile)

all_raw_data['classification'] = all_raw_data.apply(get_classification,axis=1)
all_raw_data['galaxy_metadata'] = all_raw_data['subject_data'].apply(get_galaxy_metadata)
all_raw_data['lambdacen'] = all_raw_data['classification'].apply(calc_lambda_central)
all_raw_data['galaxy_id'] = [i['galID'] for i in all_raw_data['galaxy_metadata']]

# Select the workflow we are concerned with
section_groups = all_raw_data.groupby('workflow_name')
workflow_data = section_groups.get_group(workflow)


# Create new columns for purpose of filtering based on time
# I wanted to be able to filter on time because some students found
# the link to the project in Canvas before the instructor had demonstrated
# what to do, and thus gave bad data. So, this would be a way to throw out
# data that was recorded before a certain date and hour. For now though,
# these lines are commented out, and all data is taken in.

#workflow_data['day']=[int(i.split()[0].split('-')[-1]) for i in workflow_data['created_at']]
#workflow_data['hour']=[int(i.split()[1].split(':')[0]) for i in workflow_data['created_at']]
#workflow_data=workflow_data.loc[(workflow_data['day']>23) & (workflow_data['hour']>0)]


# Group data by galaxy id
gal_groups = workflow_data.groupby('galaxy_id')

# for each galaxy, calculate the average central wavelength
galaxy_names = [name for name, group in gal_groups]
nclass = []
lambdacen = []
lambdaerr = []
ra = []
dec = []
z = []

for galname in m_galaxy_names:
    lambdacen.append(gal_groups.get_group(galname)['lambdacen'].mean())
    lambdaerr.append(gal_groups.get_group(galname)['lambdacen'].std())
    ra.append(gal_groups.get_group(galname)['galaxy_metadata'].iloc[0]['ra'])
    dec.append(gal_groups.get_group(galname)['galaxy_metadata'].iloc[0]['dec'])
    z.append(gal_groups.get_group(galname)['galaxy_metadata'].iloc[0]['z'])
    nclass.append(gal_groups.get_group(galname)['lambdacen'].count())

# Approximate the distances based on the redshifts
dist = [i * 3e5 / 68 for i in z]

# create new dataframe with counts of classifications for each galaxy
results = pd.DataFrame({'Galaxy ID' : galaxy_names,
                   'RA' : ra,
                   'Dec' : dec,
                   'Dist' : dist,
                   'N Class' : nclass,
                   'lambda_av' : lambdacen,
                   'lambda_err' : lambdaerr})
results = results[['Galaxy ID', 'N Class', 'RA', 'Dec', 'Dist', 'lambda_av', 'lambda_err']]

# export data frame as csv file
results.to_csv(outfile, index=False)
