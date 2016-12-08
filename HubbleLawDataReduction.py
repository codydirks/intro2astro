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

#Raw input file exported from the project builder
infile = "intro2astro-hubbles-law-classifications.csv"

#Name of output csv file where results will be stored
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
nu_raw_data = section_groups.get_group(workflow)


# Create new columns for purpose of filtering based on time
# I wanted to be able to filter on time because some students found
# the link to the project in Canvas before the instructor had demonstrated
# what to do, and thus gave bad data. So, this would be a way to throw out
# data that was recorded before a certain date and hour. For now though,
# these lines are commented out, and all data is taken in.

#nu_raw_data['day']=[int(i.split()[0].split('-')[-1]) for i in nu_raw_data['created_at']]
#nu_raw_data['hour']=[int(i.split()[1].split(':')[0]) for i in nu_raw_data['created_at']]
#nu_data=nu_raw_data.loc[(nu_raw_data['day']>23) & (nu_raw_data['hour']>0)]
nu_data=nu_raw_data


# Group data by galaxy id
nu_gal_groups = nu_data.groupby('galaxy_id')

# for each galaxy, calculate the average central wavelength
m_galaxy_names = [name for name, group in nu_gal_groups]
m_nclass = []
m_lambdacen = []
m_lambdaerr = []
m_ra = []
m_dec = []
m_z = []

for galname in m_galaxy_names:
    m_lambdacen.append(nu_gal_groups.get_group(galname)['lambdacen'].mean())
    m_lambdaerr.append(nu_gal_groups.get_group(galname)['lambdacen'].std())
    m_ra.append(nu_gal_groups.get_group(galname)['galaxy_metadata'].iloc[0]['ra'])
    m_dec.append(nu_gal_groups.get_group(galname)['galaxy_metadata'].iloc[0]['dec'])
    m_z.append(nu_gal_groups.get_group(galname)['galaxy_metadata'].iloc[0]['z'])
    m_nclass.append(nu_gal_groups.get_group(galname)['lambdacen'].count())

# Approximate the distances based on the redshifts
m_dist = [i * 3e5 / 68 for i in m_z]

# create new dataframe with counts of classifications for each galaxy
nu_results = pd.DataFrame({'Galaxy ID' : m_galaxy_names,
                   'RA' : m_ra,
                   'Dec' : m_dec,
                   'Dist' : m_dist,
                   'N Class' : m_nclass,
                   'lambda_av' : m_lambdacen,
                   'lambda_err' : m_lambdaerr})
nu_results = nu_results[['Galaxy ID', 'N Class', 'RA', 'Dec', 'Dist', 'lambda_av', 'lambda_err']]

# export data frame as csv file
nu_results.to_csv(outfile, index=False)
