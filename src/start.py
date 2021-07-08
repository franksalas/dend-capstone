
# required
import pandas as pd
import numpy as np

# Pandas options
pd.set_option('max_columns', 1000)
pd.set_option('max_rows', 100)
pd.set_option('display.max_colwidth', 100)


# python
import sys,os
import re
import glob
import pickle
import os
import io
# Visualization

# seaborn
import seaborn as sns
sns.set(rc={'figure.figsize':(10,5)})
import matplotlib.pyplot as plt
import matplotlib
# increase size of standar plots
plt.rcParams["figure.figsize"] = [10, 5]
#plt.style.use('ggplot')

# plotly
import plotly
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
init_notebook_mode(connected=True)

# AWS
import boto3
import awswrangler as wr

# plot save paths

print(f'''
python\t{sys.version}
---------------------
Versions:
----------------------
pandas      {pd.__version__}
numpy       {np.__version__}
matplotlib  {matplotlib.__version__}
seaborn     {sns.__version__}
plotly      {plotly.__version__}
boto3       {boto3.__version__}
awswrangler {wr.__version__}
----------------------
''')

print('''
Loaded Libraries
-------------------
import pandas as pd
import numpy as np
import sys,os
import re
import glob
import pickle
import seaborn as sns
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
init_notebook_mode(connected=True)


## AWS
import boto3
import awswrangler as wr
----------------


GLOBAL VARIABLES
--------------------------
# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id = AWS_KEY_ID,
    aws_secret_access_key = AWS_SECRET,
)
    
# Creating the high level object oriented interface
resource = boto3.resource(
    's3',
    aws_access_key_id = AWS_KEY_ID,
    aws_secret_access_key = AWS_SECRET,
)

HERE_DIR: list current directory path
------------------------------

# SAVE PLOTS
To save a plot call
`save_plots(fig_variable,'name_of_plot')`

source file: src/start.py

''')

# GLOBAL VARIABLES
# --------------------------
# HERE_DIR: list current directory path
# RAW_DIR: list files in `data\\raw` directory
# INTER_DIR: list files in `data\\interim` directory
# FINAL_DIR: list files in `data\\processed` directory
# SRC_DIR: list files in `src\` directory
# ------------------------------
# source file: src/start.py


sys.path.append("..") # Adds higher directory to python modules path.

#global
HERE_DIR  = os.getcwd()
# RAW_DIR = os.listdir('../data/raw')
# FINAL_DIR = os.listdir('../data/final')
# INTER_DIR = os.listdir('../data/interim')
# SRC_DIR = os.listdir('../src')







# AWS Stuff
AWS_KEY_ID = os.environ.get('AWS_KEY_ID')
AWS_SECRET = os.environ.get('AWS_SECRET_KEY')



# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id = AWS_KEY_ID,
    aws_secret_access_key = AWS_SECRET,
)
    
# Creating the high level object oriented interface
resource = boto3.resource(
    's3',
    aws_access_key_id = AWS_KEY_ID,
    aws_secret_access_key = AWS_SECRET,
)











## Save plots 
# must be from /notebooks not /notebooks/folderx
# pickle plots save
pkl_directory_saves = os.path.join( '..','reports','figures','pkl_plots/')
# create directory, if it does not exist                                 
if not os.path.exists(pkl_directory_saves):
    os.makedirs(pkl_directory_saves)
    
## plot image save
img_directory_saves = os.path.join( '..','reports','figures','img/')
# create directory, if it does not exist                                 
if not os.path.exists(img_directory_saves):
    os.makedirs(img_directory_saves)
    

# plotly save
plt_directory_saves = os.path.join( '..','reports','figures','plotly/')
# create directory, if it does not exist                                 
if not os.path.exists(plt_directory_saves):
    os.makedirs(plt_directory_saves)


# save plot function
# helper functions
#example save_plots(fig_2,'02_close_status_problem_ticket')
def save_plots(plt_figure,name):
    # change background
    #plt_figure = iplot(plt_figure)
    plt_figure.update_layout(plot_bgcolor='rgba(0,0,0,0)',  # white background
                    #paper_bgcolor='rgba(0,0,0,0)', # clear background but  makes png downloads lables w/errors
                    xaxis=dict(showgrid=True,gridcolor='#ddd'),  # grid color to grayish
                    yaxis=dict(showgrid=True,gridcolor='#ddd'),  # grid color to grayish
                             hovermode='closest',)
    # generate plot
    #iplot(plt_figure)
    # save to html
    html_name = '{}.html'.format(name)
    plot(plt_figure,filename=plt_directory_saves + html_name)
    # save to pkl format
    filename = pkl_directory_saves+'{}'.format(name)
    outfile = open(filename,'wb')
    pickle.dump(plt_figure,outfile)
    outfile.close()