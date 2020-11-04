'''
-------------------------------------------
SI 618 Project Part 1 - Data Visualization
by Yin Kwong John Lee
-------------------------------------------
This script aims examine whether is a good indicator exitst for identifying vaible businesses. This is done by graphically representing the company performances and to observe proper trends.
'''

import pandas as pd
import matplotlib.pyplot as plt
import os 

CURRENT_DIR="./project_part1_report_lykjohn" # . being the path where the project_part1_report_lykjohn.zip is unzipped
os.chdir(CURRENT_DIR)

## Here, I tidied the data headers manually because csv header was set as false to avoid repeated headers for spark `-getmerge`. Headers may also be tidied with pandas.
corporate_viability=pd.read_csv("corporate_viability.csv")
## drop ticker and location columns because unecessary for our visualization
corporate_viability=corporate_viability.drop(["ticker","location"], axis=1)

## rearrange columns for visualizaiton purposes
col=corporate_viability.columns.to_list()
col=col[1:]+col[:1]
corporate_viability=corporate_viability[col]
## Visualization with seaborn
import seaborn as sns
sns.pairplot(corporate_viability,diag_kind=None, height=2)


















