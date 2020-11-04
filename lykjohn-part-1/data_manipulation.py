'''
-------------------------------------------
SI 618 Project Part 1 - Data Manipulation
by Yin Kwong John Lee
-------------------------------------------
This script aims to preporcess the data from financial statements such that selected business metrics are calculated and tidied for further computation.
'''

## Importing libraries
import pandas as pd
import os 
CURRENT_DIR="./project_part1_report_lykjohn" # . being the path where the project_part1_report_lykjohn.zip is unzipped
os.chdir(CURRENT_DIR)


#-----------------------Combining Financial Statements------------------------
## Importing data frames
# market_income=pd.read_csv("./data/market_income.csv")
# market_balance=pd.read_csv("./data/market_balance.csv")
# market_cashflow=pd.read_csv("./data/market_cashflow.csv")

## Merge income statement with balance sheets on ticker and fiscal year
# sections_df=market_income.merge(market_balance, on=["ticker", "fiscal_year"], how="inner")

## Then merge with cash flow statement data on ticker and fiscal year
# sections_df=sections_df.merge(market_cashflow, on=["ticker", "fiscal_year"], how="inner")

# sections_df.to_csv("company_sections.csv", index=False)

#------------------------- Loading and Merging Data------------------------
## Merge  with income statement data on company data on ticker
company_info=pd.read_csv("./data/company_info.csv")
company_sections=pd.read_csv("./data/company_sections.csv")

financial_df=company_info.merge(company_sections, on="ticker", how="inner")

#---------------------------Structuring Data----------------------------------

## Drop rows with any missing values in parametric columns
financial_df=financial_df.dropna(how="any",subset=['num_shs_outstand', 'revenue', 'pretax_income', 'net_income', 'total_current_assets', 'total_assets', 'st_debt','total_current_liabilities', 'lt_debt', 'total_liabilities','retained_earnings', 'cash_operating_activities'])

## We assume missing dividend values to mean that the company does not pay dividend. Therefore, we set the missing values to 0. Also, replace the NaN exchange mediums and locations with "Unknown"'s
financial_df=financial_df.fillna(value={"dividends_paid":0,"location":"Unknown"})

## Replace invalid locations with "Unknown"
import re
r=re.compile(r"^((?!.*\d+).)*$")
financial_df.loc[financial_df['location'].apply(lambda x: not bool(r.match(x))),"location"]="Unknown"

#--------------------Combining Data (Appending Metrices)--------------------------
## 1) Constructing Return on Equity (ROE)=Pre-tax Income/ (Total Assets - Total Liabilities)
financial_df["ROE"]=financial_df["pretax_income"]/(financial_df["total_assets"]-financial_df["total_liabilities"])

## 2) Constructing Retrun on Total Capital (ROTC)=Pre-Tax Income/ ( (Total Assets - Total Liabilities) + Short-term Debt + Long-term Debt)
financial_df["ROTC"]=financial_df["pretax_income"]/(financial_df["total_assets"]-financial_df["total_liabilities"]+financial_df["st_debt"]+financial_df["lt_debt"])

## 3) Constructing Net Profit Margin (NPM)= Pre-tax Income/ Revenue
financial_df["NPM"]=financial_df["pretax_income"]/financial_df["revenue"]

## 4) Constructing Current Ratio= Total Current Assets/ Total Current Liabilities
financial_df["CURRENT_RATIO"]=financial_df["total_current_assets"]/financial_df["total_current_liabilities"]

## 5) Constructing Debt-EBIT (Earnings Before Income Tax) Ratio = (Short-term Debt + Long-term Debt)/ Pre-tax Income
financial_df["DEBT_EBIT"]=(financial_df["st_debt"]+financial_df["lt_debt"])/financial_df["pretax_income"]

## 6) Dividend Payout Ratio = Dividends Paid/ After-Tax Income
financial_df["DIV_PAYOUT_RATIO"]=abs(financial_df["dividends_paid"]/financial_df["net_income"])

## 7) Earnings Per Share (EPS) = Net Income/ Number of Shares Outstanding
financial_df["EPS"]=financial_df["net_income"]/financial_df["num_shs_outstand"]

## 8) Retained Earnings Per Share (REPS)= Retained Earnings/Number of Shares Outstanding
financial_df["REPS"]=financial_df["retained_earnings"]/financial_df["num_shs_outstand"]


##-----------------------Export to csv and json format-------------------------
financial_df.to_csv("company_metrics.csv", index=False)
financial_df.to_json("company_metrics.json", orient="records", lines=True)





