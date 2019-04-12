
NETWORKING & LINUX
SSH (http://blog.robertelder.org/what-is-ssh/)
SSH is a network protocol for securely communicating between computers.  Often when people refer to 'using SSH', they are referring to using an SSH client to connect to another computer's SSH server in order to remotely run commands on that computer.  Any computer is capable of running both an SSH client and a server.
SSH also supports public-key cryptography which has a number of security benefits over traditional password-based authentication.
Most people are used to the type of authentication where you specify a username and a password which gets sent to a server.  The server then checks if your password matches and if it does you are allowed access.  Public key cryptography is a bit different and works by requiring the user to create a 'key pair' which consists of:
•	A public key that you can distribute to anyone.
•	A private key that should be kept secret by the person who created it.
We won't go into the details of how public key cryptography works (because it requires a lot of math), but you just need to know these details:
•	There is a complex mathematical relationship between the public and private key.
•	A public key can be used to encrypt messages, but not decrypt them.
•	A private key can decrypt messages encrypted with the public key.
•	 Once you've created these two files, the general idea is that you can log into remote computers by distributing the public key to the server you want to log into.  The private key will always be kept secret on your machine, and you'll need it every time you want to log into a remote computer.
•	 Once you've created these two files, the general idea is that you can log into remote computers by distributing the public key to the server you want to log into.  The private key will always be kept secret on your machine, and you'll need it every time you want to log into a remote computer.




SPARK (Python)
RDD (sc.parallelize)
RDD stands for Resilient Distributed Dataset, these are the elements that run and operate on multiple nodes to do parallel processing on a cluster. RDDs are immutable elements, which means once you create an RDD you cannot change it. RDDs are fault tolerant as well, hence in case of any failure, they recover automatically. You can apply multiple operations on these RDDs to achieve a certain task.

Transformation − These are the operations, which are applied on a RDD to create a new RDD. Filter, groupBy and map are the examples of transformations.
Action − These are the operations that are applied on RDD, which instructs Spark to perform computation and send the result back to the driver.
There are two types of shared variables supported by Apache Spark −
•	Broadcast-Broadcast variables are used to save the copy of data across all nodes. This variable is cached on all the machines and not sent on machines with tasks. 
•	Accumulator-Accumulator variables are used for aggregating the information through associative and commutative operations. For example, you can use an accumulator for a sum operation or counters (in MapReduce).
StorageLevel
class pyspark.StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication = 1)
DISK_ONLY = StorageLevel(True, False, False, False, 1)
MEMORY_AND_DISK_2 = StorageLevel(True, True, False, False, 2)
MEMORY_ONLY_SER_2 = StorageLevel(False, True, False, False, 2)
OFF_HEAP = StorageLevel(True, True, True, False, 1)


HIVE
ORC formatted table can use alter commands to add columns but the columns are added in the end after partition.
For Normal managed and external table added columns will be positioned before the partitioned columns.
ALTER TABLE CRP_AMERICAS_AUTOHOME_USA.T_AH_CTRL_PT ADD columns (SALVG_CLCT_AMT DECIMAL(25,6),SBRGT_AMT DECIMAL(25,6)) CASCADE;
Using cascade for partitioned tables updates the metadata of the table and all metadata across all partitions.



1.	Lets say you have a external table test_1 in hive. And you want to rename it test_2 which should point test_2 location not test_1. Then you need to convert this table into Managed table using below command. test_1 -> pointing to test_1 location
2.	ALTER TABLE db_name.test_1 SET TBLPROPERTIES('EXTERNAL'='FALSE');
3.	Rename the table name.
4.	ALTER TABLE db_name.test_1 RENAME TO db_name.test_2;
5.	Again convert the managed table after renaming to external table.
6.	ALTER TABLE db_name.test_2 SET TBLPROPERTIES('EXTERNAL'='TRUE');
db_name.test_2 table will point the test_2 location. If we do it without making the managed table it will point the test_1 location.

REPLACE COLUMNS removes all existing columns and adds the new set of columns. This can be done only for tables with a native SerDe (DynamicSerDe, MetadataTypedColumnsetSerDe, LazySimpleSerDe and ColumnarSerDe).







-------------------------------------------------------------
Skip to content
Skip to breadcrumbs
Skip to header menu
Skip to action menu
Skip to quick search

Quick Search   


Browse 
Chakravarti, Deepan (Cognizant) 
Dashboard 
MetLifeCorp 
…
Home 
Variable Annuity Dynamic Hedging 
Edit 
Share 
Add 
Tools 
 Variable Annuity Dynamic Hedging 


Skip to end of metadata 

Page restrictions apply 
Attachments:45 
Added by Sundararajan, Muralirajan(Cognizant), last edited by Balasubramanian, Aravinth (Cognizant) on Mar 23, 2017  (view change) 
Go to start of metadata 

Introduction
Overview
Scope of VADH
Process Flow
Tools and Technologies
Release
Glossary
New Joinee setup
Variable Annuity Dynamic Hedging Process Flow

Both domestic and international data extract files are loaded into Hedge database for performing the transformation. Under RBC, only domestic extract files are loaded. After transforming the data, HPC Trading Grids will be populated in High Performance Computing (HPC) Grid Server which is then loaded into the website through internet. After extraction, data will be fetched through Asset Liability Management (ALM) and Trade Positioning System (TPS) in order to retrieve the policy level details and subsequently reports will be populated for end users.
TPS will get the trading information from Bloomberg Live Link, which is 24-hour business & financial live television network and Derivative positions.
 
Landscape Diagram:
Note: Click on the image to see it fully

Data Process Flow Diagram

The major Process modules in the Hedge application are as follows,
MainFrame Extracts
Oracle Database Loading
Transformation and Inforce generation
Grid processing
MainFrame Extracts
Records from the DAACT files are read.Necessary fields from the DAACT file are extracted.Finally, the extracted fields are written into an extract file which will be later loaded in Hedge/RBC Database
Extract the following records to load into the Hedge/RBC Database
RBC Policy extracts
RBC Rolling premium records
Hedge records
NAV records
Structured settlement records
RBC Policy Extracts: This consists of the Statutory reserve for the policies as on date. Monthly process.
RBC Rolling Premium Records:This consists of the Statutory reserves for 20 years. It holds the history of the Stat reserves for the past 20 years. Monthly process.
Hedge Records:This consists of the GAAP records for the policies. Monthly process.
NAV Records: This holds the Net Asset Value for the policies. This is a daily process but the loading of the database happens monthly.
Structured Settlement Records:This is a kind of settlement made when a person got injured (Planned payouts). For example, if a policy holder lost his leg, the insurance company will make some calculations for the loss and pays that person some amount every month for a certain period of time.Processing of all the above types except the Structured settlement records are done in the Web part. Mainframe just extracts the records and transmits them to the Oracle Database.
The records are extracted based on some conditions, normally all the variable and active policies are extracted in the Mainframe Jobs for all the Admin systems and sent to the server. At the end of the extract job, a Trigger filetis created which signals the Maestro  to trigger the load Job on the server.
Click here for more details
Extract file Details 
          We write out around 8 files in the Hedge mainframe extraction. Out of which two extract files (policy and fund) are important. The following are the names of the files, 
Data Policy Files
Data Fund Files
Bypass Files
Trigger files
Policy Control
Fund control
Bypass control
Control Point 
 View more file Details... 
Data Policy File         
The Data Policy file contains the Policy information. There are two kinds of policies,
Variable Policies
Fixed Policies
Here in Hedge, we deal with Variable Annuity Dynamic Hedging, So we take the variable policies and ignore fixed policies while extracting Data Policy file through Hedge mainframe jobs.
Data Fund Files
The Data Fund files contain the Fund details of the Policies extracted.
Note:  Data Control files and Fund Control files also get created along with the Data Policy files and Data Fund files.
Bypass Files
All the records that are not active or fixed are written into Bypass file.
Trigger files
Trigger files or Dummy files. These files will act as Trigger file to kick start the maestro load job.
Policy Control
It contains total policy count and account value on policy file
Fund Control
It contains total policy count and account value on fund file
Bypass Control
It contains total policy count and account value on bypass file
Control Point
It contains Control details for few important amount fields
Summary of Mainframe process
            The mainframe part will create the Policy files, Fund files, Bypass files and Trigger files and these files will be placed on the server. These files get loaded into the Oracle Database
Click here for Mainframe Job details
Oracle DataBase loading
In the loading stage, Admin files are loaded into the Oracle tables from Mainframes where DAACT (Annuity) team provides the admin data. Those admin data is loaded into data policy and data fund tables respectively.The data 
policy table contains all the policy details and data fund table contains the fund details for the particular policy in data policy table.
Prerequisites for loading the Extracted data  
           Consider the data policy file, it contains Valuation date. The date in data policy file should match date in the fund file. And this date should match (valuation date -1) on the input global parameters table. Input global parameter table in the oracle DB should contain the first day of the next month.
           For example:
           If we validate for March month end,
The data policy file should contain the last day of the month (i.e., 03/31/2012)
The Valuation date column in input global parameter table should contain the first date of the next month (i.e., 04/01/2012)
           Note: Developers should take care of updating the date in the input global parameters table.
Transformation 
Transformation process is the heart of Hedge. This transformation process is carried out for all the LOB’s (There will be a slight difference in the business rules for each LOB) .Once the data is loaded into data policy and data fund tables, the business provides certain calculations to populate the data into Inforce table and the process is called as transformation process.The transformation process will club the values from data policy and data fund tables and finally populates into Inforce table and this process is handled through PL/SQL packages and procedures. Finally, the process will extract the inforce table data into inforce text file and the respective file is shared with client/Business.
Until Transformation, process flow for both GAAP and STAT will be similar.
The important components of inforce transformation are:
Data policy
Data fund
Lookup tables
Business Rules.
The Inforce transformation process reads data from Data policy and Data Fund tables using the business rules & Lookup information, and loads data into the Inforce Table.

 
For EV15 and Fili, we have Weekly and Monthly Load and Transformation process. Whereas for all the other lob’s we just have Monthly process. 
Above process holds good for lobs other than EV15 and Fili.The 2 LOB’s which follows Weekly Trading Grid are EV15 and Fidelity LOB also called as Filly LOB.Here the process is to read the Data Policy and Data Fund using Business rules and load into Weekly inforce Table and finally the weekly inforce Table data gets loaded to the Inforce Month table in the month end.
To Summarize, the Inforce transformation involves 3 Steps as can be referred in the table below:






MGHedge Inforce Transformation from DAACT - 3 Steps of Transformations






Input
Output
Programming 
Language
Logic
Tab
DAACT
DAACT Extract
COBOL
Step 1 mapping
Direct DAACT
DAACT Extract
Policy/Fund Tables
Oracle
Step 2 Load Policy and Fund to Oracle
Load from raw DAACT extract files to Oracle table
Policy/Fund Tables
Oracle Inforce Table
Oracle
Step 3 inforce transformation logic
MGHEdge inforce Transformation, mapping tables
Oracle Inforce Table
MG Hedge inforce.txt file
Perl 
Step 4 pull inforce from table for run
All runtime files are pulled (in different forms from the Oracle inforce table)
 
Click here for transformation process flow diagram
GRID processing
The client/business validates the inforce file generated from transformation process and provides a set of asset, assumption and scenario files. Based on the files provided by the client/business and the DLL provided by the Milliman (Third party), XML is generated which contains the file path of all the inforce, asset and DLL’s. Then the generated XML is submitted to the HPC Grid Server and finally a set of valuation result file is generated and will be shared with the client/business.
Preparatory Job 
         For any stream the first job is the preparatory job. The following will explain the process of Preparatory job, 
It will contain a set of excel sheets which got macros in it.
When this job gets running, the perl script will try to run the macros contained in the excel sheet and we will get parameter files after running the excel through perl script. 
The parameter file will get uploaded on the server and we do not have any control to the data in the parameter file.
　
Insplit Job or Inforce Job  
       Insplit Job or inforce job will select the data from the inforce table and write it on to a text file. For example, If FAS file is running, the inforce job will  select the data from the inforce table and write on to a text file.
　
Need for Insplit or Inforce Job (HPC GRID) 
      The original text file contains huge amount of data which needs to be processed as fast as possible. In order to process the file quickly, In Hedge     there is a process called HPC (High Performance Computing). Consider the following diagram to understand HPC process.
Fig. Splitting of Original file using HPC process
　
This HPC process is done using the HPC software (High Performance Computing Software). The HPC process is done by MIAMI Team.
Note: We go for HPC software to process the original data as fast as possible with the parameter file generated from the Preparatory Job. We also use Milliman software, it’s a software where business rules are applied. 
The steps specified above is common for all the LOB’s in Domestic system except FASCORE and International system.

Outputs from HPC will be post processed and the results are displayed in the smarte website.
 
 
Team Snap


Project Details
Project Name: Variable Annuity Dynamic Hedging (VADH)
Project ID: 1000044892
SOW: 1503048
LOB: Corp-Build
Type: Application Development
Team Lead: Varun Chakravarthi
Sr. Manager: Subramaniam, Venkateswaran 
MetLife CRE: Lance Forst
Sub CRE: Brain Cartwright
DL: MetCorpHedge
Project Share:
\\CTSINNVLCLRC\Met-CORP-HOME\ROOT\MetLife_CORP_HOME\2. Applications\Legacy\Mainframe\MF_New applications\Public\1. Application documents\VAH-Variable Annuity Hedging
Onsite Head count: 5
Onsite Location: Greenville SC (US)
Offshore Head count: 16
Offshore Location: Chennai (India) 
Milestones of VADH
2012
EDJ implementation Phase I
2013
EDJ Implementation Phase II
ANNDW – MSLS Data load
DLL consistency for Fidelity, EV15 and TLA - GAAP
DLL consistency for all Admin systems- STAT
UK weekly FAS Attribution
Structured Index Annuity Towers Oracle install
SMARTE Lock and Load process
MSLS - GAAP reserve repository Feed
UK elixir elimination
2014 
Fidelity Cut over to ANNDW
Flag Phase I
Automation of Parameter Generator
Flag Phase I - web changes
Web & Batch - To create a new control report 3 in SMARTE
GAAP - MSLS Reserve Repository feed change - Raymond
MSLS Inforce changes for ALM - Felix
Default Logic for 99999 funds for Fascore & COVAVA - transformation process
Email notification to business after creation of the VACARVM asset files
GAAP FILI Cutover to Azure
GAAP TLA Cutover to Azure
Phase 1 DLL Changes - GAAP & STAT
Flag Phase 2 - GAAP & STAT
Phase 2 DLL Changes - GAAP & STAT
2015
EV Phase 1
GAAP-STAT Transformation Sync for EV15, FILI & TLA
LWG Rider 2 Changes
MEL Variable Annuity Re-price
EDP to Delaware
Korea Re-price
GPO Rider Updates - GAAP & STAT
RPS DLL Consistency
Changing RR Valuation Source for JV & UK MEL
PathWise Post Processing - Hive/ Hadoop
Flex Choice LWG Updates - GMWB cohort table - GAAP & STAT
Fund Expansion - GAAP & STAT
Milliman Phase 1 DLL Changes - GAAP & STAT
EDP NY Feed
2016
Trading grid archival changes
Pathwise Assets Automation
Merging UK MGHedge & PW trading grid
SIFI RA CFO Pathwise
US Retail Split
 
LikeBe the first to like this
Labels 
None 
Edit Labels 
9 Child Pages 
Page: Data Processing flow diagram Page: Greeks/Risk Analytics Page: Grid process flow diagram Page: Hedging website Page: Mainframe job details Page: New Joiners set up Page: Technical flow diagram Page: Transformation process flow diagram Page: VADH – Mainframe Details 

Add Comment 

Please raise a ticket here for any queries 

https://c2wiki.cognizant.com/login.action?os_destination=%2Fdisplay%2FMetLifeCorp%2FVariable%2BAnnuity%2BDynamic%2BHedging












