# Rough
********************create :

import sys
import os
import csv
import logging
import time
import HDPUtil
import ConfigUtil


def CopyHQLtoHDFS(csvfile):

    # Copy HQL files from local to HDFS
    localpath = os.path.join(home,'HQL/DDL')
    with open(csvfile) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            hdfspath = row['hql_path']
            hqlfile = row['hql']
            HDPUtil.FileCopy(localpath, hdfspath, hqlfile,username=username, password=password, appconfig=appconfig).SerialCopytoHDFS()


if __name__ == '__main__':

    # Capture Command line arguments
    process = sys.argv[1] #<stat/gaap>
    wfname = sys.argv[2]

    # Set logging config
    logging.basicConfig(level=logging.INFO)
    logging.info('creating %s tables with work flow %s', process, wfname)

    # Set current working directory
    cwd = os.path.dirname(os.path.abspath(__file__))
    os.chdir(cwd)

    # Extract HDP config data
    appconfig = ConfigUtil.configread('Config/Config.xml', 'appsettings')
    appconfig.update(ConfigUtil.configread('Config/Config.xml', process.upper()))
    appconfig.update(ConfigUtil.configread('Config/Config.xml', 'AzureHDP'))
    home = appconfig["Home"]
    logging.info('Home Directory is ==> ' + os.path.abspath(home))
    username = appconfig['HDP_FuncID']+'@'+appconfig['HDP_VAFuncIDDomain']
    password = appconfig['HDP_PWD']

    # Build workflow, jobflow local path variables
    tmpltpath = os.path.join(home,'Oozie/Template/Hive')
    tmplt = os.path.join(tmpltpath, wfname + '.csv')
    wfpath = os.path.join(home, 'Oozie/Workflow/Hive')
    jfpath = os.path.join(home,'Oozie/Jobflow/Hive')
    jffilename = wfname + '_jf.xml'
    wffilename = wfname + '_wf.xml'
    jf = os.path.join(jfpath,jffilename)

    # Build workflow HDFS path variables    
    hdfswfpath = appconfig['HDFSScriptBaseDir'] + process.lower() + '/oozie/hive'  # should start with '/'
    hdfswf = hdfswfpath+'/'+wffilename

    # Create Workflow from template csv
    logging.info('Creating Hive Workflow for %s %s', process, wfname)
    print('process is ' + str(process) + ' wfname is ' + str(wfname))
    HDPUtil.CreateWF(tmplt, wfpath,appconfig).HiveWF()

    # Copy workflow from local to HDFS
    logging.info("HDFS Copy from:%s to:%s file:%s", wfpath, hdfswfpath, wffilename)
    HDPUtil.FileCopy(wfpath, hdfswfpath, wffilename,username, password,appconfig=appconfig).SerialCopytoHDFS()

    # Copy HQL files from local to HDFS
    CopyHQLtoHDFS(tmplt)

    # Create Job properties from template csv
    logging.info('Creating Hive Jobflow for %s %s', process, wfname)
    HDPUtil.CreateJF(tmplt,hdfswf,jfpath,appconfig).HiveJF()

    # Run Hive Workflow on Oozie using job properties
    logging.info('Submitting Oozie job %s', jf)
    job = HDPUtil.RunWF(username, password,appconfig=appconfig).SubmitWF(jf)
    job_id = job['id']

    # Poll OOzie workflow for completion
    time.sleep(int(appconfig['OoziePollIntreval']))
    running = True
    logging.info("Polling Oozie Wokflow")
    while running:
        r = HDPUtil.RunWF(username, password,appconfig=appconfig).PollWF(job_id)
        if (r['status'] <> 'RUNNING') and (r['status'] <> 'PREP'):
            running = False
        else:
            # wait before polling oozie for job status
            logging.info("%s...",r['status'])
            time.sleep(int(appconfig['OoziePollIntreval']))

    if r['status'] <> "SUCCEEDED":
        logging.error("Oozie job %s Failed with status %s", job_id, r['status'])
        sys.exit(1)
    else:
        logging.info("Oozie job %s Completed", job_id)
        
        
        ********************** wf_create
        
        import sys
import os
import logging
import csv
import HDPUtil
import ConfigUtil
import DateUtil

def CopyHQLtoHDFS(csvfile):
    # Copy HQL files from local to HDFS
    localpath = home+'/TMP'
    with open(csvfile) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            hdfspath = appconfig['HDFSScriptBaseDir'] +process.lower()+ '/hql/dml/all'
            hqlfile = row['hql']
            infile=home+'/HQL/DML/'+hqlfile
            outfile=home+'/TMP/'+hqlfile
            with open(infile, "r") as fin:
                with open(outfile, "w") as fout:
                    for line in fin:
                        fout.write(line.replace('dddddd', processdate))
            HDPUtil.FileCopy(localpath, hdfspath, hqlfile,username=username, password=password, appconfig=appconfig ).SerialCopytoHDFS()


if __name__ == '__main__':

    # Capture Command line arguments
    process = sys.argv[1] #<GAAP/STAT>
    data_type = sys.argv[2] #<weekly/monthly>
    run_type = sys.argv[3] #<DATA_FUND_FILI_WKLY>

    # Set logging config
    logging.basicConfig(level=logging.INFO)
    logging.info('The parameters obtained are ' + process + ' ' + data_type + ' ' + run_type)
    
    # Set current working directory
    cwd = os.path.dirname(os.path.abspath(__file__))
    os.chdir(cwd)
    drive = cwd.split('\\')[0]
    config_xml = drive + "\\HDP\\Scripts\\Config\\Config.xml"
    # Extract HDP config data
    appconfig = ConfigUtil.configread(config_xml, 'appsettings')
    appconfig.update(ConfigUtil.configread(config_xml, process.upper()))
    appconfig.update(ConfigUtil.configread(config_xml, 'AzureHDP'))
    home = appconfig["Home"]
    logging.info('Home Directory is ==> ' + os.path.abspath(home))
    username = appconfig['HDP_FuncID'] + '@' + appconfig['HDP_VAFuncIDDomain']
    password = appconfig['HDP_PWD']
    # Get process date
    processdate=None
    if data_type =='weekly':
        dir_name,tmppath=os.path.splitdrive(home)
        date_file = dir_name +"\\HDP\\Scripts\\Weekly_processing_dt.txt"
        with open(date_file, 'r') as f:
            date = f.read().split('/')
            processdate = str(date[2]).replace("\n","") + '-' + date[0] + '-' + date[1]
    else:
        #date   = DateUtil.getdate(appconfig)
        #processdate = date.get_oracle_global_lstdate_YYYYMMDD()
        processdate = '2018-12-31'
        
    logging.info("Process date %s", processdate)
    # Build workflow local path variables
    tmpltpath = home+"/Oozie/Template/Hive/DML"
    tmplt = tmpltpath+'/'+ data_type + '.csv'
    logging.info('Template path is ==> ' + os.path.abspath(tmplt))
    
    wfpath = home+"/Oozie/Workflow/Hive"
    wffilename = 'fhed_'+process+'_'+data_type + '_' + run_type + '_load_wf.xml'
    logging.info('Workflow path is ==> ' + os.path.abspath(wfpath) + '\\' + wffilename)
    
    # Create runcsv from template csv for workflow creation
    runcsvfile = 'fhed_'+process+'_'+data_type + '_' + run_type + '_load.csv'
    runcsv = os.path.join(home, 'TMP', runcsvfile)
    logging.info('runcsv path is ==> ' + os.path.abspath(runcsv))
    
    runcsv_fh = open(runcsv, 'wb')
    with open(tmplt, 'rb') as csvfile:
       row_repl = csvfile.readline()
       runcsv_fh.write(row_repl)
       for row in csvfile:
            if row.split(',')[0] == run_type:
                row_repl = row
                runcsv_fh.write(row_repl)

    runcsv_fh.close()

    # Build workflow HDFS path variables
    hdfspath = appconfig['HDFSScriptBaseDir'] +process.lower()+ '/oozie/hive/all'  # should start with '/'

    # Create Workflow from template csv
    logging.info('Creating hive workflow for %s %s', process, data_type)
    HDPUtil.CreateWF(runcsv, wfpath,appconfig).HiveWF()

    # Copy workflow from local to HDFS
    logging.info('HDFS copy from: %s to: %s file: %s', os.path.abspath(wfpath), hdfspath, wffilename)
    HDPUtil.FileCopy(wfpath, hdfspath, wffilename, username=username, password=password ,appconfig=appconfig).SerialCopytoHDFS()
    # Copy HQL from local to HDFS
    CopyHQLtoHDFS(runcsv)


********************submit
import sys
import os
import logging
import time
import HDPUtil
import DateUtil
import ConfigUtil
import Decrypt
import glob

if __name__ == '__main__':

    # Capture command line arguments
    process = sys.argv[1]  # <STAT/GAAP/COMMON>
    data_type = sys.argv[2]  # <monthly>
    run_type = sys.argv[3]  # <HQL or TBL Name>
    system_type = sys.argv[4]  # STAT/GAAP/COMMON/LOOKUP/TAX_CFC

    # Set logging config
    logging.basicConfig(level=logging.INFO)
    logging.info('The parameters obtained are ' + process + ' ' + data_type + ' ' + run_type+ ' ' + system_type)

    # Set home directory
    cwd = os.path.dirname(os.path.abspath(__file__))
    os.chdir(cwd)
    drive = cwd.split('\\')[0]
    config_xml = drive + "\\HDP\\Scripts\\Config\\Config.xml"
    # Extract HDP config data
    appconfig = ConfigUtil.configread(config_xml, 'appsettings')
    appconfig.update(ConfigUtil.configread(config_xml, process.upper()))
    appconfig.update(ConfigUtil.configread(config_xml, 'AzureHDP'))
    home = appconfig["Home"]
    username = appconfig['HDP_FuncID'] + '@' + appconfig['HDP_VAFuncIDDomain']
    password = appconfig['HDP_PWD']

    # Build jobflow local path variables
    tmpltpath = os.path.join(home, 'Oozie/Template/Hive/DML')
    tmplt = os.path.join(tmpltpath, data_type + '.csv')
    jfpath = os.path.join(home, 'Oozie/Jobflow/Hive')
    runname = 'fhed_' + process + '_' + data_type + '_' + run_type + '_load'

    processdate = None
    # Get process date
    if data_type == 'weekly':
        dir_name, tmppath = os.path.splitdrive(home)
        date_file = dir_name +"\\HDP\\Scripts\\Weekly_processing_dt.txt"
        with open(date_file, 'r') as f:
            date = f.read().split('/')
            processdate = str(date[2]).replace("\n","") + '-' + date[0] + '-' + date[1]
    else:
        #date   = DateUtil.getdate(appconfig)
        #processdate = date.get_oracle_global_lstdate_YYYYMMDD()
         processdate = '2018-12-31'

    logging.info("Process date %s", processdate)


    # Copy Datafiles from Local_Extract_BaseDir to HDFS
    if (system_type.upper() == 'TAX_CFC'):
        local_file = []
        localpath = appconfig['Local_Extract_BaseDir']
        datafile = glob.glob(os.path.join(localpath, '*' + run_type + '.*'))
        for file in datafile:
            hdfspath = appconfig['HDFSDataBaseDir'] + process.upper() + '/' + data_type.lower() + '/' + processdate + '/' + run_type.upper()
            HDPUtil.FileCopy(localpath, hdfspath, file.lower(), username=username, password=password,appconfig=appconfig).SerialCopytoHDFS()

    # Copy Datafiles from Local_Extract_BaseDir_hdp to HDFS
    elif (system_type.upper() == 'STAT' or system_type.upper() == 'GAAP'):
        local_file = []
        localpath = appconfig['Local_Extract_BaseDir_hdp'] + '/' + process.upper() +  '/' + data_type.lower()
        #print (localpath)#make changes /lookup or /STAT/monthly
        datafile = glob.glob(os.path.join(localpath, run_type + '.*'))

        for file in datafile:
            hdfspath = appconfig['HDFSDataBaseDir'] + process.upper() + '/' + data_type.lower() + '/' + processdate + '/' + run_type.upper()
            HDPUtil.FileCopy(localpath, hdfspath, file.lower(), username=username, password=password,appconfig=appconfig).SerialCopytoHDFS()
    elif (system_type.upper() == 'COMMON' ):
        local_file = []
        localpath = appconfig['Local_Extract_BaseDir_hdp'] + '/' + process.upper() + '/DATA/' + data_type.lower() # Assumption: Process for Common tables will always be COMMON
        #print (localpath)#make changes /lookup or /STAT/monthly
        datafile = glob.glob(os.path.join(localpath, run_type + '.*'))

        for file in datafile:
            hdfspath = appconfig['HDFSDataBaseDir'] + process.upper() + '/' + data_type.lower() + '/' + processdate + '/' + run_type.upper()
            HDPUtil.FileCopy(localpath, hdfspath, file.lower(), username=username, password=password,appconfig=appconfig).SerialCopytoHDFS()
    elif (system_type.upper() == 'LOOKUP' ):
        local_file = []
        localpath = appconfig['Local_Extract_BaseDir_hdp'] +  '/' + process.upper() + '/' + system_type.upper() # Assumption: Process for Lookup tables will always be COMMON
        #print (localpath)#make changes /lookup or /STAT/monthly
        datafile = glob.glob(os.path.join(localpath, run_type + '.*'))

        for file in datafile:
            hdfspath = appconfig['HDFSDataBaseDir'] + process.upper() + '/' + data_type.lower() + '/' + processdate + '/' + run_type.upper()
            HDPUtil.FileCopy(localpath, hdfspath, file.lower(), username=username, password=password,appconfig=appconfig).SerialCopytoHDFS()



    # Create runcsv from template csv for workflow creation
    runcsvfile = runname + '_' + processdate + '.csv'
    runcsv = os.path.join(home, 'TMP', runcsvfile)
    logging.info('runcsv path is ==> ' + os.path.abspath(runcsv))

    runcsv_fh = open(runcsv, 'wb')

    with open(tmplt, 'rb') as csvfile:
        row_repl = csvfile.readline()
        runcsv_fh.write(row_repl)
        for row in csvfile:
            row = row.replace('HQLPATH', appconfig['HDFSScriptBaseDir'] + process.lower() + '/hql/dml/all')
            if row.split(',')[0] == run_type:
                row_repl = row
                runcsv_fh.write(row_repl)

    runcsv_fh.close()

    # Build jobflow HDFS path variables
    wfpath = appconfig[
                 'HDFSScriptBaseDir'] + process + '/oozie/hive/all/fhed_' + process + '_' + data_type + '_' + run_type + '_load_wf.xml'

    # Create job properties for Oozie submission
    logging.info("Creating Hive Jobflow for %s %s", process, data_type)
    HDPUtil.CreateJF(runcsv, wfpath, jfpath, appconfig=appconfig).HiveJF()

    # Run hive workflow on oozie
    jf = os.path.join(home, 'Oozie/Jobflow/hive',
                      'fhed_' + process + '_' + data_type + '_' + run_type + '_load_' + processdate + '_jf.xml')
    logging.info("Submitting Oozie job %s", jf)

    job = HDPUtil.RunWF(username=username, password=password, appconfig=appconfig).SubmitWF(jf)
    job_id = job['id']

    # Poll OOzie workflow for completion
    time.sleep(int(appconfig['OoziePollIntreval']))
    running = True
    while running:
        r = HDPUtil.RunWF(username=username, password=password, appconfig=appconfig).PollWF(job_id)
        if (r['status'] <> 'RUNNING') and (r['status'] <> 'PREP'):
            running = False
        else:
            logging.info("%s...", r['status'])
            # wait before polling oozie for job status
            time.sleep(int(appconfig['OoziePollIntreval']))

    if r['status'] <> "SUCCEEDED":
        logging.error("Oozie job %s Failed with status %s", job_id, r['status'])
        sys.exit(1)
    else:
        logging.info("Oozie job %s Completed", job_id)





************SPARK JOBS*********
***create
import sys
import os
import logging
import csv
import HDPUtil
import ConfigUtil


def CopyHQLtoHDFS(csvfile):

    # Copy HQL files from local to HDFS
    localpath = home+'/HQL/DML'
    with open(csvfile) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            hdfspath = appconfig['HDFSScriptBaseDir'] +process.lower()+ '/hql/dml/all'
            hqlfile = row['argument'].split(',')[0].split('/')[1]
            HDPUtil.FileCopy(localpath, hdfspath, hqlfile,username=username, password=password, appconfig=appconfig).SerialCopytoHDFS()


if __name__ == '__main__':

    # Capture Command line arguments
    process = sys.argv[1] #<stat/gaap>
    data_type = sys.argv[2] #<extract/inforce/reserve)
    run_type = sys.argv[3] #<ev15/val-rsv/rbc>
    
    # Set logging config
    logging.basicConfig(level=logging.INFO)
    logging.info('The parameters obtained are ' + process + ' ' + data_type + ' ' + run_type)
    
    # Set current working directory
    cwd = os.path.dirname(os.path.abspath(__file__))
    os.chdir(cwd)

    # Extract HDP config data
    appconfig = ConfigUtil.configread('Config/Config.xml', 'appsettings')
    appconfig.update(ConfigUtil.configread('Config/Config.xml', process.upper()))
    appconfig.update(ConfigUtil.configread('Config/Config.xml', 'AzureHDP'))
    home = appconfig["Home"]
    logging.info('Home Directory is ==> ' + os.path.abspath(home))
    username = appconfig['HDP_FuncID'] + '@' + appconfig['HDP_VAFuncIDDomain']
    password = appconfig['HDP_PWD']  

    # Build workflow local path variables
    tmpltpath = home+"/Oozie/Template/Spark"
    tmplt = tmpltpath+'/'+ data_type + '.csv'
    logging.info('Template path is ==> ' + os.path.abspath(tmplt))
    
    wfpath = home+"/Oozie/Workflow/Spark"
    wffilename = 'fhed_'+process+'_'+data_type + '_' + run_type + '_load_wf.xml'
    logging.info('Workflow path is ==> ' + os.path.abspath(wfpath) + '\\' + wffilename)
    
    # Create runcsv from template csv for workflow creation
    runcsvfile = 'fhed_'+process+'_'+data_type + '_' + run_type + '_load.csv'
    runcsv = os.path.join(home, 'TMP', runcsvfile)
    logging.info('runcsv path is ==> ' + os.path.abspath(runcsv))
    
    runcsv_fh = open(runcsv, 'wb')
    with open(tmplt, 'rb') as csvfile:
       row_repl = csvfile.readline()
       runcsv_fh.write(row_repl)
       for row in csvfile:
           if 'extract' in data_type and 'coverage' not in run_type:
                   if run_type <> 'indx':
                       if row.split(',')[0] == 'policy' or row.split(',')[0] == 'fund':
                           row_repl = row.replace('SSSS', run_type)
                           logging.info('Replaced SSSS with run_type ==>' + run_type)
                           runcsv_fh.write(row_repl)
                   else:
                       if row.split(',')[0] == 'fund':
                           row_repl = row.replace('SSSS', run_type)
                           logging.info('Replaced SSSS with run_type ==> ' + run_type)
                           runcsv_fh.write(row_repl)
           else:
               if row.split(',')[0] == run_type:
                   row_repl = row
                   runcsv_fh.write(row_repl)

    runcsv_fh.close()

    # Build workflow HDFS path variables
    hdfspath = appconfig['HDFSScriptBaseDir'] +process.lower()+ '/oozie/spark/all'  # should start with '/'

    # Create Workflow from template csv
    logging.info('Creating spark workflow for %s %s', process, data_type)
    HDPUtil.CreateWF(runcsv, wfpath,appconfig).SparkWF()

    # Copy workflow from local to HDFS
    logging.info('HDFS copy from: %s to: %s file: %s', os.path.abspath(wfpath), hdfspath, wffilename)
    HDPUtil.FileCopy(wfpath, hdfspath, wffilename, username=username, password=password,appconfig=appconfig).SerialCopytoHDFS()

    # Copy HQL from local to HDFS
    CopyHQLtoHDFS(runcsv)

*******submit

import sys
import os
import logging
import time
import HDPUtil
import DateUtil
import ConfigUtil
import Decrypt
import glob

if __name__ == '__main__':

    # Capture command line arguments
    process = sys.argv[1]
    data_type = sys.argv[2]
    run_type = sys.argv[3]

    # Set logging config
    logging.basicConfig(level=logging.INFO)
    logging.info('The parameters obtained are ' + process + ' ' + data_type + ' ' + run_type)

    # Set home directory
    cwd = os.path.dirname(os.path.abspath(__file__))
    os.chdir(cwd)

    # Extract HDP config data
    appconfig = ConfigUtil.configread('Config/Config.xml', 'appsettings')
    appconfig.update(ConfigUtil.configread('Config/Config.xml', process.upper()))
    appconfig.update(ConfigUtil.configread('Config/Config.xml', 'AzureHDP'))
    home = appconfig["Home"]
    username = appconfig['HDP_FuncID'] + '@' + appconfig['HDP_VAFuncIDDomain']
    password = appconfig['HDP_PWD']

    # Build jobflow local path variables
    tmpltpath = os.path.join(home,'Oozie/Template/Spark')
    tmplt = os.path.join(tmpltpath, data_type + '.csv')
    jfpath = os.path.join(home,'Oozie/Jobflow/spark')
    runname = 'fhed_'+process+'_'+data_type + '_' + run_type + '_load'


    # Get process date    
    date   = DateUtil.getdate(appconfig)
    processdate = date.get_oracle_global_lstdate_YYYYMMDD()
    print "process date:     ",processdate
    #processdate = '2018-12-31'
    logging.info("Process date %s", processdate)

    
    # Copy Datafiles to HDFS
    local_file = []
    if process == 'stat':
        localpath = appconfig['Local_Extract_BaseDir']
        datafile = glob.glob(os.path.join(localpath,'*'+run_type+'*'))
    else:
        if run_type == 'fili':
            fld = 'fidelity'
        elif run_type == 'tla':
            fld = 'tla-dom'
        else:
            fld = run_type
        localpath = appconfig['Local_Extract_BaseDir'].replace('SYS', fld)
        datafile = glob.glob(os.path.join(localpath, '*inforce*'))
    for file in datafile:
        if process.lower() == 'stat':
            if 'data_fund' in file.lower():
                hdfspath = appconfig['HDFSDataBaseDir'] + process.upper() + '/fund/' + processdate + '/' + run_type.upper()
            elif 'data_policy' in file.lower():
                hdfspath = appconfig['HDFSDataBaseDir'] + process.upper() + '/policy/' + processdate + '/' + run_type.upper()
            else:
                hdfspath = appconfig['HDFSDataBaseDir'] + process.upper() + '/' + run_type.lower() + '/' + processdate
        else:
            hdfspath = appconfig['HDFSDataBaseDir'] + process.upper() + '/' + data_type.lower() + '/' + processdate +'/'+ run_type.upper()
        HDPUtil.FileCopy(localpath, hdfspath, file.lower(),username=username, password=password, appconfig=appconfig).SerialCopytoHDFS()

    # Create runcsv from template csv for workflow creation
    runcsvfile = runname+'_' + processdate + '.csv'
    runcsv = os.path.join(home, 'TMP', runcsvfile)
    runcsv_fh = open(runcsv, 'wb')
    with open(tmplt, 'rb') as csvfile:
        row_repl = csvfile.readline()
        runcsv_fh.write(row_repl)
        for row in csvfile:
            row = row.replace('DDDD-DD-DD', processdate)
            row = row.replace('HQLPATH','${nameNode}' + appconfig['HDFSScriptBaseDir'] + process.lower() + '/hql/dml/all')
            row = row.replace('PYPATH', appconfig['HDFSOpenSourceDir'] + '/Pyspark')
            row = row.replace('ENV', appconfig['RUN_MODE'].lower())
            if 'extract' in data_type and 'coverage' not in run_type:
                if run_type <> 'indx':
                    if row.split(',')[0] == 'policy' or row.split(',')[0] == 'fund':
                        row_repl = row.replace('SSSS', run_type)
                        runcsv_fh.write(row_repl)
                else:
                    if row.split(',')[0] == 'fund':
                        row_repl = row.replace('SSSS', run_type)
                        runcsv_fh.write(row_repl)
            else:
                if row.split(',')[0] == run_type:
                    row_repl = row
                    runcsv_fh.write(row_repl)

    runcsv_fh.close()

    # Build jobflow HDFS path variables
    wfpath = appconfig['HDFSScriptBaseDir'] + process+'/oozie/spark/all/fhed_'+process+'_'+data_type + '_' + run_type + '_load_wf.xml'

    # Create job properties for Oozie submission
    logging.info("Creating Spark Jobflow for %s %s", process, data_type)
    HDPUtil.CreateJF(runcsv, wfpath, jfpath,appconfig=appconfig, srcSys=process).SparkJF()

    # Run spark workflow on oozie
    jf = os.path.join(home,'Oozie/Jobflow/spark', 'fhed_'+process+'_'+data_type + '_' + run_type+'_load_' + processdate+ '_jf.xml')
    logging.info("Submitting Oozie job %s", jf)

    job = HDPUtil.RunWF(username=username, password=password, appconfig=appconfig).SubmitWF(jf)
    job_id = job['id']


    # Poll OOzie workflow for completion
    time.sleep(int(appconfig['OoziePollIntreval']))
    running = True
    while running:
        r = HDPUtil.RunWF(username=username, password=password, appconfig=appconfig).PollWF(job_id)
        if  (r['status'] <> 'RUNNING') and (r['status'] <> 'PREP' ):
            running = False
        else:
            logging.info("%s...", r['status'])
            # wait before polling oozie for job status
            time.sleep(int(appconfig['OoziePollIntreval']))

    if r['status'] <> "SUCCEEDED":
        logging.error("Oozie job %s Failed with status %s", job_id,r['status'])
        sys.exit(1)
    else:
        logging.info("Oozie job %s Completed", job_id)





