from base64 import b64encode
from os import makedirs
from os.path import join, basename
from sys import argv
import json
import requests
import cassandra
import re
import pandas as pd
import datetime
from google.cloud import translate
import time 
from concurrent.futures import ProcessPoolExecutor
import numpy as np
from requests.adapters import HTTPAdapter
import socket
import os, ssl
import uuid 

ENDPOINT_URL = 'https://vision.googleapis.com/v1/images:annotate'
RESULTS_DIR = 'jsons'
makedirs(RESULTS_DIR, exist_ok=True)
#os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/home/Election-Analysis/ocr-text-extraction-260613-64b85a48c5b7.json"
s = requests.Session()
s.mount('https://',HTTPAdapter(max_retries=2))

from cassandra.cluster import Cluster
cluster = Cluster(['192.168.0.131', '192.168.0.132','192.168.0.133'])
session = cluster.connect('electionanalysis')


def multiprocessing(func, args, workers):
    print("Start multiprocess")
    with ProcessPoolExecutor(workers) as ex:
        print("Invoking the function")
        res = ex.map(func, args)
    return list(res)

def make_image_data_list(image_filenames):
    """
    image_filenames is a list of filename strings
    Returns a list of dicts formatted as the Vision API
        needs them to be
    """
    img_requests = []
    for imgname in image_filenames:
        with open(imgname, 'rb') as f:
            ctxt = b64encode(f.read()).decode()
            img_requests.append({
                    'image': {'content': ctxt},
                    'features': [{
                        'type': 'DOCUMENT_TEXT_DETECTION'
                    }]
            })
    return img_requests
    
def load_datafromdf(effectivedf):
    hostname= socket.gethostname()
    print("thread no:--------------------------->")
    threadName = effectivedf.iloc[0]["Threadnumber"]    
    apiKey=effectivedf.iloc[0]["google_key"]
    #time.sleep(int(threadName.split("_")[1])*2)
    threadnumber = int(threadName.split("_")[1])
    print("thread no:"+str(threadnumber))
    from cassandra.cluster import Cluster
    cluster = Cluster(['192.168.0.131', '192.168.0.132','192.168.0.133'])
    sessionInner = cluster.connect('electionanalysis')
    #sessionInner=sessionlist[threadnumber-1]
    #clusterInner = Cluster(['192.168.0.110', '192.168.0.111','192.168.0.112'])
    #sessionInner = clusterInner.connect('electionanalysis')
    print("Inside the function")
    starttime=time.time()
    print("Printing effectiveDF of type=====> {} with values {}".format(type(effectivedf),effectivedf))
    for index, row in effectivedf.iterrows():
        print("Inside the DF iterator")       
        #print ('Inserted to DB')

        insrtStmt= sessionInner.prepare("INSERT INTO googlevision_inbound_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?,?)")
        sessionInner.execute_async(insrtStmt, [row['queue_id'],row['composite_file_refid'],str(row['file_path']),str(datetime.datetime.now()),str(datetime.datetime.now()),'In Progress','Processing Completed',str(row['file_path']),str(['processing_path']),str(hostname)])
 
    for index, row in effectivedf.iterrows():
        Comp_ref_id = row['composite_file_refid']
        Hostname = row['hostname']
        image_filenames=[str(row["processing_path"])+"combinedImage.png"]
        combinationFolders=str(row["combining_folder_list"])        
        imagecounter= str(row["image_count_list"])   
        insrtStmt= sessionInner.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
        sessionInner.execute_async(insrtStmt, ['Load_Election_Data.py',str(image_filenames),str(row["queue_id"]),'NA',str(datetime.datetime.now()),str(threadName)+'-------> Processing Indiudual Combination File--',str(hostname)])  

      
        try:
            response = request_ocr(apiKey, image_filenames,threadName)
            if response.status_code != 200 or response.json().get('error'):
                insrtStmt= sessionInner.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
                sessionInner.execute_async(insrtStmt, ['Load_Election_Data.py',str(image_filenames),str(row["queue_id"]),str(200),str(datetime.datetime.now()),str(threadName)+'------>Exception occured while Processing--TestCase--',str(hostname)])  
                #print(response.text)
                insrtStmt= sessionInner.prepare("INSERT INTO googlevision_inbound_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?,?)")
                sessionInner.execute_async(insrtStmt, [row['queue_id'],row['composite_file_refid'],str(row['file_path']),str(datetime.datetime.now()),str(datetime.datetime.now()),'Error','Error-While Processing',str(row['file_path']),str(['processing_path']),str(hostname)])
                
                
            else:
                for idx, resp in enumerate(response.json()['responses']):
                    # save to JSON file
                    #imgname = image_filenames[idx]
                    #jpath = join(RESULTS_DIR, basename(imgname) + '.json')
                    #with open(jpath, 'w') as f:
                        #datatxt = json.dumps(resp, indent=2)
                        #print("Wrote", len(datatxt), "bytes to", jpath)
                        #f.write(datatxt)
                    load_db(resp,dfConfig,threadName,sessionInner,combinationFolders,imagecounter,Comp_ref_id,Hostname)
                insrtStmt= sessionInner.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
                sessionInner.execute_async(insrtStmt, ['Load_Election_Data.py',str(image_filenames),str(row["queue_id"]),'NA',str(datetime.datetime.now()),str(threadName)+'------>Processing Indiudual Combination File Completed--',str(hostname)])  
                insrtStmt= sessionInner.prepare("INSERT INTO googlevision_inbound_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?,?)")
                sessionInner.execute_async(insrtStmt, [row['queue_id'],row['composite_file_refid'],str(row['file_path']),str(datetime.datetime.now()),str(datetime.datetime.now()),'Complete','Processing Completed',str(row['file_path']),str(['processing_path']),str(hostname)])
                 
        except Exception as e:
            insrtStmt= sessionInner.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
            sessionInner.execute_async(insrtStmt, ['Load_Election_Data.py',str(image_filenames),str(row["queue_id"]),str(e),str(datetime.datetime.now()),str(threadName)+'----->Exception occured while Processing--Test Case--',str(hostname)])  
            #Modifying on the 12th of November
            insrtStmt= sessionInner.prepare("INSERT INTO googlevision_inbound_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?,?)")
            sessionInner.execute_async(insrtStmt, [row['queue_id'],row['composite_file_refid'],str(row['file_path']),str(datetime.datetime.now()),str(datetime.datetime.now()),'Error','Processing Completed',str(row['file_path']),str(['processing_path']),str(hostname)])
       #clusterInner.shutdown()
        endtime=time.time()
        print("time taken = {}".format(endtime-starttime))

def make_image_data(image_filenames):
    """Returns the image data lists as bytes"""
    imgdict = make_image_data_list(image_filenames)
    return json.dumps({"requests": imgdict }).encode()


def request_ocr(api_key, image_filenames,threadname):
    response = requests.post(ENDPOINT_URL,
                             data=make_image_data(image_filenames),
                             params={'key': api_key},
                             headers={'Content-Type': 'application/json'}) 
    return response
    
def sample_translate_text(text, project_id):
    """Translating Text."""

    client = translate.TranslationServiceClient()

    parent = client.location_path(project_id, "global")

    # Detail on supported types can be found here:
    # https://cloud.google.com/translate/docs/supported-formats
    response = client.translate_text(
        parent=parent,
        contents=[text],
        mime_type="text/plain",  # mime types: text/plain, text/html
        target_language_code="en-US",
    )
    # Display the translation for each input text provided
    for translation in response.translations:
        textReturn= translation.translated_text
        
    return textReturn

def load_db(resp,dfConfig,threadName,session,combinationFolders,imagecounter,Comp_ref_id,Hostname):
    hostname= socket.gethostname()
    ocrPerfFile = "/home/Election-Analysis/logs/ocr_perorm_"+threadName+".txt"
    fOcr = open(ocrPerfFile, "a");
    fOcr.write("\nStart wrirting to the file-------------->")

    # print the plaintext to screen for convenience
    print("---------------------------------------------")
    print("dfconfig...>"+str(dfConfig))
    t = resp['textAnnotations'][0]
    print("    Bounding Polygon:")
    print(t['boundingPoly'])
    print("    Text:")
    word= t['description']
    word=word.replace("Photo is","")
    word=word.replace("Avalable","")
    word=word.replace("Avaiable","")
    word=word.replace("Available","")
    indivudualRecord=""
    Retry_flag = 'N'
    Gender_count= word.count('ലിംഗം')
    sum = 0 
    print("imagecounter")
    print(imagecounter, type(imagecounter))
    print(imagecounter.split(","))
    
    for i in imagecounter.split(","):
        sum = sum + int(i) 
    if sum == Gender_count:
        Retry_flag = 'Y'
    
    #from cassandra.cluster import Cluster
    #cluster = Cluster(['192.168.0.110', '192.168.0.111'])
    #session = cluster.connect('electionanalysis')
    
    #Writing to Logfile

    #fOcr.write("Just after opening the file------->" + word)
    #print("Lookup the list>")    
    fatherSearchList=    dfConfig.loc[(dfConfig['config_field'] == 'father_name') & (dfConfig['config_name'] == 'father_search_regex')]
    motherSearchList=     dfConfig.loc[(dfConfig['config_field'] == 'mother_name') & (dfConfig['config_name'] == 'mother_search_regex')] 
    husbandNameSearchList= dfConfig.loc[(dfConfig['config_field']=='husband_name') & (dfConfig['config_name'] == 'husband_name_regex')]
    #print("The husband Name search List is"+str(husbandNameSearchList))     
    fatherNameSearchList= dfConfig.loc[(dfConfig['config_field']=='father_name') & (dfConfig['config_name'] == 'father_name_regex')]
    motherNameSearchList= dfConfig.loc[(dfConfig['config_field']=='mother_name') & (dfConfig['config_name'] == 'mother_name_regex')]   
    husbandSearchList= dfConfig.loc[(dfConfig['config_field'] == 'husband_name') & (dfConfig['config_name'] == 'husband_search_regex')]
    sexSearchList= dfConfig.loc[(dfConfig['config_field'] == 'sex') & (dfConfig['config_name'] == 'sex_regex')]
    ageSearchList= dfConfig.loc[(dfConfig['config_field'] == 'age') & (dfConfig['config_name'] == 'age_regex')]
    hnumberSearchList= dfConfig.loc[(dfConfig['config_field'] == 'house_name') & (dfConfig['config_name'] == 'hnumber_name_regex')]
    #fOcr.write("Before starting regex search on data" + str(datetime.datetime.now()) + "\n")
    count=0  
    time.sleep(5) 
    for line in word.split('\n'):
        try:
            recordExtract=False
            indivudualRecord= indivudualRecord+" "+line
            lastLineExists= line.find('ലിംഗം')
            guardianNameList=[]  
            recordCount=1
            if lastLineExists!=-1:
            
                genID= uuid.uuid1()
                #print("The extracted Indivudual record is =======>"+ indivudualRecord)
                indivudualRecord=indivudualRecord.replace('\n', ' ')

                motherExists=False
                for index, row in motherSearchList.iterrows():
                    motherExists=re.search(row["config_value"],indivudualRecord)    
                    if motherExists:
                        motherExists=True
                        motherPattern= row["config_value"]
                        break
                
                fatherExists=False
                for index, row in fatherSearchList.iterrows():
                    fatherExists= re.search(row["config_value"],indivudualRecord)
                    if fatherExists:
                        fatherExists=True
                        fatherPattern= row["config_value"]
                        break
                
                husbandExists=False
                for index, row in husbandSearchList.iterrows():
                    husbandExists= re.search(row["config_value"],indivudualRecord)
                    if husbandExists:
                        husbandExists=True
                        husbandPattern= row["config_value"]
                        break
                
                motherNameExists=False
                for index, row in motherNameSearchList.iterrows():
                    motherNameExists= re.search(row["config_value"],indivudualRecord)
                    if motherNameExists:
                        motherNameExists=True
                        motherNamePattern= row["config_value"]
                        break
                
                #fatherNameExists=False
                for index, row in fatherNameSearchList.iterrows():
                    #print("The value of the father config pattern is:---->"+row["config_value"])
                    fatherNameExists=re.search(row["config_value"],indivudualRecord)
                    #print("The result of regex is "+str(fatherNameExists))
                    if fatherNameExists:
                        fatherNameExists=True
                        print("Inside fatherName exists Pattern")
                        fatherNamePattern= row["config_value"]
                        break
                        
                husbandNameExists=False
                for index, row in husbandNameSearchList.iterrows():
                    #print("The value of the Husband Name pattern is:---->"+row["config_value"])
                    husbandNameExists=re.search(row["config_value"],indivudualRecord)
                    if husbandNameExists:
                        husbandNameExists=True
                        print("Inside husbandName exists Pattern")
                        husbandNamePattern= row["config_value"]
                        break
                
                hnumberExists=False
                for index, row in hnumberSearchList.iterrows():
                    hnumberExists=re.search(row["config_value"],indivudualRecord)            
                    #print("The houseNumber Pattern Used is "+row["config_value"])
                    if hnumberExists:
                        hnumberExists=True
                        hnumberPattern= row["config_value"]
                        break
                        
                if fatherExists:
                    #print("Details of father present. regex from db is "+fatherPattern)
                    voterNameList= re.search('പേര്.(.*)'+fatherPattern, indivudualRecord)
                                          
                if husbandExists:
                    #print("Details of husband present. regex from db is"+husbandPattern)
                    voterNameList= re.search('പേര്.(.*)'+husbandPattern, indivudualRecord)
                
                if motherExists:
                    #print("Details of mother present,the regex from DB is"+motherPattern)
                    voterNameList= re.search('പേര്.(.*)'+motherPattern, indivudualRecord)

                if fatherNameExists:
                    #print("Details of GuardianName present,the regex from DB is"+fatherNamePattern)
                    guardianNameList= re.search(fatherNamePattern+'(.*)വ.ട്ട.', indivudualRecord) 
                if husbandNameExists:
                    guardianNameList= re.search(husbandNamePattern+'(.*)വ.ട്ട.', indivudualRecord)
                    #print("Details of GuardianName present,the regex from DB is"+husbandNamePattern+":")
                if motherNameExists:
                    guardianNameList= re.search(motherNamePattern+'(.*)വ.ട്ട.', indivudualRecord)
                    #print("Details of GuardianName present,the regex from DB is"+motherNamePattern+":"+str(guardianNameList.group(1)))
                    
                if(( not fatherExists) & (not motherExists) & (not husbandExists)):
                    raise Exception('Could Not locate Father/Mother/Husband on SearchRegex for the record: '+str(recordCount))
                
                if((not fatherNameExists) & (not motherNameExists) & (not husbandNameExists)):
                    raise Exception('Could Not locate FatherName/MotherName/HusbandName on SearchRegex for the record: '+str(recordCount))
                
                houseNumberList= re.search(hnumberPattern+'(.*)വയസ്സ്', indivudualRecord)
                UniqueID=indivudualRecord.split('പേര്')[0]
                UniqueID=UniqueID.lstrip()
                #UniqueID = re.sub(r"^\W+", "", UniqueID)
                UniqueIDMatch=re.search('[a-zA-Z]{2,3}\d{7,10}',UniqueID)
                if UniqueIDMatch:
                    #print("The unique ID is"+str(UniqueIDMatch.group(0)))
                    UniqueID=str(UniqueIDMatch.group(0))
                    

                #print(voterNameList.group(1))
                fullname= voterNameList.group(1)
                fullname=fullname.lstrip()
                nameList =fullname.split(' ')
                fname= nameList[0]
                if len(nameList)>1:
                    lname= nameList[1]
                else:
                    lname=" "
                #print("before gdian name")
                guardianName= guardianNameList.group(1)
                #print("afte gdian name")
                houseNumber= houseNumberList.group(1)
                #houseName= houseNameList.group(1)
                agelist=re.search('വയസ്സ്.(.*)ലിംഗം', indivudualRecord)
                age=agelist.group(1)
                age= age.lstrip()
                sexComb=indivudualRecord.split('ലിംഗം')[1]
                sexList=sexComb.split()
                sex= sexList[1]
                fname=fname.lstrip()
                lname=lname.lstrip()
                fullname=fullname.lstrip()
                guardianName=guardianName.lstrip()
                houseNumber=houseNumber.lstrip()
                #fOcr.write("Before Writing to DB" + str(datetime.datetime.now()) + "\n")
                insrtStmt= session.prepare("INSERT INTO voter_data (booth,id_eng,queue_id,fname_local,lname_local,name_local,guardian_name_local,house_number_local,sex,age,hostname) VALUES (?,?,?,?,?,?,?,?,?,?,?)")
                session.execute_async(insrtStmt, ["testbooth",UniqueID,genID,fname,lname,fullname,guardianName,houseNumber,sex,age,str(hostname)])
                #fOcr.write("After Writing to DB" + str(datetime.datetime.now()) + "\n")
                indivudualRecord=""
                #ft.write("After DB Load: " + str(datetime.datetime.now()))
                recordCount=recordCount+1
                #ft.close()
        except Exception  as e:
            aggregatecount=0
            folderNum=0
            FolderPointer=0
            imgv=imagecounter.split(",")
            flagBreak=False  
            for count in imgv:
                count =int(count)
                aggregatecount=aggregatecount+count               
                if aggregatecount>recordCount and flagBreak==False:
                   flagBreak=True
                   imageNum=count-(aggregatecount-recordCount)
                   FolderPointer=folderNum
                   combList=combinationFolders.split(",")
                   folderLocation= str(combList[FolderPointer])
                   imageFullPath=folderLocation+str(imageNum)+".png"
                folderNum=folderNum+1
            insrtStmt= session.prepare("INSERT INTO img_file_reprocessing (queue_id,composite_file_refid,img_name,received_date,last_modified_date,status,status_description,processing_path,hostname,retry_flag) VALUES (?,?,?,?,?,?,?,?,?,?)")
            session.execute_async(insrtStmt, [uuid.uuid1(),Comp_ref_id,str(imageNum)+".png",str(datetime.datetime.now()),str(datetime.datetime.now()),'Processing Started','Processing Started',imageFullPath,Hostname,Retry_flag])
            print("before writing to the file\n")
            fOcr.write("Just after opening the file------->" + str(indivudualRecord))  
            print(str(e))
    fOcr.close()
    response ='success'
    return response



if __name__ == '__main__':
    hostname= socket.gethostname()
    selectStmtStartedProcessing= session.prepare("SELECT * FROM googlevision_inbound_queue where STATUS='Processing Started'")
    selectStmtComplete= session.prepare("SELECT * FROM googlevision_inbound_queue where STATUS='Complete'")
    selectStmtError= session.prepare("SELECT * FROM googlevision_inbound_queue where STATUS='Error'")
    selectStmtInProgress= session.prepare("SELECT * FROM googlevision_inbound_queue where STATUS='In Progress'")
    dfProcessingStarted=pd.DataFrame(list(session.execute(selectStmtStartedProcessing)))
    dfProcessingComplete = pd.DataFrame(list(session.execute(selectStmtComplete)))
    dfError = pd.DataFrame(list(session.execute(selectStmtError)))
    dfInProgress = pd.DataFrame(list(session.execute(selectStmtInProgress)))
    if dfProcessingStarted.empty==False:
        dfProcessingStarted = dfProcessingStarted.loc[(dfProcessingStarted['hostname']== str(hostname))]
    if dfProcessingComplete.empty==False:
        dfProcessingComplete = dfProcessingComplete.loc[(dfProcessingComplete['hostname']== str(hostname))]
    if dfError.empty==False:
        dfError = dfError.loc[(dfError['hostname']== str(hostname))]
    if dfInProgress.empty==False and dfError.empty==False:
        dfInProgress = dfInProgress.loc[(dfInProgress['hostname']== str(hostname))]
        dfInProgress=dfInProgress[~(dfInProgress['queue_id'].isin(dfError['queue_id']))]
        
    selectConfig= session.prepare("SELECT * FROM election_anallysis_configurations")
    dfConfig=pd.DataFrame(list(session.execute(selectConfig)))
    maxThreadNumber= dfConfig.loc[(dfConfig['config_field'] == 'max_thread_number') & (dfConfig['config_name'] == 'max_thread_number')]
    
    #remove in progress and completed
    if dfProcessingStarted.empty==False and dfProcessingComplete.empty==False  and dfInProgress.empty==False:
        dfProcessingStarted=dfProcessingStarted[~(dfProcessingStarted['queue_id'].isin(dfProcessingComplete['queue_id']))]
        dfProcessingStarted=dfProcessingStarted[~(dfProcessingStarted['queue_id'].isin(dfInProgress['queue_id']))]
        #print(selectStmtStartedProcessing)
    
    # audir tabel info msg
    #dfLimited=dfProcessingStarted.head(16)
    dfLimited=dfProcessingStarted
    maxThreads= int(str(maxThreadNumber.iloc[0]["config_value"]))
    effectiveDF=np.array_split(dfLimited,maxThreads)
    count=0
    for item in effectiveDF:
        item["Threadnumber"]="Thread_"+str(count)
        
        count=count+1
        if count==5:
           confName="1"
        elif count==6:
           confName="2" 
        elif count==7:
           confName="3"
        elif count==8:
           confName="4"
        else:
           confName=str(count)
           
        apiKey=dfConfig.loc[(dfConfig['config_field'] == 'google_key') & (dfConfig['config_name'] == confName)]    
        apiKeyValue=str(apiKey.iloc[0]["config_value"])        
        print("Extracting the API key"+str(apiKeyValue))
        item["google_key"]=str(apiKeyValue)
        print("Assigned the API key")       
    if maxThreads>dfLimited.shape[0]:
        maxThreads=dfLimited.shape[0]   
    print(str(dfProcessingStarted.shape[0]))            
       
    #effectiveDF=[effectivedf1,effectivedf2,effectivedf3,effectivedf4,effectivedf5,effectivedf6,effectivedf7,effectivedf8,effectivedf9,effectivedf10]
    #effectiveDF=[effectivedf1]        
    result= multiprocessing(load_datafromdf, effectiveDF, maxThreads)
    print("There response from the multiprocess is {} " .format(result))
    #load_datafromdf(effectivedf)
        
        
        



