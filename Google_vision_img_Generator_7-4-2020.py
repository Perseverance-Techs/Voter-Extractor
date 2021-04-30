import cv2
import numpy as np
from PIL import Image
import datetime
import pandas as pd
import uuid 
from os import listdir
import os
import tempfile
from pdf2image import convert_from_path
from PIL import Image
from base64 import b64encode
from os import makedirs
from os.path import join, basename
from sys import argv
import json
import requests
import cassandra
import re
import os, ssl
import socket

def sort_contours(cnts, method="left-to-right"):
    # initialize the reverse flag and sort index
    reverse = False
    i = 0

    # handle if we need to sort in reverse
    if method == "right-to-left" or method == "bottom-to-top":
        reverse = True

    # handle if we are sorting against the y-coordinate rather than
    # the x-coordinate of the bounding box
    if method == "top-to-bottom" or method == "bottom-to-top":
        i = 1

    # construct the list of bounding boxes and sort them from top to
    # bottom
    boundingBoxes = [cv2.boundingRect(c) for c in cnts]
    (cnts, boundingBoxes) = zip(*sorted(zip(cnts, boundingBoxes),
                                        key=lambda b: b[1][i], reverse=reverse))

    # return the list of sorted contours and bounding boxes
    return (cnts, boundingBoxes)

def box_extraction(dfProcessingStarted, composite_ref_id, file_name):
    hostname= socket.gethostname()
    print("Number of folders to be processed "  + str(dfProcessingStarted.shape[0]))
    onlyfiles = []
    uniqueID= uuid.uuid1()
    outboundPath="/home/Election-Analysis/googlevisioninbound/"
    googleInboundPath=outboundPath+str(uniqueID)+"/"
    
    totalCount=0
    count=0
    images_list=[]
    Combined_Folder_list =[]
    image_count_list =[]
    for index, row in dfProcessingStarted.iterrows():
        try:
            cropped_dir_path = row['processing_path']
            fName="abc"
            Combined_Folder_list.append(cropped_dir_path)
            image_count=0
            
            #/home/Election-Analysis/Individualvotersinbound/15.png0c522938-7692-11ea-b73c-0800271a438a/
            #onlyfiles = [f for f in listdir(processing_path) if isfile(join(processing_path, f))]
            for file in listdir(cropped_dir_path):                
                image_count=image_count+1
                onlyfiles.append(os.path.join(cropped_dir_path, file))
            #print(onlyfiles)
            image_count_list.append(str(image_count))
            #onlyfiles.append(os.path.join(cropped_dir_path, "combinationImage-0.png"))  
            count=count+1
            if dfProcessingStarted.shape[0]==count:

                print("totalNumber:"+str(len(onlyfiles)))
                #for i in onlyfiles:
                    #fName=str(i)
                    #print("Printing the name of images in path---------->"+str(i))
                    #images_list.append(str(i))
                    #totalCount=totalCount+1   
                    #count=count+1
                    #if (dfProcessingStarted.shape[0]==count):
                #print("Count has reached 300")
                #count=0
                #for i in onlyfiles:
                    #count=count+1
                    #if count==len(onlyfiles):
                imgs = [Image.open(str(i)) for i in onlyfiles]
                # If you're using an older version of Pillow, you might have to use .size[0] instead of .width
                # and later on, .size[1] instead of .height
                min_img_width = min(i.width for i in imgs)
                total_height = 0
                print("Before Looping the images")
                for i, img in enumerate(imgs):
                # If the image is larger than the minimum width, resize it
                    print("Looping the imnage List")
                    if img.width > min_img_width:
                        imgs[i] = img.resize((min_img_width, int(img.height / img.width * min_img_width)), Image.ANTIALIAS)
                    total_height += imgs[i].height
                img_merge = Image.new(imgs[0].mode, (min_img_width, total_height))
                y = 0
                print("Before Merging the  images")
                for img in imgs:
                    img_merge.paste(img, (0, y))
                    y += img.height
                if not os.path.exists(googleInboundPath):
                    print("The image path to be created is"+googleInboundPath)
                    os.mkdir(googleInboundPath)
                img_merge.save(googleInboundPath+'combinedImage.png') 
                for img in imgs:
                    img.close()

            insrtStmt= session.prepare("INSERT INTO votelist_individual_page_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?,?)")
            session.execute_async(insrtStmt, [row['queue_id'],row['composite_file_refid'],str(row['file_path']),str(datetime.datetime.now()),str(datetime.datetime.now()),'Complete','Processing Completed',str(row['file_path']),str(['processing_path']),str(hostname)])  
            
            
        except Exception as e:
            print("Error {} in {}" .format(str(e), str(row['file_path'])))
            insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
            session.execute(insrtStmt, ['Google_vision_img_Generator.py',str(['processing_path']),str(row['queue_id']),str(e),str(datetime.datetime.now()),'--An Error Occured while converting the image-->'+fName,str(hostname)]) 
            insrtStmt= session.prepare("INSERT INTO votelist_individual_page_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?,?)")
            session.execute_async(insrtStmt, [row['queue_id'],row['composite_file_refid'],str(row['file_path']),str(datetime.datetime.now()),str(datetime.datetime.now()),'ERROR','Error During Processing',str(row['file_path']),str(['processing_path']),str(hostname)])  

    try:       
        insrtStmt= session.prepare("INSERT INTO googlevision_inbound_queue(queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname,combining_folder_list,image_count_list) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
   
        session.execute(insrtStmt, [uniqueID,str(composite_ref_id),file_name,str(datetime.datetime.now()),str(datetime.datetime.now()),'Processing Started','Processing Started',file_name,googleInboundPath,str(hostname),','.join(Combined_Folder_list),','.join(image_count_list)]) 
        print("After Loading the status") 
    except Exception as e:    
         print("Error {} " .format(str(e)))

    
from cassandra.cluster import Cluster
cluster = Cluster(['192.168.0.131', '192.168.0.132'])            
session = cluster.connect('electionanalysis')    
hostname= socket.gethostname()
selectStmtCompFiles= session.prepare("SELECT composite_file_refid FROM votelist_individual_page_queue where STATUS='processing started'")
dfCompositeFiles= set((list(session.execute(selectStmtCompFiles))))
#if dfCompositeFiles.empty==False:
    #dfCompositeFiles = dfCompositeFiles.loc[(dfCompositeFiles['hostname']== str(hostname))]
print('------------------------------>Started Procesiing')
#print(dfCompositeFiles)  
selectStmtStartedProcessing= session.prepare("SELECT * FROM votelist_individual_page_queue where STATUS='processing started'") 
selectStmtComplete= session.prepare("SELECT * FROM votelist_individual_page_queue where STATUS='Complete'") 
selectStmtError= session.prepare("SELECT * FROM votelist_individual_page_queue where STATUS='ERROR'") 
dfProcessingStarted = pd.DataFrame(list(session.execute(selectStmtStartedProcessing)))
if dfProcessingStarted.empty==False:
    dfProcessingStarted = dfProcessingStarted.loc[(dfProcessingStarted['hostname']== str(hostname))]
dfProcessingComplete = pd.DataFrame(list(session.execute(selectStmtComplete)))
dfError = pd.DataFrame(list(session.execute(selectStmtError)))
if dfError.empty==False:
    dfError = dfError.loc[(dfError['hostname']== str(hostname))]
#print('------------------------------>Started Procesiing')
if dfProcessingComplete.empty==False:
    dfProcessingComplete = dfProcessingComplete.loc[(dfProcessingComplete['hostname']== str(hostname))]
#print(dfProcessingStarted)
selectConfig= session.prepare("SELECT * FROM election_anallysis_configurations")
dfConfig=pd.DataFrame(list(session.execute(selectConfig)))
maxFolderNumber= dfConfig.loc[(dfConfig['config_field'] == 'max_folder_number') & (dfConfig['config_name'] == 'max_folder_number')]
maxFolders= int(str(maxFolderNumber.iloc[0]["config_value"]))
CompositeFiles=[]
if dfProcessingStarted.empty==False and dfProcessingComplete.empty==False:
    print('taht are started processing are '+ dfProcessingStarted["file_name"])
    print ('The files taht are started processing are '+ dfProcessingStarted["file_name"])
    print ('The files taht are not completed processing are '+ dfProcessingComplete["file_name"])  
    dfProcessingStarted=dfProcessingStarted[~(dfProcessingStarted['queue_id'].isin(dfProcessingComplete['queue_id']))]
                #dfPdffiles = dfPdffiles.loc[(dfPdffiles['hostname']== str(hostname))]
if dfProcessingStarted.empty==False:   
    CompositeFiles=set(dfProcessingStarted['composite_file_refid'].to_list())

    
print("Total records to be processed " + str(dfProcessingStarted.shape[0]))
print("list of files",CompositeFiles)
if len(CompositeFiles)>0:
    
    for file in CompositeFiles:
        selectStmtPDFFiles= session.prepare("SELECT * FROM votelist_full_pdf_queue where STATUS='processing started'")     
        dfPdffiles= pd.DataFrame((list(session.execute(selectStmtPDFFiles))))
        #if dfPdffiles.empty==False:
            #dfPdffiles = dfPdffiles.loc[(dfPdffiles['hostname']== str(hostname))]
        file_name= dfPdffiles.loc[(dfPdffiles['queue_id'] == uuid.UUID(file))]
        print(file_name)
        if file_name.empty==False:
            file_name= str(file_name.iloc[0]["file_path"])
        try:
            print('------------------------------>Insisde composite file')
            print('------------------------------>The file name is'+file)
            #file=file[0]
            print('------------------------------>Insisde composite file'+str(file))
            dfProcessingStarted=dfProcessingStarted.loc[(dfProcessingStarted['composite_file_refid'] == str(file))]
            print(dfProcessingStarted)
            if not dfProcessingStarted.empty:
                print('------------------------------>Starting the process')
                numberSPlit= (dfProcessingStarted.shape[0])// maxFolders
                effectiveDF=np.array_split(dfProcessingStarted,numberSPlit)
     
                print('------------------------------>After splitting the DF into {} dfs' .format(len(effectiveDF)))
                for i in effectiveDF:
                    box_extraction(i, file, file_name)
        except Exception as e:
            insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
            session.execute(insrtStmt, ['Google_vision_img_Generator.py',str(file),str(file),str(e),str(datetime.datetime.now()),'--An Error Occured while Splitting the dataframe into different datframes--',str(hostname)])      




