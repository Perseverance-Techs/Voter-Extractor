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
    print("Number of folders to be processed "  + str(dfProcessingStarted.shape[0]))
    onlyfiles = []
    uniqueID= uuid.uuid1()
    outboundPath="/home/Election-Analysis/googlevisioninbound/"
    googleInboundPath=outboundPath+str(uniqueID)+"/"
    
    totalCount=0
    count=0
    images_list=[]
    for index, row in dfProcessingStarted.iterrows():
        try:
            count=count+1  
            cropped_dir_path = row['processing_path']
            fName="abc"
			
            #/home/Election-Analysis/Individualvotersinbound/15.png0c522938-7692-11ea-b73c-0800271a438a/
            #onlyfiles = [f for f in listdir(processing_path) if isfile(join(processing_path, f))]
            #for file in listdir(cropped_dir_path):
                #onlyfiles.append(os.path.join(cropped_dir_path, file))
            #print(onlyfiles)
            onlyfiles.append(os.path.join(cropped_dir_path, "combinationImage-0.png"))            
            for i in onlyfiles:
                fName=str(i)
                #print("Printing the name of images in path---------->"+str(i))
                images_list.append(str(i))
                totalCount=totalCount+1   
				count=0
                if (dfProcessingStarted.shape[0]==count):
                    print("Count has reached 300")
					for i in len(images_list)
						count=count+1
					    if(count==20)
							imgs = [Image.open(i) for i in images_list]
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
					#Image.close(i)
            #insrtStmt= sessionInner.prepare("INSERT INTO votelist_individual_page_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path) VALUES (?,?,?,?,?,?,?,?,?)")
            #sessionInner.execute_async(insrtStmt, [row['queue_id'],row['composite_file_refid'],str(row['file_path']),str(datetime.datetime.now()),str(datetime.datetime.now()),'Complete','Processing Completed',str(row['file_path']),str(['processing_path'])])  
            
            
        except Exception as e:
            print("Error {} in {}" .format(str(e), str(row['file_path'])))
            insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message) VALUES (?,?,?,?,?,?)")
            session.execute(insrtStmt, ['Google_vision_img_Generator.py',str(['processing_path']),row['queue_id'],str(e),str(datetime.datetime.now()),'--An Error Occured while converting the image-->'+fName]) 
            insrtStmt= sessionInner.prepare("INSERT INTO votelist_individual_page_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path) VALUES (?,?,?,?,?,?,?,?,?)")
            sessionInner.execute_async(insrtStmt, [row['queue_id'],row['composite_file_refid'],str(row['file_path']),str(datetime.datetime.now()),str(datetime.datetime.now()),'ERROR','Error During Processing',str(row['file_path']),str(['processing_path'])])  

            
    #insrtStmt= sessionInner.prepare("INSERT INTO googlevision_inbound_queue(queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path) VALUES (?,?,?,?,?,?,?,?,?)")
    #sessionInner.execute_async(insrtStmt, [uniqueID,str(composite_file_refid),file_name,str(datetime.datetime.now()),str(datetime.datetime.now()),'Processing Started','Processing Started',file_name,googleInboundPath]) 
    #print("After Loading the status")    
    
from cassandra.cluster import Cluster
cluster = Cluster(['192.168.1.55', '192.168.1.56'])            
session = cluster.connect('electionanalysis')    
selectStmtCompFiles= session.prepare("SELECT composite_file_refid FROM votelist_individual_page_queue where STATUS='processing started'") 
dfCompositeFiles= set((list(session.execute(selectStmtCompFiles))))
print('------------------------------>Started Procesiing')
#print(dfCompositeFiles)  
selectStmtStartedProcessing= session.prepare("SELECT * FROM votelist_individual_page_queue where STATUS='processing started'") 
selectStmtComplete= session.prepare("SELECT * FROM votelist_individual_page_queue where STATUS='Complete'") 
selectStmtError= session.prepare("SELECT * FROM votelist_individual_page_queue where STATUS='ERROR'") 
dfProcessingStarted = pd.DataFrame(list(session.execute(selectStmtStartedProcessing)))
dfProcessingComplete = pd.DataFrame(list(session.execute(selectStmtComplete)))
dfError = pd.DataFrame(list(session.execute(selectStmtError)))
#print('------------------------------>Started Procesiing')
#print(dfProcessingStarted)
selectConfig= session.prepare("SELECT * FROM election_anallysis_configurations")
dfConfig=pd.DataFrame(list(session.execute(selectConfig)))
maxFolderNumber= dfConfig.loc[(dfConfig['config_field'] == 'max_folder_number') & (dfConfig['config_name'] == 'max_folder_number')]
maxFolders= int(str(maxFolderNumber.iloc[0]["config_value"]))

if dfProcessingStarted.empty==False and dfProcessingComplete.empty==False:
    print('taht are started processing are '+ dfProcessingStarted["file_name"])
    print ('The files taht are started processing are '+ dfProcessingStarted["file_name"])
    print ('The files taht are not completed processing are '+ dfProcessingComplete["file_name"])  
    dfProcessingStarted=dfProcessingStarted[~(dfProcessingStarted['queue_id'].isin(dfProcessingComplete['queue_id']))]
print("Total records to be processed " + str(dfProcessingStarted.shape[0]))
for file in dfCompositeFiles:
    selectStmtPDFFiles= session.prepare("SELECT * FROM votelist_full_pdf_queue where STATUS='processing started'")     
    dfPdffiles= pd.DataFrame((list(session.execute(selectStmtPDFFiles))))
    
    file_name= dfPdffiles.loc[(dfPdffiles['queue_id'] == uuid.UUID(file[0]))]
    
    file_name= str(file_name.iloc[0]["file_path"])
    try:
        print('------------------------------>Insisde composite file')
        print('------------------------------>The file name is'+file[0])
        file=file[0]
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
        insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message) VALUES (?,?,?,?,?,?)")
        session.execute(insrtStmt, ['Google_vision_img_Generator.py',str(file),str(file),str(e),str(datetime.datetime.now()),'--An Error Occured while Splitting the dataframe into different datframes--'])      




