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

def box_extraction(img_for_box_extraction_path, cropped_dir_path):
    print("The image Path is------> "+img_for_box_extraction_path)
    print("The directory Path for output is-----> "+cropped_dir_path)
    img = cv2.imread(img_for_box_extraction_path, 0)  # Read the image
    image = cv2.imread(img_for_box_extraction_path)
    gray = cv2.cvtColor(image,cv2.COLOR_BGR2GRAY)
    thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

    # Remove horizontal
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (10,1))
    detected_lines = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)
    cnts = cv2.findContours(detected_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cnts = cnts[0] if len(cnts) == 2 else cnts[1]
    for c in cnts:
        cv2.drawContours(image, [c], -1, (255,255,255), 2)

    # Remove vertical
    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1,10))
    detected_lines = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, vertical_kernel, iterations=2)
    cnts = cv2.findContours(detected_lines, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cnts = cnts[0] if len(cnts) == 2 else cnts[1]
    for c in cnts:
        cv2.drawContours(image, [c], -1, (255,255,255), 2)
        
    cv2.imwrite(img_for_box_extraction_path, image)    
    
from cassandra.cluster import Cluster
cluster = Cluster(['192.168.0.131', '192.168.0.132','192.168.0.133'])            
session = cluster.connect('electionanalysis')    
selectStmtStartedProcessing= session.prepare("SELECT * FROM votelist_full_pdf_queue where STATUS='processing started'") 
selectStmtComplete= session.prepare("SELECT * FROM votelist_full_pdf_queue where STATUS='Complete'") 
selectStmtError= session.prepare("SELECT * FROM votelist_full_pdf_queue where STATUS='ERROR'") 
dfProcessingStarted = pd.DataFrame(list(session.execute(selectStmtStartedProcessing)))
hostname= socket.gethostname()
if dfProcessingStarted.empty==False:
    dfProcessingStarted = dfProcessingStarted.loc[(dfProcessingStarted['hostname']== str(hostname))]
dfProcessingComplete = pd.DataFrame(list(session.execute(selectStmtComplete)))
if dfProcessingComplete.empty==False:
    dfProcessingComplete = dfProcessingComplete.loc[(dfProcessingComplete['hostname']== str(hostname))]
dfError = pd.DataFrame(list(session.execute(selectStmtError)))
if dfError.empty==False:
    dfError = dfError.loc[(dfError['hostname']== str(hostname))]

if dfProcessingStarted.empty==False and dfProcessingComplete.empty==False:
    print('taht are started processing are '+ dfProcessingStarted["file_name"])
    print ('The files taht are started processing are '+ dfProcessingStarted["file_name"])
    print ('The files taht are not completed processing are '+ dfProcessingComplete["file_name"])  
    dfProcessingStarted=dfProcessingStarted[~(dfProcessingStarted['queue_id'].isin(dfProcessingComplete['queue_id']))]
    #print(selectStmtStartedProcessing)
# audir tabel info msg 
for index, row in dfProcessingStarted.iterrows():
    processing_path= (row['processing_path'])
    composite_refid = (row['queue_id'])
    print(processing_path)
    from os.path import isfile, join
    onlyfiles = [f for f in listdir(processing_path) if isfile(join(processing_path, f))]
    print("The files in the list are"+str(onlyfiles))
    rootPath="/home/Election-Analysis/"
    count =0 
    for i in onlyfiles:
        if count>1 and count<47:
            print(i)
            output_path=rootPath+"Individualvotersinbound/"+str(i)+str(uuid.uuid1())+"/"
            # audir tabel info msg
            uniqueID= uuid.uuid1()
            print('generated uuid is :'+str(uniqueID))
            
            insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
            session.execute(insrtStmt, ['Ocr-Image-Generator-Combiner.py',processing_path,str(uniqueID),'NA',str(datetime.datetime.now()),'--Processing Started--',str(hostname)])
                    
            insrtStmt= session.prepare("INSERT INTO votelist_individual_page_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?,?)")
            session.execute(insrtStmt, [uniqueID,str(composite_refid),processing_path+"/"+str(i),str(datetime.datetime.now()),str(datetime.datetime.now()),'Not Processed','Processing not started',processing_path,output_path,str(hostname)])  
                
            try:
                box_extraction(processing_path+str(i), output_path)
            except Exception as e:
                insrtStmt= session.prepare("INSERT INTO votelist_individual_page_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (now(),?,?,?,?,?,?,?,?,?)")
                session.execute(insrtStmt, [str(composite_refid),processing_path+"/"+str(i),str(datetime.datetime.now()),str(datetime.datetime.now()),'Error','Proccess Errored out',processing_path,output_path,str(hostname)]) 
                
                #audit table error msg add
                insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
                session.execute(insrtStmt, ['Ocr-Image-Generator-Combiner.py',processing_path,str(uniqueID),str(e),str(datetime.datetime.now()),'--An error occured--',str(hostname)])
            
            print("Image Exists")
            insrtStmt= session.prepare("INSERT INTO votelist_individual_page_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (now(),?,?,?,?,?,?,?,?,?)")
            session.execute(insrtStmt, [str(composite_refid),processing_path+"/"+str(i),str(datetime.datetime.now()),str(datetime.datetime.now()),'processing started','Processing Started',processing_path,output_path,str(hostname)])   
            # audir tabel info msg
            insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
            session.execute(insrtStmt, ['Ocr-Image-Generator-Combiner.py',output_path,str(uniqueID),'NA',str(datetime.datetime.now()),'--Process Complete--',str(hostname)])
            print("Loaded Data")
        count=count+1
        
    insrtStmt= session.prepare("INSERT INTO votelist_full_pdf_queue (queue_id,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?)")
    session.execute(insrtStmt, [row['queue_id'],str(row['file_path']),str(datetime.datetime.now()),str(datetime.datetime.now()),'Complete','Processing Completed',str(row['file_path']),processing_path,str(hostname)])
    
        



