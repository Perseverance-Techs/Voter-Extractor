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
from pytesseract import image_to_string

from cassandra.cluster import Cluster
cluster = Cluster(['192.168.0.131', '192.168.0.132','192.168.0.133'])            
session = cluster.connect('electionanalysis')    
ENDPOINT_URL = 'https://vision.googleapis.com/v1/images:annotate'

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


def make_image_data(image_filenames):
    """Returns the image data lists as bytes"""
    imgdict = make_image_data_list(image_filenames)
    return json.dumps({"requests": imgdict }).encode()


def get_contour_precedence(contour, cols):
    tolerance_factor = 10
    origin = cv2.boundingRect(contour)
    return ((origin[1] // tolerance_factor) * tolerance_factor) * cols + origin[0]


def box_extraction(img_for_box_extraction_path, cropped_dir_path):
    try: 
        #img = cv2.imread(img_for_box_extraction_path, 0)  # Read the image
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
            
        scale_percent = 200 # percent of original size
        width = int(image.shape[1] * scale_percent / 100)
        height = int(image.shape[0] * scale_percent / 100)
        dim = (width, height)
        # resize image
        image = cv2.resize(image, dim, interpolation = cv2.INTER_AREA)
        
        if not os.path.exists(cropped_dir_path):
            os.makedirs(cropped_dir_path)    
        cv2.imwrite(cropped_dir_path+"Removed.png", image)     

        
        image = cv2.imread(cropped_dir_path+"Removed.png")
        # Load image, grayscale, Gaussian blur, Otsu's threshold
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        blur = cv2.GaussianBlur(gray, (7,7), 0)
        thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

        # Create rectangular structuring element and dilate
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (6,6))
        dilate = cv2.dilate(thresh, kernel, iterations=4)

        # Find contours and draw rectangle
        cnts = cv2.findContours(dilate, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        cnts = cnts[0] if len(cnts) == 2 else cnts[1]
        
        for c in cnts:
            x,y,w,h = cv2.boundingRect(c)
            cv2.rectangle(image, (x, y), (x + w, y + h), (36,255,12), 3)

        #cv2.imshow('thresh', thresh)
        #cv2.imshow('dilate', dilate)
        #cv2.imshow('image', image)
        cv2.waitKey()
        cv2.imwrite(cropped_dir_path+"Boxed.png",image) 

        # Load image, grayscale, Otsu's threshold 
        image = cv2.imread(cropped_dir_path+"Boxed.png")
        original = image.copy()
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

        # Find contours, obtain bounding box, extract and save ROI
        ROI_number = 0
        cnts = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        cnts = cnts[0] if len(cnts) == 2 else cnts[1]
        #sorted_ctrs = sorted(cnts, key=lambda ctr: cv2.boundingRect(cnts)[0] + cv2.boundingRect(cnts)[1] * thresh.shape[1] )
       
        cnts.sort(key=lambda x:get_contour_precedence(x, thresh.shape[1]))
        
        yCordOrig= 0
        yCordMod= 0
        contCount=0
        line=1
        
        newline=True;
        
        
        for c in cnts:
            contCount=contCount+1
            x,y,w,h = cv2.boundingRect(c)
            cv2.rectangle(image, (x, y), (x + w, y + h), (36,255,12), 2)
            
            resultBoxes=[]
            
            print("The y cordinate of the axis is--------------------------->"+str(y))
            if contCount==1:
                yCordOrig= y
                yCordMod=y
                
            yCordMod=y  
            
            if ((yCordMod!=yCordOrig) and abs(yCordMod-yCordOrig)>5):
                newline=True
                line=line+1     
            
            if (newline or ((yCordMod==yCordOrig) or abs(yCordMod-yCordOrig)<2)):
            #Extract the rectangle to the extraction Path.
                ROI = original[y:y+h, x:x+w]         
                if not os.path.exists(cropped_dir_path+'line_'+str(line)+'/'):
                    os.makedirs(cropped_dir_path+'line_'+str(line)+'/')
           
                #cv2.imwrite(cropped_dir_path+'line_'+str(line)+'/'+str(ROI_number)+'.png', ROI)
                
                low_green = np.array([25, 52, 72])
                high_green = np.array([102, 255, 255])
                #lower_val = np.array([0,0,0])
                #upper_val = np.array([179,100,130])
                # convert BGR to HSV
                imgHSV = cv2.cvtColor(ROI, cv2.COLOR_BGR2HSV)
                # create the Mask
                
                mask = cv2.inRange(imgHSV, low_green, high_green)
                ROI[mask>0]=(255, 255, 255)
            
                scale_percent = 800 # percent of original size
                width = int(ROI.shape[1] * scale_percent / 100)
                height = int(ROI.shape[0] * scale_percent / 100)
                dim = (width, height)
                # resize image
                ROI = cv2.resize(ROI, dim, interpolation = cv2.INTER_CUBIC)
            
               
                gray = cv2.cvtColor(ROI, cv2.COLOR_BGR2GRAY)
                sharpen_kernel = np.array([[-1,-1,-1], [-1,9,-1], [-1,-1,-1]])
                sharpen = cv2.filter2D(gray, -1, sharpen_kernel)
                

                #thresh= cv2.adaptiveThreshold(sharpen, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 1000, 2)
                thresh = cv2.threshold(sharpen, 200, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]   
                kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3,3))
                close = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel, iterations=1)
                result = 255 - close
                inbondPath=cropped_dir_path+'line_'+str(line)+'/'                
                result = cv2.GaussianBlur(result, (5, 5), 0)
                
                kernel = np.ones((1,1), np.uint8)
                result = cv2.dilate(result, kernel, iterations=1)
                result = cv2.erode(result, kernel, iterations=1)
                '''
                result = cv2.GaussianBlur(result, (5,5), 0)
                result = cv2.medianBlur(result,5)                
                result = cv2.threshold(result, 240, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]                    
                result = cv2.GaussianBlur(result, (5, 5), 0)     
                '''                
                scale_percent = 30# percent of original size
                width = int(result.shape[1] * scale_percent / 100)
                height = int(result.shape[0] * scale_percent / 100)
                dim = (width, height)
                # resize image
                result = cv2.resize(result, dim, interpolation = cv2.INTER_CUBIC)
                
                cv2.imwrite(inbondPath+str(ROI_number)+'.png',result)   
                
                ROI_number += 1    
                
            if ((yCordMod!=yCordOrig) and abs(yCordMod-yCordOrig)>5):
                
                yCordOrig= y
                print("We have reached a new Line")
                inbondPath=cropped_dir_path+'line_'+str(line-1)+'/'
                
                onlyfiles = [f for f in os.listdir(inbondPath) if os.path.isfile(join(inbondPath, f))]
                count=0
                totalCount=0
                countComb=0    
                images_list = []

                for i in onlyfiles:
                    images_list.append(inbondPath+'/'+str(i))
                    totalCount=totalCount+1
                    count=count+1
                    print("The image in the path is "+str(i))
                 
                    if count==32 or len(onlyfiles)==totalCount:
                        print("Count has reached the total count")
                        imgs = [Image.open(i) for i in images_list]
                        # If you're using an older version of Pillow, you might have to use .size[0] instead of .width
                        # and later on, .size[1] instead of .height
                        min_img_width = min(i.width for i in imgs)
                        total_height = 0
                        
                        #max_image_width = max(i.width for i in imgs) 
                        basewidth=1500
                        
                        for i, img in enumerate(imgs):
                                              
                        # If the image is larger than the minimum width, resize it
                            '''
                            print("Looping the imnage List")
                            if img.width > min_img_width:
                                imgs[i] = img.resize((min_img_width, int(((img.height / img.width * min_img_width)+1))), Image.ANTIALIAS)
                            '''
                            wpercent = (basewidth / float(img.size[0]))
                            hsize = int((float(img.size[1]) * float(wpercent)))
                            
                            print("the modified width of the image is------------->"+str(basewidth))
                            print("the modified height of the image is------------->"+str(hsize))
                            
                            #imgs[i] = img.resize((max_image_width,image_height), Image.ANTIALIAS)
                            
                            imgs[i] = img.resize((800, 800), Image.ANTIALIAS)
                            total_height += imgs[i].height
                        img_merge = Image.new(imgs[0].mode, (800, (total_height+len(onlyfiles)*200)),(255))
                        y = 0
                        for img in imgs:
                            img_merge.paste(img, (0, y))
                            y += img.height+200
                        #img_merge.save(cropped_dir_path+'combinationImage-'+str(countComb)+'.png')   
                        count=0        
                        countComb=countComb+1 
                        ocrImage_list=images_list
                        images_list = []   
                print('Saving the image at the path'+str(inbondPath+str(countComb)+'.png'))
                img_merge.save(inbondPath+'Combined'+str(countComb)+'.png')
                #resultSrting = ','.join(resultBoxes)


                uniqueID= uuid.uuid1()
                print('generated uuid is :'+str(uniqueID))



                low_green = np.array([25, 52, 72])
                high_green = np.array([102, 255, 255])
                #lower_val = np.array([0,0,0])
                #upper_val = np.array([179,100,130])
               
                img = cv2.imread(inbondPath+'Combined'+str(countComb)+'.png')
                #img = cv2.resize(img, (900, 650), interpolation=cv2.INTER_CUBIC)
                #img = cv2.resize(img, None, fx=5, fy=5, interpolation=cv2.INTER_LINEAR)
 
                # convert BGR to HSV
                imgHSV = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
                # create the Mask
                
                mask = cv2.inRange(imgHSV, low_green, high_green)
                img[mask>0]=(255, 255, 255)

                cv2.imwrite(inbondPath+'OCRInbound.png',img) 
                
                print("Got response")    
                resultSrting='NA'                    
                for i in range(2):
                    if i== 0:
                        topImage=14
                        firstImg=0
                    if i==1:
                        topImage=len(ocrImage_list)-1
                        firstImg=18                        
                    
                    api_key='AIzaSyDxJBtT03kmyTsrxUOdgAaXMOrZIo49M3E'                
                    
                    image_filenames=[inbondPath+'OCRInbound.png']
                    print("the ocr image list is"+str(ocrImage_list))
                    if len(ocrImage_list)>25:
                        ocrImage_listBatch=ocrImage_list[firstImg:topImage]
                        print("sending google OCR Request, with image "+str(ocrImage_listBatch))
                        try:
                            response = requests.post(ENDPOINT_URL,
                                     data=make_image_data(ocrImage_listBatch),
                                     params={'key': api_key},
                                     headers={'Content-Type': 'application/json'}) 
                        except Exception as e:
                            print(str(e))   
                            
                    
                        
                        if response.status_code != 200 or response.json().get('error'):
                            print("The response obtained from the OCR is invalid"+str(response.json()))
                        else:
                            print("The response obtained from the OCR is invalid"+str(response.json()))                    
                            for idx, resp in enumerate(response.json()['responses']):
                                    # print the plaintext to screen for convenience
                                print("---------------------------------------------")
                                
                                try:
                                    t = resp['textAnnotations'][0]
                                    word= t['description']
                                    resultSrting=resultSrting+str(word)
                                except Exception as e:
                                    print(str(e))   
                print("the extracted result from OCR is---->"+str(resultSrting))                         
                insrtStmt= session.prepare("INSERT INTO composite_results(file_name,resultIdentifier,result_composite,status) VALUES (?,?,?,?)")
                session.execute(insrtStmt, [img_for_box_extraction_path,str(uniqueID),resultSrting,'not processed'])                         
                                           
                print("saved!!!!!!!!!!!!!!")                  
                          
    except Exception as e:
        print(str(e))   
        raise
 
    
selectStmtStartedProcessing= session.prepare("SELECT * FROM results_full_pdf_queue where STATUS='processing started'") 
selectStmtComplete= session.prepare("SELECT * FROM results_full_pdf_queue where STATUS='Complete'") 
selectStmtError= session.prepare("SELECT * FROM results_full_pdf_queue where STATUS='ERROR'") 
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
        if count>=0 and count<8:
            print(i)
            output_path=rootPath+"indivudual-results/"+str(i)+str(uuid.uuid1())+"/"
            # audir tabel info msg
            uniqueID= uuid.uuid1()
            print('generated uuid is :'+str(uniqueID))
            
            insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
            session.execute(insrtStmt, ['individual_Result_img_Generator.py',processing_path,str(uniqueID),'NA',str(datetime.datetime.now()),'--Processing Started--',str(hostname)])
                    
            insrtStmt= session.prepare("INSERT INTO result_individual_page_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?,?)")
            session.execute(insrtStmt, [uniqueID,str(composite_refid),processing_path+"/"+str(i),str(datetime.datetime.now()),str(datetime.datetime.now()),'Not Processed','Processing not started',processing_path,output_path,str(hostname)])  
                
            try:
                box_extraction(processing_path+str(i), output_path)
            except Exception as e:
                insrtStmt= session.prepare("INSERT INTO result_individual_page_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (now(),?,?,?,?,?,?,?,?,?)")
                session.execute(insrtStmt, [str(composite_refid),processing_path+"/"+str(i),str(datetime.datetime.now()),str(datetime.datetime.now()),'Error','Proccess Errored out',processing_path,output_path,str(hostname)]) 
                
                #audit table error msg add
                insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
                session.execute(insrtStmt, ['individual_Result_img_Generator.py',processing_path,str(uniqueID),str(e),str(datetime.datetime.now()),'--An error occured--',str(hostname)])
            
            print("Image Exists")
            insrtStmt= session.prepare("INSERT INTO result_individual_page_queue (queue_id,composite_file_refid,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (now(),?,?,?,?,?,?,?,?,?)")
            session.execute(insrtStmt, [str(composite_refid),processing_path+"/"+str(i),str(datetime.datetime.now()),str(datetime.datetime.now()),'processing started','Processing Started',processing_path,output_path,str(hostname)])   
            # audir tabel info msg
            insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
            session.execute(insrtStmt, ['individual_Result_img_Generator.py',output_path,str(uniqueID),'NA',str(datetime.datetime.now()),'--Process Complete--',str(hostname)])
            print("Loaded Data")
        count=count+1
        
    insrtStmt= session.prepare("INSERT INTO results_full_pdf_queue(queue_id,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?)")
    session.execute(insrtStmt, [row['queue_id'],str(row['file_path']),str(datetime.datetime.now()),str(datetime.datetime.now()),'Complete','Processing Completed',str(row['file_path']),processing_path,str(hostname)])
    
        



