import cassandra
import re
import os, ssl
import datetime
import os
import tempfile
from os.path import isfile, join
import uuid 
import socket 
from os import listdir
import os
from pdf2image import convert_from_path
from PIL import Image

def convert_pdf(output_path):
    hostname= socket.gethostname()
    print ('Writimg the image'+str(hostname))
    try:
        # save temp image files in temp dir, delete them after we are finished
        print("before looping the files")
        onlyfiles = [f for f in listdir('/home/Election-Analysis/Composite-Inbound') if isfile(join('/home/Election-Analysis/Composite-Inbound', f))]
        file_path= '/home/Election-Analysis/Composite-Inbound'
        from cassandra.cluster import Cluster
        cluster = Cluster(['192.168.225.31', '192.168.225.32','192.168.225.33'])            
        session = cluster.connect('electionanalysis')
        
        #print ('Inserted to DB')
        insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
        session.execute(insrtStmt, ['Composite-Filespiltter.py','NA','NA','NA',str(datetime.datetime.now()),'--Processing Started--',str(hostname)])                        
        
    except Exception as e:
        #print ('Inserted to DB')
        insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
        session.execute(insrtStmt, ['Composite-Filespiltter.py','NA','NA',str(e),str(datetime.datetime.now()),'--Processing Started--',str(hostname)])                        

        
    for i in onlyfiles:
        print(i)
        file_path= '/home/Election-Analysis/Composite-Inbound'
        file_path=file_path+"/"+i
        with tempfile.TemporaryDirectory() as temp_dir:
            #connect to db 
            uniqueID= uuid.uuid1()
            print('generated uuid is :'+str(uniqueID))
            output_path='/home/Election-Analysis/PagesOutbound'
            output_path=output_path+"/"+str(i)+str(uuid.uuid1())+"/"
            if not os.path.exists(output_path):
                os.makedirs(output_path)
                
            #print ('Inserted to DB')
            insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
            session.execute(insrtStmt, ['Composite-Filespiltter.py',file_path,str(uniqueID),'NA',str(datetime.datetime.now()),'--Individual file split started--',str(hostname)])                        
        
                
            insrtStmt= session.prepare("INSERT INTO votelist_full_pdf_queue (queue_id,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?)")
            session.execute(insrtStmt, [uniqueID,file_path,str(datetime.datetime.now()),str(datetime.datetime.now()),'Not Processed','Processing not started',file_path,output_path,str(hostname)])               
                              
            # convert pdf to multiple image
            images = convert_from_path(file_path, output_folder=temp_dir)
            # save images to temporary directory
            temp_images = []
            for i in range(len(images)):
                try:
                    print ('Writimg the image====>'+ str(i))
                    image_path = f'{temp_dir}/{i}.jpg'            
                    #print ('Writimg the image to path====>'+ image_path)
                    #images[i].save(image_path, 'JPEG')
                    images[i].save(output_path+str(i)+'.png', 'PNG')
                except Exception as e:
                    insrtStmt= session.prepare("INSERT INTO votelist_full_pdf_queue (queue_id,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?)")
                    session.execute(insrtStmt, [uniqueID,file_path,str(datetime.datetime.now()),str(datetime.datetime.now()),'Error','Error During Processing',file_path,output_path,str(hostname)])          
                    #print ('Inserted to DB')
                    insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
                    session.execute(insrtStmt, ['Composite-Filespiltter.py',file_path,str(uniqueID),str(e),str(datetime.datetime.now()),'--Error during indivudual File Split--',str(hostname)])
            
            insrtStmt= session.prepare("INSERT INTO votelist_full_pdf_queue (queue_id,file_name,received_date,last_modified_date,status,status_description,file_path,processing_path,hostname) VALUES (?,?,?,?,?,?,?,?,?)")
            session.execute(insrtStmt, [uniqueID,file_path,str(datetime.datetime.now()),str(datetime.datetime.now()),'processing started','processing started',file_path,output_path,str(hostname)])            
            
            #print ('Inserted to DB')
            insrtStmt= session.prepare("INSERT INTO election_process_audit (process_name,file_name,identifier, error_message,exection_datetime,info_message,hostname) VALUES (?,?,?,?,?,?,?)")
            session.execute(insrtStmt, ['Composite-Filespiltter.py',file_path,str(uniqueID),'NA',str(datetime.datetime.now()),'--Individual file split ended--',str(hostname)])
            
                                          #temp_images.append(image_path)
convert_pdf('/home/Election-Analysis/PagesOutbound')
