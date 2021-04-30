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
import os, ssl
import shutil
from cassandra.cluster import Cluster
cluster = Cluster(['192.168.0.131', '192.168.0.132'])
session = cluster.connect('electionanalysis')

session.execute("TRUNCATE TABLE  electionanalysis.votelist_full_pdf_queue")
session.execute("TRUNCATE TABLE  electionanalysis.votelist_individual_page_queue")
#session.execute("TRUNCATE TABLE  electionanalysis.voter_data")
session.execute("TRUNCATE TABLE  electionanalysis.img_file_reprocessing")
session.execute("TRUNCATE TABLE  electionanalysis.googlevision_inbound_queue")
session.execute("TRUNCATE TABLE  electionanalysis.img_file_reprocessing")
session.execute("TRUNCATE TABLE  electionanalysis.election_process_audit")

def rem_folder(path):
    for root, dirs, files in os.walk(path):
        if files:
            for f in files:
                os.unlink(os.path.join(root, f))
        if dirs:
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))				
rem_folder('/home/Election-Analysis/googlevisioninbound')
rem_folder('/home/Election-Analysis/Individualvotersinbound')
rem_folder('/home/Election-Analysis/PagesOutbound')
rem_folder('/home/Election-Analysis/logs')				
