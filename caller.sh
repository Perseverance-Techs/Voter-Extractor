#!/bin/sh
/usr/bin/python3.6 /home/Election-Analysis/apps/Composite-Filespiltter.py
/usr/bin/python3.6 /home/Election-Analysis/apps/individual_Voter_img_Generator.py
for i in {1..40}
do
	/usr/bin/python3.6 /home/Election-Analysis/apps/Google_vision_img_Generator_7-4-2020.py
done

for i in {1..3}
do
	/usr/bin/python3.6 /home/Election-Analysis/apps/Load_Election_Data.py
done
