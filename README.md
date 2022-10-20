# archive-compressor
Compress files from Edit Share
## Modes

####a MAIN mode:  
Create new db table with today date name like '2020-02-02' and fill with new data.  
If this table exists, script finished the work with status 0  
To run in main mode: ARC_Compressor.py --main  
Machine in main mode should be ONLY ONE!

####a HELPER mode:
Detect last table in db, find tasks than is not in_work and take them.  
To run in helper mode: ARC_Compressor.py

a helper mode machines could be as many as you want, but main mode machine should be only one 

####a AME transcoding option  
If installed Adobe Media Encoder and available, files not transcoded via ffmpeg will trying to transcode via AME  
To run in helper mode: ARC_Compressor.py --ame

## Parameters
Configure this in configuration.json file