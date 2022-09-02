# archive-compressor
Compress files from Edit Share
## Parameters

a main mode:  
Create new db table with today date name like '2020-02-02' and fill with new data.  
If this table exists, script finished the work with status 0

a helper mode:
Detect last table in db, find tasks than is not in_work and take them.  

A helper mode machines could be as many as you want, but main mode machine should be only one 
