It was quite a good feeling using the elastic mapreduce for the first time.
Firstly I created a bucket from the S3 tab and gave a name to it and uploaded my code.jar and sample.txt file.
Next I created a EMR instance from the console and kept all the other options and parameters as default.
I did not create a ssh key as it was optional.
Next by using the add step option. I putin the path of the jar file previously uploaded to the S3 and gave the input and output arguments which coreesponded to the text file previously uploaded and a new output .txt file for the results which will be stored in S3 bucket created by me.
After successful conpletion I examined the output for correctness.


Another Issue I faced is regarding the error "Too many files" that on on the terminal.
So I changed the UFile limit size and restarted the namenode and the datanode and this fixed the issue.
