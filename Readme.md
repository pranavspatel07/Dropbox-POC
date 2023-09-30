# Dropbox-POC

`get_box_data.go `

    - It will get the token from the API call which we're using to get the metadata of folders and file from box.com. 

    - It will also publish the metadata in desire format as per the requirement and publish the metadata into kineses

`process_data.go` 

    - It will subscribe the data from the producer of Kineses and process the data to store into s3.
